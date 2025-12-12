"""
Gestor de bloqueos distribuidos para el servicio de Metadata.
Implementa bloqueos de lectura/escritura para archivos.
"""
import time
import threading
import logging
from typing import Dict, Optional, Set, Tuple
from collections import defaultdict
from ..Common.models import LockInfo
from ..Common.constants import LockType, DistributedResponseCode

logger = logging.getLogger(__name__)

# Tiempo de expiración de bloqueos en segundos
LOCK_TIMEOUT = 60


class LockManager:
    """
    Gestiona bloqueos de lectura/escritura sobre archivos.
    Implementa un esquema de lectores múltiples / escritor único.
    """
    
    def __init__(self, lock_timeout: int = LOCK_TIMEOUT):
        self.lock_timeout = lock_timeout
        self._lock = threading.RLock()
        
        # Bloqueos de escritura: file_id -> LockInfo
        self._write_locks: Dict[str, LockInfo] = {}
        
        # Bloqueos de lectura: file_id -> Set[holder_id]
        self._read_locks: Dict[str, Set[str]] = defaultdict(set)
        
        # Metadatos de lectura: (file_id, holder_id) -> LockInfo
        self._read_lock_info: Dict[Tuple[str, str], LockInfo] = {}
        
        # Iniciar thread de limpieza de bloqueos expirados
        self._cleanup_thread = threading.Thread(target=self._cleanup_expired_locks, daemon=True)
        self._cleanup_running = True
        self._cleanup_thread.start()
    
    def acquire_read_lock(self, file_id: str, holder_id: str) -> Tuple[DistributedResponseCode, Optional[LockInfo]]:
        """
        Adquiere un bloqueo de lectura sobre un archivo.
        Múltiples lectores pueden tener bloqueos de lectura simultáneos.
        """
        with self._lock:
            # Verificar si hay un bloqueo de escritura activo
            if file_id in self._write_locks:
                write_lock = self._write_locks[file_id]
                if not write_lock.is_expired():
                    logger.debug(f"Read lock failed for {file_id}: write lock held by {write_lock.holder}")
                    return (DistributedResponseCode.LOCK_FAILED, None)
                else:
                    # Bloqueo expirado, limpiarlo
                    del self._write_locks[file_id]
            
            # Crear bloqueo de lectura
            lock_info = LockInfo(
                file_id=file_id,
                lock_type=LockType.READ.value,
                holder=holder_id,
                acquired_at=time.time(),
                expires_at=time.time() + self.lock_timeout
            )
            
            self._read_locks[file_id].add(holder_id)
            self._read_lock_info[(file_id, holder_id)] = lock_info
            
            logger.debug(f"Read lock acquired for {file_id} by {holder_id}")
            return (DistributedResponseCode.SUCCESS, lock_info)
    
    def acquire_write_lock(self, file_id: str, holder_id: str) -> Tuple[DistributedResponseCode, Optional[LockInfo]]:
        """
        Adquiere un bloqueo de escritura sobre un archivo.
        Solo un escritor puede tener el bloqueo y no debe haber lectores.
        """
        with self._lock:
            # Verificar si hay un bloqueo de escritura activo
            if file_id in self._write_locks:
                write_lock = self._write_locks[file_id]
                if not write_lock.is_expired():
                    if write_lock.holder == holder_id:
                        # Ya tenemos el bloqueo, extender tiempo
                        write_lock.expires_at = time.time() + self.lock_timeout
                        return (DistributedResponseCode.SUCCESS, write_lock)
                    logger.debug(f"Write lock failed for {file_id}: already held by {write_lock.holder}")
                    return (DistributedResponseCode.LOCK_FAILED, None)
                else:
                    del self._write_locks[file_id]
            
            # Verificar si hay bloqueos de lectura activos (que no sean del mismo holder)
            if file_id in self._read_locks:
                active_readers = set()
                for reader in list(self._read_locks[file_id]):
                    lock_key = (file_id, reader)
                    if lock_key in self._read_lock_info:
                        if not self._read_lock_info[lock_key].is_expired():
                            if reader != holder_id:
                                active_readers.add(reader)
                        else:
                            # Limpiar bloqueo expirado
                            self._read_locks[file_id].discard(reader)
                            del self._read_lock_info[lock_key]
                
                if active_readers:
                    logger.debug(f"Write lock failed for {file_id}: active readers {active_readers}")
                    return (DistributedResponseCode.LOCK_FAILED, None)
            
            # Crear bloqueo de escritura
            lock_info = LockInfo(
                file_id=file_id,
                lock_type=LockType.WRITE.value,
                holder=holder_id,
                acquired_at=time.time(),
                expires_at=time.time() + self.lock_timeout
            )
            
            self._write_locks[file_id] = lock_info
            
            # Liberar cualquier bloqueo de lectura que teníamos
            if holder_id in self._read_locks.get(file_id, set()):
                self._read_locks[file_id].discard(holder_id)
                if (file_id, holder_id) in self._read_lock_info:
                    del self._read_lock_info[(file_id, holder_id)]
            
            logger.debug(f"Write lock acquired for {file_id} by {holder_id}")
            return (DistributedResponseCode.SUCCESS, lock_info)
    
    def release_lock(self, file_id: str, holder_id: str) -> DistributedResponseCode:
        """Libera cualquier bloqueo que el holder tenga sobre el archivo"""
        with self._lock:
            released = False
            
            # Intentar liberar bloqueo de escritura
            if file_id in self._write_locks:
                if self._write_locks[file_id].holder == holder_id:
                    del self._write_locks[file_id]
                    released = True
                    logger.debug(f"Write lock released for {file_id} by {holder_id}")
            
            # Intentar liberar bloqueo de lectura
            if file_id in self._read_locks and holder_id in self._read_locks[file_id]:
                self._read_locks[file_id].discard(holder_id)
                if (file_id, holder_id) in self._read_lock_info:
                    del self._read_lock_info[(file_id, holder_id)]
                if not self._read_locks[file_id]:
                    del self._read_locks[file_id]
                released = True
                logger.debug(f"Read lock released for {file_id} by {holder_id}")
            
            return DistributedResponseCode.SUCCESS if released else DistributedResponseCode.NOT_FOUND
    
    def release_all_locks(self, holder_id: str):
        """Libera todos los bloqueos de un holder (usado cuando un nodo se desconecta)"""
        with self._lock:
            # Liberar bloqueos de escritura
            to_remove = [fid for fid, lock in self._write_locks.items() if lock.holder == holder_id]
            for file_id in to_remove:
                del self._write_locks[file_id]
            
            # Liberar bloqueos de lectura
            for file_id in list(self._read_locks.keys()):
                if holder_id in self._read_locks[file_id]:
                    self._read_locks[file_id].discard(holder_id)
                    if (file_id, holder_id) in self._read_lock_info:
                        del self._read_lock_info[(file_id, holder_id)]
                    if not self._read_locks[file_id]:
                        del self._read_locks[file_id]
            
            logger.info(f"Released all locks for holder {holder_id}")
    
    def is_locked_for_write(self, file_id: str) -> bool:
        """Verifica si un archivo tiene un bloqueo de escritura activo"""
        with self._lock:
            if file_id in self._write_locks:
                lock = self._write_locks[file_id]
                if not lock.is_expired():
                    return True
                del self._write_locks[file_id]
            return False
    
    def is_locked_for_read(self, file_id: str) -> bool:
        """Verifica si un archivo tiene bloqueos de lectura activos"""
        with self._lock:
            if file_id not in self._read_locks:
                return False
            
            # Limpiar bloqueos expirados
            for holder_id in list(self._read_locks[file_id]):
                lock_key = (file_id, holder_id)
                if lock_key in self._read_lock_info:
                    if self._read_lock_info[lock_key].is_expired():
                        self._read_locks[file_id].discard(holder_id)
                        del self._read_lock_info[lock_key]
            
            return len(self._read_locks.get(file_id, set())) > 0
    
    def get_lock_info(self, file_id: str) -> Optional[LockInfo]:
        """Obtiene información del bloqueo de escritura de un archivo"""
        with self._lock:
            return self._write_locks.get(file_id)
    
    def _cleanup_expired_locks(self):
        """Thread que limpia bloqueos expirados periódicamente"""
        while self._cleanup_running:
            time.sleep(10)  # Ejecutar cada 10 segundos
            try:
                with self._lock:
                    now = time.time()
                    
                    # Limpiar bloqueos de escritura expirados
                    expired_write = [fid for fid, lock in self._write_locks.items() if lock.is_expired()]
                    for file_id in expired_write:
                        del self._write_locks[file_id]
                        logger.debug(f"Cleaned expired write lock for {file_id}")
                    
                    # Limpiar bloqueos de lectura expirados
                    for file_id in list(self._read_locks.keys()):
                        for holder_id in list(self._read_locks[file_id]):
                            lock_key = (file_id, holder_id)
                            if lock_key in self._read_lock_info:
                                if self._read_lock_info[lock_key].is_expired():
                                    self._read_locks[file_id].discard(holder_id)
                                    del self._read_lock_info[lock_key]
                                    logger.debug(f"Cleaned expired read lock for {file_id} by {holder_id}")
                        
                        if not self._read_locks[file_id]:
                            del self._read_locks[file_id]
                            
            except Exception as e:
                logger.error(f"Error in lock cleanup: {e}")
    
    def stop(self):
        """Detiene el thread de limpieza"""
        self._cleanup_running = False

    # === Snapshot / replicación ===

    def export_state(self) -> Dict:
        """Exporta bloqueos actuales (solo los no expirados)"""
        with self._lock:
            # Limpiar expirados antes de exportar
            for fid in list(self._write_locks.keys()):
                if self._write_locks[fid].is_expired():
                    del self._write_locks[fid]
            for fid in list(self._read_locks.keys()):
                for holder_id in list(self._read_locks[fid]):
                    lock_key = (fid, holder_id)
                    if lock_key in self._read_lock_info and self._read_lock_info[lock_key].is_expired():
                        self._read_locks[fid].discard(holder_id)
                        del self._read_lock_info[lock_key]
                if not self._read_locks.get(fid):
                    self._read_locks.pop(fid, None)

            return {
                'write_locks': {fid: lock.to_dict() for fid, lock in self._write_locks.items()},
                'read_locks': {
                    fid: [self._read_lock_info[(fid, holder)].to_dict()
                          for holder in holders
                          if (fid, holder) in self._read_lock_info]
                    for fid, holders in self._read_locks.items()
                }
            }

    def import_state(self, state: Dict):
        """Restaura bloqueos desde un snapshot"""
        with self._lock:
            self._write_locks.clear()
            self._read_locks.clear()
            self._read_lock_info.clear()

            for fid, lock_dict in state.get('write_locks', {}).items():
                lock = LockInfo.from_dict(lock_dict)
                if not lock.is_expired():
                    self._write_locks[fid] = lock

            for fid, lock_list in state.get('read_locks', {}).items():
                for lock_dict in lock_list:
                    lock = LockInfo.from_dict(lock_dict)
                    if lock.is_expired():
                        continue
                    holder = lock.holder
                    self._read_locks[fid].add(holder)
                    self._read_lock_info[(fid, holder)] = lock

