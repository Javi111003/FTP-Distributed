"""
Cliente para comunicarse con el servicio de Metadata.
Implementa estabilidad en el seguimiento del líder para evitar oscilaciones.
"""
import logging
import socket
import time
import threading
from typing import Dict, Optional, List, Tuple, Any

from ..Common.constants import (
    METADATA_RPC_PORT, MessageType, DistributedResponseCode
)
from ..Common.rpc_protocol import RPCClient, RPCMessage
from ..Common.models import FileMetadata, UserInfo

logger = logging.getLogger(__name__)

# Constantes para estabilidad
LEADER_STABILITY_PERIOD = 10  # Segundos que un líder debe estar estable antes de aceptar cambio
LEADER_CHANGE_COOLDOWN = 5    # Segundos mínimos entre cambios de líder
DISCOVERY_INTERVAL = 3        # Intervalo de descubrimiento del líder


class MetadataClient:
    """
    Cliente para interactuar con el servicio de Metadata.
    Proporciona una API de alto nivel para las operaciones del Router.
    Implementa estabilidad en el tracking del líder.
    """
    
    def __init__(self, metadata_host: str = 'metadata', metadata_port: int = METADATA_RPC_PORT):
        self.metadata_host = metadata_host
        self.metadata_port = metadata_port
        self._rpc_client = RPCClient()
        self._leader_host = metadata_host
        self._leader_port = metadata_port
        
        # Variables para descubrimiento proactivo del líder
        self._leader_lock = threading.RLock()
        self._last_leader_update = 0
        self._known_metadata_nodes: List[Tuple[str, int]] = []
        
        # Variables para estabilidad del líder
        self._current_leader_id: Optional[str] = None
        self._current_leader_term: int = 0
        self._leader_confirmed_at: float = 0  # Cuándo se confirmó el líder actual
        self._last_leader_change: float = 0   # Última vez que cambió el líder
        self._consecutive_confirmations: int = 0  # Confirmaciones consecutivas del mismo líder
        self._pending_leader: Optional[Dict] = None  # Líder pendiente de confirmación
        
        # Iniciar thread de descubrimiento del líder
        self._leader_discovery_running = True
        self._discovery_thread = threading.Thread(
            target=self._leader_discovery_loop,
            daemon=True
        )
        self._discovery_thread.start()
        logger.info("Leader discovery thread started")
    
    def _leader_discovery_loop(self):
        """Loop que descubre el líder periódicamente con estabilidad"""
        initial_delay = True
        
        while self._leader_discovery_running:
            try:
                # Pequeño delay inicial para que los metadatas arranquen
                if initial_delay:
                    time.sleep(2)
                    initial_delay = False
                
                # Descubrir todos los nodos metadata disponibles
                self._discover_metadata_nodes()
                
                # Recolectar respuestas de todos los nodos
                leader_responses = []
                for host, port in self._known_metadata_nodes:
                    result = self._query_leader_info(host, port)
                    if result:
                        leader_responses.append(result)
                
                # Determinar el líder estable
                if leader_responses:
                    self._determine_stable_leader(leader_responses)
                
                time.sleep(DISCOVERY_INTERVAL)
                
            except Exception as e:
                logger.debug(f"Leader discovery loop error: {e}")
                time.sleep(1)
    
    def _discover_metadata_nodes(self):
        """Resuelve TODAS las IPs del servicio metadata"""
        try:
            _, _, ipaddrlist = socket.gethostbyname_ex(self.metadata_host)
            
            with self._leader_lock:
                self._known_metadata_nodes = [
                    (ip, self.metadata_port) for ip in ipaddrlist
                ]
            
            logger.debug(f"Discovered metadata nodes: {self._known_metadata_nodes}")
            
        except socket.gaierror as e:
            logger.debug(f"Could not resolve metadata host '{self.metadata_host}': {e}")
            with self._leader_lock:
                self._known_metadata_nodes = [(self.metadata_host, self.metadata_port)]
    
    def _query_leader_info(self, host: str, port: int) -> Optional[Dict]:
        """Consulta a un nodo metadata información completa del líder"""
        try:
            msg = RPCMessage(MessageType.LEADER_QUERY, {})
            response = self._rpc_client.call(host, port, msg, timeout=3)
            
            if response and response.payload:
                leader_host = response.payload.get('leader_host')
                leader_port = response.payload.get('leader_port')
                leader_id = response.payload.get('leader_id')
                term = response.payload.get('term', 0)
                is_leader = response.payload.get('is_leader', False)
                data_state = response.payload.get('data_state', {})
                
                if leader_host and leader_port and leader_id:
                    return {
                        'leader_id': leader_id,
                        'leader_host': leader_host,
                        'leader_port': leader_port,
                        'term': term,
                        'is_leader': is_leader,
                        'from_host': host,
                        'data_state': data_state
                    }
            
            return None
            
        except Exception as e:
            logger.debug(f"Could not query leader from {host}:{port}: {e}")
            return None
    
    def _determine_stable_leader(self, responses: List[Dict]):
        """
        Determina el líder estable basándose en múltiples respuestas.
        Solo cambia de líder si hay consenso y estabilidad.
        """
        with self._leader_lock:
            current_time = time.time()
            
            # Contar votos para cada líder
            leader_votes: Dict[str, List[Dict]] = {}
            for resp in responses:
                lid = resp['leader_id']
                if lid not in leader_votes:
                    leader_votes[lid] = []
                leader_votes[lid].append(resp)
            
            if not leader_votes:
                return
            
            # Encontrar el líder con más votos
            best_leader_id = max(leader_votes.keys(), key=lambda x: len(leader_votes[x]))
            votes = leader_votes[best_leader_id]
            
            # Verificar que hay mayoría
            total_responses = len(responses)
            if len(votes) < (total_responses + 1) // 2:
                # No hay mayoría clara, no cambiar
                logger.debug(f"No leader majority: {best_leader_id} has {len(votes)}/{total_responses} votes")
                return
            
            # Obtener información del mejor líder
            best_response = votes[0]
            new_leader_host = best_response['leader_host']
            new_leader_port = best_response['leader_port']
            new_term = best_response['term']
            
            # Si es el mismo líder, incrementar confirmaciones
            if best_leader_id == self._current_leader_id:
                self._consecutive_confirmations += 1
                self._last_leader_update = current_time
                return
            
            # Es un líder diferente - verificar si debemos cambiar
            time_since_last_change = current_time - self._last_leader_change
            
            # Cooldown entre cambios de líder
            if time_since_last_change < LEADER_CHANGE_COOLDOWN:
                logger.debug(f"Leader change cooldown active ({LEADER_CHANGE_COOLDOWN - time_since_last_change:.1f}s)")
                return
            
            # Si el líder actual está confirmado y estable, requerir mayor término o mayoría fuerte
            if self._current_leader_id and self._consecutive_confirmations >= 3:
                leader_time = current_time - self._leader_confirmed_at
                if leader_time < LEADER_STABILITY_PERIOD:
                    # Líder actual es estable, solo cambiar si el nuevo tiene término mayor
                    if new_term <= self._current_leader_term:
                        logger.debug(f"Current leader {self._current_leader_id} is stable, ignoring {best_leader_id}")
                        return
            
            # Cambiar al nuevo líder
            old_leader = self._current_leader_id
            self._current_leader_id = best_leader_id
            self._current_leader_term = new_term
            self._leader_host = new_leader_host
            self._leader_port = new_leader_port
            self._leader_confirmed_at = current_time
            self._last_leader_change = current_time
            self._last_leader_update = current_time
            self._consecutive_confirmations = 1
            
            logger.info(f"✅ Leader updated: {best_leader_id} @ {new_leader_host}:{new_leader_port} (term {new_term})")
    
    def _query_leader(self, host: str, port: int) -> bool:
        """Consulta a un nodo metadata quién es el líder (compatibilidad)"""
        result = self._query_leader_info(host, port)
        if result:
            with self._leader_lock:
                # Solo actualizar si no tenemos líder o este tiene término mayor
                if not self._current_leader_id or result['term'] > self._current_leader_term:
                    self._leader_host = result['leader_host']
                    self._leader_port = result['leader_port']
                    self._last_leader_update = time.time()
            return True
        return False
    
    def _call(self, msg: RPCMessage, retry_count: int = 2) -> Optional[RPCMessage]:
        """Realiza una llamada RPC con reintentos y redirecciones al líder"""
        with self._leader_lock:
            leader_host = self._leader_host
            leader_port = self._leader_port
            candidates = [(leader_host, leader_port)] + list(self._known_metadata_nodes)
        
        # Quitar duplicados preservando orden
        seen = set()
        uniq_candidates = []
        for host, port in candidates:
            if (host, port) not in seen:
                seen.add((host, port))
                uniq_candidates.append((host, port))

        for host, port in uniq_candidates:
            for attempt in range(retry_count):
                try:
                    response = self._rpc_client.call(host, port, msg)
                    if response:
                        # Manejo de redirección de líder
                        if response.payload.get('status') in (
                            DistributedResponseCode.NOT_LEADER.value,
                            DistributedResponseCode.LEADER_REDIRECT.value
                        ) or response.msg_type == MessageType.REPL_REDIRECT:
                            new_leader_host = response.payload.get('leader_host')
                            new_leader_port = response.payload.get('leader_port')
                            new_leader_id = response.payload.get('leader_id')
                            new_term = response.payload.get('term', 0)
                            
                            if new_leader_host and new_leader_port:
                                with self._leader_lock:
                                    # Solo aceptar redirección si tiene término válido
                                    if new_term >= self._current_leader_term:
                                        self._leader_host = new_leader_host
                                        self._leader_port = new_leader_port
                                        if new_leader_id:
                                            self._current_leader_id = new_leader_id
                                            self._current_leader_term = new_term
                                        self._last_leader_update = time.time()
                                
                                logger.debug(f"Redirected to leader: {new_leader_host}:{new_leader_port}")
                                host, port = new_leader_host, new_leader_port
                                continue
                        else:
                            # Éxito
                            with self._leader_lock:
                                self._leader_host = host
                                self._leader_port = port
                                self._last_leader_update = time.time()
                            return response

                    if attempt < retry_count - 1:
                        time.sleep(0.1 * (2 ** attempt))
                except Exception as e:
                    logger.debug(f"RPC call error to {host}:{port}: {e}")
                    if attempt < retry_count - 1:
                        time.sleep(0.1 * (2 ** attempt))
                    continue

        # Segundo intento: refrescar nodos
        self._discover_metadata_nodes()
        with self._leader_lock:
            refreshed = list(self._known_metadata_nodes)
        for host, port in refreshed:
            try:
                response = self._rpc_client.call(host, port, msg)
                if response:
                    with self._leader_lock:
                        self._leader_host = host
                        self._leader_port = port
                        self._last_leader_update = time.time()
                    return response
            except Exception as e:
                logger.debug(f"RPC retry after refresh failed to {host}:{port}: {e}")
                continue

        logger.error("Failed to call metadata after trying all candidates")
        return None
    
    def authenticate(self, username: str, password: str) -> Tuple[bool, Optional[Dict]]:
        """Autentica un usuario"""
        msg = RPCMessage(
            MessageType.AUTH_REQUEST,
            {'username': username, 'password': password}
        )
        response = self._call(msg)
        
        if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
            return (True, response.payload.get('user'))
        
        return (False, None)
    
    def lookup_file(self, path: str) -> Tuple[bool, Optional[Dict], List[Dict]]:
        """
        Busca un archivo o directorio.
        Retorna: (success, metadata, replica_nodes)
        """
        msg = RPCMessage(
            MessageType.LOOKUP_FILE,
            {'path': path}
        )
        response = self._call(msg)
        
        if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
            return (
                True,
                response.payload.get('metadata'),
                response.payload.get('replica_nodes', [])
            )
        
        return (False, None, [])
    
    def create_file(self, path: str, owner: str, size: int = 0) -> Tuple[bool, Optional[Dict], List[Dict]]:
        """
        Crea un nuevo archivo.
        Retorna: (success, metadata, storage_nodes)
        """
        msg = RPCMessage(
            MessageType.CREATE_FILE,
            {'path': path, 'owner': owner, 'size': size}
        )
        response = self._call(msg)
        
        if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
            return (
                True,
                response.payload.get('metadata'),
                response.payload.get('storage_nodes', [])
            )
        
        return (False, None, [])
    
    def delete_file(self, path: str) -> Tuple[bool, Optional[str], List[Dict]]:
        """
        Elimina un archivo.
        Retorna: (success, file_id, storage_nodes)
        """
        msg = RPCMessage(
            MessageType.DELETE_FILE,
            {'path': path}
        )
        response = self._call(msg)
        
        if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
            return (
                True,
                response.payload.get('file_id'),
                response.payload.get('storage_nodes', [])
            )
        
        return (False, None, [])
    
    def rename(self, old_path: str, new_path: str) -> bool:
        """Renombra un archivo o directorio"""
        msg = RPCMessage(
            MessageType.RENAME_FILE,
            {'old_path': old_path, 'new_path': new_path}
        )
        response = self._call(msg)
        
        return response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value
    
    def list_directory(self, path: str) -> Tuple[bool, List[Dict]]:
        """Lista el contenido de un directorio"""
        msg = RPCMessage(
            MessageType.LIST_DIR,
            {'path': path}
        )
        response = self._call(msg)
        
        if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
            return (True, response.payload.get('entries', []))
        
        return (False, [])
    
    def create_directory(self, path: str, owner: str) -> bool:
        """Crea un directorio"""
        msg = RPCMessage(
            MessageType.MKDIR,
            {'path': path, 'owner': owner}
        )
        response = self._call(msg)
        
        return response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value
    
    def remove_directory(self, path: str, recursive: bool = True) -> bool:
        """Elimina un directorio"""
        msg = RPCMessage(
            MessageType.RMDIR,
            {'path': path, 'recursive': recursive}
        )
        response = self._call(msg)
        
        return response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value
    
    def get_replicas(self, file_id: str = None, path: str = None) -> List[Dict]:
        """Obtiene información de réplicas de un archivo"""
        msg = RPCMessage(
            MessageType.GET_REPLICAS,
            {'file_id': file_id, 'path': path}
        )
        response = self._call(msg)
        
        if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
            return response.payload.get('replicas', [])
        
        return []
    
    def update_file_meta(self, path: str, size: int = None, version: int = None) -> bool:
        """Actualiza los metadatos de un archivo"""
        msg = RPCMessage(
            MessageType.UPDATE_FILE_META,
            {'path': path, 'size': size, 'version': version}
        )
        response = self._call(msg)
        
        return response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value
    
    def acquire_lock(self, file_id: str, holder_id: str, lock_type: str = 'READ') -> bool:
        """Adquiere un bloqueo sobre un archivo"""
        msg = RPCMessage(
            MessageType.LOCK_REQUEST,
            {'file_id': file_id, 'holder_id': holder_id, 'lock_type': lock_type}
        )
        response = self._call(msg)
        
        return response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value
    
    def release_lock(self, file_id: str, holder_id: str) -> bool:
        """Libera un bloqueo"""
        msg = RPCMessage(
            MessageType.UNLOCK_REQUEST,
            {'file_id': file_id, 'holder_id': holder_id}
        )
        response = self._call(msg)
        
        return response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value
    
    def check_permission(self, path: str, username: str, permission: int) -> bool:
        """Verifica permisos de un usuario sobre un archivo"""
        msg = RPCMessage(
            MessageType.CHECK_PERMISSION,
            {'path': path, 'username': username, 'permission': permission}
        )
        response = self._call(msg)
        
        return response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value
