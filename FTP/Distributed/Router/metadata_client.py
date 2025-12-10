"""
Cliente para comunicarse con el servicio de Metadata.
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


class MetadataClient:
    """
    Cliente para interactuar con el servicio de Metadata.
    Proporciona una API de alto nivel para las operaciones del Router.
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
        
        # Iniciar thread de descubrimiento del líder
        self._leader_discovery_running = True
        self._discovery_thread = threading.Thread(
            target=self._leader_discovery_loop,
            daemon=True
        )
        self._discovery_thread.start()
        logger.info("Leader discovery thread started")
    
    def _leader_discovery_loop(self):
        """Loop que descubre el líder periódicamente"""
        while self._leader_discovery_running:
            try:
                # Descubrir todos los nodos metadata disponibles
                self._discover_metadata_nodes()
                
                # Consultar a cada uno para encontrar el líder
                for host, port in self._known_metadata_nodes:
                    if self._query_leader(host, port):
                        break
                
                time.sleep(10)  # Esperar 10 segundos antes de volver a consultar
                
            except Exception as e:
                logger.debug(f"Leader discovery loop error: {e}")
                time.sleep(5)
    
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
            # Fallback: usar el nombre DNS
            with self._leader_lock:
                self._known_metadata_nodes = [(self.metadata_host, self.metadata_port)]
    
    def _query_leader(self, host: str, port: int) -> bool:
        """Consulta a un nodo metadata quién es el líder"""
        try:
            msg = RPCMessage(MessageType.LEADER_QUERY, {})
            response = self._rpc_client.call(host, port, msg)
            
            if response:
                leader_host = response.payload.get('leader_host')
                leader_port = response.payload.get('leader_port')
                leader_id = response.payload.get('leader_id')
                
                if leader_host and leader_port:
                    with self._leader_lock:
                        old_leader = self._leader_host
                        self._leader_host = leader_host
                        self._leader_port = leader_port
                        self._last_leader_update = time.time()
                    
                    if old_leader != leader_host:
                        logger.info(f"Leader updated: {leader_id} @ {leader_host}:{leader_port}")
                    
                    return True
            
            return False
            
        except Exception as e:
            logger.debug(f"Could not query leader from {host}:{port}: {e}")
            return False
    
    def _call(self, msg: RPCMessage, retry_count: int = 2) -> Optional[RPCMessage]:
        """Realiza una llamada RPC con reintentos y redirecciones al líder"""
        with self._leader_lock:
            leader_host = self._leader_host
            leader_port = self._leader_port
        
        for attempt in range(retry_count):
            try:
                response = self._rpc_client.call(leader_host, leader_port, msg)
                
                if response:
                    # Si es NOT_LEADER, extraer IP del líder y reintentar
                    if response.payload.get('status') == DistributedResponseCode.NOT_LEADER.value:
                        new_leader_host = response.payload.get('leader_host')
                        new_leader_port = response.payload.get('leader_port')
                        
                        if new_leader_host and new_leader_port:
                            with self._leader_lock:
                                self._leader_host = new_leader_host
                                self._leader_port = new_leader_port
                                self._last_leader_update = time.time()
                            
                            logger.debug(f"Redirected to new leader: {new_leader_host}:{new_leader_port}")
                            leader_host = new_leader_host
                            leader_port = new_leader_port
                            continue
                    
                    return response
                
                # Sin respuesta, reintentar con backoff exponencial
                if attempt < retry_count - 1:
                    backoff = 0.1 * (2 ** attempt)
                    time.sleep(backoff)
                
            except Exception as e:
                logger.debug(f"RPC call error: {e}")
                if attempt < retry_count - 1:
                    time.sleep(0.1 * (2 ** attempt))
        
        logger.error(f"Failed to call leader after {retry_count} attempts")
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

