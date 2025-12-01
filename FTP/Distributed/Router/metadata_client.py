"""
Cliente para comunicarse con el servicio de Metadata.
"""
import logging
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
    
    def _call(self, msg: RPCMessage) -> Optional[RPCMessage]:
        """Realiza una llamada RPC, siguiendo redirecciones al líder"""
        response = self._rpc_client.call(self._leader_host, self._leader_port, msg)
        
        # Si recibimos una redirección al líder, intentar con el nuevo líder
        if response and response.payload.get('status') == DistributedResponseCode.NOT_LEADER.value:
            new_leader_host = response.payload.get('leader_host')
            new_leader_port = response.payload.get('leader_port')
            if new_leader_host and new_leader_port:
                self._leader_host = new_leader_host
                self._leader_port = new_leader_port
                return self._rpc_client.call(self._leader_host, self._leader_port, msg)
        
        return response
    
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

