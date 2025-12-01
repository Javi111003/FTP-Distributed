"""
Cliente para comunicarse con los nodos de Storage.
"""
import logging
from typing import Optional, List, Tuple

from ..Common.constants import (
    STORAGE_RPC_PORT, MessageType, DistributedResponseCode
)
from ..Common.rpc_protocol import RPCClient, RPCMessage

logger = logging.getLogger(__name__)


class StorageClient:
    """
    Cliente para interactuar con los nodos de Storage.
    Maneja la transferencia de archivos y replicación.
    """
    
    def __init__(self):
        self._rpc_client = RPCClient()
    
    def store_file(self, host: str, port: int, file_id: str, data: bytes,
                  version: int = 1, replicate_to: List[dict] = None) -> Tuple[bool, Optional[str]]:
        """
        Almacena un archivo en un nodo de storage.
        Retorna: (success, checksum)
        """
        try:
            msg = RPCMessage(
                MessageType.STORE_FILE,
                {
                    'file_id': file_id,
                    'data': data.hex(),
                    'version': version,
                    'replicate_to': replicate_to or []
                }
            )
            response = self._rpc_client.call(host, port, msg)
            
            if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
                return (True, response.payload.get('checksum'))
            
            return (False, None)
        except Exception as e:
            logger.error(f"Error storing file on {host}:{port}: {e}")
            return (False, None)
    
    def retrieve_file(self, host: str, port: int, file_id: str) -> Optional[bytes]:
        """Recupera un archivo de un nodo de storage"""
        try:
            msg = RPCMessage(
                MessageType.RETRIEVE_FILE,
                {'file_id': file_id}
            )
            response = self._rpc_client.call(host, port, msg)
            
            if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
                data_hex = response.payload.get('data')
                if data_hex:
                    return bytes.fromhex(data_hex)
            
            return None
        except Exception as e:
            logger.error(f"Error retrieving file from {host}:{port}: {e}")
            return None
    
    def delete_file(self, host: str, port: int, file_id: str) -> bool:
        """Elimina un archivo de un nodo de storage"""
        try:
            msg = RPCMessage(
                MessageType.DELETE_LOCAL,
                {'file_id': file_id}
            )
            response = self._rpc_client.call(host, port, msg)
            
            return response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value
        except Exception as e:
            logger.error(f"Error deleting file on {host}:{port}: {e}")
            return False
    
    def retrieve_from_any(self, replicas: List[dict], file_id: str) -> Optional[bytes]:
        """
        Intenta recuperar un archivo de cualquiera de las réplicas disponibles.
        Útil para tolerancia a fallos.
        """
        for replica in replicas:
            host = replica.get('host')
            port = replica.get('port')
            if host and port:
                data = self.retrieve_file(host, port, file_id)
                if data is not None:
                    return data
        return None
    
    def store_with_replication(self, replicas: List[dict], file_id: str, 
                               data: bytes, version: int = 1) -> int:
        """
        Almacena un archivo en múltiples réplicas.
        Retorna el número de réplicas exitosas.
        """
        if not replicas:
            return 0
        
        # Almacenar en la primera réplica y pedir que replique a las demás
        primary = replicas[0]
        others = replicas[1:] if len(replicas) > 1 else []
        
        success, _ = self.store_file(
            primary['host'], primary['port'],
            file_id, data, version,
            replicate_to=others
        )
        
        if success:
            return 1 + len(others)  # Asumimos que la replicación es exitosa
        
        # Si falla la primaria, intentar con las otras
        successful = 0
        for replica in others:
            success, _ = self.store_file(
                replica['host'], replica['port'],
                file_id, data, version
            )
            if success:
                successful += 1
        
        return successful
    
    def delete_from_all(self, replicas: List[dict], file_id: str) -> int:
        """
        Elimina un archivo de todas las réplicas.
        Retorna el número de eliminaciones exitosas.
        """
        successful = 0
        for replica in replicas:
            if self.delete_file(replica['host'], replica['port'], file_id):
                successful += 1
        return successful

