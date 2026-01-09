"""
Servidor de almacenamiento para el sistema distribuido FTP.
Gestiona el almacenamiento físico de archivos y la replicación.
"""
import os
import time
import uuid
import hashlib
import shutil
import logging
import threading
from pathlib import Path
from typing import Dict, Optional, List, Tuple, BinaryIO

from ..Common.constants import (
    STORAGE_RPC_PORT, METADATA_RPC_PORT, MessageType, NodeType, NodeState,
    DistributedResponseCode, HEARTBEAT_INTERVAL, MIN_REPLICAS_FOR_WRITE
)
from ..Common.rpc_protocol import RPCServer, RPCMessage, RPCClient
from ..Common.models import NodeInfo, ReplicaInfo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StorageServer:
    """
    Servidor de almacenamiento.
    Almacena archivos físicamente y participa en la replicación.
    """
    
    def __init__(self, host: str = '0.0.0.0', port: int = STORAGE_RPC_PORT,
                 data_dir: str = '/data/storage',
                 metadata_host: str = 'metadata', metadata_port: int = METADATA_RPC_PORT):
        self.host = host
        self.port = port
        self.data_dir = Path(data_dir)
        self.metadata_host = metadata_host
        self.metadata_port = metadata_port
        
        # Generar ID único para este nodo
        self.node_id = os.getenv('NODE_ID', f"storage-{uuid.uuid4().hex[:8]}")
        
        # Información del nodo
        self.node_info = NodeInfo(
            node_id=self.node_id,
            node_type=NodeType.STORAGE,
            host=os.getenv('HOSTNAME', host),
            port=port,
            state=NodeState.UP
        )
        
        # Crear directorio de datos
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Índice local de archivos: file_id -> (path, version, size, checksum)
        self._file_index: Dict[str, dict] = {}
        self._index_lock = threading.RLock()
        
        # Servidor RPC
        self.rpc_server = RPCServer(host, port)
        self._register_handlers()
        
        # Cliente RPC para comunicarse con metadata y otros storage
        self._rpc_client = RPCClient()
        
        # Estado
        self._running = False
        self._heartbeat_thread: Optional[threading.Thread] = None
        
        # Cargar índice existente
        self._load_index()
    
    def _register_handlers(self):
        """Registra los manejadores RPC"""
        self.rpc_server.register_handler(MessageType.HEARTBEAT, self._handle_heartbeat)
        self.rpc_server.register_handler(MessageType.STORE_FILE, self._handle_store_file)
        self.rpc_server.register_handler(MessageType.RETRIEVE_FILE, self._handle_retrieve_file)
        self.rpc_server.register_handler(MessageType.DELETE_LOCAL, self._handle_delete_local)
        self.rpc_server.register_handler(MessageType.REPLICATE_FILE, self._handle_replicate_file)
        self.rpc_server.register_handler(MessageType.FILE_VERSION_LIST, self._handle_version_list)
        self.rpc_server.register_handler(MessageType.SYNC_REQUEST, self._handle_sync_request)
    
    def _load_index(self):
        """Carga el índice de archivos desde disco"""
        index_path = self.data_dir / ".index.json"
        if index_path.exists():
            try:
                import json
                with open(index_path, 'r') as f:
                    self._file_index = json.load(f)
                logger.info(f"Loaded {len(self._file_index)} files from index")
            except Exception as e:
                logger.error(f"Error loading index: {e}")
                self._scan_files()
        else:
            self._scan_files()
    
    def _save_index(self):
        """Guarda el índice de archivos a disco"""
        try:
            import json
            index_path = self.data_dir / ".index.json"
            with open(index_path, 'w') as f:
                json.dump(self._file_index, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving index: {e}")
    
    def _scan_files(self):
        """Escanea el directorio de datos para reconstruir el índice"""
        logger.info("Scanning data directory...")
        with self._index_lock:
            self._file_index.clear()
            for file_path in self.data_dir.rglob("*"):
                if file_path.is_file() and not file_path.name.startswith('.'):
                    file_id = file_path.stem
                    stat = file_path.stat()
                    self._file_index[file_id] = {
                        'path': str(file_path.relative_to(self.data_dir)),
                        'size': stat.st_size,
                        'version': 1,
                        'checksum': None
                    }
            self._save_index()
        logger.info(f"Found {len(self._file_index)} files")
    
    def _get_file_path(self, file_id: str) -> Path:
        """Obtiene la ruta física de un archivo"""
        # Usar primeros 2 caracteres del file_id como subdirectorio
        subdir = file_id[:2] if len(file_id) >= 2 else "00"
        dir_path = self.data_dir / subdir
        dir_path.mkdir(parents=True, exist_ok=True)
        return dir_path / file_id
    
    def _compute_checksum(self, file_path: Path) -> str:
        """Calcula el checksum MD5 de un archivo"""
        hash_md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def start(self):
        """Inicia el servidor de almacenamiento"""
        self._running = True
        
        # Iniciar RPC server
        self.rpc_server.start()
        
        # Registrarse con el servicio de metadata
        self._register_with_metadata()
        
        # Iniciar heartbeat
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()
        
        logger.info(f"Storage server started: {self.node_id} on {self.host}:{self.port}")
        
        # Mantener el servidor corriendo
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
    
    def stop(self):
        """Detiene el servidor"""
        self._running = False
        
        # Desregistrarse de metadata
        self._unregister_from_metadata()
        
        self.rpc_server.stop()
        logger.info("Storage server stopped")
    
    def _discover_leader(self) -> tuple:
        """Descubre el líder actual consultando a los metadata conocidos"""
        metadata_hosts = [self.metadata_host, 'metadata1', 'metadata2', 'metadata3']
        
        for host in metadata_hosts:
            try:
                leader_query = RPCMessage(MessageType.LEADER_QUERY, {})
                response = self._rpc_client.call(host, self.metadata_port, leader_query)
                if response:
                    leader_host = response.payload.get('leader_host')
                    leader_port = response.payload.get('leader_port')
                    if leader_host and leader_port:
                        logger.info(f"Discovered leader: {leader_host}:{leader_port}")
                        return (leader_host, leader_port)
            except Exception as e:
                logger.debug(f"Failed to query leader from {host}: {e}")
                continue
        
        return (self.metadata_host, self.metadata_port)

    def _register_with_metadata(self):
        """Se registra con el servicio de metadata (preferentemente con el líder)"""
        max_retries = 10
        
        for attempt in range(max_retries):
            try:
                # Primero intentar descubrir el líder
                leader_host, leader_port = self._discover_leader()
                
                msg = RPCMessage(
                    MessageType.REGISTER_NODE,
                    {'node': self.node_info.to_dict()}
                )
                
                # Intentar registrarse con el líder
                response = self._rpc_client.call(leader_host, leader_port, msg)
                if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
                    logger.info(f"✅ Registered with metadata leader at {leader_host}:{leader_port}")
                    return
                
                # Si falla, intentar con el alias DNS
                if leader_host != self.metadata_host:
                    response = self._rpc_client.call(self.metadata_host, self.metadata_port, msg)
                    if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
                        logger.info(f"✅ Registered with metadata service (fallback)")
                        return
                        
            except Exception as e:
                logger.warning(f"Failed to register with metadata (attempt {attempt+1}): {e}")
            time.sleep(2)
        
        logger.error("Failed to register with metadata service after all retries")
    
    def _unregister_from_metadata(self):
        """Se desregistra del servicio de metadata"""
        try:
            msg = RPCMessage(
                MessageType.UNREGISTER_NODE,
                {'node_id': self.node_id, 'node_type': NodeType.STORAGE.value}
            )
            self._rpc_client.call(self.metadata_host, self.metadata_port, msg)
        except Exception as e:
            logger.debug(f"Error unregistering: {e}")
    
    def _heartbeat_loop(self):
        """Envía heartbeats periódicos al servicio de metadata"""
        leader_check_counter = 0

        while self._running:
            try:
                # Consultar líder cada 3 heartbeats para no sobrecargar
                current_leader_host = self.metadata_host
                current_leader_port = self.metadata_port

                if leader_check_counter % 3 == 0:
                    try:
                        leader_query = RPCMessage(MessageType.LEADER_QUERY, {})
                        # Intentar con todos los metadatas conocidos
                        for attempt_host in [self.metadata_host, 'metadata1', 'metadata2', 'metadata3']:
                            try:
                                response = self._rpc_client.call(attempt_host, self.metadata_port, leader_query)
                                if response:
                                    new_leader_host = response.payload.get('leader_host')
                                    new_leader_port = response.payload.get('leader_port')
                                    if new_leader_host and new_leader_port:
                                        current_leader_host = new_leader_host
                                        current_leader_port = new_leader_port
                                        logger.warning(f"Storage {self.node_id} FOLLOWING LEADER: {current_leader_host}:{current_leader_port}")
                                        break
                            except:
                                continue
                    except Exception as e:
                        logger.debug(f"Leader discovery failed: {e}")

                leader_check_counter += 1

                # Enviar heartbeat al líder actual
                msg = RPCMessage(
                    MessageType.HEARTBEAT,
                    {'node_id': self.node_id, 'timestamp': time.time()}
                )
                response = self._rpc_client.call(current_leader_host, current_leader_port, msg)
                if response:
                    logger.warning(f"STORAGE {self.node_id}: HEARTBEAT SENT to {current_leader_host}:{current_leader_port}")
                else:
                    logger.error(f"STORAGE {self.node_id}: HEARTBEAT FAILED to {current_leader_host}:{current_leader_port}")

            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
            time.sleep(HEARTBEAT_INTERVAL)
    
    # === Operaciones de archivos ===
    
    def store_file(self, file_id: str, data: bytes, version: int = 1) -> Tuple[bool, str]:
        """Almacena un archivo localmente"""
        try:
            file_path = self._get_file_path(file_id)
            
            with open(file_path, 'wb') as f:
                f.write(data)
            
            checksum = self._compute_checksum(file_path)
            
            with self._index_lock:
                self._file_index[file_id] = {
                    'path': str(file_path.relative_to(self.data_dir)),
                    'size': len(data),
                    'version': version,
                    'checksum': checksum
                }
                self._save_index()
            
            logger.info(f"[store] file_id={file_id} size={len(data)} version={version} checksum={checksum}")
            return (True, checksum)
            
        except Exception as e:
            logger.error(f"Error storing file {file_id}: {e}")
            return (False, str(e))
    
    def retrieve_file(self, file_id: str) -> Optional[bytes]:
        """Recupera un archivo del almacenamiento local"""
        with self._index_lock:
            if file_id not in self._file_index:
                logger.warning(f"[retrieve] miss file_id={file_id}")
                return None
            file_info = self._file_index[file_id]
        
        file_path = self.data_dir / file_info['path']
        
        try:
            with open(file_path, 'rb') as f:
                data = f.read()
                logger.info(f"[retrieve] file_id={file_id} size={len(data)}")
                return data
        except Exception as e:
            logger.error(f"Error retrieving file {file_id}: {e}")
            return None
    
    def delete_file(self, file_id: str) -> bool:
        """Elimina un archivo del almacenamiento local"""
        with self._index_lock:
            if file_id not in self._file_index:
                return False
            file_info = self._file_index[file_id]
        
        file_path = self.data_dir / file_info['path']
        
        try:
            file_path.unlink()
            with self._index_lock:
                del self._file_index[file_id]
                self._save_index()
            logger.info(f"[delete] file_id={file_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting file {file_id}: {e}")
            return False
    
    def get_file_info(self, file_id: str) -> Optional[dict]:
        """Obtiene información de un archivo"""
        with self._index_lock:
            return self._file_index.get(file_id)
    
    def has_file(self, file_id: str) -> bool:
        """Verifica si un archivo existe localmente"""
        with self._index_lock:
            return file_id in self._file_index
    
    # === Replicación ===
    
    def replicate_to_peer(self, file_id: str, peer_host: str, peer_port: int) -> bool:
        """Replica un archivo a otro nodo de storage"""
        data = self.retrieve_file(file_id)
        if data is None:
            return False
        
        with self._index_lock:
            version = self._file_index.get(file_id, {}).get('version', 1)
        
        try:
            msg = RPCMessage(
                MessageType.REPLICATE_FILE,
                {
                    'file_id': file_id,
                    'data': data.hex(),  # Convertir a hex para JSON
                    'version': version
                }
            )
            response = self._rpc_client.call(peer_host, peer_port, msg)
            ok = response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value
            if ok:
                logger.info(f"[replicate] file_id={file_id} -> {peer_host}:{peer_port}")
            else:
                logger.warning(f"[replicate] failed file_id={file_id} -> {peer_host}:{peer_port}")
            return ok
        except Exception as e:
            logger.error(f"Error replicating to {peer_host}:{peer_port}: {e}")
            return False
    
    # === Handlers RPC ===
    
    def _handle_heartbeat(self, msg: RPCMessage) -> RPCMessage:
        """Responde a heartbeats"""
        return RPCMessage(
            MessageType.HEARTBEAT_ACK,
            {'node_id': self.node_id, 'timestamp': time.time()},
            msg.request_id
        )
    
    def _handle_store_file(self, msg: RPCMessage) -> RPCMessage:
        """Maneja solicitudes de almacenamiento"""
        file_id = msg.payload.get('file_id')
        data_hex = msg.payload.get('data')
        version = msg.payload.get('version', 1)
        replicate_to = msg.payload.get('replicate_to', [])  # Lista de peers para replicar
        
        try:
            data = bytes.fromhex(data_hex)
            success, checksum = self.store_file(file_id, data, version)
            
            if success and replicate_to:
                # Replicar a otros nodos en background
                def replicate_async():
                    successful_replicas = 1  # Contando este nodo
                    for peer in replicate_to:
                        if self.replicate_to_peer(file_id, peer['host'], peer['port']):
                            successful_replicas += 1
                    logger.debug(f"Replicated {file_id} to {successful_replicas} nodes")
                
                threading.Thread(target=replicate_async, daemon=True).start()
            
            return RPCMessage(
                MessageType.STORE_RESPONSE,
                {
                    'status': DistributedResponseCode.SUCCESS.value if success 
                             else DistributedResponseCode.ERROR.value,
                    'checksum': checksum if success else None
                },
                msg.request_id
            )
        except Exception as e:
            logger.error(f"Error in store handler: {e}")
            return RPCMessage(
                MessageType.STORE_RESPONSE,
                {'status': DistributedResponseCode.ERROR.value, 'error': str(e)},
                msg.request_id
            )
    
    def _handle_retrieve_file(self, msg: RPCMessage) -> RPCMessage:
        """Maneja solicitudes de recuperación"""
        file_id = msg.payload.get('file_id')
        
        data = self.retrieve_file(file_id)
        
        if data is not None:
            return RPCMessage(
                MessageType.RETRIEVE_RESPONSE,
                {
                    'status': DistributedResponseCode.SUCCESS.value,
                    'data': data.hex(),
                    'size': len(data)
                },
                msg.request_id
            )
        
        return RPCMessage(
            MessageType.RETRIEVE_RESPONSE,
            {'status': DistributedResponseCode.NOT_FOUND.value},
            msg.request_id
        )
    
    def _handle_delete_local(self, msg: RPCMessage) -> RPCMessage:
        """Maneja solicitudes de eliminación local"""
        file_id = msg.payload.get('file_id')
        
        success = self.delete_file(file_id)
        
        return RPCMessage(
            MessageType.DELETE_LOCAL_RESPONSE,
            {
                'status': DistributedResponseCode.SUCCESS.value if success 
                         else DistributedResponseCode.NOT_FOUND.value
            },
            msg.request_id
        )
    
    def _handle_replicate_file(self, msg: RPCMessage) -> RPCMessage:
        """Maneja solicitudes de replicación desde otro nodo"""
        file_id = msg.payload.get('file_id')
        data_hex = msg.payload.get('data')
        version = msg.payload.get('version', 1)
        
        try:
            data = bytes.fromhex(data_hex)
            
            # Verificar si ya tenemos una versión más reciente
            with self._index_lock:
                existing = self._file_index.get(file_id)
                if existing and existing.get('version', 0) >= version:
                    return RPCMessage(
                        MessageType.REPLICATE_RESPONSE,
                        {'status': DistributedResponseCode.SUCCESS.value, 'note': 'already_current'},
                        msg.request_id
                    )
            
            success, checksum = self.store_file(file_id, data, version)
            
            return RPCMessage(
                MessageType.REPLICATE_RESPONSE,
                {
                    'status': DistributedResponseCode.SUCCESS.value if success 
                             else DistributedResponseCode.ERROR.value
                },
                msg.request_id
            )
        except Exception as e:
            return RPCMessage(
                MessageType.REPLICATE_RESPONSE,
                {'status': DistributedResponseCode.ERROR.value, 'error': str(e)},
                msg.request_id
            )
    
    def _handle_version_list(self, msg: RPCMessage) -> RPCMessage:
        """Envía lista de versiones de archivos para sincronización"""
        with self._index_lock:
            files = {
                fid: info.get('version', 1)
                for fid, info in self._file_index.items()
            }
        
        return RPCMessage(
            MessageType.FILE_VERSION_RESPONSE,
            {'files': files, 'node_id': self.node_id},
            msg.request_id
        )
    
    def _handle_sync_request(self, msg: RPCMessage) -> RPCMessage:
        """Maneja solicitud de sincronización completa"""
        # Reportar nuestras versiones a metadata para verificar
        with self._index_lock:
            files = {
                fid: info.get('version', 1)
                for fid, info in self._file_index.items()
            }
        
        try:
            # Enviar al metadata
            sync_msg = RPCMessage(
                MessageType.FILE_VERSION_LIST,
                {'node_id': self.node_id, 'files': files}
            )
            response = self._rpc_client.call(
                self.metadata_host, self.metadata_port, sync_msg
            )
            
            if response:
                updates_needed = response.payload.get('updates_needed', [])
                # TODO: Solicitar archivos desactualizados de otros nodos
                logger.info(f"Sync: {len(updates_needed)} files need updating")
            
            return RPCMessage(
                MessageType.SYNC_RESPONSE,
                {'status': DistributedResponseCode.SUCCESS.value},
                msg.request_id
            )
        except Exception as e:
            return RPCMessage(
                MessageType.SYNC_RESPONSE,
                {'status': DistributedResponseCode.ERROR.value, 'error': str(e)},
                msg.request_id
            )


def main():
    """Punto de entrada principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Storage Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host")
    parser.add_argument("--port", type=int, default=STORAGE_RPC_PORT, help="Port")
    parser.add_argument("--data-dir", default="/data/storage", help="Data directory")
    parser.add_argument("--metadata-host", default="metadata", help="Metadata service host")
    parser.add_argument("--metadata-port", type=int, default=METADATA_RPC_PORT, help="Metadata service port")
    
    args = parser.parse_args()
    
    server = StorageServer(
        args.host, args.port, args.data_dir,
        args.metadata_host, args.metadata_port
    )
    server.start()


if __name__ == "__main__":
    main()

