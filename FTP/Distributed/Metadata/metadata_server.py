"""
Servidor principal del servicio de Metadata.
Coordina todos los subcomponentes y expone la API RPC.
"""
import os
import time
import uuid
import logging
import threading
from typing import Dict, Optional, List, Any

from ..Common.constants import (
    METADATA_RPC_PORT, MessageType, NodeType, NodeState,
    DistributedResponseCode, REPLICATION_FACTOR
)
from ..Common.rpc_protocol import RPCServer, RPCMessage, RPCClient
from ..Common.models import NodeInfo, FileMetadata

from .namespace import FileSystemNamespace
from .lock_manager import LockManager
from .replica_manager import ReplicaManager
from .leader_election import LeaderElection
from .heartbeat_manager import HeartbeatManager
from .auth_service import AuthService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MetadataServer:
    """
    Servidor principal de Metadata.
    Gestiona el espacio de nombres, réplicas, bloqueos, autenticación y coordinación.
    """
    
    def __init__(self, host: str = '0.0.0.0', port: int = METADATA_RPC_PORT,
                 data_dir: str = '/data/metadata'):
        self.host = host
        self.port = port
        self.data_dir = data_dir
        
        # Generar ID único para este nodo
        self.node_id = os.getenv('NODE_ID', f"metadata-{uuid.uuid4().hex[:8]}")
        
        # Información del nodo
        self.node_info = NodeInfo(
            node_id=self.node_id,
            node_type=NodeType.METADATA,
            host=os.getenv('HOSTNAME', host),
            port=port,
            state=NodeState.UP
        )
        
        # Crear directorio de datos
        os.makedirs(data_dir, exist_ok=True)
        
        # Inicializar subcomponentes
        self.namespace = FileSystemNamespace(f"{data_dir}/namespace.json")
        self.lock_manager = LockManager()
        self.replica_manager = ReplicaManager(REPLICATION_FACTOR)
        self.auth_service = AuthService(f"{data_dir}/users.json")
        
        # Heartbeat manager con callbacks
        self.heartbeat_manager = HeartbeatManager(
            on_node_down=self._on_node_down,
            on_node_up=self._on_node_up
        )
        
        # Leader election con callbacks
        self.leader_election = LeaderElection(
            self.node_id,
            self.node_info,
            on_become_leader=self._on_become_leader,
            on_leader_change=self._on_leader_change
        )
        
        # Servidor RPC
        self.rpc_server = RPCServer(host, port)
        self._register_handlers()
        
        # Estado
        self._running = False
        self._rpc_client = RPCClient()
    
    def _register_handlers(self):
        """Registra los manejadores RPC"""
        # Heartbeat y elección
        self.rpc_server.register_handler(MessageType.HEARTBEAT, self._handle_heartbeat)
        self.rpc_server.register_handler(MessageType.LEADER_ELECTION, self._handle_leader_election)
        self.rpc_server.register_handler(MessageType.LEADER_ELECTED, self._handle_leader_elected)
        self.rpc_server.register_handler(MessageType.LEADER_QUERY, self._handle_leader_query)
        
        # Registro de nodos
        self.rpc_server.register_handler(MessageType.REGISTER_NODE, self._handle_register_node)
        self.rpc_server.register_handler(MessageType.UNREGISTER_NODE, self._handle_unregister_node)
        
        # Autenticación
        self.rpc_server.register_handler(MessageType.AUTH_REQUEST, self._handle_auth_request)
        
        # Operaciones de namespace
        self.rpc_server.register_handler(MessageType.LOOKUP_FILE, self._handle_lookup)
        self.rpc_server.register_handler(MessageType.CREATE_FILE, self._handle_create_file)
        self.rpc_server.register_handler(MessageType.DELETE_FILE, self._handle_delete_file)
        self.rpc_server.register_handler(MessageType.RENAME_FILE, self._handle_rename)
        self.rpc_server.register_handler(MessageType.LIST_DIR, self._handle_list_dir)
        self.rpc_server.register_handler(MessageType.MKDIR, self._handle_mkdir)
        self.rpc_server.register_handler(MessageType.RMDIR, self._handle_rmdir)
        
        # Gestión de réplicas
        self.rpc_server.register_handler(MessageType.GET_REPLICAS, self._handle_get_replicas)
        self.rpc_server.register_handler(MessageType.UPDATE_FILE_META, self._handle_update_file_meta)
        
        # Bloqueos
        self.rpc_server.register_handler(MessageType.LOCK_REQUEST, self._handle_lock_request)
        self.rpc_server.register_handler(MessageType.UNLOCK_REQUEST, self._handle_unlock_request)
        
        # Permisos
        self.rpc_server.register_handler(MessageType.CHECK_PERMISSION, self._handle_check_permission)
        
        # Sincronización
        self.rpc_server.register_handler(MessageType.SYNC_REQUEST, self._handle_sync_request)
        self.rpc_server.register_handler(MessageType.FILE_VERSION_LIST, self._handle_version_list)
    
    def start(self):
        """Inicia el servidor de metadata"""
        self._running = True
        
        # Iniciar RPC server
        self.rpc_server.start()
        
        # Iniciar heartbeat manager
        self.heartbeat_manager.start()
        
        # Iniciar leader election
        self.leader_election.start()
        
        logger.info(f"Metadata server started: {self.node_id} on {self.host}:{self.port}")
        
        # Mantener el servidor corriendo
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
    
    def stop(self):
        """Detiene el servidor"""
        self._running = False
        self.rpc_server.stop()
        self.heartbeat_manager.stop()
        self.leader_election.stop()
        self.lock_manager.stop()
        logger.info("Metadata server stopped")
    
    # === Callbacks ===
    
    def _on_become_leader(self):
        """Callback cuando este nodo se convierte en líder"""
        logger.info(f"Node {self.node_id} is now the leader")
        # Iniciar tareas de mantenimiento
        threading.Thread(target=self._leader_maintenance_loop, daemon=True).start()
    
    def _on_leader_change(self, new_leader_id: str):
        """Callback cuando cambia el líder"""
        logger.info(f"Leader changed to: {new_leader_id}")
    
    def _on_node_down(self, node_id: str):
        """Callback cuando un nodo cae"""
        logger.warning(f"Node down: {node_id}")
        # Liberar bloqueos del nodo
        self.lock_manager.release_all_locks(node_id)
        # Actualizar estado de réplicas
        self.replica_manager.update_node_state(node_id, NodeState.DOWN)
    
    def _on_node_up(self, node_id: str):
        """Callback cuando un nodo se recupera"""
        logger.info(f"Node recovered: {node_id}")
        self.replica_manager.update_node_state(node_id, NodeState.UP)
    
    def _leader_maintenance_loop(self):
        """Loop de mantenimiento que solo corre en el líder"""
        while self._running and self.leader_election.is_leader():
            try:
                # Verificar replicación
                under_replicated = self.replica_manager.get_under_replicated_files()
                if under_replicated:
                    logger.info(f"Found {len(under_replicated)} under-replicated files")
                    # TODO: Programar re-replicación
                
                time.sleep(30)  # Cada 30 segundos
            except Exception as e:
                logger.error(f"Error in maintenance loop: {e}")
    
    # === Handlers RPC ===
    
    def _handle_heartbeat(self, msg: RPCMessage) -> RPCMessage:
        """Maneja heartbeats de otros nodos"""
        node_id = msg.payload.get('node_id')
        if node_id:
            self.heartbeat_manager.receive_heartbeat(node_id)
        
        # Si es heartbeat del líder, pasarlo a leader election
        if 'leader_id' in msg.payload:
            self.leader_election.handle_leader_heartbeat(msg)
        
        return RPCMessage(
            MessageType.HEARTBEAT_ACK,
            {'node_id': self.node_id, 'timestamp': time.time()},
            msg.request_id
        )
    
    def _handle_leader_election(self, msg: RPCMessage) -> RPCMessage:
        """Maneja mensajes de elección de líder"""
        return self.leader_election.handle_election_message(msg)
    
    def _handle_leader_elected(self, msg: RPCMessage) -> RPCMessage:
        """Maneja anuncio de nuevo líder"""
        self.leader_election.handle_leader_announcement(msg)
        return RPCMessage(
            MessageType.LEADER_RESPONSE,
            {'status': 'OK'},
            msg.request_id
        )
    
    def _handle_leader_query(self, msg: RPCMessage) -> RPCMessage:
        """Responde quién es el líder actual"""
        leader = self.leader_election.get_leader()
        return RPCMessage(
            MessageType.LEADER_RESPONSE,
            {
                'leader_id': self.leader_election.get_leader_id(),
                'leader_host': leader.host if leader else None,
                'leader_port': leader.port if leader else None
            },
            msg.request_id
        )
    
    def _handle_register_node(self, msg: RPCMessage) -> RPCMessage:
        """Registra un nuevo nodo en el clúster"""
        try:
            node_data = msg.payload.get('node')
            node = NodeInfo.from_dict(node_data)
            
            # Registrar según tipo
            if node.node_type == NodeType.STORAGE:
                self.replica_manager.register_storage_node(node)
            elif node.node_type == NodeType.METADATA:
                self.leader_election.register_peer(node)
            
            self.heartbeat_manager.register_node(node)
            
            # Si es un storage node nuevo, crear directorio home si es necesario
            
            return RPCMessage(
                MessageType.REGISTER_RESPONSE,
                {
                    'status': DistributedResponseCode.SUCCESS.value,
                    'leader_id': self.leader_election.get_leader_id()
                },
                msg.request_id
            )
        except Exception as e:
            logger.error(f"Error registering node: {e}")
            return RPCMessage(
                MessageType.REGISTER_RESPONSE,
                {'status': DistributedResponseCode.ERROR.value, 'error': str(e)},
                msg.request_id
            )
    
    def _handle_unregister_node(self, msg: RPCMessage) -> RPCMessage:
        """Elimina un nodo del clúster"""
        node_id = msg.payload.get('node_id')
        node_type = msg.payload.get('node_type')
        
        if node_type == NodeType.STORAGE.value:
            self.replica_manager.unregister_storage_node(node_id)
        elif node_type == NodeType.METADATA.value:
            self.leader_election.unregister_peer(node_id)
        
        self.heartbeat_manager.unregister_node(node_id)
        self.lock_manager.release_all_locks(node_id)
        
        return RPCMessage(
            MessageType.REGISTER_RESPONSE,
            {'status': DistributedResponseCode.SUCCESS.value},
            msg.request_id
        )
    
    def _handle_auth_request(self, msg: RPCMessage) -> RPCMessage:
        """Maneja solicitudes de autenticación"""
        username = msg.payload.get('username')
        password = msg.payload.get('password')
        
        code, user = self.auth_service.authenticate(username, password)
        
        response_payload = {'status': code.value}
        if user:
            response_payload['user'] = user.to_dict()
            response_payload['home_dir'] = user.home_dir
        
        return RPCMessage(
            MessageType.AUTH_RESPONSE,
            response_payload,
            msg.request_id
        )
    
    def _handle_lookup(self, msg: RPCMessage) -> RPCMessage:
        """Busca un archivo o directorio"""
        path = msg.payload.get('path')
        meta = self.namespace.get_file(path)
        
        if meta:
            # Obtener nodos de réplica
            replica_nodes = []
            if not meta.is_directory:
                nodes = self.replica_manager.get_replica_nodes(meta.file_id)
                replica_nodes = [{'host': n.host, 'port': n.port, 'node_id': n.node_id} for n in nodes]
            
            return RPCMessage(
                MessageType.LOOKUP_RESPONSE,
                {
                    'status': DistributedResponseCode.SUCCESS.value,
                    'metadata': meta.to_dict(),
                    'replica_nodes': replica_nodes
                },
                msg.request_id
            )
        
        return RPCMessage(
            MessageType.LOOKUP_RESPONSE,
            {'status': DistributedResponseCode.NOT_FOUND.value},
            msg.request_id
        )
    
    def _handle_create_file(self, msg: RPCMessage) -> RPCMessage:
        """Crea un nuevo archivo"""
        path = msg.payload.get('path')
        owner = msg.payload.get('owner', 'anonymous')
        size = msg.payload.get('size', 0)
        
        # Seleccionar nodos para réplicas
        selected_nodes = self.replica_manager.select_replicas_for_file(path)
        
        if len(selected_nodes) < 1:
            return RPCMessage(
                MessageType.CREATE_RESPONSE,
                {'status': DistributedResponseCode.NODE_UNAVAILABLE.value},
                msg.request_id
            )
        
        # Crear en namespace
        code, meta = self.namespace.create_file(
            path, owner, size, 
            [n.node_id for n in selected_nodes]
        )
        
        if code == DistributedResponseCode.SUCCESS and meta:
            # Asignar réplicas
            self.replica_manager.assign_replicas(
                meta.file_id, selected_nodes, size
            )
            
            return RPCMessage(
                MessageType.CREATE_RESPONSE,
                {
                    'status': code.value,
                    'metadata': meta.to_dict(),
                    'storage_nodes': [
                        {'host': n.host, 'port': n.port, 'node_id': n.node_id}
                        for n in selected_nodes
                    ]
                },
                msg.request_id
            )
        
        return RPCMessage(
            MessageType.CREATE_RESPONSE,
            {'status': code.value},
            msg.request_id
        )
    
    def _handle_delete_file(self, msg: RPCMessage) -> RPCMessage:
        """Elimina un archivo"""
        path = msg.payload.get('path')
        
        meta = self.namespace.get_file(path)
        if not meta:
            return RPCMessage(
                MessageType.DELETE_RESPONSE,
                {'status': DistributedResponseCode.NOT_FOUND.value},
                msg.request_id
            )
        
        # Obtener nodos de réplica para notificar eliminación
        replica_nodes = self.replica_manager.get_replica_nodes(meta.file_id)
        
        # Eliminar del namespace
        code = self.namespace.delete_file(path)
        
        if code == DistributedResponseCode.SUCCESS:
            # Eliminar réplicas del gestor
            self.replica_manager.remove_all_replicas(meta.file_id)
        
        return RPCMessage(
            MessageType.DELETE_RESPONSE,
            {
                'status': code.value,
                'file_id': meta.file_id,
                'storage_nodes': [
                    {'host': n.host, 'port': n.port, 'node_id': n.node_id}
                    for n in replica_nodes
                ]
            },
            msg.request_id
        )
    
    def _handle_rename(self, msg: RPCMessage) -> RPCMessage:
        """Renombra un archivo o directorio"""
        old_path = msg.payload.get('old_path')
        new_path = msg.payload.get('new_path')
        
        code = self.namespace.rename(old_path, new_path)
        
        return RPCMessage(
            MessageType.RENAME_RESPONSE,
            {'status': code.value},
            msg.request_id
        )
    
    def _handle_list_dir(self, msg: RPCMessage) -> RPCMessage:
        """Lista contenido de un directorio"""
        path = msg.payload.get('path')
        
        code, entries = self.namespace.list_directory(path)
        
        return RPCMessage(
            MessageType.LIST_RESPONSE,
            {
                'status': code.value,
                'entries': [e.to_dict() for e in entries]
            },
            msg.request_id
        )
    
    def _handle_mkdir(self, msg: RPCMessage) -> RPCMessage:
        """Crea un directorio"""
        path = msg.payload.get('path')
        owner = msg.payload.get('owner', 'anonymous')
        
        code, meta = self.namespace.create_directory(path, owner)
        
        return RPCMessage(
            MessageType.MKDIR_RESPONSE,
            {
                'status': code.value,
                'metadata': meta.to_dict() if meta else None
            },
            msg.request_id
        )
    
    def _handle_rmdir(self, msg: RPCMessage) -> RPCMessage:
        """Elimina un directorio"""
        path = msg.payload.get('path')
        recursive = msg.payload.get('recursive', True)
        
        code = self.namespace.delete_directory(path, recursive)
        
        return RPCMessage(
            MessageType.RMDIR_RESPONSE,
            {'status': code.value},
            msg.request_id
        )
    
    def _handle_get_replicas(self, msg: RPCMessage) -> RPCMessage:
        """Obtiene información de réplicas de un archivo"""
        file_id = msg.payload.get('file_id')
        path = msg.payload.get('path')
        
        if path and not file_id:
            meta = self.namespace.get_file(path)
            if meta:
                file_id = meta.file_id
        
        if not file_id:
            return RPCMessage(
                MessageType.REPLICAS_RESPONSE,
                {'status': DistributedResponseCode.NOT_FOUND.value},
                msg.request_id
            )
        
        replicas = self.replica_manager.get_replicas(file_id)
        nodes = []
        for replica in replicas:
            node = self.replica_manager.get_storage_node(replica.node_id)
            if node:
                nodes.append({
                    'node_id': node.node_id,
                    'host': node.host,
                    'port': node.port,
                    'state': node.state.value,
                    'is_primary': replica.is_primary,
                    'version': replica.version
                })
        
        return RPCMessage(
            MessageType.REPLICAS_RESPONSE,
            {
                'status': DistributedResponseCode.SUCCESS.value,
                'replicas': nodes
            },
            msg.request_id
        )
    
    def _handle_update_file_meta(self, msg: RPCMessage) -> RPCMessage:
        """Actualiza metadatos de un archivo"""
        path = msg.payload.get('path')
        size = msg.payload.get('size')
        version = msg.payload.get('version')
        replicas = msg.payload.get('replicas')
        
        if size is not None:
            code = self.namespace.update_file_size(path, size, version)
        elif replicas is not None:
            code = self.namespace.update_file_replicas(path, replicas)
        else:
            code = DistributedResponseCode.ERROR
        
        return RPCMessage(
            MessageType.UPDATE_META_RESPONSE,
            {'status': code.value},
            msg.request_id
        )
    
    def _handle_lock_request(self, msg: RPCMessage) -> RPCMessage:
        """Maneja solicitudes de bloqueo"""
        file_id = msg.payload.get('file_id')
        holder_id = msg.payload.get('holder_id')
        lock_type = msg.payload.get('lock_type', 'READ')
        
        if lock_type == 'WRITE':
            code, lock_info = self.lock_manager.acquire_write_lock(file_id, holder_id)
        else:
            code, lock_info = self.lock_manager.acquire_read_lock(file_id, holder_id)
        
        return RPCMessage(
            MessageType.LOCK_RESPONSE,
            {
                'status': code.value,
                'lock_type': lock_type if code == DistributedResponseCode.SUCCESS else None
            },
            msg.request_id
        )
    
    def _handle_unlock_request(self, msg: RPCMessage) -> RPCMessage:
        """Maneja solicitudes de desbloqueo"""
        file_id = msg.payload.get('file_id')
        holder_id = msg.payload.get('holder_id')
        
        code = self.lock_manager.release_lock(file_id, holder_id)
        
        return RPCMessage(
            MessageType.UNLOCK_RESPONSE,
            {'status': code.value},
            msg.request_id
        )
    
    def _handle_check_permission(self, msg: RPCMessage) -> RPCMessage:
        """Verifica permisos de un usuario sobre un archivo"""
        path = msg.payload.get('path')
        username = msg.payload.get('username')
        permission = msg.payload.get('permission')
        
        has_perm = self.namespace.check_permission(path, username, permission)
        
        return RPCMessage(
            MessageType.PERMISSION_RESPONSE,
            {
                'status': DistributedResponseCode.SUCCESS.value if has_perm 
                         else DistributedResponseCode.PERMISSION_DENIED.value
            },
            msg.request_id
        )
    
    def _handle_sync_request(self, msg: RPCMessage) -> RPCMessage:
        """Maneja solicitudes de sincronización de estado"""
        # Exportar todo el estado
        state = {
            'namespace': self.namespace.export_state(),
            'users': self.auth_service.export_state(),
            'replicas': self.replica_manager.export_state()
        }
        
        return RPCMessage(
            MessageType.SYNC_RESPONSE,
            {'status': DistributedResponseCode.SUCCESS.value, 'state': state},
            msg.request_id
        )
    
    def _handle_version_list(self, msg: RPCMessage) -> RPCMessage:
        """Maneja lista de versiones de un nodo de storage para sincronización"""
        node_id = msg.payload.get('node_id')
        files = msg.payload.get('files', {})  # {file_id: version}
        
        # Comparar versiones y determinar qué archivos necesitan actualización
        updates_needed = []
        for file_id, reported_version in files.items():
            replicas = self.replica_manager.get_replicas(file_id)
            for replica in replicas:
                if replica.node_id == node_id:
                    if replica.version > reported_version:
                        updates_needed.append({
                            'file_id': file_id,
                            'current_version': replica.version,
                            'reported_version': reported_version
                        })
                    break
        
        return RPCMessage(
            MessageType.FILE_VERSION_RESPONSE,
            {
                'status': DistributedResponseCode.SUCCESS.value,
                'updates_needed': updates_needed
            },
            msg.request_id
        )


def main():
    """Punto de entrada principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Metadata Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host")
    parser.add_argument("--port", type=int, default=METADATA_RPC_PORT, help="Port")
    parser.add_argument("--data-dir", default="/data/metadata", help="Data directory")
    
    args = parser.parse_args()
    
    server = MetadataServer(args.host, args.port, args.data_dir)
    server.start()


if __name__ == "__main__":
    main()

