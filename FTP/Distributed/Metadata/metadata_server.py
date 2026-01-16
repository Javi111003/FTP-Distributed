"""
Servidor principal del servicio de Metadata.
Coordina todos los subcomponentes y expone la API RPC.
"""
import json
import os
import socket
import time
import uuid
import logging
import threading
from typing import Dict, Optional, List, Any, Tuple

from ..Common.constants import (
    METADATA_RPC_PORT, MessageType, NodeType, NodeState,
    DistributedResponseCode, REPLICATION_FACTOR, HEARTBEAT_INTERVAL
)
from ..Common.rpc_protocol import RPCServer, RPCMessage, RPCClient
from ..Common.models import NodeInfo, ReplicaInfo, LockInfo, FileMetadata

from .namespace import FileSystemNamespace
from .lock_manager import LockManager
from .replica_manager import ReplicaManager
from .leader_election import LeaderElection
from .heartbeat_manager import HeartbeatManager
from .auth_service import AuthService
from .split_brain_reconciliation import SplitBrainReconciliation

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
        self.log_path = f"{data_dir}/oplog.jsonl"
        self.snapshot_path = f"{data_dir}/snapshot.json"
        
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
            on_leader_change=self._on_leader_change,
            heartbeat_manager=self.heartbeat_manager,
            get_data_state=self._get_data_state  # Función para obtener estado de datos
        )
        
        # Split-brain reconciliation
        self.split_brain_reconciliation = SplitBrainReconciliation(self)
        
        # Servidor RPC
        self.rpc_server = RPCServer(host, port)
        self._register_handlers()
        
        # Estado
        self._running = False
        self._rpc_client = RPCClient()
        self._registered_peers = set()  # Rastrear peers con los que ya nos registramos
        self._registration_lock = threading.Lock()
        self._log_lock = threading.RLock()
        self._oplog: List[Dict[str, Any]] = []
        self._commit_index = -1
        self._last_applied = -1
        self._applied_ops = set()
        self._current_term = 0
        # Lock global para proteger instalación de snapshots y evitar race conditions
        self._global_state_lock = threading.RLock()

        # Monitor de consistencia automática
        self._consistency_thread: Optional[threading.Thread] = None
        self._last_consistency_check = 0
        self._consistency_check_interval = 15  # segundos

    def _rebuild_replica_manager_from_namespace(self):
        """Reconstruye el estado del replica_manager desde el namespace cargado"""
        try:
            logger.info("Rebuilding replica manager state from namespace...")

            # Primero, marcar todos los nodos storage conocidos como potencialmente UP
            # Esto es necesario porque al cargar desde disco, no tenemos heartbeat info
            storage_nodes_registered = 0

            # Iterar sobre todos los archivos en el namespace para encontrar nodos storage
            referenced_nodes = set()
            for path, meta in self.namespace._namespace.items():
                if not meta.is_directory and hasattr(meta, 'replicas') and meta.replicas:
                    referenced_nodes.update(meta.replicas)

            # Para cada nodo referenciado, intentar registrarlo si no existe
            for node_id in referenced_nodes:
                if node_id not in self.replica_manager._storage_nodes:
                    # Intentar inferir la info del nodo desde el nombre
                    if 'storage-' in node_id:
                        try:
                            slot = int(node_id.split('-')[1])
                            # Crear entrada del nodo con estado UP (asumir que están disponibles)
                            node_info = NodeInfo(
                                node_id=node_id,
                                node_type=NodeType.STORAGE,
                                host=f"storage{slot}",
                                port=5001,
                                state=NodeState.UP  # Asumir UP por defecto al cargar desde disco
                            )
                            self.replica_manager.register_storage_node(node_info)
                            storage_nodes_registered += 1
                            logger.info(f"Reconstructed storage node {node_id} from namespace (assuming UP)")
                        except Exception as e:
                            logger.warning(f"Could not reconstruct node {node_id}: {e}")

            # Ahora reconstruir las réplicas para cada archivo
            replicas_registered = 0
            for path, meta in self.namespace._namespace.items():
                if not meta.is_directory and hasattr(meta, 'replicas') and meta.replicas:
                    try:
                        # Reconstruir estado de réplicas para este archivo
                        replicas_state = []
                        for replica_node_id in meta.replicas:
                            # Verificar que el nodo existe
                            if replica_node_id in self.replica_manager._storage_nodes:
                                replica_info = {
                                    'file_id': meta.file_id,
                                    'node_id': replica_node_id,
                                    'version': meta.version if hasattr(meta, 'version') else 1,
                                    'size': meta.size if hasattr(meta, 'size') else 0,
                                    'is_primary': len(replicas_state) == 0,  # Primera réplica es primaria
                                    'last_sync': time.time()
                                }
                                replicas_state.append(replica_info)

                        if replicas_state:
                            # Aplicar el estado reconstruido
                            self.replica_manager.apply_replicas_state(meta.file_id, replicas_state)
                            replicas_registered += len(replicas_state)
                            logger.debug(f"Reconstructed {len(replicas_state)} replicas for {meta.file_id}")

                    except Exception as e:
                        logger.warning(f"Could not reconstruct replicas for {meta.file_id}: {e}")

            logger.info(f"Replica manager rebuild complete: {storage_nodes_registered} nodes, {replicas_registered} replicas")

            # Verificar y replicar archivos huérfanos (réplicas en nodos inexistentes)
            self._check_and_rebalance_orphaned_files()

        except Exception as e:
            logger.error(f"Error rebuilding replica manager from namespace: {e}")

    def _check_and_rebalance_orphaned_files(self):
        """Verifica archivos con réplicas en nodos inexistentes y las rebalancea"""
        logger.info("🔍 Checking for orphaned replicas (files with replicas on non-existent nodes)...")

        orphaned_files = []
        current_storage_nodes = set(self.replica_manager._storage_nodes.keys())

        # Encontrar archivos que tienen réplicas en nodos que ya no existen
        for file_id, replicas in self.replica_manager._replicas.items():
            orphaned_replicas = []
            active_replicas = []

            for replica in replicas:
                if replica.node_id not in current_storage_nodes:
                    orphaned_replicas.append(replica)
                elif replica.node_id in self.replica_manager._storage_nodes and \
                     self.replica_manager._storage_nodes[replica.node_id].state == NodeState.UP:
                    active_replicas.append(replica)

            # Si tiene réplicas huérfanas pero no tiene suficientes réplicas activas
            if orphaned_replicas and len(active_replicas) < self.replica_manager.replication_factor:
                orphaned_files.append((file_id, orphaned_replicas, active_replicas))

        if orphaned_files:
            logger.warning(f"🚨 Found {len(orphaned_files)} files with orphaned replicas that need rebalancing")

            for file_id, orphaned_replicas, active_replicas in orphaned_files:
                logger.info(f"🔄 Rebalancing file {file_id}: {len(active_replicas)} active, {len(orphaned_replicas)} orphaned")

                # Intentar replicar desde réplicas activas a nodos disponibles
                available_nodes = [node for node in self.replica_manager._storage_nodes.values()
                                 if node.state == NodeState.UP and
                                 not any(r.node_id == node.node_id for r in active_replicas)]

                if active_replicas and available_nodes:
                    # Tomar la primera réplica activa como fuente
                    source_replica = active_replicas[0]
                    target_node = available_nodes[0]  # Usar el primer nodo disponible

                    logger.info(f"📋 Replicating {file_id} from {source_replica.node_id} to {target_node.node_id}")

                    # Ejecutar la replicación
                    try:
                        # Obtener el archivo desde la réplica activa
                        from ..Router.storage_client import StorageClient
                        storage_client = StorageClient()

                        data = storage_client.retrieve_file(
                            self.replica_manager._storage_nodes[source_replica.node_id].host,
                            self.replica_manager._storage_nodes[source_replica.node_id].port,
                            file_id
                        )

                        if data:
                            # Almacenar en el nuevo nodo
                            success, checksum = storage_client.store_file(
                                target_node.host,
                                target_node.port,
                                file_id,
                                data,
                                source_replica.version
                            )

                            if success:
                                # Actualizar el replica manager
                                new_replica = ReplicaInfo(
                                    file_id=file_id,
                                    node_id=target_node.node_id,
                                    version=source_replica.version
                                )
                                self.replica_manager._replicas[file_id].append(new_replica)
                                self.replica_manager._node_files[target_node.node_id].add(file_id)

                                logger.info(f"✅ Successfully rebalanced {file_id} to {target_node.node_id}")
                            else:
                                logger.error(f"❌ Failed to store {file_id} on {target_node.node_id}")
                        else:
                            logger.error(f"❌ Could not retrieve {file_id} from {source_replica.node_id}")
                    except Exception as e:
                        logger.error(f"❌ Error during rebalance of {file_id}: {e}")
                else:
                    logger.warning(f"⚠️ Cannot rebalance {file_id}: no active replicas or no available nodes")
        else:
            logger.info("✅ No orphaned replicas found - all files properly replicated")

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
        self.rpc_server.register_handler(MessageType.GET_PEERS, self._handle_get_peers)
        
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
        
        # Replicación interna de metadata
        self.rpc_server.register_handler(MessageType.REPL_APPEND, self._handle_repl_append)
        self.rpc_server.register_handler(MessageType.REPL_SNAPSHOT, self._handle_repl_snapshot)
        self.rpc_server.register_handler(MessageType.REPL_REDIRECT, self._handle_repl_redirect)

        # Consulta de estado actual (para reconciliación de split-brain)
        self.rpc_server.register_handler(MessageType.GET_CURRENT_NAMESPACE, self._handle_get_current_namespace)
    
    def start(self):
        """Inicia el servidor de metadata"""
        self._running = True
        
        # Cargar estado persistido (snapshot + log)
        self._load_persistent_state()

        # Reconstruir estado del replica_manager desde namespace cargado
        self._rebuild_replica_manager_from_namespace()
        
        # Nota: La limpieza de réplicas huérfanas se ejecutará después
        # de que los storages se registren (cuando somos líder)

        # Iniciar RPC server
        self.rpc_server.start()
        
        # Iniciar heartbeat manager
        self.heartbeat_manager.start()
        
        # Esperar un momento para que el servidor RPC esté listo
        time.sleep(0.5)
        
        # Descubrir y registrarse con otros nodos metadata
        self._discover_and_register_with_peers()
        
        # Iniciar leader election
        self.leader_election.start()
        
        # Iniciar envío de heartbeats a peers metadata
        self._heartbeat_thread = threading.Thread(
            target=self._metadata_heartbeat_loop,
            daemon=True
        )
        self._heartbeat_thread.start()

        # Nota: El monitor de consistencia se inicia DESPUÉS de la reconciliación de split-brain

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

    def start_consistency_monitor_after_reconciliation(self):
        """Inicia el monitor de consistencia DESPUÉS de la reconciliación de split-brain"""
        def consistency_worker():
            try:
                # Esperar 10 segundos después de la reconciliación para estabilidad
                logger.info("⏳ Waiting 10 seconds after split-brain reconciliation...")
                time.sleep(10)

                # Ejecutar verificación de consistencia 3 veces
                for check_round in range(1, 4):
                    logger.info(f"🔍 POST-RECONCILIATION CONSISTENCY CHECK #{check_round}/3")

                    self._check_cluster_consistency()

                    if check_round < 3:
                        time.sleep(self._consistency_check_interval)

                logger.info("✅ Post-reconciliation consistency monitoring completed")

            except Exception as e:
                logger.error(f"Post-reconciliation consistency monitor error: {e}")

        self._consistency_thread = threading.Thread(target=consistency_worker, daemon=True)
        self._consistency_thread.start()

    def _check_cluster_consistency(self):
        """Verifica que todos los followers tengan la misma info que el líder"""
        if not self.leader_election.is_leader():
            return  # Solo el líder hace las verificaciones

        # Obtener peers activos
        active_peers = []
        with self.leader_election._lock:
            active_peers = list(self.leader_election._peers.values())

        if not active_peers:
            logger.debug("No active peers to check consistency")
            return

        logger.info(f"🔍 Checking consistency with {len(active_peers)} peers...")

        # Verificar cada peer
        inconsistent_peers = []
        for peer in active_peers:
            if not self._verify_peer_consistency(peer):
                inconsistent_peers.append(peer)

        # Sincronizar peers inconsistentes
        if inconsistent_peers:
            logger.warning(f"🚨 Found {len(inconsistent_peers)} inconsistent peers, synchronizing...")
            for peer in inconsistent_peers:
                self._sync_peer_with_leader(peer)
        else:
            logger.info("✅ All peers are consistent with leader")

    def _verify_peer_consistency(self, peer: NodeInfo) -> bool:
        """Verifica si un peer tiene namespace consistente con el líder"""
        try:
            # Consultar namespace del peer
            msg = RPCMessage(MessageType.GET_CURRENT_NAMESPACE, {})
            response = self._rpc_client.call(peer.host, peer.port, msg)

            if not response or not response.payload.get('namespace'):
                logger.warning(f"❌ No response from {peer.node_id}")
                return False

            peer_namespace = response.payload['namespace']
            leader_namespace = self.namespace._namespace

            # Verificación rápida: contar archivos
            peer_count = len(peer_namespace)
            leader_count = len(leader_namespace)

            if peer_count != leader_count:
                logger.warning(f"❌ {peer.node_id}: {peer_count} files vs leader {leader_count} files")
                return False

            # Verificación más profunda: comparar algunos archivos clave
            sample_files = list(leader_namespace.keys())[:3]  # Primeros 3 archivos

            for path in sample_files:
                if path not in peer_namespace:
                    logger.warning(f"❌ {peer.node_id}: missing file {path}")
                    return False

                leader_meta = leader_namespace[path]
                peer_meta = peer_namespace[path]

                if leader_meta.file_id != peer_meta.get('file_id'):
                    logger.warning(f"❌ {peer.node_id}: file {path} has different version")
                    return False

            logger.debug(f"✅ {peer.node_id}: consistent ({peer_count} files)")
            return True

        except Exception as e:
            logger.warning(f"❌ Error checking {peer.node_id}: {e}")
            return False

    def _sync_peer_with_leader(self, peer: NodeInfo):
        """Sincroniza un peer con el estado actual del líder"""
        try:
            logger.info(f"🔄 Synchronizing {peer.node_id} with leader...")

            # Crear snapshot completo del líder
            snapshot = self._create_snapshot()

            # Enviar snapshot con lock global para evitar race conditions
            msg = RPCMessage(
                MessageType.REPL_SNAPSHOT,
                {
                    'snapshot': snapshot,
                    'from_leader': True,
                    'force_install': True,
                    'consistency_sync': True  # Flag especial para distinguir de reconciliación
                }
            )

            with self._global_state_lock:  # 🔒 Proteger contra operaciones concurrentes
                response = self._rpc_client.call(peer.host, peer.port, msg)

            if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
                logger.info(f"✅ Successfully synchronized {peer.node_id}")
            else:
                logger.error(f"❌ Failed to synchronize {peer.node_id}")

        except Exception as e:
            logger.error(f"❌ Error synchronizing {peer.node_id}: {e}")
        
    def _metadata_heartbeat_loop(self):
        """Envía heartbeats periódicos a otros nodos metadata"""
        while self._running:
            try:
                with self.leader_election._lock:
                    peers_copy = dict(self.leader_election._peers)

                for peer in peers_copy.values():
                    # Verificar estado del peer antes de enviar heartbeat
                    peer_state = self.heartbeat_manager.get_node_state(peer.node_id)

                    if peer_state == NodeState.DOWN:
                        # *** NUEVO: Intentar reconectar con peers DOWN cada 10 segundos ***
                        if time.time() % 10 < HEARTBEAT_INTERVAL:  # Cada ~10 segundos
                            self._send_reconnection_heartbeat(peer)
                        continue

                    # Heartbeats normales para nodos UP/SUSPECT
                    try:
                        msg = RPCMessage(
                            MessageType.HEARTBEAT,
                            {'node_id': self.node_id, 'timestamp': time.time()}
                        )
                        self._rpc_client.call(peer.host, peer.port, msg)
                    except Exception as e:
                        # Silenciar errores de DNS que son esperados cuando un nodo está DOWN
                        if "Temporary failure in name resolution" not in str(e):
                            logger.debug(f"Heartbeat to {peer.node_id} failed: {e}")

                time.sleep(HEARTBEAT_INTERVAL)
            except Exception as e:
                logger.error(f"Error in metadata heartbeat loop: {e}")

    def _send_reconnection_heartbeat(self, peer: NodeInfo):
        """Envía heartbeat de reconexión a un peer marcado como DOWN"""
        try:
            msg = RPCMessage(
                MessageType.HEARTBEAT,
                {
                    'node_id': self.node_id,
                    'timestamp': time.time(),
                    'reconnection_attempt': True  # Indica que es intento de reconexión
                }
            )
            response = self._rpc_client.call(peer.host, peer.port, msg)

            if response:
                logger.warning(f"🔗🔗🔗 RECONNECTED with DOWN peer {peer.node_id}! 🔗🔗🔗")
                # Marcar como UP inmediatamente (esto activará _on_node_up)
                self.heartbeat_manager.receive_heartbeat(peer.node_id)
                # Activar verificación de split-brain
                threading.Thread(
                    target=self._check_reconnected_peer_for_split_brain,
                    args=(peer,),
                    daemon=True
                ).start()

        except Exception as e:
            # Silenciar errores de reconexión fallida (muy común)
            pass

    def _check_reconnected_peer_for_split_brain(self, peer: NodeInfo):
        """Verifica split-brain con un peer que acaba de reconectarse"""
        try:
            logger.info(f"🔍 Checking split-brain with reconnected peer {peer.node_id}...")

            # Consultar estado de líder del peer
            msg = RPCMessage(MessageType.LEADER_QUERY, {})
            response = self._rpc_client.call(peer.host, peer.port, msg)

            if response:
                peer_leader_id = response.payload.get('leader_id')
                peer_term = response.payload.get('term', 0)

                logger.info(
                    f"🔍 Peer {peer.node_id} reports: leader={peer_leader_id}, term={peer_term} | "
                    f"We have: leader={self.leader_election.get_leader_id()}, term={self.leader_election.get_term()}"
                )

                # Verificar split-brain
                if self.split_brain_reconciliation.detect_split_brain(peer, peer_term, peer_leader_id):
                    logger.warning(f"⚠️⚠️⚠️ SPLIT-BRAIN DETECTED after reconnection! ⚠️⚠️⚠️")
                    # Obtener todos los peers para reconciliación
                    with self.leader_election._lock:
                        all_peers = list(self.leader_election._peers.values())
                    self.split_brain_reconciliation.initiate_reconciliation(
                        all_peers,
                        on_complete_callback=self.start_consistency_monitor_after_reconciliation
                    )
                else:
                    logger.info(f"✅ No split-brain with reconnected peer {peer.node_id}")

        except Exception as e:
            logger.warning(f"Could not check split-brain with reconnected peer {peer.node_id}: {e}")

    # === Callbacks ===
    
    def _on_become_leader(self):
        """Callback cuando este nodo se convierte en líder"""
        logger.info(f"🟢 Node {self.node_id} is now the leader")
        
        # Sincronizar estado desde peers antes de hacer cualquier otra cosa
        threading.Thread(target=self._sync_state_from_peers, daemon=True).start()
        
        # Ejecutar limpieza de réplicas huérfanas (después de sincronización)
        threading.Thread(target=self._initial_leader_cleanup, daemon=True).start()
        
        # Iniciar tareas de mantenimiento
        threading.Thread(target=self._leader_maintenance_loop, daemon=True).start()
        
        # Iniciar replicación periódica de storage_nodes a followers
        threading.Thread(target=self._storage_sync_loop, daemon=True).start()
    
    def _initial_leader_cleanup(self):
        """Limpieza inicial cuando nos convertimos en líder"""
        try:
            # Esperar un momento para que los storages se registren
            logger.info("⏳ Waiting for storage nodes to register before cleanup...")
            time.sleep(10)
            
            # Limpiar réplicas huérfanas
            self._cleanup_orphaned_replicas()
            
            # Verificar y rebalancear archivos subreplicados
            self._check_and_rebalance_orphaned_files()
            
            logger.info("✅ Initial leader cleanup completed")
        except Exception as e:
            logger.error(f"Error in initial leader cleanup: {e}")
    
    def _on_leader_change(self, new_leader_id: str):
        """Callback cuando cambia el líder"""
        logger.info(f"🔄 Leader changed to: {new_leader_id}")
        
        # Si NO soy el nuevo líder, solicitar sincronización del líder
        if new_leader_id != self.node_id:
            threading.Thread(target=self._request_sync_from_leader, daemon=True).start()
    
    def _sync_state_from_peers(self):
        """
        Sincroniza el estado desde los peers cuando nos convertimos en líder.
        Esto asegura que tengamos toda la información de storage nodes, réplicas Y namespace.
        """
        logger.info("🔄 Syncing state from peers as new leader...")
        time.sleep(2)  # Pequeño delay para estabilización
        
        try:
            with self.leader_election._lock:
                peers = list(self.leader_election._peers.values())
            
            merged_storage_nodes = {}
            merged_replicas = {}
            merged_namespace = {}
            # Obtener paths de archivos existentes directamente
            my_namespace_paths = set(self.namespace._namespace.keys())
            
            for peer in peers:
                try:
                    # Solicitar snapshot del peer
                    msg = RPCMessage(
                        MessageType.REPL_SNAPSHOT,
                        {'request_type': 'full_snapshot', 'requester_id': self.node_id}
                    )
                    response = self._rpc_client.call(peer.host, peer.port, msg)
                    
                    if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
                        snapshot = response.payload.get('snapshot', {})
                        
                        # Merge storage nodes
                        replica_state = snapshot.get('replicas', {})
                        peer_storage_nodes = replica_state.get('storage_nodes', {})
                        for node_id, node_dict in peer_storage_nodes.items():
                            if node_id not in merged_storage_nodes:
                                merged_storage_nodes[node_id] = node_dict
                                logger.info(f"📦 Discovered storage node {node_id} from peer {peer.node_id}")
                        
                        # Merge replicas
                        peer_replicas = replica_state.get('replicas', {})
                        for file_id, replicas in peer_replicas.items():
                            if file_id not in merged_replicas:
                                merged_replicas[file_id] = replicas
                            else:
                                # Merge replica lists
                                existing_nodes = {r.get('node_id') for r in merged_replicas[file_id]}
                                for r in replicas:
                                    if r.get('node_id') not in existing_nodes:
                                        merged_replicas[file_id].append(r)
                        
                        # *** MERGE NAMESPACE - CRÍTICO ***
                        # El namespace exportado tiene estructura: {'namespace': {...files...}, 'id_index': {...}}
                        namespace_container = snapshot.get('namespace', {})
                        peer_namespace = namespace_container.get('namespace', {}) if isinstance(namespace_container, dict) and 'namespace' in namespace_container else namespace_container
                        
                        for path, file_data in peer_namespace.items():
                            if path not in my_namespace_paths and path not in merged_namespace:
                                # Archivo nuevo que no tenemos
                                merged_namespace[path] = file_data
                                logger.info(f"📄 Discovered file {path} from peer {peer.node_id}")
                            elif path in my_namespace_paths:
                                # Comparar versiones - quedarse con la más reciente
                                my_meta = self.namespace._namespace.get(path)
                                my_version = my_meta.version if my_meta else 0
                                peer_version = file_data.get('version', 0)
                                my_modified = my_meta.modified_at if my_meta else 0
                                peer_modified = file_data.get('modified_at', 0)
                                
                                if peer_version > my_version or (peer_version == my_version and peer_modified > my_modified):
                                    merged_namespace[path] = file_data
                                    logger.info(f"📄 Updating file {path} (peer has newer version)")
                        
                        logger.info(f"✅ Got state from peer {peer.node_id}: {len(peer_storage_nodes)} storage nodes, {len(peer_namespace)} files")
                        
                except Exception as e:
                    logger.warning(f"Failed to sync from peer {peer.node_id}: {e}")
            
            # Aplicar storage nodes descubiertos
            for node_id, node_dict in merged_storage_nodes.items():
                if node_id not in self.replica_manager._storage_nodes:
                    try:
                        node = NodeInfo.from_dict(node_dict)
                        node.state = NodeState.UP
                        self.replica_manager.register_storage_node(node)
                        self.heartbeat_manager.register_node(node)
                        logger.info(f"➕ Added storage node {node_id} from peer sync")
                    except Exception as e:
                        logger.warning(f"Failed to add storage node {node_id}: {e}")
            
            # Aplicar replicas descubiertas
            for file_id, replicas in merged_replicas.items():
                if file_id not in self.replica_manager._replicas:
                    try:
                        self.replica_manager.apply_replicas_state(file_id, replicas)
                        logger.debug(f"Added replicas for {file_id}")
                    except Exception as e:
                        logger.warning(f"Failed to add replicas for {file_id}: {e}")
            
            # *** APLICAR NAMESPACE MERGED ***
            files_added = 0
            for path, file_data in merged_namespace.items():
                try:
                    # Importar archivo directamente al namespace
                    self.namespace._namespace[path] = FileMetadata.from_dict(file_data)
                    files_added += 1
                    logger.info(f"➕ Added/Updated file {path} from peer sync")
                except Exception as e:
                    logger.warning(f"Failed to add file {path}: {e}")
            
            # Guardar namespace actualizado
            if files_added > 0:
                self.namespace._persist_to_disk()
                logger.info(f"💾 Saved namespace with {files_added} new/updated files")
            
            logger.info(f"✅ State sync completed: {len(merged_storage_nodes)} storage nodes, {files_added} files merged")
            
        except Exception as e:
            logger.error(f"Error syncing state from peers: {e}")
    
    def _request_sync_from_leader(self):
        """Solicita sincronización del líder cuando cambia el liderazgo"""
        time.sleep(3)  # Esperar a que el líder se estabilice
        
        leader = self.leader_election.get_leader()
        if not leader or leader.node_id == self.node_id:
            return
        
        try:
            logger.info(f"📥 Requesting full sync from leader {leader.node_id}")
            
            msg = RPCMessage(
                MessageType.REPL_SNAPSHOT,
                {'request_type': 'full_snapshot', 'requester_id': self.node_id}
            )
            response = self._rpc_client.call(leader.host, leader.port, msg)
            
            if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
                snapshot = response.payload.get('snapshot')
                if snapshot:
                    self._install_snapshot(snapshot)
                    logger.info("✅ Successfully synced from leader")
            else:
                logger.warning("Failed to get snapshot from leader")
                
        except Exception as e:
            logger.error(f"Error requesting sync from leader: {e}")
    
    def _storage_sync_loop(self):
        """Loop que sincroniza storage nodes del líder a los followers periódicamente"""
        while self._running and self.leader_election.is_leader():
            try:
                time.sleep(15)  # Cada 15 segundos
                
                if not self.leader_election.is_leader():
                    break
                
                # Obtener lista de storage nodes y réplicas
                storage_state = self.replica_manager.export_state()
                
                with self.leader_election._lock:
                    peers = list(self.leader_election._peers.values())
                
                for peer in peers:
                    try:
                        # Enviar estado de storage nodes a cada follower
                        msg = RPCMessage(
                            MessageType.SYNC_REQUEST,
                            {
                                'sync_type': 'storage_nodes',
                                'storage_nodes': storage_state.get('storage_nodes', {}),
                                'from_leader': True
                            }
                        )
                        self._rpc_client.call(peer.host, peer.port, msg)
                    except Exception as e:
                        logger.debug(f"Failed to sync storage nodes to {peer.node_id}: {e}")
                
            except Exception as e:
                logger.error(f"Error in storage sync loop: {e}")
    
    def _redirect_to_leader(self, response_type: MessageType, request_id: str) -> RPCMessage:
        """Redirige al cliente al líder actual"""
        leader = self.leader_election.get_leader()
        leader_id = self.leader_election.get_leader_id()
        leader_host = leader.host if leader else None
        leader_port = leader.port if leader else None
        return RPCMessage(
            response_type,
            {
                'status': DistributedResponseCode.NOT_LEADER.value,
                'leader_id': leader_id,
                'leader_host': leader_host,
                'leader_port': leader_port
            },
            request_id
        )
    
    def get_leader_contact(self) -> Tuple[Optional[str], Optional[int]]:
        """Expone host/puerto del líder actual para otras capas"""
        leader = self.leader_election.get_leader()
        return (leader.host, leader.port) if leader else (None, None)
    
    def _on_node_down(self, node_id: str):
        """Callback cuando un nodo cae"""
        logger.warning(f"Node down: {node_id}")
        # Liberar bloqueos del nodo
        self.lock_manager.release_all_locks(node_id)
        # Actualizar estado de réplicas
        self.replica_manager.update_node_state(node_id, NodeState.DOWN)

        # Si es un storage node, ejecutar rebalanceo automático
        if node_id.startswith('storage-'):
            logger.info(f"Storage node {node_id} failed, starting automatic rebalance...")
            rebalance_ops = self.replica_manager.rebalance_after_storage_failure(node_id)
            if rebalance_ops:
                logger.info(f"Executing {len(rebalance_ops)} rebalance operations for failed storage {node_id}")
                self._execute_rebalance_operations(rebalance_ops)
            else:
                logger.info(f"No rebalance operations needed for failed storage {node_id}")

    def _execute_rebalance_operations(self, rebalance_ops: List[Tuple[str, str]]):
        """
        Ejecuta las operaciones de rebalanceo copiando archivos entre storages.
        Incluye verificación completa de integridad y rollback en caso de fallo.
        rebalance_ops: Lista de (file_id, target_node_id)
        """
        successful_ops = []
        failed_ops = []

        for file_id, target_node_id in rebalance_ops:
            try:
                # Obtener información de la réplica objetivo
                target_node = self.replica_manager.get_storage_node(target_node_id)
                if not target_node:
                    logger.error(f"Target node {target_node_id} not found for rebalance of {file_id}")
                    failed_ops.append((file_id, target_node_id, "Target node not found"))
                    continue

                # Intentar recuperar el archivo de cualquier réplica activa disponible
                replicas = self.replica_manager.get_replicas(file_id)
                active_replicas = [
                    r for r in replicas
                    if r.node_id in self.replica_manager._storage_nodes
                    and self.replica_manager._storage_nodes[r.node_id].state == NodeState.UP
                    and r.node_id != target_node_id  # No copiar de sí mismo
                ]

                if not active_replicas:
                    logger.error(f"No active replicas available for rebalance of {file_id}")
                    failed_ops.append((file_id, target_node_id, "No active replicas"))
                    continue

                # Intentar copiar desde la primera réplica activa
                source_replica = active_replicas[0]
                source_node = self.replica_manager.get_storage_node(source_replica.node_id)

                if not source_node:
                    logger.error(f"Source node {source_replica.node_id} not found for rebalance of {file_id}")
                    failed_ops.append((file_id, target_node_id, "Source node not found"))
                    continue

                # Recuperar el archivo del source con verificación
                data = self._retrieve_file_with_verification(file_id, source_node)
                if data is None:
                    failed_ops.append((file_id, target_node_id, "Failed to retrieve from source"))
                    continue

                # Verificar checksum si está disponible
                expected_checksum = self._get_expected_checksum(file_id, source_replica)

                # Almacenar en el target con verificación
                success, actual_checksum = self._store_file_with_verification(
                    file_id, data, source_replica.version, target_node, expected_checksum
                )

                if success:
                    # Verificar que el archivo se puede leer correctamente del target
                    verification_data = self._retrieve_file_with_verification(file_id, target_node)
                    if verification_data is None or verification_data != data:
                        logger.error(f"Verification failed for {file_id} on {target_node_id}")
                        # Intentar limpiar el archivo corrupto
                        self._cleanup_failed_rebalance(file_id, target_node)
                        failed_ops.append((file_id, target_node_id, "Verification failed"))
                        continue

                    # Actualizar el estado de réplicas en memoria solo después de verificación completa
                    self.replica_manager.apply_replicas_state(file_id, [
                        {'node_id': r.node_id, 'version': r.version, 'size': r.size}
                        for r in active_replicas
                    ] + [{'node_id': target_node_id, 'version': source_replica.version, 'size': len(data)}])

                    successful_ops.append((file_id, target_node_id))
                    logger.info(f"Successfully rebalanced and verified {file_id} to {target_node_id}")
                else:
                    failed_ops.append((file_id, target_node_id, "Failed to store"))

            except Exception as e:
                logger.error(f"Error during rebalance of {file_id} to {target_node_id}: {e}")
                failed_ops.append((file_id, target_node_id, str(e)))

        # Reporte final
        if successful_ops:
            logger.info(f"Rebalance completed: {len(successful_ops)} successful, {len(failed_ops)} failed")
        if failed_ops:
            logger.warning(f"Failed rebalance operations: {failed_ops}")

        return successful_ops, failed_ops

    def _retrieve_file_with_verification(self, file_id: str, node: NodeInfo) -> Optional[bytes]:
        """Recupera un archivo con verificación de integridad"""
        from ..Common.rpc_protocol import RPCClient, RPCMessage
        from ..Common.constants import MessageType

        try:
            rpc_client = RPCClient()
            retrieve_msg = RPCMessage(MessageType.RETRIEVE_FILE, {'file_id': file_id})
            response = rpc_client.call(node.host, node.port, retrieve_msg)

            if not response or response.payload.get('status') != 0:
                logger.error(f"Failed to retrieve {file_id} from {node.node_id}")
                return None

            # Obtener los datos (mantener como bytes, no convertir a hex)
            data_hex = response.payload.get('data')
            if not data_hex:
                logger.error(f"No data received for {file_id} from {node.node_id}")
                return None

            # Verificar que el hex sea válido
            try:
                data = bytes.fromhex(data_hex)
                return data
            except ValueError as e:
                logger.error(f"Invalid hex data for {file_id} from {node.node_id}: {e}")
                return None

        except Exception as e:
            logger.error(f"Error retrieving file {file_id} from {node.node_id}: {e}")
            return None

    def _store_file_with_verification(self, file_id: str, data: bytes, version: int,
                                    target_node: NodeInfo, expected_checksum: Optional[str] = None) -> Tuple[bool, Optional[str]]:
        """Almacena un archivo con verificación de integridad"""
        from ..Common.rpc_protocol import RPCClient, RPCMessage
        from ..Common.constants import MessageType

        try:
            rpc_client = RPCClient()

            # Calcular checksum local antes de enviar
            import hashlib
            actual_checksum = hashlib.md5(data).hexdigest()

            # Verificar checksum si se proporcionó
            if expected_checksum and actual_checksum != expected_checksum:
                logger.error(f"Checksum mismatch for {file_id}: expected {expected_checksum}, got {actual_checksum}")
                return False, None

            # Almacenar el archivo (mantener como bytes, no convertir a hex innecesariamente)
            store_msg = RPCMessage(
                MessageType.STORE_FILE,
                {
                    'file_id': file_id,
                    'data': data.hex(),  # Solo convertir a hex para transporte RPC
                    'version': version,
                    'checksum': actual_checksum,  # Incluir checksum para verificación
                    'replicate_to': []  # No replicar más
                }
            )
            store_response = rpc_client.call(target_node.host, target_node.port, store_msg)

            if store_response and store_response.payload.get('status') == 0:
                return True, actual_checksum
            else:
                logger.error(f"Failed to store {file_id} on {target_node.node_id}")
                return False, None

        except Exception as e:
            logger.error(f"Error storing file {file_id} on {target_node.node_id}: {e}")
            return False, None

    def _get_expected_checksum(self, file_id: str, replica) -> Optional[str]:
        """Obtiene el checksum esperado para un archivo desde los metadatos"""
        try:
            # Buscar en el namespace si hay información de checksum
            # Por ahora retornamos None, pero esto se puede extender
            return getattr(replica, 'checksum', None)
        except:
            return None

    def _cleanup_failed_rebalance(self, file_id: str, target_node: NodeInfo):
        """Limpia archivos parcialmente transferidos en caso de fallo"""
        try:
            from ..Common.rpc_protocol import RPCClient, RPCMessage
            from ..Common.constants import MessageType

            rpc_client = RPCClient()
            delete_msg = RPCMessage(MessageType.DELETE_LOCAL, {'file_id': file_id})
            rpc_client.call(target_node.host, target_node.port, delete_msg)
            logger.info(f"Cleaned up failed rebalance for {file_id} on {target_node.node_id}")
        except Exception as e:
            logger.warning(f"Failed to cleanup {file_id} on {target_node.node_id}: {e}")

    def _on_node_up(self, node_id: str):
        """Callback cuando un nodo se recupera"""
        logger.info(f"🟢 Node recovered: {node_id}")
        self.replica_manager.update_node_state(node_id, NodeState.UP)

        # *** NUEVO: Verificar split-brain para nodos metadata recuperados ***
        if node_id.startswith('metadata-'):
            logger.warning(f"🔗 METADATA NODE RECOVERED: {node_id} - Activating split-brain check...")
            peer_node = self.leader_election._peers.get(node_id)
            if peer_node:
                threading.Thread(
                    target=self._check_reconnected_peer_for_split_brain,
                    args=(peer_node,),
                    daemon=True
                ).start()

        # Si es un storage y somos líder, verificar si necesita sincronización
        if node_id.startswith('storage-') and self.leader_election.is_leader():
            node = self.replica_manager.get_storage_node(node_id)
            if node:
                threading.Thread(
                    target=self._sync_files_to_new_storage,
                    args=(node,),
                    daemon=True
                ).start()
    
    def _replicate_storage_registration(self, storage_node: NodeInfo):
        """
        Replica el registro de un storage node a todos los followers.
        Esto asegura que todos los metadata tengan la misma lista de storage nodes.
        """
        with self.leader_election._lock:
            peers = list(self.leader_election._peers.values())
        
        if not peers:
            return
        
        logger.info(f"📤 Replicating storage registration {storage_node.node_id} to {len(peers)} peers")
        
        for peer in peers:
            try:
                msg = RPCMessage(
                    MessageType.REGISTER_NODE,
                    {
                        'node': storage_node.to_dict(),
                        'from_leader': True  # Indica que viene del líder
                    }
                )
                self._rpc_client.call(peer.host, peer.port, msg)
                logger.debug(f"Replicated storage {storage_node.node_id} to {peer.node_id}")
            except Exception as e:
                logger.warning(f"Failed to replicate storage registration to {peer.node_id}: {e}")

    def _sync_files_to_new_storage(self, new_node: NodeInfo):
        """
        Sincroniza archivos a un nuevo storage node.
        Replica archivos subreplicados a este nodo.
        """
        logger.info(f"🔄 Starting proactive sync for storage {new_node.node_id}")
        
        try:
            from ..Router.storage_client import StorageClient
            storage_client = StorageClient()
            
            # Esperar un momento para que el nodo esté completamente listo
            time.sleep(2)
            
            # Obtener todos los archivos del namespace
            all_files = self.namespace.get_all_files()
            synced_count = 0
            
            for file_meta in all_files:
                try:
                    file_id = file_meta.file_id
                    
                    # Obtener réplicas activas de este archivo
                    active_replicas = []
                    for replica in self.replica_manager.get_replicas(file_id):
                        node = self.replica_manager.get_storage_node(replica.node_id)
                        if node and node.state == NodeState.UP and node.node_id != new_node.node_id:
                            active_replicas.append((replica, node))
                    
                    # Verificar si el archivo necesita más réplicas
                    current_replica_count = len(active_replicas)
                    
                    # También verificar si este archivo ya está en el nuevo nodo
                    already_in_new_node = any(
                        r.node_id == new_node.node_id 
                        for r in self.replica_manager.get_replicas(file_id)
                    )
                    
                    # Si hay réplicas activas y necesitamos más (o el archivo está huérfano)
                    if active_replicas and (current_replica_count < self.replica_manager.replication_factor or not already_in_new_node):
                        # No replicar si ya alcanzamos el factor de replicación
                        if current_replica_count >= self.replica_manager.replication_factor and already_in_new_node:
                            continue
                        
                        # Tomar la primera réplica activa como fuente
                        source_replica, source_node = active_replicas[0]
                        
                        logger.info(f"📤 Syncing {file_id} from {source_node.node_id} to {new_node.node_id}")
                        
                        # Recuperar datos de la fuente
                        data = storage_client.retrieve_file(
                            source_node.host, source_node.port, file_id
                        )
                        
                        if data:
                            # Almacenar en el nuevo nodo
                            success, checksum = storage_client.store_file(
                                new_node.host, new_node.port,
                                file_id, data, source_replica.version
                            )
                            
                            if success:
                                # Registrar la nueva réplica
                                new_replica = ReplicaInfo(
                                    file_id=file_id,
                                    node_id=new_node.node_id,
                                    version=source_replica.version,
                                    size=len(data),
                                    is_primary=False
                                )
                                
                                # Actualizar replica manager
                                with self.replica_manager._lock:
                                    if file_id not in self.replica_manager._replicas:
                                        self.replica_manager._replicas[file_id] = []
                                    
                                    # Verificar que no exista ya
                                    existing = [r for r in self.replica_manager._replicas[file_id] 
                                               if r.node_id == new_node.node_id]
                                    if not existing:
                                        self.replica_manager._replicas[file_id].append(new_replica)
                                        self.replica_manager._node_files[new_node.node_id].add(file_id)
                                
                                # Actualizar namespace con la nueva réplica
                                current_replicas = file_meta.replicas if hasattr(file_meta, 'replicas') else []
                                if new_node.node_id not in current_replicas:
                                    current_replicas.append(new_node.node_id)
                                    self.namespace.update_file_replicas(file_meta.path, current_replicas)
                                
                                synced_count += 1
                                logger.info(f"✅ Synced {file_id} to {new_node.node_id}")
                            else:
                                logger.warning(f"❌ Failed to store {file_id} on {new_node.node_id}")
                        else:
                            logger.warning(f"❌ Could not retrieve {file_id} from {source_node.node_id}")
                    
                except Exception as e:
                    logger.error(f"Error syncing file {file_meta.file_id}: {e}")
                    continue
            
            logger.info(f"✅ Proactive sync complete for {new_node.node_id}: {synced_count} files synced")
            
            # Limpiar réplicas huérfanas después de la sincronización
            self._cleanup_orphaned_replicas()
            
        except Exception as e:
            logger.error(f"Error in proactive sync for {new_node.node_id}: {e}")
    
    def _cleanup_orphaned_replicas(self):
        """Limpia réplicas que apuntan a nodos que ya no existen"""
        logger.info("🧹 Cleaning up orphaned replicas...")
        
        cleaned_count = 0
        current_storage_nodes = set(self.replica_manager._storage_nodes.keys())
        
        # Limpiar réplicas en el namespace
        for path, meta in self.namespace._namespace.items():
            if not meta.is_directory and hasattr(meta, 'replicas') and meta.replicas:
                # Filtrar solo réplicas que existen en nodos activos
                valid_replicas = [
                    node_id for node_id in meta.replicas 
                    if node_id in current_storage_nodes
                ]
                
                if len(valid_replicas) != len(meta.replicas):
                    removed = set(meta.replicas) - set(valid_replicas)
                    logger.info(f"🧹 Removing orphaned replicas for {meta.file_id}: {removed}")
                    meta.replicas = valid_replicas
                    cleaned_count += len(removed)
        
        if cleaned_count > 0:
            self.namespace._persist_to_disk()
            logger.info(f"✅ Cleaned {cleaned_count} orphaned replica references")
        else:
            logger.info("✅ No orphaned replicas found")
    
    def _leader_maintenance_loop(self):
        """Loop de mantenimiento que solo corre en el líder"""
        maintenance_cycle = 0
        
        while self._running and self.leader_election.is_leader():
            try:
                maintenance_cycle += 1
                
                # Verificar replicación
                under_replicated = self.replica_manager.get_under_replicated_files()
                if under_replicated:
                    logger.info(f"🔍 Found {len(under_replicated)} under-replicated files")
                    
                    # Ejecutar re-replicación para archivos subreplicados
                    self._replicate_under_replicated_files(under_replicated)
                
                # Cada 5 ciclos (2.5 minutos), limpiar réplicas huérfanas
                if maintenance_cycle % 5 == 0:
                    self._cleanup_orphaned_replicas()
                
                time.sleep(30)  # Cada 30 segundos
            except Exception as e:
                logger.error(f"Error in maintenance loop: {e}")
    
    def _replicate_under_replicated_files(self, under_replicated: List):
        """Replica archivos que tienen menos réplicas de las necesarias"""
        try:
            from ..Router.storage_client import StorageClient
            storage_client = StorageClient()
            
            available_nodes = self.replica_manager.get_available_storage_nodes()
            if not available_nodes:
                logger.warning("No storage nodes available for replication")
                return
            
            for item in under_replicated[:10]:  # Procesar máximo 10 por ciclo
                try:
                    # item puede ser (file_id, count) o solo file_id
                    file_id = item[0] if isinstance(item, tuple) else item
                    
                    # Obtener réplicas activas
                    active_replicas = []
                    for replica in self.replica_manager.get_replicas(file_id):
                        node = self.replica_manager.get_storage_node(replica.node_id)
                        if node and node.state == NodeState.UP:
                            active_replicas.append((replica, node))
                    
                    if not active_replicas:
                        logger.warning(f"No active replicas for {file_id}, cannot replicate")
                        continue
                    
                    # Encontrar nodos que no tienen este archivo
                    existing_nodes = {r[0].node_id for r in active_replicas}
                    target_nodes = [n for n in available_nodes if n.node_id not in existing_nodes]
                    
                    if not target_nodes:
                        continue
                    
                    # Necesitamos replicar a cuántos nodos?
                    needed = self.replica_manager.replication_factor - len(active_replicas)
                    if needed <= 0:
                        continue
                    
                    source_replica, source_node = active_replicas[0]
                    
                    # Recuperar datos
                    data = storage_client.retrieve_file(
                        source_node.host, source_node.port, file_id
                    )
                    
                    if not data:
                        logger.warning(f"Could not retrieve {file_id} from {source_node.node_id}")
                        continue
                    
                    # Replicar a los nodos necesarios
                    for target_node in target_nodes[:needed]:
                        success, _ = storage_client.store_file(
                            target_node.host, target_node.port,
                            file_id, data, source_replica.version
                        )
                        
                        if success:
                            # Registrar nueva réplica
                            new_replica = ReplicaInfo(
                                file_id=file_id,
                                node_id=target_node.node_id,
                                version=source_replica.version,
                                size=len(data),
                                is_primary=False
                            )

                            with self.replica_manager._lock:
                                if file_id not in self.replica_manager._replicas:
                                    self.replica_manager._replicas[file_id] = []
                                self.replica_manager._replicas[file_id].append(new_replica)
                                self.replica_manager._node_files[target_node.node_id].add(file_id)

                            # *** CRÍTICO: Actualizar namespace con las réplicas completas ***
                            # Obtener path del archivo y lista completa de réplicas
                            file_path = self.namespace._id_index.get(file_id)
                            if file_path:
                                # Obtener TODAS las réplicas del replica_manager (no solo UP)
                                all_replicas = [replica.node_id for replica in self.replica_manager.get_replicas(file_id)]

                                # Actualizar namespace con la lista completa de réplicas
                                self.namespace.update_file_replicas(file_path, all_replicas)
                                logger.info(f"✅ Updated namespace replicas for {file_path}: {all_replicas}")

                            logger.info(f"✅ Replicated {file_id} to {target_node.node_id}")
                        else:
                            logger.warning(f"❌ Failed to replicate {file_id} to {target_node.node_id}")
                
                except Exception as e:
                    logger.error(f"Error replicating file: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"Error in _replicate_under_replicated_files: {e}")
    
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
        data_state = self._get_data_state()
        
        return RPCMessage(
            MessageType.LEADER_RESPONSE,
            {
                'leader_id': self.leader_election.get_leader_id(),
                'leader_host': leader.host if leader else None,
                'leader_port': leader.port if leader else None,
                'term': self.leader_election.get_term(),
                'data_state': data_state,
                'is_leader': self.leader_election.is_leader()
            },
            msg.request_id
        )
    
    def _handle_register_node(self, msg: RPCMessage) -> RPCMessage:
        """Registra un nuevo nodo en el clúster"""
        try:
            node_data = msg.payload.get('node')
            node = NodeInfo.from_dict(node_data)
            
            # Evitar auto-registro
            if node.node_id == self.node_id:
                logger.debug("Ignoring self-registration attempt")
                return RPCMessage(
                    MessageType.REGISTER_RESPONSE,
                    {'status': DistributedResponseCode.ERROR.value, 'error': 'Cannot register self'},
                    msg.request_id
                )
            
            # Verificar si ya está registrado
            already_registered = False
            if node.node_type == NodeType.METADATA:
                with self.leader_election._lock:
                    already_registered = node.node_id in self.leader_election._peers
            
            # Registrar según tipo
            if node.node_type == NodeType.STORAGE:
                # Verificar si viene del líder (replicación)
                from_leader = msg.payload.get('from_leader', False)
                
                # Si NO somos líder y NO viene del líder, reenviar el registro al líder
                if not self.leader_election.is_leader() and not from_leader:
                    leader = self.leader_election.get_leader()
                    if leader and leader.node_id != self.node_id:
                        logger.info(f"📤 Forwarding STORAGE registration {node.node_id} to leader {leader.node_id}")
                        try:
                            forward_response = self._rpc_client.call(leader.host, leader.port, msg)
                            if forward_response:
                                # Aún así registramos localmente para heartbeats
                                self.replica_manager.register_storage_node(node)
                                self.heartbeat_manager.register_node(node)
                                return forward_response
                        except Exception as e:
                            logger.warning(f"Failed to forward registration to leader: {e}")
                            # Continuar con registro local como fallback
                
                logger.info(f"💾 STORAGE NODE REGISTERED: {node.node_id} @ {node.host}:{node.port} (from_leader={from_leader})")
                self.replica_manager.register_storage_node(node)
                
                # Si somos líder y NO es replicación, replicar el registro a los followers
                if self.leader_election.is_leader() and not from_leader:
                    # Replicar registro de storage a todos los peers
                    self._replicate_storage_registration(node)
                    
                    threading.Thread(
                        target=self._sync_files_to_new_storage,
                        args=(node,),
                        daemon=True
                    ).start()
            elif node.node_type == NodeType.METADATA:
                self.leader_election.register_peer(node)
                
                # AUTO-REGISTRO MUTUO: solo si NO estamos ya registrados con él
                if not already_registered:
                    with self._registration_lock:
                        if node.node_id not in self._registered_peers:
                            self._registered_peers.add(node.node_id)
                            # Hacer registro en thread separado para evitar deadlock
                            threading.Thread(
                                target=self._register_with_peer,
                                args=(node,),
                                daemon=True
                            ).start()
                
                # DETECCIÓN DE SPLIT-BRAIN: verificar si el peer tiene info diferente de líder
                peer_leader_id = msg.payload.get('my_leader_id')
                peer_term = msg.payload.get('my_term', 0)
                if peer_leader_id is not None and peer_term > 0:
                    peer_info = {
                        'leader_id': peer_leader_id,
                        'term': peer_term
                    }
                    # Verificar en thread separado para no bloquear
                    threading.Thread(
                        target=self.split_brain_reconciliation.handle_peer_reconnect,
                        args=(node, peer_info),
                        daemon=True
                    ).start()
            
            self.heartbeat_manager.register_node(node)
            
            return RPCMessage(
                MessageType.REGISTER_RESPONSE,
                {
                    'status': DistributedResponseCode.SUCCESS.value,
                    'leader_id': self.leader_election.get_leader_id(),
                    'my_peers': self._get_all_peer_info()  # Enviar lista de peers
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
        # Verificar si somos el líder
        if not self.leader_election.is_leader():
            return self._redirect_to_leader(MessageType.AUTH_RESPONSE, msg.request_id)
        
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
        # Verificar si somos el líder
        if not self.leader_election.is_leader():
            return self._redirect_to_leader(MessageType.LOOKUP_RESPONSE, msg.request_id)
        
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
        # Verificar si somos el líder
        if not self.leader_election.is_leader():
            return self._redirect_to_leader(MessageType.CREATE_RESPONSE, msg.request_id)
        
        path = msg.payload.get('path')
        owner = msg.payload.get('owner', 'anonymous')
        size = msg.payload.get('size', 0)
        
        # Seleccionar nodos para réplicas
        logger.info(f"📁 CREATE_FILE solicitado: {path} (owner: {owner})")
        selected_nodes = self.replica_manager.select_replicas_for_file(path)

        if len(selected_nodes) < 1:
            logger.error(f"❌ CREATE_FILE FALLÓ - {path}: No hay nodos de storage disponibles para réplicas")
            return RPCMessage(
                MessageType.CREATE_RESPONSE,
                {'status': DistributedResponseCode.NODE_UNAVAILABLE.value},
                msg.request_id
            )
        
        # Crear en namespace
        logger.info(f"📝 CREANDO ARCHIVO EN NAMESPACE: {path} con réplicas {[n.node_id for n in selected_nodes]}")
        code, meta = self.namespace.create_file(
            path, owner, size,
            [n.node_id for n in selected_nodes]
        )
        logger.info(f"📝 RESULTADO NAMESPACE: code={code}, meta={meta is not None}")
        
        if code == DistributedResponseCode.SUCCESS and meta:
            # Asignar réplicas
            logger.info(f"🔄 ASIGNANDO RÉPLICAS para {path} (file_id: {meta.file_id}) a {len(selected_nodes)} nodos")
            replicas = self.replica_manager.assign_replicas(
                meta.file_id, selected_nodes, size
            )
            logger.info(f"✅ RÉPLICAS ASIGNADAS: {len(replicas)} réplicas creadas para {meta.file_id}")
            storage_nodes = [
                {'host': n.host, 'port': n.port, 'node_id': n.node_id}
                for n in selected_nodes
            ]
            logger.info(f"🔄 REPLICANDO OPERACIÓN create_file para {meta.file_id}")
            self._replicate_operation(
                "create_file",
                {
                    'metadata': meta.to_dict(),
                    'replicas': [r.to_dict() for r in replicas],
                    'storage_nodes': storage_nodes
                }
            )
            logger.info(f"✅ OPERACIÓN REPLICADA para {meta.file_id}")
            
            return RPCMessage(
                MessageType.CREATE_RESPONSE,
                {
                    'status': code.value,
                    'metadata': meta.to_dict(),
                    'storage_nodes': storage_nodes
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
        # Verificar si somos el líder
        if not self.leader_election.is_leader():
            return self._redirect_to_leader(MessageType.DELETE_RESPONSE, msg.request_id)
        
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
            self._replicate_operation(
                "delete_file",
                {
                    'path': path,
                    'file_id': meta.file_id
                }
            )
        
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
        # Verificar si somos el líder
        if not self.leader_election.is_leader():
            return self._redirect_to_leader(MessageType.RENAME_RESPONSE, msg.request_id)
        
        old_path = msg.payload.get('old_path')
        new_path = msg.payload.get('new_path')
        
        code = self.namespace.rename(old_path, new_path)
        if code == DistributedResponseCode.SUCCESS:
            self._replicate_operation("rename", {'old_path': old_path, 'new_path': new_path})
        
        return RPCMessage(
            MessageType.RENAME_RESPONSE,
            {'status': code.value},
            msg.request_id
        )
    
    def _handle_list_dir(self, msg: RPCMessage) -> RPCMessage:
        """Lista contenido de un directorio"""
        # Verificar si somos el líder
        if not self.leader_election.is_leader():
            return self._redirect_to_leader(MessageType.LIST_RESPONSE, msg.request_id)
        
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
        # Verificar si somos el líder
        if not self.leader_election.is_leader():
            return self._redirect_to_leader(MessageType.MKDIR_RESPONSE, msg.request_id)
        
        path = msg.payload.get('path')
        owner = msg.payload.get('owner', 'anonymous')
        
        code, meta = self.namespace.create_directory(path, owner)
        
        if code == DistributedResponseCode.SUCCESS and meta:
            self._replicate_operation("mkdir", {'metadata': meta.to_dict()})
        
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
        # Verificar si somos el líder
        if not self.leader_election.is_leader():
            return self._redirect_to_leader(MessageType.RMDIR_RESPONSE, msg.request_id)
        
        path = msg.payload.get('path')
        recursive = msg.payload.get('recursive', True)
        
        code = self.namespace.delete_directory(path, recursive)
        if code == DistributedResponseCode.SUCCESS:
            self._replicate_operation("rmdir", {'path': path, 'recursive': recursive})
        
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
        # Verificar si somos el líder
        if not self.leader_election.is_leader():
            return self._redirect_to_leader(MessageType.UPDATE_META_RESPONSE, msg.request_id)
        
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
        
        if code == DistributedResponseCode.SUCCESS:
            self._replicate_operation(
                "update_file_meta",
                {'path': path, 'size': size, 'version': version, 'replicas': replicas}
            )
        
        return RPCMessage(
            MessageType.UPDATE_META_RESPONSE,
            {'status': code.value},
            msg.request_id
        )
    
    def _handle_lock_request(self, msg: RPCMessage) -> RPCMessage:
        """Maneja solicitudes de bloqueo"""
        # Verificar si somos el líder
        if not self.leader_election.is_leader():
            return self._redirect_to_leader(MessageType.LOCK_RESPONSE, msg.request_id)
        
        file_id = msg.payload.get('file_id')
        holder_id = msg.payload.get('holder_id')
        lock_type = msg.payload.get('lock_type', 'READ')
        
        if lock_type == 'WRITE':
            code, lock_info = self.lock_manager.acquire_write_lock(file_id, holder_id)
        else:
            code, lock_info = self.lock_manager.acquire_read_lock(file_id, holder_id)

        if code == DistributedResponseCode.SUCCESS and lock_info:
            self._replicate_operation(
                "lock_acquire",
                {
                    'lock': lock_info.to_dict()
                }
            )
        
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
        # Verificar si somos el líder
        if not self.leader_election.is_leader():
            return self._redirect_to_leader(MessageType.UNLOCK_RESPONSE, msg.request_id)
        
        file_id = msg.payload.get('file_id')
        holder_id = msg.payload.get('holder_id')
        
        code = self.lock_manager.release_lock(file_id, holder_id)
        if code == DistributedResponseCode.SUCCESS:
            self._replicate_operation(
                "lock_release",
                {'file_id': file_id, 'holder_id': holder_id}
            )
        
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
        sync_type = msg.payload.get('sync_type')
        from_leader = msg.payload.get('from_leader', False)
        
        # Si es sincronización de storage_nodes desde el líder
        if sync_type == 'storage_nodes' and from_leader:
            storage_nodes = msg.payload.get('storage_nodes', {})
            added_count = 0
            
            for node_id, node_dict in storage_nodes.items():
                try:
                    if node_id not in self.replica_manager._storage_nodes:
                        node = NodeInfo.from_dict(node_dict)
                        node.state = NodeState.UP
                        self.replica_manager.register_storage_node(node)
                        self.heartbeat_manager.register_node(node)
                        added_count += 1
                        logger.info(f"📦 Synced storage node {node_id} from leader")
                    else:
                        # Actualizar estado si el nodo ya existe
                        existing = self.replica_manager._storage_nodes[node_id]
                        if existing.state == NodeState.DOWN:
                            existing.state = NodeState.UP
                except Exception as e:
                    logger.warning(f"Failed to sync storage node {node_id}: {e}")
            
            if added_count > 0:
                logger.info(f"✅ Synced {added_count} storage nodes from leader")
            
            return RPCMessage(
                MessageType.SYNC_RESPONSE,
                {'status': DistributedResponseCode.SUCCESS.value, 'added': added_count},
                msg.request_id
            )
        
        # Exportar todo el estado (comportamiento original)
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


    # === Replicación de metadata (log + snapshot) ===

    def _load_persistent_state(self):
        """Carga snapshot y log al iniciar con validación de integridad"""
        try:
            if os.path.exists(self.snapshot_path):
                snapshot = self._load_snapshot_with_validation()
                if snapshot:
                    self._install_snapshot(snapshot)
                    self._commit_index = snapshot.get('commit_index', -1)
                    self._last_applied = self._commit_index
                    logger.info(f"Snapshot loaded up to index {self._commit_index}")
        except Exception as e:
            logger.error(f"Could not load snapshot: {e}")
            # En caso de snapshot corrupto, intentar recuperación sin él
            self._commit_index = -1
            self._last_applied = -1

        try:
            if os.path.exists(self.log_path):
                valid_entries = self._load_log_with_validation()
                if valid_entries:
                    self._oplog = valid_entries
                    # Aplicar solo entradas commited
                    applied_count = 0
                    for entry in self._oplog:
                        if entry.get('index', -1) <= self._commit_index:
                            if entry.get('index', -1) > self._last_applied:
                                self._apply_log_entry(entry, persist=False)
                                self._last_applied = entry['index']
                                applied_count += 1
                        else:
                            # Log entries beyond commit_index are kept but not applied
                            logger.debug(f"Log entry {entry.get('index')} not applied (not committed)")

                    if self._oplog:
                        self._current_term = max(self._current_term, self._oplog[-1].get('term', 0))
                    logger.info(f"Loaded {len(self._oplog)} log entries, applied {applied_count} committed entries")
                else:
                    logger.warning("Log file corrupted or invalid, starting with empty log")
                    self._oplog = []
        except Exception as e:
            logger.error(f"Could not load log: {e}")
            self._oplog = []
            # En caso crítico, resetear estado
            if self._last_applied < self._commit_index:
                logger.warning("Inconsistent state detected, resetting to last committed state")
                self._last_applied = self._commit_index

    def _load_snapshot_with_validation(self) -> Optional[Dict]:
        """Carga snapshot con validación de integridad"""
        try:
            with open(self.snapshot_path, 'r') as f:
                snapshot = json.load(f)

            # Validar estructura básica del snapshot
            required_keys = ['namespace', 'commit_index', 'term']
            if not all(key in snapshot for key in required_keys):
                logger.error("Snapshot missing required keys")
                return None

            # Validar que commit_index sea válido
            if not isinstance(snapshot.get('commit_index'), int) or snapshot['commit_index'] < -1:
                logger.error("Invalid commit_index in snapshot")
                return None

            # Validar estructura del namespace
            namespace = snapshot.get('namespace', {})
            if not isinstance(namespace, dict):
                logger.error("Invalid namespace structure in snapshot")
                return None

            # Calcular checksum del snapshot para detectar corrupción
            # Usar el mismo método que en _create_snapshot (sin checksum en el cálculo)
            import hashlib
            snapshot_copy = snapshot.copy()
            stored_checksum = snapshot_copy.pop('checksum', None)  # Remover checksum para cálculo
            snapshot_str = json.dumps(snapshot_copy, sort_keys=True)
            checksum = hashlib.md5(snapshot_str.encode()).hexdigest()

            if stored_checksum and stored_checksum != checksum:
                logger.error("Snapshot checksum mismatch - file corrupted")
                return None

            logger.info(f"Snapshot validation passed (checksum: {checksum[:8]}...)")
            return snapshot

        except json.JSONDecodeError as e:
            logger.error(f"Snapshot JSON corrupted: {e}")
            return None
        except Exception as e:
            logger.error(f"Error validating snapshot: {e}")
            return None

    def _load_log_with_validation(self) -> Optional[List[Dict]]:
        """Carga log con validación de integridad y consistencia"""
        valid_entries = []
        expected_index = 0

        try:
            with open(self.log_path, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        entry = json.loads(line)

                        # Validar estructura básica de la entrada
                        required_keys = ['index', 'term', 'op_type', 'payload']
                        if not all(key in entry for key in required_keys):
                            logger.error(f"Invalid log entry structure at line {line_num}")
                            return None

                        # Validar secuencia de índices
                        if entry.get('index') != expected_index:
                            logger.error(f"Log index discontinuity at line {line_num}: expected {expected_index}, got {entry.get('index')}")
                            return None

                        # Validar que el term no sea negativo
                        if entry.get('term', 0) < 0:
                            logger.error(f"Invalid term in log entry at line {line_num}")
                            return None

                        # Validar payload
                        if not isinstance(entry.get('payload'), dict):
                            logger.error(f"Invalid payload in log entry at line {line_num}")
                            return None

                        valid_entries.append(entry)
                        expected_index += 1

                    except json.JSONDecodeError as e:
                        logger.error(f"Corrupted JSON in log at line {line_num}: {e}")
                        return None

            # Verificar que no hay entradas duplicadas
            indices = [entry['index'] for entry in valid_entries]
            if len(indices) != len(set(indices)):
                logger.error("Duplicate indices found in log")
                return None

            logger.info(f"Log validation passed: {len(valid_entries)} entries")
            return valid_entries

        except Exception as e:
            logger.error(f"Error validating log: {e}")
            return None

    def _append_log_entry(self, entry: Dict[str, Any]):
        """Añade entrada al log en memoria y disco"""
        with self._log_lock:
            self._oplog.append(entry)
            os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
            with open(self.log_path, 'a') as f:
                f.write(json.dumps(entry) + "\n")

    def _apply_log_entry(self, entry: Dict[str, Any], persist: bool = True):
        """Aplica una entrada del log al estado local"""
        op_id = entry.get('op_id')
        if op_id and op_id in self._applied_ops:
            return
        self._apply_operation(entry.get('op_type'), entry.get('payload', {}))
        if op_id:
            self._applied_ops.add(op_id)
        if persist:
            self._last_applied = max(self._last_applied, entry.get('index', -1))
            self._commit_index = max(self._commit_index, self._last_applied)

    def _apply_operation(self, op_type: str, payload: Dict[str, Any]):
        """Aplica una operación de log de forma idempotente"""
        if not op_type:
            return

        if op_type == "mkdir":
            meta_dict = payload.get('metadata')
            if meta_dict:
                meta = FileMetadata.from_dict(meta_dict)
                self.namespace.upsert_entry(meta)

        elif op_type == "create_file":
            meta_dict = payload.get('metadata')
            replicas = payload.get('replicas', [])
            storage_nodes = payload.get('storage_nodes', [])
            if meta_dict:
                meta = FileMetadata.from_dict(meta_dict)
                self.namespace.upsert_entry(meta)
                self._apply_storage_nodes(storage_nodes)
                self.replica_manager.apply_replicas_state(meta.file_id, replicas)

        elif op_type == "delete_file":
            path = payload.get('path')
            file_id = payload.get('file_id')
            if path:
                self.namespace.delete_file(path)
            if file_id:
                self.replica_manager.remove_all_replicas(file_id)

        elif op_type == "rename":
            self.namespace.rename(payload.get('old_path'), payload.get('new_path'))

        elif op_type == "rmdir":
            self.namespace.delete_directory(payload.get('path'), payload.get('recursive', True))

        elif op_type == "update_file_meta":
            path = payload.get('path')
            size = payload.get('size')
            version = payload.get('version')
            replicas = payload.get('replicas')
            if size is not None:
                self.namespace.update_file_size(path, size, version)
            if replicas is not None:
                self.namespace.update_file_replicas(path, replicas)

        elif op_type == "lock_acquire":
            lock_dict = payload.get('lock')
            if lock_dict:
                lock = LockInfo.from_dict(lock_dict)
                with self.lock_manager._lock:
                    if lock.lock_type == 'WRITE':
                        self.lock_manager._write_locks[lock.file_id] = lock
                    else:
                        self.lock_manager._read_locks[lock.file_id].add(lock.holder)
                        self.lock_manager._read_lock_info[(lock.file_id, lock.holder)] = lock

        elif op_type == "lock_release":
            self.lock_manager.release_lock(payload.get('file_id'), payload.get('holder_id'))

    def _apply_storage_nodes(self, storage_nodes: List[Dict[str, Any]]):
        """Registra nodos de storage incluidos en replicación"""
        for n in storage_nodes or []:
            try:
                node = NodeInfo.from_dict({
                    **n,
                    'node_type': NodeType.STORAGE.value,
                    'state': n.get('state', NodeState.UP.value)
                })
                self.replica_manager.register_storage_node(node)
            except Exception:
                # En caso de datos incompletos, ignorar
                continue

    def _create_log_entry(self, op_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        with self._log_lock:
            next_index = len(self._oplog)
        return {
            'index': next_index,
            'term': self.leader_election.get_term(),
            'op_type': op_type,
            'payload': payload,
            'op_id': payload.get('op_id') or str(uuid.uuid4())
        }

    def _replicate_operation(self, op_type: str, payload: Dict[str, Any]) -> bool:
        """
        Replica operación a followers con garantías de consistencia.
        Espera confirmación de quorum antes de aplicar y commitear.
        """
        entry = self._create_log_entry(op_type, payload)

        # Primero, asegurar que la entrada está persistida localmente
        self._append_log_entry(entry)

        peers = list(self.leader_election._peers.values())
        if not peers:
            # Single node cluster - aplicar y commitear inmediatamente
            self._apply_log_entry(entry)
            self._commit_index = entry['index']
            return True

        # Calcular quorum requerido
        quorum = (len(peers) + 1) // 2 + 1
        logger.debug(f"Replicating operation {op_type} to {len(peers)} peers, quorum={quorum}")

        # Enviar a todos los peers y esperar respuestas
        replication_results = self._replicate_to_quorum(entry, peers, quorum)

        if replication_results['success_count'] >= quorum:
            # Quorum alcanzado - aplicar la operación y actualizar commit_index
            self._apply_log_entry(entry)
            self._commit_index = entry['index']
            logger.debug(f"Operation {op_type} committed at index {entry['index']}")
            return True
        else:
            # Quorum no alcanzado - operación no se puede aplicar
            logger.warning(f"Quorum not reached for operation {op_type} at index {entry['index']} "
                         f"({replication_results['success_count']}/{quorum} acks)")

            # Marcar entrada como no-commited (podría usarse para retry más tarde)
            self._mark_entry_as_uncommitted(entry['index'])
            return False

    def _replicate_to_quorum(self, entry: Dict[str, Any], peers: List[NodeInfo], quorum: int) -> Dict[str, Any]:
        """
        Replica entrada a peers hasta alcanzar quorum.
        Retorna estadísticas de la replicación.
        """
        import concurrent.futures
        import time

        success_count = 1  # Contar al líder
        failed_peers = []
        timeout = 5.0  # Timeout por peer

        # Usar ThreadPoolExecutor para replicación paralela pero con timeout
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(peers), 5)) as executor:
            # Crear futures para cada peer
            future_to_peer = {
                executor.submit(self._send_append_to_peer_with_timeout, peer, entry, timeout): peer
                for peer in peers
            }

            # Esperar resultados con timeout global
            start_time = time.time()
            for future in concurrent.futures.as_completed(future_to_peer):
                peer = future_to_peer[future]
                try:
                    if future.result():
                        success_count += 1
                        logger.debug(f"Replication successful to {peer.node_id}")
                    else:
                        failed_peers.append(peer.node_id)
                        logger.debug(f"Replication failed to {peer.node_id}")
                except concurrent.futures.TimeoutError:
                    failed_peers.append(peer.node_id)
                    logger.warning(f"Replication timeout to {peer.node_id}")
                except Exception as e:
                    failed_peers.append(peer.node_id)
                    logger.warning(f"Replication error to {peer.node_id}: {e}")

        return {
            'success_count': success_count,
            'failed_peers': failed_peers,
            'total_peers': len(peers) + 1,  # +1 para el líder
            'quorum_required': quorum
        }

    def _send_append_to_peer_with_timeout(self, peer: NodeInfo, entry: Dict[str, Any], timeout: float = 5.0) -> bool:
        """Envía append a peer con timeout específico"""
        import time
        start_time = time.time()

        try:
            result = self._send_append_to_peer(peer, entry)
            elapsed = time.time() - start_time

            if result:
                logger.debug(f"Append to {peer.node_id} succeeded in {elapsed:.2f}s")
            else:
                logger.debug(f"Append to {peer.node_id} failed in {elapsed:.2f}s")

            return result
        except Exception as e:
            elapsed = time.time() - start_time
            logger.warning(f"Append to {peer.node_id} exception after {elapsed:.2f}s: {e}")
            return False

    def _mark_entry_as_uncommitted(self, index: int):
        """Marca una entrada como no-commited (para futura recuperación)"""
        # Por ahora solo loggeamos, pero esto podría extenderse para
        # mantener un registro de entradas no-commited para retry
        logger.info(f"Entry at index {index} marked as uncommitted - quorum not reached")

        # Podríamos añadir metadata a la entrada para marcarla como uncommitted
        # Esto sería útil para operaciones de recuperación manual
        try:
            if index < len(self._oplog):
                self._oplog[index]['uncommitted'] = True
                # Re-persistir el log con la marca
                self._persist_log_entry_with_metadata(index, {'uncommitted': True})
        except Exception as e:
            logger.warning(f"Could not mark entry {index} as uncommitted: {e}")

    def _persist_log_entry_with_metadata(self, index: int, metadata: Dict[str, Any]):
        """Actualiza metadata de una entrada en el log persistido"""
        try:
            # Leer el log completo
            if os.path.exists(self.log_path):
                with open(self.log_path, 'r') as f:
                    lines = f.readlines()

                if index < len(lines):
                    # Parsear la línea existente
                    existing_entry = json.loads(lines[index].strip())

                    # Añadir metadata
                    existing_entry.update(metadata)

                    # Reescribir la línea
                    lines[index] = json.dumps(existing_entry) + "\n"

                    # Reescribir el archivo completo
                    with open(self.log_path, 'w') as f:
                        f.writelines(lines)

                    logger.debug(f"Updated metadata for log entry {index}")
        except Exception as e:
            logger.warning(f"Could not update log entry metadata: {e}")

    def _send_append_to_peer(self, peer: NodeInfo, entry: Dict[str, Any]) -> bool:
        msg = RPCMessage(
            MessageType.REPL_APPEND,
            {
                'leader_id': self.node_id,
                'entry': entry,
                'commit_index': self._commit_index
            }
        )
        try:
            response = self._rpc_client.call(peer.host, peer.port, msg)
            if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
                return True
            if response and response.payload.get('status') == DistributedResponseCode.SYNC_REQUIRED.value:
                # Enviar snapshot y considerar ack si instala
                if self._send_snapshot_to_peer(peer):
                    return True
        except Exception as e:
            logger.debug(f"Append failed to {peer.node_id}: {e}")
        return False

    def _send_snapshot_to_peer(self, peer: NodeInfo) -> bool:
        snapshot = self._create_snapshot()
        msg = RPCMessage(
            MessageType.REPL_SNAPSHOT,
            {
                'leader_id': self.node_id,
                'snapshot': snapshot
            }
        )
        try:
            response = self._rpc_client.call(peer.host, peer.port, msg)
            return response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value
        except Exception as e:
            logger.debug(f"Snapshot send failed to {peer.node_id}: {e}")
            return False

    def _create_snapshot(self) -> Dict[str, Any]:
        """Construye snapshot del estado con checksum para integridad"""
        state = {
            'namespace': self.namespace.export_state(),
            'locks': self.lock_manager.export_state(),
            'users': self.auth_service.export_state(),
            'replicas': self.replica_manager.export_state(),
            'commit_index': self._commit_index,
            'term': self.leader_election.get_term()
        }

        # Calcular checksum del estado (sin checksum) para consistencia
        import hashlib
        state_copy = state.copy()  # No incluir checksum en el cálculo
        state_str = json.dumps(state_copy, sort_keys=True)
        checksum = hashlib.md5(state_str.encode()).hexdigest()
        state['checksum'] = checksum

        try:
            os.makedirs(os.path.dirname(self.snapshot_path), exist_ok=True)
            with open(self.snapshot_path, 'w') as f:
                json.dump(state, f, indent=2, sort_keys=True)
            logger.info(f"Snapshot created with checksum {checksum[:8]}...")
        except Exception as e:
            logger.warning(f"Could not persist snapshot: {e}")
        return state

    def _install_snapshot(self, snapshot: Dict[str, Any]):
        """Instala snapshot recibido, incluyendo registro de storage_nodes"""
        try:
            if not snapshot:
                return
            
            logger.info("📦 Installing snapshot...")
            
            # Importar estado de cada componente
            self.namespace.import_state(snapshot.get('namespace', {}))
            self.lock_manager.import_state(snapshot.get('locks', {}))
            self.auth_service.import_state(snapshot.get('users', {}))
            
            # Importar réplicas y storage_nodes
            replicas_state = snapshot.get('replicas', {})
            self.replica_manager.import_state(replicas_state)
            
            # Registrar storage_nodes en heartbeat_manager
            storage_nodes = replicas_state.get('storage_nodes', {})
            for node_id, node_dict in storage_nodes.items():
                try:
                    if node_id not in [n.node_id for n in self.heartbeat_manager._nodes.values() if hasattr(self.heartbeat_manager, '_nodes')]:
                        node = NodeInfo.from_dict(node_dict)
                        node.state = NodeState.UP
                        self.heartbeat_manager.register_node(node)
                        logger.debug(f"Registered storage {node_id} from snapshot")
                except Exception as e:
                    logger.debug(f"Could not register storage {node_id} from snapshot: {e}")
            
            self._commit_index = snapshot.get('commit_index', -1)
            self._last_applied = self._commit_index
            
            logger.info(f"✅ Snapshot installed: {len(storage_nodes)} storage nodes, "
                       f"commit_index={self._commit_index}")
            
        except Exception as e:
            logger.error(f"Failed to install snapshot: {e}")

    # === Handlers de replicación interna ===

    def _handle_repl_append(self, msg: RPCMessage) -> RPCMessage:
        entry = msg.payload.get('entry')
        if not entry:
            return RPCMessage(
                MessageType.REPL_APPEND_RESPONSE,
                {'status': DistributedResponseCode.ERROR.value},
                msg.request_id
            )

        with self._log_lock:
            expected_index = len(self._oplog)
            if entry.get('index') != expected_index:
                return RPCMessage(
                    MessageType.REPL_APPEND_RESPONSE,
                    {
                        'status': DistributedResponseCode.SYNC_REQUIRED.value,
                        'expected_index': expected_index
                    },
                    msg.request_id
                )

            self._append_log_entry(entry)
            self._apply_log_entry(entry)

        return RPCMessage(
            MessageType.REPL_APPEND_RESPONSE,
            {
                'status': DistributedResponseCode.SUCCESS.value,
                'last_index': entry.get('index')
            },
            msg.request_id
        )

    def _handle_repl_snapshot(self, msg: RPCMessage) -> RPCMessage:
        # Tipos especiales de requests para reconciliación de split-brain
        request_type = msg.payload.get('request_type')
        
        if request_type == 'state_summary':
            # Devolver resumen del estado (sin todo el snapshot)
            with self._log_lock:
                file_count = len(self.namespace._namespace)
                return RPCMessage(
                    MessageType.REPL_SNAPSHOT_RESPONSE,
                    {
                        'status': DistributedResponseCode.SUCCESS.value,
                        'term': self.leader_election.get_term(),
                        'leader_id': self.leader_election.get_leader_id(),
                        'commit_index': self._commit_index,
                        'last_applied': self._last_applied,
                        'oplog_length': len(self._oplog),
                        'file_count': file_count
                    },
                    msg.request_id
                )
        
        elif request_type == 'full_snapshot':
            # Devolver snapshot completo (para sincronización)
            snapshot = self._create_snapshot()
            return RPCMessage(
                MessageType.REPL_SNAPSHOT_RESPONSE,
                {
                    'status': DistributedResponseCode.SUCCESS.value,
                    'snapshot': snapshot,
                    'term': self.leader_election.get_term(),
                    'leader_id': self.leader_election.get_leader_id(),
                    'commit_index': self._commit_index
                },
                msg.request_id
            )
        
        # Si no somos líder y nos piden snapshot, redirigir
        if not self.leader_election.is_leader() and 'snapshot' not in msg.payload:
            # Pero para reconciliación, permitir responder aunque no seamos líder
            if request_type in ['state_summary', 'full_snapshot']:
                snapshot = self._create_snapshot()
                return RPCMessage(
                    MessageType.REPL_SNAPSHOT_RESPONSE,
                    {
                        'status': DistributedResponseCode.SUCCESS.value,
                        'snapshot': snapshot,
                        'term': self.leader_election.get_term(),
                        'leader_id': self.leader_election.get_leader_id(),
                        'commit_index': self._commit_index
                    },
                    msg.request_id
                )
            return self._redirect_to_leader(MessageType.REPL_SNAPSHOT_RESPONSE, msg.request_id)

        # Si recibimos snapshot para instalar
        snapshot = msg.payload.get('snapshot')
        if snapshot:
            # 🔒 LOCK GLOBAL: Proteger instalación completa contra race conditions
            with self._global_state_lock:
                self._install_snapshot(snapshot)
                # Limpiar log porque snapshot ya incluye estado
                with self._log_lock:
                    self._oplog = []
                    try:
                        if os.path.exists(self.log_path):
                            os.remove(self.log_path)
                    except OSError:
                        pass
            return RPCMessage(
                MessageType.REPL_SNAPSHOT_RESPONSE,
                {'status': DistributedResponseCode.SUCCESS.value},
                msg.request_id
            )

        # Si somos líder y nos piden snapshot
        snapshot = self._create_snapshot()
        return RPCMessage(
            MessageType.REPL_SNAPSHOT_RESPONSE,
            {
                'status': DistributedResponseCode.SUCCESS.value,
                'snapshot': snapshot
            },
            msg.request_id
        )

    def _handle_repl_redirect(self, msg: RPCMessage) -> RPCMessage:
        """Permite a followers responder rápido con datos del líder actual"""
        return self._redirect_to_leader(MessageType.REPL_REDIRECT, msg.request_id)

    def _handle_get_current_namespace(self, msg: RPCMessage) -> RPCMessage:
        """Devuelve el namespace actual del nodo (para reconciliación de split-brain)"""
        try:
            current_namespace = self.namespace._namespace
            namespace_data = {path: meta.to_dict() for path, meta in current_namespace.items()}

            logger.debug(f"Returning current namespace with {len(namespace_data)} files")

            return RPCMessage(
                MessageType.CURRENT_NAMESPACE_RESPONSE,
                {'namespace': namespace_data},
                msg.request_id
            )
        except Exception as e:
            logger.error(f"Error getting current namespace: {e}")
            return RPCMessage(
                MessageType.CURRENT_NAMESPACE_RESPONSE,
                {'namespace': {}, 'error': str(e)},
                msg.request_id
            )


    # === Métodos de descubrimiento y auto-registro ===
    
    def _discover_and_register_with_peers(self):
        """Descubre otros nodos metadata vía DNS y se registra con ellos"""
        try:
            metadata_service = os.getenv('METADATA_SERVICE', 'metadata')
            logger.info(f"Discovering peers via DNS alias '{metadata_service}'")

            # Resolver todas las IPs del alias
            try:
                _, _, ipaddrlist = socket.gethostbyname_ex(metadata_service)
            except socket.gaierror:
                logger.warning(f"Could not resolve DNS alias '{metadata_service}'")
                return
            
            # Obtener nuestra propia IP para evitar auto-registro
            try:
                self_host = socket.gethostbyname(self.node_info.host)
            except socket.gaierror:
                self_host = None
            
            # Registrarse con cada peer descubierto
            for ip in ipaddrlist:
                if ip == self_host:
                    continue  # Saltar nuestra propia IP
                
                try:
                    msg = RPCMessage(
                        MessageType.REGISTER_NODE,
                        {
                            'node': self.node_info.to_dict(),
                            'my_leader_id': self.leader_election.get_leader_id(),
                            'my_term': self.leader_election.get_term()
                        }
                    )
                    response = self._rpc_client.call(ip, METADATA_RPC_PORT, msg)
                    
                    if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
                        # Procesar peers que nos envió el otro nodo
                        peers_data = response.payload.get('my_peers', [])
                        
                        # Registrar localmente todos los peers descubiertos
                        discovered_peers = []
                        for peer_dict in peers_data:
                            try:
                                peer = NodeInfo.from_dict(peer_dict)
                                if peer.node_id != self.node_id:
                                    with self.leader_election._lock:
                                        if peer.node_id not in self.leader_election._peers:
                                            self.leader_election.register_peer(peer)
                                            self.heartbeat_manager.register_node(peer)
                                            discovered_peers.append(peer)
                                            logger.info(f"Discovered peer {peer.node_id}")
                            except Exception as e:
                                logger.debug(f"Could not process peer: {e}")
                        
                        # Registrarse activamente con cada peer descubierto
                        for peer in discovered_peers:
                            with self._registration_lock:
                                if peer.node_id not in self._registered_peers:
                                    self._registered_peers.add(peer.node_id)
                                    threading.Thread(
                                        target=self._register_with_peer_actively,
                                        args=(peer,),
                                        daemon=True
                                    ).start()
                        
                        logger.info(f"Successfully registered with {ip}, discovered {len(discovered_peers)} peers")
                        return  # Ya nos registramos con un peer, suficiente
                        
                except Exception as e:
                    logger.debug(f"Could not register with peer {ip}: {e}")
                    
        except Exception as e:
            logger.warning(f"Error in peer discovery: {e}")
    
    def _register_with_peer_actively(self, peer: NodeInfo):
        """Se registra activamente con un peer descubierto"""
        if peer.node_id == self.node_id:
            return
        
        try:
            # Pequeño delay para evitar sobrecarga
            time.sleep(0.1)
            
            msg = RPCMessage(
                MessageType.REGISTER_NODE,
                {
                    'node': self.node_info.to_dict(),
                    'my_leader_id': self.leader_election.get_leader_id(),
                    'my_term': self.leader_election.get_term()
                }
            )
            response = self._rpc_client.call(peer.host, peer.port, msg)
            
            if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
                logger.info(f"Successfully registered actively with peer {peer.node_id}")
            else:
                logger.warning(f"Failed to register with peer {peer.node_id}")
        except Exception as e:
            logger.warning(f"Could not register with peer {peer.node_id}: {e}")
    
    def _register_with_peer(self, peer: NodeInfo):
        """Se registra con otro nodo metadata para descubrimiento mutuo"""
        if peer.node_id == self.node_id:
            return
        
        # Verificar si ya estamos registrados
        with self._registration_lock:
            if peer.node_id in self._registered_peers:
                logger.debug(f"Already registered with peer {peer.node_id}, skipping")
                return
            # Marcar inmediatamente para evitar doble registro
            self._registered_peers.add(peer.node_id)
        
        try:
            msg = RPCMessage(
                MessageType.REGISTER_NODE,
                {
                    'node': self.node_info.to_dict(),
                    'my_leader_id': self.leader_election.get_leader_id(),
                    'my_term': self.leader_election.get_term()
                }
            )
            response = self._rpc_client.call(peer.host, peer.port, msg)
            
            if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
                logger.info(f"Successfully registered with peer {peer.node_id}")
            else:
                # Si falla, remover de registered_peers para reintentar
                with self._registration_lock:
                    self._registered_peers.discard(peer.node_id)
        except Exception as e:
            logger.debug(f"Could not register with peer {peer.node_id}: {e}")
            # Si falla, remover de registered_peers para reintentar
            with self._registration_lock:
                self._registered_peers.discard(peer.node_id)
    
    def _share_peers_with_node(self, new_node: NodeInfo):
        """Comparte todos nuestros peers con un nuevo nodo para crear malla completa"""
        # REMOVIDO - ya no necesario porque enviamos la lista en la respuesta de registro
        pass
    
    def _register_received_peers(self, peers_data: List[Dict]):
        """Registra peers recibidos de otro nodo"""
        # Este método ya no se usa, reemplazado por lógica en _discover_and_register_with_peers
        for peer_dict in peers_data:
            try:
                peer = NodeInfo.from_dict(peer_dict)
                if peer.node_id != self.node_id:
                    # Verificar si ya lo conocemos
                    with self.leader_election._lock:
                        if peer.node_id not in self.leader_election._peers:
                            self.leader_election.register_peer(peer)
                            self.heartbeat_manager.register_node(peer)
                            logger.info(f"Registered peer {peer.node_id} from gossip")
            except Exception as e:
                logger.debug(f"Could not register received peer: {e}")
    
    def _get_data_state(self) -> Dict:
        """
        Obtiene el estado de datos del nodo para comparación en elección de líder.
        Retorna información sobre archivos, storage nodes y commit index.
        """
        try:
            with self._log_lock:
                file_count = len(self.namespace._namespace)
                storage_count = len(self.replica_manager._storage_nodes)
                commit_index = self._commit_index
                oplog_length = len(self._oplog)
                
                return {
                    'file_count': file_count,
                    'storage_count': storage_count,
                    'commit_index': commit_index,
                    'oplog_length': oplog_length,
                    'term': self.leader_election.get_term() if hasattr(self, 'leader_election') else 0
                }
        except Exception as e:
            logger.debug(f"Error getting data state: {e}")
            return {'file_count': 0, 'storage_count': 0, 'commit_index': -1, 'oplog_length': 0}
    
    def _get_all_peer_info(self) -> List[Dict]:
        """Obtiene información de todos los peers como diccionarios"""
        peers = []
        for peer in self.leader_election._peers.values():
            peers.append(peer.to_dict())
        # También incluirnos a nosotros mismos
        peers.append(self.node_info.to_dict())
        return peers
    
    def _handle_get_peers(self, msg: RPCMessage) -> RPCMessage:
        """Maneja solicitudes para obtener lista de peers"""
        return RPCMessage(
            MessageType.PEERS_RESPONSE,
            {
                'status': DistributedResponseCode.SUCCESS.value,
                'peers': self._get_all_peer_info()
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

