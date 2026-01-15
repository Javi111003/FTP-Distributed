"""
Reconciliación de split-brain para el servicio de Metadata.
Maneja la sincronización y merge de estados cuando la red se reconecta
después de una partición.
"""
import time
import threading
import logging
import hashlib
from typing import Dict, List, Optional, Tuple, Any, Set
from ..Common.models import NodeInfo, FileMetadata
from ..Common.constants import NodeType, NodeState, MessageType, DistributedResponseCode
from ..Common.rpc_protocol import RPCClient, RPCMessage

logger = logging.getLogger(__name__)


class SplitBrainReconciliation:
    """
    Maneja la detección y reconciliación de split-brain.
    
    Cuando la red se particiona, cada partición puede elegir su propio líder
    y continuar operando. Al reconectar, este módulo:
    1. Detecta la presencia de múltiples líderes
    2. Resuelve el conflicto usando criterios claros (más datos, menor ID)
    3. Sincroniza completamente storage_nodes, namespace y réplicas
    4. Versiona automáticamente archivos con conflictos
    """
    
    def __init__(self, metadata_server):
        self.metadata_server = metadata_server
        self.node_id = metadata_server.node_id
        self._rpc_client = RPCClient()
        self._lock = threading.RLock()
        
        # Tracking de reconciliaciones en progreso
        self._reconciliation_in_progress = False
        self._last_reconciliation = 0
        self._reconciliation_cooldown = 15  # segundos - aumentado para estabilidad
        
        # Historial de términos vistos
        self._seen_terms: Dict[str, int] = {}
        
    def detect_split_brain(self, peer_node: NodeInfo, peer_term: int, peer_leader_id: str) -> bool:
        """
        Detecta si hay un split-brain comparando el estado del peer con el nuestro.
        """
        with self._lock:
            my_term = self.metadata_server.leader_election.get_term()
            my_leader_id = self.metadata_server.leader_election.get_leader_id()
            am_i_leader = self.metadata_server.leader_election.is_leader()
            
            # LOG DE DEBUG - siempre mostrar qué se está comparando
            logger.info(
                f"🔍 SPLIT-BRAIN CHECK: Comparing with peer {peer_node.node_id} | "
                f"Peer reports: leader={peer_leader_id}, term={peer_term} | "
                f"I have: leader={my_leader_id}, term={my_term}, am_leader={am_i_leader}"
            )
            
            # Caso 1: Ambos somos líderes
            if am_i_leader and peer_leader_id == peer_node.node_id:
                logger.warning(
                    f"🔴🔴🔴 SPLIT-BRAIN DETECTED! 🔴🔴🔴\n"
                    f"    REASON: Both nodes claim to be leaders\n"
                    f"    My node: {self.node_id} (term={my_term})\n"
                    f"    Peer node: {peer_node.node_id} (term={peer_term})"
                )
                return True
            
            # Caso 2: Yo soy líder pero hay otro líder reportado
            if am_i_leader and peer_leader_id and peer_leader_id != self.node_id:
                if peer_leader_id != my_leader_id:
                    logger.warning(
                        f"🔴🔴🔴 SPLIT-BRAIN DETECTED! 🔴🔴🔴\n"
                        f"    REASON: I am leader but peer reports different leader\n"
                        f"    I am leader: {self.node_id}\n"
                        f"    Peer {peer_node.node_id} says leader is: {peer_leader_id}"
                    )
                    return True
            
            # Caso 3: Líderes diferentes reportados (más flexible)
            if peer_leader_id and my_leader_id and peer_leader_id != my_leader_id:
                logger.warning(
                    f"🔴🔴🔴 SPLIT-BRAIN DETECTED! 🔴🔴🔴\n"
                    f"    REASON: Different leaders in the cluster\n"
                    f"    My leader: {my_leader_id}\n"
                    f"    Peer {peer_node.node_id} says leader is: {peer_leader_id}"
                )
                return True
            
            # Caso 4: El peer cree que es líder pero yo tengo otro líder
            if peer_leader_id == peer_node.node_id and my_leader_id and my_leader_id != peer_leader_id:
                logger.warning(
                    f"🔴🔴🔴 SPLIT-BRAIN DETECTED! 🔴🔴🔴\n"
                    f"    REASON: Peer claims to be leader but I have a different leader\n"
                    f"    My leader: {my_leader_id}\n"
                    f"    Peer {peer_node.node_id} claims to be leader"
                )
                return True
            
            # No hay split-brain
            logger.info(
                f"✅ SPLIT-BRAIN CHECK: No split-brain detected with {peer_node.node_id} "
                f"(same leader: {my_leader_id})"
            )
            return False
    
    def initiate_reconciliation(self, peer_nodes: List[NodeInfo]):
        """Inicia el proceso de reconciliación con los peers."""
        with self._lock:
            if self._reconciliation_in_progress:
                logger.info("⏳ Reconciliation already in progress, skipping")
                return
            
            time_since_last = time.time() - self._last_reconciliation
            if time_since_last < self._reconciliation_cooldown:
                logger.info(f"⏳ Reconciliation cooldown: {self._reconciliation_cooldown - time_since_last:.1f}s remaining")
                return
            
            self._reconciliation_in_progress = True
            self._last_reconciliation = time.time()
        
        logger.warning(
            f"🔄🔄🔄 STARTING SPLIT-BRAIN RECONCILIATION 🔄🔄🔄\n"
            f"    Peers to reconcile: {[p.node_id for p in peer_nodes]}"
        )
        
        threading.Thread(
            target=self._reconciliation_worker,
            args=(peer_nodes,),
            daemon=True
        ).start()
    
    def _reconciliation_worker(self, peer_nodes: List[NodeInfo]):
        """Worker thread que ejecuta la reconciliación"""
        try:
            logger.info("📊 Step 1: Collecting state from all peers...")
            
            # Paso 1: Recolectar estado completo de todos los peers
            peer_states = self._collect_peer_states(peer_nodes)
            
            if not peer_states:
                logger.warning("⚠️ No peers responded, skipping reconciliation")
                return
            
            logger.info("🏆 Step 2: Determining canonical leader...")
            
            # Paso 2: Determinar el líder canónico usando todos los criterios
            canonical_leader = self._determine_canonical_leader(peer_states)
            
            logger.warning(
                f"🏆🏆🏆 CANONICAL LEADER ELECTED: {canonical_leader} 🏆🏆🏆\n"
                f"    I am: {self.node_id}\n"
                f"    Will I be leader? {'YES ✅' if canonical_leader == self.node_id else 'NO ❌'}"
            )
            
            logger.info("🔄 Step 3: Forcing leader election result...")
            
            # Paso 3: Usar el leader_election para forzar el resultado
            self._force_leader_election_result(canonical_leader, peer_states)
            
            logger.info("📦 Step 4: Synchronizing state...")
            
            # Paso 4: Sincronizar estado completo
            if canonical_leader == self.node_id:
                logger.info("📤 I am the canonical leader - merging states from peers...")
                # Soy el líder canónico, merge estados de los demás
                self._merge_all_peer_states(peer_states)
                # *** CRÍTICO: Guardar namespace mergeado al disco ***
                logger.info("💾 Persisting merged namespace to disk...")
                self.metadata_server.namespace._persist_to_disk()
                logger.info("✅ Namespace persisted - snapshot will include all merged files")
                # Replicar mi estado a todos los followers
                self._replicate_state_to_followers(peer_nodes)
            else:
                logger.info(f"📥 I am NOT the leader - synchronizing from {canonical_leader}...")
                # No soy líder, sincronizar desde el líder
                self._synchronize_with_leader(canonical_leader, peer_states)
            
            # Verificar estado final
            if canonical_leader == self.node_id:
                final_files = len(self.metadata_server.namespace._namespace)
                logger.warning(
                    f"✅✅✅ SPLIT-BRAIN RECONCILIATION COMPLETED ✅✅✅\n"
                    f"    Final leader: {canonical_leader}\n"
                    f"    My role: LEADER\n"
                    f"    Final namespace: {final_files} files\n"
                    f"    State replicated to all followers"
                )
            else:
                logger.warning(
                    f"✅✅✅ SPLIT-BRAIN RECONCILIATION COMPLETED ✅✅✅\n"
                    f"    Final leader: {canonical_leader}\n"
                    f"    My role: FOLLOWER\n"
                    f"    Synchronized with leader successfully"
                )
            
        except Exception as e:
            logger.error(f"❌❌❌ ERROR during reconciliation: {e}", exc_info=True)
        finally:
            with self._lock:
                self._reconciliation_in_progress = False
    
    def _collect_peer_states(self, peer_nodes: List[NodeInfo]) -> Dict[str, Dict]:
        """Recolecta el estado completo de cada peer."""
        peer_states = {}
        
        for peer in peer_nodes:
            try:
                # Solicitar snapshot completo
                msg = RPCMessage(
                    MessageType.REPL_SNAPSHOT,
                    {'request_type': 'full_snapshot', 'requester_id': self.node_id}
                )
                
                response = self._rpc_client.call(peer.host, peer.port, msg)
                
                if response and response.payload:
                    snapshot = response.payload.get('snapshot', {})
                    replica_state = snapshot.get('replicas', {})
                    
                    peer_states[peer.node_id] = {
                        'node': peer,
                        'term': response.payload.get('term', 0),
                        'leader_id': response.payload.get('leader_id'),
                        'commit_index': response.payload.get('commit_index', -1),
                        'file_count': len(snapshot.get('namespace', {}).get('namespace', {})),
                        'storage_count': len(replica_state.get('storage_nodes', {})),
                        'snapshot': snapshot
                    }
                    
                    logger.info(
                        f"📊 Collected state from {peer.node_id}: "
                        f"term={peer_states[peer.node_id]['term']}, "
                        f"files={peer_states[peer.node_id]['file_count']}, "
                        f"storages={peer_states[peer.node_id]['storage_count']}"
                    )
            except Exception as e:
                logger.warning(f"Failed to collect state from {peer.node_id}: {e}")
        
        return peer_states
    
    def _determine_canonical_leader(self, peer_states: Dict[str, Dict]) -> str:
        """
        Determina el líder canónico usando criterios claros.
        
        Criterios (en orden):
        1. Mayor número total de datos (storage_count * 10 + file_count)
        2. Mayor commit_index
        3. Mayor término
        4. Menor node_id (desempate lexicográfico)
        """
        # Incluir mi propio estado
        my_data_state = self.metadata_server._get_data_state()
        
        all_candidates = [{
            'node_id': self.node_id,
            'term': self.metadata_server.leader_election.get_term(),
            'commit_index': my_data_state.get('commit_index', -1),
            'file_count': my_data_state.get('file_count', 0),
            'storage_count': my_data_state.get('storage_count', 0),
            'is_leader': self.metadata_server.leader_election.is_leader()
        }]
        
        for node_id, state in peer_states.items():
            all_candidates.append({
                'node_id': node_id,
                'term': state.get('term', 0),
                'commit_index': state.get('commit_index', -1),
                'file_count': state.get('file_count', 0),
                'storage_count': state.get('storage_count', 0),
                'is_leader': state.get('leader_id') == node_id
            })
        
        # Calcular score para cada candidato
        for c in all_candidates:
            c['score'] = (
                c['storage_count'] * 100 +  # Storage nodes son muy importantes
                c['file_count'] * 10 +
                c['commit_index'] +
                c['term'] * 1000  # Término tiene peso alto
            )
        
        # Ordenar: mayor score primero, menor node_id como desempate final
        all_candidates.sort(key=lambda x: (-x['score'], x['node_id']))
        
        winner = all_candidates[0]['node_id']
        
        logger.info(f"📊 Leader candidates by score:")
        for c in all_candidates[:5]:  # Mostrar top 5
            logger.info(
                f"  {c['node_id']}: score={c['score']} "
                f"(storages={c['storage_count']}, files={c['file_count']}, "
                f"term={c['term']}, commit={c['commit_index']})"
            )
        
        return winner
    
    def _force_leader_election_result(self, canonical_leader: str, peer_states: Dict[str, Dict]):
        """Fuerza el resultado de la elección de líder."""
        
        # Preparar estados para el force_reconciliation
        election_states = {}
        for node_id, state in peer_states.items():
            election_states[node_id] = {
                'term': state.get('term', 0),
                'data_state': {
                    'file_count': state.get('file_count', 0),
                    'storage_count': state.get('storage_count', 0),
                    'commit_index': state.get('commit_index', -1)
                },
                'is_leader': state.get('leader_id') == node_id
            }
        
        # Llamar a force_reconciliation del leader_election
        result = self.metadata_server.leader_election.force_reconciliation(election_states)
        
        logger.info(f"🔄 Leader election forced to: {result}")
    
    def _merge_all_peer_states(self, peer_states: Dict[str, Dict]):
        """Merge completo de estados de todos los peers."""
        logger.info(f"🔀 Merging complete states from {len(peer_states)} peers")

        all_storage_nodes = {}
        all_replicas = {}

        # PASO 1: Recolectar TODOS los namespaces (incluyendo el del líder canónico)
        all_namespaces = self._collect_all_namespaces(peer_states)

        for peer_id, peer_state in peer_states.items():
            try:
                snapshot = peer_state.get('snapshot', {})

                # Recolectar storage nodes
                replica_state = snapshot.get('replicas', {})
                peer_storage_nodes = replica_state.get('storage_nodes', {})
                for node_id, node_dict in peer_storage_nodes.items():
                    if node_id not in all_storage_nodes:
                        all_storage_nodes[node_id] = node_dict

                # Recolectar replicas
                peer_replicas = replica_state.get('replicas', {})
                for file_id, replicas in peer_replicas.items():
                    if file_id not in all_replicas:
                        all_replicas[file_id] = replicas
                    else:
                        # Merge replica lists
                        existing_nodes = {r.get('node_id') for r in all_replicas[file_id]}
                        for r in replicas:
                            if r.get('node_id') not in existing_nodes:
                                all_replicas[file_id].append(r)

            except Exception as e:
                logger.error(f"Error merging state from {peer_id}: {e}")

        # PASO 2: Crear namespace unificado completo
        unified_namespace = self._create_unified_namespace(all_namespaces)

        # PASO 3: Reemplazar completamente el namespace del líder con el unificado
        self._replace_namespace_with_unified(unified_namespace)

        # Aplicar storage nodes
        added_storage = 0
        for node_id, node_dict in all_storage_nodes.items():
            if node_id not in self.metadata_server.replica_manager._storage_nodes:
                try:
                    node = NodeInfo.from_dict(node_dict)
                    node.state = NodeState.UP
                    self.metadata_server.replica_manager.register_storage_node(node)
                    self.metadata_server.heartbeat_manager.register_node(node)
                    added_storage += 1
                    logger.info(f"➕ Added storage node {node_id} from reconciliation")
                except Exception as e:
                    logger.warning(f"Failed to add storage node {node_id}: {e}")

        # Aplicar replicas
        for file_id, replicas in all_replicas.items():
            try:
                self.metadata_server.replica_manager.apply_replicas_state(file_id, replicas)
            except Exception as e:
                logger.warning(f"Failed to apply replicas for {file_id}: {e}")

        # Contar archivos totales después del merge
        final_file_count = len(self.metadata_server.namespace._namespace)
        logger.info(f"✅ Merge completed: {added_storage} storage nodes added, {final_file_count} total files in unified namespace")

    def _collect_all_namespaces(self, peer_states: Dict[str, Dict]) -> Dict[str, Dict]:
        """Recolecta TODOS los namespaces actuales de todos los nodos (incluyendo el líder canónico)."""
        all_namespaces = {}

        # Incluir el namespace del líder canónico
        leader_namespace = self.metadata_server.namespace._namespace
        all_namespaces[self.node_id] = {
            path: meta.to_dict() for path, meta in leader_namespace.items()
        }
        logger.info(f"📦 Collected {len(leader_namespace)} files from leader {self.node_id}")

        # Consultar namespace ACTUAL de todos los peers (no snapshots guardados)
        for peer_id, peer_state in peer_states.items():
            try:
                peer_node = peer_state.get('node')
                if not peer_node:
                    logger.warning(f"⚠️ No node info for peer {peer_id}")
                    all_namespaces[peer_id] = {}
                    continue

                # Consultar namespace actual del peer
                msg = RPCMessage(MessageType.GET_CURRENT_NAMESPACE, {})
                response = self._rpc_client.call(peer_node.host, peer_node.port, msg)

                if response and response.payload.get('namespace'):
                    peer_namespace = response.payload['namespace']
                    all_namespaces[peer_id] = peer_namespace
                    logger.info(f"📦 Collected {len(peer_namespace)} files from peer {peer_id} (current)")
                else:
                    logger.warning(f"⚠️ No current namespace response from peer {peer_id}")
                    all_namespaces[peer_id] = {}

            except Exception as e:
                logger.error(f"Error collecting current namespace from {peer_id}: {e}")
                all_namespaces[peer_id] = {}

        total_files = sum(len(ns) for ns in all_namespaces.values())
        logger.info(f"📊 Collected current namespaces from {len(all_namespaces)} nodes ({total_files} total files)")
        return all_namespaces

    def _create_unified_namespace(self, all_namespaces: Dict[str, Dict]) -> Dict[str, Dict]:
        """Crea un namespace unificado que contiene TODOS los archivos de todos los nodos."""
        unified = {}
        conflicts_handled = 0

        logger.info("🔄 Creating unified namespace from all node namespaces...")

        # Procesar cada namespace fuente
        for source_node, namespace in all_namespaces.items():
            logger.debug(f"Processing {len(namespace)} files from {source_node}")

            for path, file_data in namespace.items():
                try:
                    file_meta = FileMetadata.from_dict(file_data)

                    if path not in unified:
                        # Archivo nuevo - agregarlo directamente
                        unified[path] = file_data
                        logger.debug(f"➕ Added {path} from {source_node}")
                    else:
                        # Archivo existente - verificar conflicto
                        existing_meta = FileMetadata.from_dict(unified[path])

                        if self._is_conflicting_file(existing_meta, file_meta):
                            # CONFLICTO - Crear versiones v1 y v2
                            conflicts_handled += 1
                            unified = self._resolve_conflict_in_unified(path, existing_meta, file_meta, source_node, unified)
                            logger.info(f"🔀 Resolved conflict for {path} - created v1/v2 versions")
                        elif file_meta.modified_at > existing_meta.modified_at:
                            # Versión más nueva - actualizar
                            unified[path] = file_data
                            logger.debug(f"📝 Updated {path} with newer version from {source_node}")
                        # Si la existente es más nueva, mantenerla

                except Exception as e:
                    logger.warning(f"Error processing file {path} from {source_node}: {e}")

        logger.info(f"✅ Unified namespace created: {len(unified)} files, {conflicts_handled} conflicts resolved")
        return unified

    def _resolve_conflict_in_unified(self, path: str, meta1: FileMetadata, meta2: FileMetadata, source_node: str, unified: Dict[str, Dict]) -> Dict[str, Dict]:
        """Resuelve conflictos en el namespace unificado creando versiones v1/v2."""
        # Crear versiones con nombres únicos
        if '/' in path:
            dir_path = path.rsplit('/', 1)[0]
            filename = path.rsplit('/', 1)[1]
        else:
            dir_path = '/'
            filename = path

        if '.' in filename:
            name_base, extension = filename.rsplit('.', 1)
            extension = '.' + extension
        else:
            name_base = filename
            extension = ''

        # Crear v1 (del namespace existente)
        v1_name = f"{name_base}_v1{extension}"
        v1_path = f"{dir_path}/{v1_name}" if dir_path != '/' else f"/{v1_name}"
        v1_meta = FileMetadata(
            file_id=meta1.file_id,
            path=v1_path,
            name=v1_name,
            size=meta1.size,
            owner=meta1.owner,
            group=meta1.group,
            permissions=meta1.permissions,
            version=meta1.version,
            created_at=meta1.created_at,
            modified_at=meta1.modified_at,
            is_directory=False,
            replicas=meta1.replicas,
            checksum=meta1.checksum
        )

        # Crear v2 (del nuevo source)
        v2_name = f"{name_base}_v2_{source_node}{extension}"
        v2_path = f"{dir_path}/{v2_name}" if dir_path != '/' else f"/{v2_name}"
        v2_meta = FileMetadata(
            file_id=meta2.file_id,
            path=v2_path,
            name=v2_name,
            size=meta2.size,
            owner=meta2.owner,
            group=meta2.group,
            permissions=meta2.permissions,
            version=meta2.version,
            created_at=meta2.created_at,
            modified_at=meta2.modified_at,
            is_directory=False,
            replicas=meta2.replicas,
            checksum=meta2.checksum
        )

        # Agregar las versiones al namespace unificado
        unified[v1_path] = v1_meta.to_dict()
        unified[v2_path] = v2_meta.to_dict()

        # Remover el archivo original conflictivo
        if path in unified:
            del unified[path]

        logger.info(f"✅ Created conflict versions: {v1_path} and {v2_path}")
        return unified

    def _replace_namespace_with_unified(self, unified_namespace: Dict[str, Dict]):
        """Reemplaza completamente el namespace del líder con el namespace unificado."""
        logger.info(f"🔄 Replacing leader namespace with unified namespace ({len(unified_namespace)} files)")

        try:
            # Limpiar el namespace actual
            self.metadata_server.namespace._namespace.clear()
            self.metadata_server.namespace._id_index.clear()

            # Agregar todos los archivos del namespace unificado
            for path, file_data in unified_namespace.items():
                try:
                    file_meta = FileMetadata.from_dict(file_data)
                    self.metadata_server.namespace._namespace[path] = file_meta
                    self.metadata_server.namespace._id_index[file_meta.file_id] = path
                except Exception as e:
                    logger.warning(f"Error adding unified file {path}: {e}")

            logger.info(f"✅ Leader namespace replaced with {len(unified_namespace)} unified files")

        except Exception as e:
            logger.error(f"Error replacing namespace with unified: {e}")
            raise

    def _replicate_state_to_followers(self, peer_nodes: List[NodeInfo]):
        """Replica el namespace unificado del líder a todos los followers disponibles."""
        logger.info(f"📤 Replicating unified namespace to {len(peer_nodes)} followers")

        # Crear snapshot con el namespace unificado que ya tiene el líder
        snapshot = self.metadata_server._create_snapshot()

        successful_sends = 0

        for peer in peer_nodes:
            try:
                msg = RPCMessage(
                    MessageType.REPL_SNAPSHOT,
                    {
                        'snapshot': snapshot,
                        'from_leader': True,
                        'force_install': True
                    }
                )
                response = self._rpc_client.call(peer.host, peer.port, msg)

                if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
                    logger.info(f"✅ Unified namespace sent to {peer.node_id}")
                    successful_sends += 1
                else:
                    logger.warning(f"❌ Failed to send unified namespace to {peer.node_id}")

            except Exception as e:
                logger.error(f"❌ Error sending to {peer.node_id}: {e}")

        logger.info(f"📊 Replication complete: {successful_sends}/{len(peer_nodes)} followers received unified namespace")
    
    def _merge_peer_namespace(self, peer_id: str, peer_state: Dict):
        """Merge el namespace de un peer con el nuestro."""
        snapshot = peer_state.get('snapshot', {})
        peer_namespace = snapshot.get('namespace', {}).get('namespace', {})
        
        if not peer_namespace:
            return
        
        logger.info(f"📁 Merging {len(peer_namespace)} namespace entries from {peer_id}")
        
        my_namespace = self.metadata_server.namespace._namespace
        
        files_added = 0
        files_updated = 0
        conflicts_resolved = 0

        for path, peer_meta_dict in peer_namespace.items():
            try:
                peer_meta = FileMetadata.from_dict(peer_meta_dict)

                if path in my_namespace:
                    my_meta = my_namespace[path]

                    # Detectar conflicto
                    if self._is_conflicting_file(my_meta, peer_meta):
                        self._resolve_file_conflict(path, my_meta, peer_meta, peer_id)
                        conflicts_resolved += 1
                        logger.debug(f"🔀 Resolved conflict for {path} from {peer_id}")
                    elif peer_meta.modified_at > my_meta.modified_at:
                        self.metadata_server.namespace.upsert_entry(peer_meta)
                        files_updated += 1
                        logger.debug(f"📝 Updated {path} with newer version from {peer_id}")
                    # Si no hay conflicto y mi versión es más nueva, mantener la mía
                else:
                    self.metadata_server.namespace.upsert_entry(peer_meta)
                    files_added += 1
                    logger.debug(f"➕ Added new file {path} from {peer_id}")

            except Exception as e:
                logger.warning(f"Error merging namespace entry {path}: {e}")

        logger.info(f"📁 Namespace merge from {peer_id}: {files_added} added, {files_updated} updated, {conflicts_resolved} conflicts resolved")
    
    def _is_conflicting_file(self, meta1: FileMetadata, meta2: FileMetadata) -> bool:
        """Determina si dos archivos están en conflicto."""
        if meta1.is_directory or meta2.is_directory:
            return False
        
        if meta1.checksum and meta2.checksum and meta1.checksum != meta2.checksum:
            return True
        
        if meta1.size != meta2.size:
            time_diff = abs(meta1.modified_at - meta2.modified_at)
            if time_diff < 300:
                return True
        
        return False
    
    def _resolve_file_conflict(
        self, 
        path: str, 
        my_meta: FileMetadata, 
        peer_meta: FileMetadata,
        peer_id: str
    ) -> Dict:
        """Resuelve conflictos de archivos creando versiones múltiples."""
        logger.warning(f"🔀 FILE CONFLICT detected for {path}")
        
        if '/' in path:
            dir_path = path.rsplit('/', 1)[0]
            filename = path.rsplit('/', 1)[1]
        else:
            dir_path = '/'
            filename = path
        
        if '.' in filename:
            name_base, extension = filename.rsplit('.', 1)
            extension = '.' + extension
        else:
            name_base = filename
            extension = ''
        
        # Crear versiones
        v1_name = f"{name_base}_v1_{self.node_id}{extension}"
        v1_path = f"{dir_path}/{v1_name}" if dir_path != '/' else f"/{v1_name}"
        v1_meta = FileMetadata(
            file_id=my_meta.file_id,
            path=v1_path,
            name=v1_name,
            size=my_meta.size,
            owner=my_meta.owner,
            group=my_meta.group,
            permissions=my_meta.permissions,
            version=my_meta.version,
            created_at=my_meta.created_at,
            modified_at=my_meta.modified_at,
            is_directory=False,
            replicas=my_meta.replicas,
            checksum=my_meta.checksum
        )
        
        v2_name = f"{name_base}_v2_{peer_id}{extension}"
        v2_path = f"{dir_path}/{v2_name}" if dir_path != '/' else f"/{v2_name}"
        v2_meta = FileMetadata(
            file_id=peer_meta.file_id,
            path=v2_path,
            name=v2_name,
            size=peer_meta.size,
            owner=peer_meta.owner,
            group=peer_meta.group,
            permissions=peer_meta.permissions,
            version=peer_meta.version,
            created_at=peer_meta.created_at,
            modified_at=peer_meta.modified_at,
            is_directory=False,
            replicas=peer_meta.replicas,
            checksum=peer_meta.checksum
        )
        
        self.metadata_server.namespace.upsert_entry(v1_meta)
        self.metadata_server.namespace.upsert_entry(v2_meta)
        
        try:
            self.metadata_server.namespace.delete_entry(path)
        except:
            pass
        
        logger.info(f"✅ Created versioned files: {v1_path} and {v2_path}")
        
        return {
            'original_path': path,
            'v1_path': v1_path,
            'v2_path': v2_path
        }
    
    def _synchronize_with_leader(self, leader_id: str, peer_states: Dict[str, Dict]):
        """Sincroniza mi estado con el líder canónico."""
        if leader_id == self.node_id:
            return
        
        logger.info(f"📥 Synchronizing with canonical leader {leader_id}")
        
        if leader_id not in peer_states:
            logger.warning(f"Cannot sync: leader {leader_id} not in peer_states")
            return
        
        leader_state = peer_states[leader_id]
        leader_node = leader_state['node']
        
        try:
            msg = RPCMessage(
                MessageType.REPL_SNAPSHOT,
                {
                    'request_type': 'full_snapshot',
                    'requester_id': self.node_id,
                    'force_sync': True
                }
            )
            
            response = self._rpc_client.call(leader_node.host, leader_node.port, msg)
            
            if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
                snapshot = response.payload.get('snapshot')
                if snapshot:
                    logger.info("📦 Installing snapshot from canonical leader")
                    self.metadata_server._install_snapshot(snapshot)
                    logger.info("✅ Synchronized with leader successfully")
            else:
                logger.error("Failed to get snapshot from leader")
                
        except Exception as e:
            logger.error(f"Error synchronizing with leader: {e}")
    
    def handle_peer_reconnect(self, peer_node: NodeInfo, peer_info: Dict):
        """Llamado cuando un peer se reconecta después de una posible partición."""
        peer_term = peer_info.get('term', 0)
        peer_leader_id = peer_info.get('leader_id')
        
        logger.info(
            f"🔗 PEER RECONNECT DETECTED: {peer_node.node_id}\n"
            f"    Checking for potential split-brain..."
        )
        
        # Detectar split-brain
        if self.detect_split_brain(peer_node, peer_term, peer_leader_id):
            logger.warning(f"⚠️ Split-brain confirmed! Initiating reconciliation...")
            
            # Obtener todos los peers conocidos
            all_peers = []
            with self.metadata_server.leader_election._lock:
                all_peers = list(self.metadata_server.leader_election._peers.values())
            
            # Iniciar reconciliación
            self.initiate_reconciliation(all_peers)
        else:
            logger.info(f"✅ No split-brain with {peer_node.node_id} - no reconciliation needed")
