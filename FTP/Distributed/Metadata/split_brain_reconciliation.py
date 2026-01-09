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
from ..Common.constants import NodeType, MessageType, DistributedResponseCode
from ..Common.rpc_protocol import RPCClient, RPCMessage

logger = logging.getLogger(__name__)


class SplitBrainReconciliation:
    """
    Maneja la detección y reconciliación de split-brain.
    
    Cuando la red se particiona, cada partición puede elegir su propio líder
    y continuar operando. Al reconectar, este módulo:
    1. Detecta la presencia de múltiples líderes
    2. Resuelve el conflicto (líder con menor ID o mayor término gana)
    3. Sincroniza y merge los oplogs divergentes
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
        self._reconciliation_cooldown = 10  # segundos
        
        # Historial de términos vistos (para detectar split-brain)
        self._seen_terms: Dict[str, int] = {}  # node_id -> term
        
    def detect_split_brain(self, peer_node: NodeInfo, peer_term: int, peer_leader_id: str) -> bool:
        """
        Detecta si hay un split-brain comparando el estado del peer con el nuestro.
        
        Returns:
            True si se detecta split-brain (múltiples líderes activos)
        """
        with self._lock:
            my_term = self.metadata_server.leader_election.get_term()
            my_leader_id = self.metadata_server.leader_election.get_leader_id()
            am_i_leader = self.metadata_server.leader_election.is_leader()
            
            # Casos de split-brain:
            # 1. Ambos somos líderes con el mismo término
            if am_i_leader and peer_leader_id == peer_node.node_id and peer_term == my_term:
                logger.warning(
                    f"🔴 SPLIT-BRAIN DETECTED: Both {self.node_id} and {peer_node.node_id} "
                    f"are leaders in term {my_term}"
                )
                return True
            
            # 2. Yo soy líder pero hay otro líder con término similar
            if am_i_leader and peer_leader_id != self.node_id and peer_leader_id != my_leader_id:
                if abs(peer_term - my_term) <= 2:  # Términos cercanos = partición reciente
                    logger.warning(
                        f"🔴 SPLIT-BRAIN DETECTED: I am leader (term {my_term}) but peer "
                        f"reports different leader {peer_leader_id} (term {peer_term})"
                    )
                    return True
            
            # 3. Términos divergentes con líderes diferentes
            if peer_leader_id and my_leader_id and peer_leader_id != my_leader_id:
                if peer_term >= my_term - 1:  # Dentro de 1 término = posible partición
                    logger.warning(
                        f"🔴 SPLIT-BRAIN DETECTED: Different leaders with close terms. "
                        f"My leader: {my_leader_id} (term {my_term}), "
                        f"Peer leader: {peer_leader_id} (term {peer_term})"
                    )
                    return True
            
            return False
    
    def initiate_reconciliation(self, peer_nodes: List[NodeInfo]):
        """
        Inicia el proceso de reconciliación con los peers.
        """
        with self._lock:
            # Evitar reconciliaciones concurrentes
            if self._reconciliation_in_progress:
                logger.info("Reconciliation already in progress, skipping")
                return
            
            # Cooldown para evitar reconciliaciones frecuentes
            time_since_last = time.time() - self._last_reconciliation
            if time_since_last < self._reconciliation_cooldown:
                logger.debug(f"Reconciliation cooldown: {self._reconciliation_cooldown - time_since_last:.1f}s remaining")
                return
            
            self._reconciliation_in_progress = True
            self._last_reconciliation = time.time()
        
        logger.info(f"🔄 Starting split-brain reconciliation with {len(peer_nodes)} peers")
        
        threading.Thread(
            target=self._reconciliation_worker,
            args=(peer_nodes,),
            daemon=True
        ).start()
    
    def _reconciliation_worker(self, peer_nodes: List[NodeInfo]):
        """Worker thread que ejecuta la reconciliación"""
        try:
            # Paso 1: Recolectar estado de todos los peers
            peer_states = self._collect_peer_states(peer_nodes)
            
            if not peer_states:
                logger.info("No peers responded, skipping reconciliation")
                return
            
            # Paso 2: Determinar el líder canónico
            canonical_leader = self._determine_canonical_leader(peer_states)
            
            logger.info(f"✅ Canonical leader determined: {canonical_leader}")
            
            # Paso 3: Si no soy el líder canónico, ceder
            if canonical_leader != self.node_id:
                self._step_down_as_leader(canonical_leader, peer_states)
            else:
                # Soy el líder canónico, merge estados de los demás
                self._merge_peer_states(peer_states)
            
            # Paso 4: Sincronizar con el nuevo líder
            self._synchronize_with_leader(canonical_leader, peer_states)
            
            logger.info("✅ Split-brain reconciliation completed successfully")
            
        except Exception as e:
            logger.error(f"Error during reconciliation: {e}", exc_info=True)
        finally:
            with self._lock:
                self._reconciliation_in_progress = False
    
    def _collect_peer_states(self, peer_nodes: List[NodeInfo]) -> Dict[str, Dict]:
        """
        Recolecta el estado actual de cada peer (oplog, namespace, término, líder).
        """
        peer_states = {}
        
        for peer in peer_nodes:
            try:
                # Consultar estado del peer
                msg = RPCMessage(
                    MessageType.REPL_SNAPSHOT,
                    {'request_type': 'state_summary', 'requester_id': self.node_id}
                )
                
                response = self._rpc_client.call(peer.host, peer.port, msg, timeout=5)
                
                if response and response.payload:
                    peer_states[peer.node_id] = {
                        'node': peer,
                        'term': response.payload.get('term', 0),
                        'leader_id': response.payload.get('leader_id'),
                        'commit_index': response.payload.get('commit_index', -1),
                        'oplog_length': response.payload.get('oplog_length', 0),
                        'last_applied': response.payload.get('last_applied', -1),
                        'file_count': response.payload.get('file_count', 0),
                        'snapshot': response.payload.get('snapshot', {})
                    }
                    logger.info(
                        f"Collected state from {peer.node_id}: "
                        f"term={peer_states[peer.node_id]['term']}, "
                        f"leader={peer_states[peer.node_id]['leader_id']}, "
                        f"files={peer_states[peer.node_id]['file_count']}"
                    )
            except Exception as e:
                logger.warning(f"Failed to collect state from {peer.node_id}: {e}")
        
        return peer_states
    
    def _determine_canonical_leader(self, peer_states: Dict[str, Dict]) -> str:
        """
        Determina cuál debe ser el líder canónico después de la reconciliación.
        
        Criterios (en orden de prioridad):
        1. Nodo con mayor término
        2. Si hay empate en término, el que tenga más operaciones (commit_index mayor)
        3. Si aún hay empate, el nodo con menor ID (lexicográfico)
        """
        # Incluir mi propio estado
        my_state = {
            'term': self.metadata_server.leader_election.get_term(),
            'commit_index': self.metadata_server._commit_index,
            'leader_id': self.metadata_server.leader_election.get_leader_id()
        }
        
        all_states = {self.node_id: my_state}
        all_states.update({nid: state for nid, state in peer_states.items()})
        
        # Solo considerar nodos que se consideran líderes
        leader_candidates = []
        for node_id, state in all_states.items():
            term = state.get('term', 0)
            commit_index = state.get('commit_index', -1)
            leader_id = state.get('leader_id')
            
            # Si este nodo se considera líder, es candidato
            if leader_id == node_id:
                leader_candidates.append((node_id, term, commit_index))
        
        if not leader_candidates:
            # Nadie es líder, elegir el de mayor término
            leader_candidates = [
                (node_id, state.get('term', 0), state.get('commit_index', -1))
                for node_id, state in all_states.items()
            ]
        
        # Ordenar por: término desc, commit_index desc, node_id asc
        leader_candidates.sort(key=lambda x: (-x[1], -x[2], x[0]))
        
        canonical = leader_candidates[0][0]
        
        logger.info(
            f"Leader candidates: {leader_candidates}. "
            f"Selected canonical leader: {canonical}"
        )
        
        return canonical
    
    def _step_down_as_leader(self, new_leader_id: str, peer_states: Dict[str, Dict]):
        """
        Si yo era líder pero perdí la reconciliación, cedo el liderazgo.
        """
        if not self.metadata_server.leader_election.is_leader():
            return
        
        logger.warning(
            f"⚠️ Stepping down as leader. New canonical leader: {new_leader_id}"
        )
        
        # Actualizar mi estado para reconocer al nuevo líder
        with self.metadata_server.leader_election._lock:
            self.metadata_server.leader_election._is_leader = False
            self.metadata_server.leader_election._current_leader = new_leader_id
            
            # Usar el término del nuevo líder
            if new_leader_id in peer_states:
                new_term = peer_states[new_leader_id].get('term', 0)
                if new_term > self.metadata_server.leader_election._term:
                    self.metadata_server.leader_election._term = new_term
    
    def _merge_peer_states(self, peer_states: Dict[str, Dict]):
        """
        Como líder canónico, merge los estados de los peers que operaron
        durante la partición.
        """
        logger.info(f"🔀 Merging states from {len(peer_states)} peers")
        
        for peer_id, peer_state in peer_states.items():
            try:
                # Merge namespace (archivos)
                self._merge_peer_namespace(peer_id, peer_state)
                
                # Merge oplog si está disponible
                self._merge_peer_oplog(peer_id, peer_state)
                
            except Exception as e:
                logger.error(f"Error merging state from {peer_id}: {e}")
    
    def _merge_peer_namespace(self, peer_id: str, peer_state: Dict):
        """
        Merge el namespace de un peer con el nuestro, manejando conflictos.
        """
        snapshot = peer_state.get('snapshot', {})
        peer_namespace = snapshot.get('namespace', {}).get('namespace', {})
        
        if not peer_namespace:
            return
        
        logger.info(f"Merging {len(peer_namespace)} entries from {peer_id}")
        
        my_namespace = self.metadata_server.namespace._namespace
        conflicts = []
        
        for path, peer_meta_dict in peer_namespace.items():
            peer_meta = FileMetadata.from_dict(peer_meta_dict)
            
            if path in my_namespace:
                my_meta = my_namespace[path]
                
                # Detectar conflicto: mismo path pero diferente contenido
                if self._is_conflicting_file(my_meta, peer_meta):
                    conflict_info = self._resolve_file_conflict(path, my_meta, peer_meta, peer_id)
                    conflicts.append(conflict_info)
                else:
                    # No hay conflicto, usar la versión más reciente
                    if peer_meta.modified_at > my_meta.modified_at:
                        logger.info(f"Updating {path} with newer version from {peer_id}")
                        self.metadata_server.namespace.upsert_entry(peer_meta)
            else:
                # Archivo no existe en mi namespace, agregarlo
                logger.info(f"Adding new file {path} from {peer_id}")
                self.metadata_server.namespace.upsert_entry(peer_meta)
        
        if conflicts:
            logger.warning(f"Resolved {len(conflicts)} file conflicts during merge")
    
    def _is_conflicting_file(self, meta1: FileMetadata, meta2: FileMetadata) -> bool:
        """
        Determina si dos archivos están en conflicto.
        Conflicto = mismo path pero diferente contenido (checksum o tamaño).
        """
        # Directorios no tienen conflictos de contenido
        if meta1.is_directory or meta2.is_directory:
            return False
        
        # Si tienen checksums diferentes, es conflicto
        if meta1.checksum and meta2.checksum and meta1.checksum != meta2.checksum:
            return True
        
        # Si tienen tamaños diferentes y timestamps similares, es conflicto
        if meta1.size != meta2.size:
            time_diff = abs(meta1.modified_at - meta2.modified_at)
            if time_diff < 300:  # Modificados dentro de 5 minutos = conflicto
                return True
        
        return False
    
    def _resolve_file_conflict(
        self, 
        path: str, 
        my_meta: FileMetadata, 
        peer_meta: FileMetadata,
        peer_id: str
    ) -> Dict:
        """
        Resuelve conflictos de archivos creando versiones múltiples.
        
        Si /path/file.txt tiene conflicto, se crean:
        - /path/file_v1_metadata1.txt (versión de metadata1)
        - /path/file_v2_metadata2.txt (versión de metadata2)
        """
        logger.warning(
            f"🔀 FILE CONFLICT detected for {path}. "
            f"Creating versioned copies."
        )
        
        # Extraer directorio y nombre base
        if '/' in path:
            dir_path = path.rsplit('/', 1)[0]
            filename = path.rsplit('/', 1)[1]
        else:
            dir_path = '/'
            filename = path
        
        # Separar nombre y extensión
        if '.' in filename:
            name_base, extension = filename.rsplit('.', 1)
            extension = '.' + extension
        else:
            name_base = filename
            extension = ''
        
        # Crear versión 1 (mi versión)
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
        
        # Crear versión 2 (versión del peer)
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
        
        # Registrar ambas versiones
        self.metadata_server.namespace.upsert_entry(v1_meta)
        self.metadata_server.namespace.upsert_entry(v2_meta)
        
        # Eliminar el archivo original conflictivo
        try:
            self.metadata_server.namespace.delete_entry(path)
        except:
            pass
        
        logger.info(
            f"✅ Created versioned files: {v1_path} and {v2_path}"
        )
        
        return {
            'original_path': path,
            'v1_path': v1_path,
            'v2_path': v2_path,
            'my_node': self.node_id,
            'peer_node': peer_id
        }
    
    def _synchronize_with_leader(self, leader_id: str, peer_states: Dict[str, Dict]):
        """
        Sincroniza mi estado con el líder canónico.
        """
        if leader_id == self.node_id:
            # Soy el líder, no necesito sincronizar
            return
        
        logger.info(f"Synchronizing with canonical leader {leader_id}")
        
        if leader_id not in peer_states:
            logger.warning(f"Cannot sync: leader {leader_id} not in peer_states")
            return
        
        leader_state = peer_states[leader_id]
        leader_node = leader_state['node']
        
        try:
            # Solicitar snapshot completo del líder
            msg = RPCMessage(
                MessageType.REPL_SNAPSHOT,
                {
                    'request_type': 'full_snapshot',
                    'requester_id': self.node_id,
                    'my_commit_index': self.metadata_server._commit_index
                }
            )
            
            response = self._rpc_client.call(leader_node.host, leader_node.port, msg, timeout=30)
            
            if response and response.payload.get('status') == DistributedResponseCode.SUCCESS.value:
                snapshot = response.payload.get('snapshot')
                if snapshot:
                    logger.info("Installing snapshot from canonical leader")
                    self.metadata_server._install_snapshot(snapshot)
                    logger.info("✅ Synchronized with leader successfully")
            else:
                logger.error("Failed to get snapshot from leader")
                
        except Exception as e:
            logger.error(f"Error synchronizing with leader: {e}")
    
    def _merge_peer_oplog(self, peer_id: str, peer_state: Dict):
        """
        Merge el oplog de un peer con el nuestro.
        
        Estrategia:
        1. Recolectar operaciones del peer que no están en nuestro log
        2. Aplicar operaciones en orden de timestamp
        3. Detectar y resolver conflictos de operaciones
        """
        snapshot = peer_state.get('snapshot', {})
        
        # Por ahora, la reconciliación se hace principalmente via namespace merge
        # El oplog es más complejo de mergear porque requiere ordenamiento temporal
        # y detección de operaciones conflictivas
        
        # TODO: Implementar merge de oplog completo si se necesita granularidad fina
        # Por ahora, el merge de namespace es suficiente para el caso de uso
        logger.debug(f"Oplog merge for {peer_id} handled via namespace merge")
    
    def handle_peer_reconnect(self, peer_node: NodeInfo, peer_info: Dict):
        """
        Llamado cuando un peer se reconecta después de una posible partición.
        
        Args:
            peer_node: Información del peer que se reconectó
            peer_info: Información del peer (term, leader_id, etc.)
        """
        peer_term = peer_info.get('term', 0)
        peer_leader_id = peer_info.get('leader_id')
        
        # Detectar split-brain
        if self.detect_split_brain(peer_node, peer_term, peer_leader_id):
            # Obtener todos los peers conocidos
            all_peers = []
            with self.metadata_server.leader_election._lock:
                all_peers = list(self.metadata_server.leader_election._peers.values())
            
            # Iniciar reconciliación
            self.initiate_reconciliation(all_peers)

