"""
Elección de líder para el servicio de Metadata.
Implementa un algoritmo de elección de líder robusto que evita oscilaciones.
"""
import time
import random
import threading
import logging
from typing import Dict, Optional, List, Callable, Tuple
from ..Common.models import NodeInfo
from ..Common.constants import (
    NodeState, NodeType, MessageType,
    LEADER_ELECTION_TIMEOUT, HEARTBEAT_INTERVAL, LEADER_LEASE_TIME
)
from ..Common.rpc_protocol import RPCClient, RPCMessage

logger = logging.getLogger(__name__)

# Constantes adicionales para estabilidad
LEADER_STABILITY_PERIOD = 15  # Segundos que un líder debe estar estable antes de aceptar otro
RECONNECTION_GRACE_PERIOD = 10  # Segundos de gracia después de reconexión para resolver líder
ELECTION_COOLDOWN = 10  # Cooldown entre elecciones


class LeaderElection:
    """
    Implementa elección de líder usando un algoritmo bully mejorado.
    Incluye mecanismos para evitar oscilaciones y manejar split-brain.
    """
    
    def __init__(self, node_id: str, node_info: NodeInfo,
                 on_become_leader: Callable = None,
                 on_leader_change: Callable = None,
                 heartbeat_manager=None,
                 get_data_state: Callable = None):
        self.node_id = node_id
        self.node_info = node_info
        self.on_become_leader = on_become_leader
        self.on_leader_change = on_leader_change
        self.heartbeat_manager = heartbeat_manager
        self.get_data_state = get_data_state  # Función para obtener estado de datos
        
        self._lock = threading.RLock()
        self._peers: Dict[str, NodeInfo] = {}
        self._current_leader: Optional[str] = None
        self._term = 0
        self._is_leader = False
        self._last_leader_heartbeat = 0
        self._missed_heartbeats = 0
        self._election_in_progress = False
        self._became_leader_at = 0
        self._last_election_attempt = 0
        
        # Nuevas variables para estabilidad
        self._leader_confirmed_at = 0  # Cuándo se confirmó el líder actual
        self._reconnection_time = 0  # Cuándo se detectó una reconexión
        self._in_reconciliation = False  # Si estamos en proceso de reconciliación
        self._leader_stability_score = 0  # Score de estabilidad del líder
        self._last_leader_change = 0  # Última vez que cambió el líder
        self._pending_leader: Optional[str] = None  # Líder pendiente de confirmación
        self._votes_received: Dict[str, str] = {}  # node_id -> voted_for (para consenso)
        
        self._running = False
        self._election_thread: Optional[threading.Thread] = None
        self._rpc_client = RPCClient()
    
    def register_peer(self, node: NodeInfo):
        """Registra un nodo peer de metadata"""
        with self._lock:
            if node.node_id == self.node_id:
                logger.debug(f"Ignoring self-registration attempt in leader election")
                return
            
            if node.node_type != NodeType.METADATA:
                logger.warning(f"Attempted to register non-metadata node {node.node_id} as peer")
                return
            
            was_registered = node.node_id in self._peers
            self._peers[node.node_id] = node
            
            if not was_registered:
                logger.info(f"Registered metadata peer: {node.node_id}")
                # Marcar que hubo una reconexión si ya teníamos un líder
                if self._current_leader:
                    self._reconnection_time = time.time()
    
    def unregister_peer(self, node_id: str):
        """Elimina un peer de metadata"""
        with self._lock:
            if node_id in self._peers:
                del self._peers[node_id]
                logger.info(f"Unregistered metadata peer: {node_id}")
                
                if node_id == self._current_leader:
                    logger.warning(f"Leader {node_id} went down, will start election")
                    self._current_leader = None
                    self._schedule_election()
    
    def _schedule_election(self):
        """Programa una elección con delay"""
        threading.Thread(target=self._delayed_start_election, daemon=True).start()
    
    def _delayed_start_election(self):
        """Inicia elección después de un delay aleatorio"""
        delay = random.uniform(1.0, 3.0)
        time.sleep(delay)
        self._start_election()
    
    def start(self):
        """Inicia el servicio de elección de líder"""
        self._running = True
        self._election_thread = threading.Thread(target=self._election_loop, daemon=True)
        self._election_thread.start()
        
        # Descubrir líder existente
        threading.Thread(target=self._initial_leader_discovery, daemon=True).start()
    
    def stop(self):
        """Detiene el servicio"""
        self._running = False
    
    def is_leader(self) -> bool:
        """Verifica si este nodo es el líder actual"""
        with self._lock:
            return self._is_leader
    
    def get_leader(self) -> Optional[NodeInfo]:
        """Obtiene información del líder actual"""
        with self._lock:
            if self._is_leader:
                return self.node_info
            if self._current_leader and self._current_leader in self._peers:
                return self._peers[self._current_leader]
            return None
    
    def get_leader_id(self) -> Optional[str]:
        """Obtiene el ID del líder actual"""
        with self._lock:
            if self._is_leader:
                return self.node_id
            return self._current_leader
    
    def get_term(self) -> int:
        """Obtiene el término actual de elección"""
        with self._lock:
            return self._term
    
    def get_data_state_info(self) -> Dict:
        """Obtiene información del estado de datos para comparación"""
        if self.get_data_state:
            try:
                return self.get_data_state()
            except:
                pass
        return {'file_count': 0, 'storage_count': 0, 'commit_index': -1}
    
    def handle_election_message(self, message: RPCMessage) -> RPCMessage:
        """Maneja un mensaje de elección de otro nodo"""
        sender_id = message.payload.get('node_id')
        sender_term = message.payload.get('term', 0)
        sender_data_state = message.payload.get('data_state', {})
        
        with self._lock:
            my_data_state = self.get_data_state_info()
            
            # Si el sender tiene término mayor, actualizar
            if sender_term > self._term:
                self._term = sender_term
                self._is_leader = False
            
            # Comparar quién debería ser líder
            should_i_lead = self._should_i_lead_over(sender_id, sender_term, sender_data_state)
            
            if should_i_lead:
                # Yo debería ser líder
                if not self._election_in_progress:
                    threading.Thread(target=self._start_election, daemon=True).start()
                return RPCMessage(
                    MessageType.LEADER_ELECTION,
                    {
                        'node_id': self.node_id,
                        'term': self._term,
                        'status': 'HIGHER_PRIORITY',
                        'data_state': my_data_state
                    },
                    message.request_id
                )
            
            return RPCMessage(
                MessageType.LEADER_ELECTION,
                {
                    'node_id': self.node_id,
                    'term': self._term,
                    'status': 'OK',
                    'data_state': my_data_state
                },
                message.request_id
            )
    
    def _should_i_lead_over(self, other_id: str, other_term: int, other_data_state: Dict) -> bool:
        """
        Determina si este nodo debería ser líder sobre otro.
        Criterios (en orden):
        1. Mayor término
        2. Más datos (file_count + storage_count)
        3. Mayor commit_index
        4. Menor node_id (desempate lexicográfico)
        """
        my_data_state = self.get_data_state_info()
        
        # 1. Comparar términos
        if self._term > other_term:
            return True
        if self._term < other_term:
            return False
        
        # Términos iguales, comparar datos
        my_score = my_data_state.get('file_count', 0) + my_data_state.get('storage_count', 0) * 10
        other_score = other_data_state.get('file_count', 0) + other_data_state.get('storage_count', 0) * 10
        
        # 2. Más datos
        if my_score > other_score:
            return True
        if my_score < other_score:
            return False
        
        # 3. Mayor commit_index
        my_commit = my_data_state.get('commit_index', -1)
        other_commit = other_data_state.get('commit_index', -1)
        if my_commit > other_commit:
            return True
        if my_commit < other_commit:
            return False
        
        # 4. Menor node_id
        return self.node_id < other_id
    
    def handle_leader_announcement(self, message: RPCMessage):
        """Maneja el anuncio de un nuevo líder"""
        leader_id = message.payload.get('leader_id')
        term = message.payload.get('term', 0)
        leader_data_state = message.payload.get('data_state', {})
        is_confirmed = message.payload.get('confirmed', False)
        
        with self._lock:
            current_time = time.time()
            
            # Si somos líder y recibimos anuncio de otro
            if self._is_leader and leader_id != self.node_id:
                time_as_leader = current_time - self._became_leader_at
                
                # Si el otro tiene término mayor, ceder inmediatamente
                if term > self._term:
                    logger.warning(f"⚠️ Stepping down: {leader_id} has higher term {term} > {self._term}")
                    self._step_down(leader_id, term)
                    return
                
                # Mismo término - resolver por datos y ID
                if term == self._term:
                    should_other_lead = not self._should_i_lead_over(leader_id, term, leader_data_state)
                    
                    if should_other_lead:
                        logger.warning(f"⚠️ Stepping down: {leader_id} has priority (more data or lower ID)")
                        self._step_down(leader_id, term)
                    else:
                        logger.info(f"🛡️ Rejecting leader announcement from {leader_id} - I have priority")
                        # Reafirmar mi liderazgo
                        self._broadcast_leader_announcement()
                    return
                
                # Término menor, ignorar
                logger.debug(f"Ignoring leader announcement with lower term {term} < {self._term}")
                return
            
            # No somos líder, aceptar al nuevo líder si tiene término válido
            if term >= self._term:
                old_leader = self._current_leader
                
                # Verificar si es el mismo líder (solo actualizar heartbeat)
                if leader_id == self._current_leader:
                    self._last_leader_heartbeat = current_time
                    self._missed_heartbeats = 0
                    return
                
                # Nuevo líder - verificar estabilidad
                time_since_last_change = current_time - self._last_leader_change
                
                # Si cambiamos de líder muy recientemente, ignorar cambios adicionales
                # a menos que el nuevo líder tenga término mayor
                if time_since_last_change < LEADER_STABILITY_PERIOD and term <= self._term:
                    if old_leader and old_leader != leader_id:
                        logger.debug(f"Ignoring leader change to {leader_id} - stability period active")
                        return
                
                # Aceptar nuevo líder
                self._term = term
                self._current_leader = leader_id
                self._last_leader_heartbeat = current_time
                self._last_leader_change = current_time
                self._missed_heartbeats = 0
                self._is_leader = False
                self._election_in_progress = False
                
                if old_leader != leader_id:
                    logger.info(f"✅ New leader accepted: {leader_id} (term {term})")
                    if self.on_leader_change:
                        threading.Thread(
                            target=self.on_leader_change,
                            args=(leader_id,),
                            daemon=True
                        ).start()
    
    def _step_down(self, new_leader_id: str, new_term: int):
        """Cede el liderazgo a otro nodo"""
        self._is_leader = False
        self._current_leader = new_leader_id
        self._term = new_term
        self._last_leader_heartbeat = time.time()
        self._last_leader_change = time.time()
        self._missed_heartbeats = 0
        
        logger.warning(f"🔴 Stepped down as leader. New leader: {new_leader_id}")
        
        if self.on_leader_change:
            threading.Thread(
                target=self.on_leader_change,
                args=(new_leader_id,),
                daemon=True
            ).start()
    
    def handle_leader_heartbeat(self, message: RPCMessage):
        """Maneja un heartbeat del líder"""
        leader_id = message.payload.get('leader_id')
        term = message.payload.get('term', 0)
        
        with self._lock:
            # Solo aceptar heartbeat del líder conocido con término válido
            if term >= self._term:
                if leader_id != self._current_leader and self._current_leader:
                    # Heartbeat de un líder diferente
                    time_since_last_change = time.time() - self._last_leader_change
                    if time_since_last_change < LEADER_STABILITY_PERIOD:
                        # Estamos en período de estabilidad, ignorar otros líderes
                        return
                
                self._current_leader = leader_id
                self._term = term
                self._last_leader_heartbeat = time.time()
                self._missed_heartbeats = 0
                
                # Si yo era líder y recibo heartbeat de otro, verificar
                if self._is_leader and leader_id != self.node_id:
                    logger.warning(f"Received heartbeat from another leader {leader_id}")
                    # El de menor ID o más datos gana
                    if leader_id < self.node_id:
                        self._step_down(leader_id, term)
    
    def _start_election(self):
        """Inicia una nueva elección"""
        with self._lock:
            if self._election_in_progress:
                logger.debug("Election already in progress, skipping")
                return
            
            # Cooldown entre elecciones
            time_since_last = time.time() - self._last_election_attempt
            if time_since_last < ELECTION_COOLDOWN:
                logger.debug(f"Election cooldown active ({ELECTION_COOLDOWN - time_since_last:.1f}s remaining)")
                return
            
            # Verificar si hay líder activo
            if self._current_leader and self._current_leader != self.node_id:
                time_since_heartbeat = time.time() - self._last_leader_heartbeat
                if time_since_heartbeat < LEADER_ELECTION_TIMEOUT:
                    logger.debug(f"Leader {self._current_leader} is active, skipping election")
                    return
                
                self._missed_heartbeats += 1
                if self._missed_heartbeats < 3:
                    logger.debug(f"Leader timeout but only {self._missed_heartbeats} missed heartbeats")
                    return
            
            self._election_in_progress = True
            self._last_election_attempt = time.time()
            self._term += 1
            current_term = self._term
            peers_copy = dict(self._peers)
            my_data_state = self.get_data_state_info()
        
        # Delay aleatorio para evitar colisiones
        random_delay = random.uniform(0.5, 2.0)
        logger.info(f"🗳️ Starting election for term {current_term} (delay: {random_delay:.2f}s)")
        time.sleep(random_delay)
        
        try:
            # Recolectar información de todos los peers
            peer_responses = {}
            active_peers = 0
            
            for peer_id, peer in peers_copy.items():
                msg = RPCMessage(
                    MessageType.LEADER_ELECTION,
                    {
                        'node_id': self.node_id,
                        'term': current_term,
                        'data_state': my_data_state
                    }
                )
                try:
                    response = self._rpc_client.call(peer.host, peer.port, msg)
                    if response:
                        active_peers += 1
                        peer_responses[peer_id] = {
                            'status': response.payload.get('status'),
                            'term': response.payload.get('term', 0),
                            'data_state': response.payload.get('data_state', {})
                        }
                except Exception as e:
                    logger.debug(f"Failed to contact peer {peer_id}: {e}")
            
            # Determinar si debemos ser líder
            with self._lock:
                if self._term != current_term:
                    logger.info("Term changed during election, aborting")
                    return
                
                should_become_leader = True
                
                for peer_id, resp in peer_responses.items():
                    if resp['status'] == 'HIGHER_PRIORITY':
                        # Un peer tiene mayor prioridad
                        should_become_leader = False
                        logger.info(f"Peer {peer_id} has higher priority, not becoming leader")
                        break
                    
                    # Verificar si el peer tiene más datos
                    if not self._should_i_lead_over(peer_id, resp['term'], resp['data_state']):
                        should_become_leader = False
                        logger.info(f"Peer {peer_id} should lead over me")
                        break
                
                if should_become_leader:
                    self._become_leader()
                else:
                    logger.info("Not becoming leader - another node has priority")
                    
        finally:
            with self._lock:
                self._election_in_progress = False
    
    def _become_leader(self):
        """Este nodo se convierte en líder"""
        with self._lock:
            old_leader = self._current_leader
            self._current_leader = self.node_id
            self._is_leader = True
            self._became_leader_at = time.time()
            self._last_leader_change = time.time()
            self._missed_heartbeats = 0
        
        logger.info(f"🟢 Node {self.node_id} became leader (term {self._term})")
        
        # Anunciar liderazgo
        self._broadcast_leader_announcement()
        
        # Callback
        if self.on_become_leader:
            threading.Thread(target=self.on_become_leader, daemon=True).start()
        
        if old_leader != self.node_id and self.on_leader_change:
            threading.Thread(
                target=self.on_leader_change,
                args=(self.node_id,),
                daemon=True
            ).start()
    
    def _broadcast_leader_announcement(self):
        """Anuncia el liderazgo a todos los peers"""
        with self._lock:
            if not self._is_leader:
                return
            peers_copy = dict(self._peers)
            term = self._term
            data_state = self.get_data_state_info()
        
        for peer in peers_copy.values():
            msg = RPCMessage(
                MessageType.LEADER_ELECTED,
                {
                    'leader_id': self.node_id,
                    'term': term,
                    'data_state': data_state,
                    'confirmed': True
                }
            )
            try:
                self._rpc_client.call(peer.host, peer.port, msg)
            except Exception as e:
                logger.debug(f"Failed to announce leadership to {peer.node_id}: {e}")
    
    def _election_loop(self):
        """Loop principal que monitorea el estado del líder"""
        while self._running:
            time.sleep(HEARTBEAT_INTERVAL)
            
            with self._lock:
                if self._is_leader:
                    self._send_leader_heartbeats()
                else:
                    if self._current_leader:
                        time_since_heartbeat = time.time() - self._last_leader_heartbeat
                        if time_since_heartbeat > LEADER_ELECTION_TIMEOUT:
                            self._missed_heartbeats += 1
                            if self._missed_heartbeats >= 3:
                                logger.warning(f"Leader {self._current_leader} timed out after {self._missed_heartbeats} missed heartbeats")
                                self._schedule_election()
                    else:
                        # No hay líder conocido
                        if not self._election_in_progress:
                            self._schedule_election()
    
    def _send_leader_heartbeats(self):
        """Envía heartbeats a todos los peers"""
        with self._lock:
            if not self._is_leader:
                return
            peers_copy = dict(self._peers)
            term = self._term
            data_state = self.get_data_state_info()
        
        for peer in peers_copy.values():
            if self.heartbeat_manager:
                peer_state = self.heartbeat_manager.get_node_state(peer.node_id)
                if peer_state == NodeState.DOWN:
                    continue
            
            msg = RPCMessage(
                MessageType.HEARTBEAT,
                {
                    'leader_id': self.node_id,
                    'term': term,
                    'data_state': data_state
                }
            )
            try:
                self._rpc_client.call(peer.host, peer.port, msg)
            except Exception as e:
                if "Temporary failure in name resolution" not in str(e):
                    logger.debug(f"Failed to send heartbeat to {peer.node_id}: {e}")
    
    def query_leader(self, peer: NodeInfo) -> Optional[Tuple[str, int, Dict]]:
        """Consulta a un peer quién es el líder"""
        msg = RPCMessage(MessageType.LEADER_QUERY, {'requester_id': self.node_id})
        try:
            response = self._rpc_client.call(peer.host, peer.port, msg)
            if response:
                return (
                    response.payload.get('leader_id'),
                    response.payload.get('term', 0),
                    response.payload.get('data_state', {})
                )
        except Exception as e:
            logger.debug(f"Failed to query leader from {peer.node_id}: {e}")
        return None

    def _initial_leader_discovery(self):
        """Descubrimiento inicial del líder al arrancar"""
        time.sleep(1)  # Pequeño delay para que otros nodos arranquen
        
        with self._lock:
            peers_copy = dict(self._peers)
        
        discovered_leaders = []
        
        for peer in peers_copy.values():
            result = self.query_leader(peer)
            if result:
                leader_id, term, data_state = result
                if leader_id:
                    discovered_leaders.append({
                        'leader_id': leader_id,
                        'term': term,
                        'data_state': data_state,
                        'from_peer': peer.node_id
                    })
        
        if discovered_leaders:
            # Elegir el líder con mayor término y más datos
            best_leader = max(
                discovered_leaders,
                key=lambda x: (x['term'], x['data_state'].get('file_count', 0), x['data_state'].get('storage_count', 0))
            )
            
            with self._lock:
                self._current_leader = best_leader['leader_id']
                self._term = best_leader['term']
                self._last_leader_heartbeat = time.time()
                self._last_leader_change = time.time()
                
                if best_leader['leader_id'] == self.node_id:
                    self._is_leader = True
                    self._became_leader_at = time.time()
                    logger.info(f"🟢 Discovered that I am the leader")
                else:
                    logger.info(f"✅ Discovered leader: {best_leader['leader_id']} (term {best_leader['term']})")
        else:
            logger.info("No leader discovered, will start election")
            time.sleep(2)
            self._start_election()

    def force_reconciliation(self, peer_states: Dict[str, Dict]) -> str:
        """
        Fuerza una reconciliación con los estados de los peers.
        Retorna el ID del líder que debería ganar.
        """
        with self._lock:
            my_data_state = self.get_data_state_info()
            
            # Añadir mi propio estado
            all_states = {
                self.node_id: {
                    'term': self._term,
                    'data_state': my_data_state,
                    'is_leader': self._is_leader
                }
            }
            all_states.update(peer_states)
            
            # Encontrar el mejor candidato
            candidates = []
            for node_id, state in all_states.items():
                term = state.get('term', 0)
                data = state.get('data_state', {})
                score = (
                    term * 1000 +
                    data.get('file_count', 0) +
                    data.get('storage_count', 0) * 10 +
                    data.get('commit_index', 0)
                )
                candidates.append((node_id, term, score))
            
            # Ordenar: mayor score primero, menor ID como desempate
            candidates.sort(key=lambda x: (-x[2], x[0]))
            
            winner_id = candidates[0][0]
            winner_term = candidates[0][1]
            
            logger.info(f"🔄 Reconciliation result: {winner_id} should be leader (term {winner_term})")
            
            if winner_id == self.node_id:
                if not self._is_leader:
                    self._term = max(self._term, winner_term) + 1
                    self._become_leader()
            else:
                if self._is_leader:
                    self._step_down(winner_id, winner_term)
                else:
                    self._current_leader = winner_id
                    self._term = winner_term
                    self._last_leader_heartbeat = time.time()
                    self._last_leader_change = time.time()
            
            return winner_id
