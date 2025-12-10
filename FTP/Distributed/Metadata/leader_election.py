"""
Elección de líder para el servicio de Metadata.
Implementa un algoritmo simple de elección de líder basado en IDs.
"""
import time
import threading
import logging
from typing import Dict, Optional, List, Callable
from ..Common.models import NodeInfo
from ..Common.constants import (
    NodeState, NodeType, MessageType,
    LEADER_ELECTION_TIMEOUT, HEARTBEAT_INTERVAL
)
from ..Common.rpc_protocol import RPCClient, RPCMessage

logger = logging.getLogger(__name__)


class LeaderElection:
    """
    Implementa elección de líder usando un algoritmo bully simplificado.
    El nodo con el ID más bajo se convierte en líder.
    """
    
    def __init__(self, node_id: str, node_info: NodeInfo,
                 on_become_leader: Callable = None,
                 on_leader_change: Callable = None):
        self.node_id = node_id
        self.node_info = node_info
        self.on_become_leader = on_become_leader
        self.on_leader_change = on_leader_change
        
        self._lock = threading.RLock()
        self._peers: Dict[str, NodeInfo] = {}  # Otros nodos de metadata
        self._current_leader: Optional[str] = None
        self._term = 0  # Término de elección
        self._is_leader = False
        self._last_leader_heartbeat = 0
        
        self._running = False
        self._election_thread: Optional[threading.Thread] = None
        self._rpc_client = RPCClient()
    
    def register_peer(self, node: NodeInfo):
        """Registra un nodo peer de metadata"""
        with self._lock:
            # Evitar auto-registro
            if node.node_id == self.node_id:
                logger.debug(f"Ignoring self-registration attempt in leader election")
                return
            
            # Verificar tipo correcto
            if node.node_type != NodeType.METADATA:
                logger.warning(f"Attempted to register non-metadata node {node.node_id} as peer")
                return
            
            # Verificar si ya está registrado
            if node.node_id in self._peers:
                logger.debug(f"Peer {node.node_id} already registered, updating info")
                self._peers[node.node_id] = node
            else:
                self._peers[node.node_id] = node
                logger.info(f"Registered metadata peer: {node.node_id}")
    
    def unregister_peer(self, node_id: str):
        """Elimina un peer de metadata"""
        with self._lock:
            if node_id in self._peers:
                del self._peers[node_id]
                logger.info(f"Unregistered metadata peer: {node_id}")
                
                # Si el líder se fue, iniciar nueva elección
                if node_id == self._current_leader:
                    self._start_election()
    
    def start(self):
        """Inicia el servicio de elección de líder"""
        self._running = True
        self._election_thread = threading.Thread(target=self._election_loop, daemon=True)
        self._election_thread.start()
        
        # NO iniciar elección automáticamente
        # Primero intentar descubrir el líder actual consultando a los peers
        self._discover_leader()
        
        # Solo iniciar elección si no hay líder conocido después de un tiempo
        threading.Thread(target=self._delayed_election_check, daemon=True).start()
    
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
    
    def handle_election_message(self, message: RPCMessage) -> RPCMessage:
        """Maneja un mensaje de elección de otro nodo"""
        sender_id = message.payload.get('node_id')
        sender_term = message.payload.get('term', 0)
        
        with self._lock:
            if sender_term > self._term:
                # Hay un término más nuevo, actualizar
                self._term = sender_term
                self._is_leader = False
            
            # Si nuestro ID es menor, respondemos que somos candidatos
            if self.node_id < sender_id:
                # Iniciar nuestra propia elección
                threading.Thread(target=self._start_election, daemon=True).start()
                return RPCMessage(
                    MessageType.LEADER_ELECTION,
                    {'node_id': self.node_id, 'term': self._term, 'status': 'LOWER'},
                    message.request_id
                )
            
            return RPCMessage(
                MessageType.LEADER_ELECTION,
                {'node_id': self.node_id, 'term': self._term, 'status': 'OK'},
                message.request_id
            )
    
    def handle_leader_announcement(self, message: RPCMessage):
        """Maneja el anuncio de un nuevo líder"""
        leader_id = message.payload.get('leader_id')
        term = message.payload.get('term', 0)
        
        with self._lock:
            if term >= self._term:
                old_leader = self._current_leader
                self._term = term
                self._current_leader = leader_id
                self._last_leader_heartbeat = time.time()
                
                if leader_id == self.node_id:
                    self._is_leader = True
                else:
                    self._is_leader = False
                
                if old_leader != leader_id and self.on_leader_change:
                    threading.Thread(
                        target=self.on_leader_change,
                        args=(leader_id,),
                        daemon=True
                    ).start()
                
                logger.info(f"New leader: {leader_id} (term {term})")
    
    def handle_leader_heartbeat(self, message: RPCMessage):
        """Maneja un heartbeat del líder"""
        leader_id = message.payload.get('leader_id')
        term = message.payload.get('term', 0)
        
        with self._lock:
            if leader_id == self._current_leader and term >= self._term:
                self._last_leader_heartbeat = time.time()
    
    def _start_election(self):
        """Inicia una nueva elección"""
        with self._lock:
            # PRIMERO: Verificar si ya hay un líder activo
            if self._current_leader and self._current_leader != self.node_id:
                # Hay un líder conocido, verificar si está vivo
                time_since_heartbeat = time.time() - self._last_leader_heartbeat
                if time_since_heartbeat < LEADER_ELECTION_TIMEOUT:
                    # El líder está vivo, no iniciar elección innecesaria
                    logger.info(f"Leader {self._current_leader} is active, skipping election")
                    return
            
            self._term += 1
            current_term = self._term
            peers_copy = dict(self._peers)
        
        logger.info(f"Starting election for term {current_term}")
        
        # Enviar mensaje de elección a todos los peers con ID menor
        lower_peers = [p for pid, p in peers_copy.items() if pid < self.node_id]
        
        if not lower_peers:
            # Somos el nodo con ID más bajo, convertirnos en líder
            self._become_leader()
            return
        
        # Enviar mensajes de elección
        responses = []
        for peer in lower_peers:
            msg = RPCMessage(
                MessageType.LEADER_ELECTION,
                {'node_id': self.node_id, 'term': current_term}
            )
            try:
                response = self._rpc_client.call(peer.host, peer.port, msg)
                if response:
                    responses.append(response)
            except Exception as e:
                logger.debug(f"Failed to contact peer {peer.node_id}: {e}")
        
        # Si no hay respuestas de nodos con ID menor, convertirnos en líder
        active_lower = any(
            r.payload.get('status') == 'LOWER' for r in responses
        )
        
        if not active_lower:
            # Esperar un momento por si hay elecciones en curso
            time.sleep(0.5)
            with self._lock:
                if self._term == current_term:  # No hubo cambios
                    self._become_leader()
    
    def _become_leader(self):
        """Este nodo se convierte en líder"""
        with self._lock:
            old_leader = self._current_leader
            self._current_leader = self.node_id
            self._is_leader = True
            peers_copy = dict(self._peers)
        
        logger.info(f"Node {self.node_id} became leader (term {self._term})")
        
        # Anunciar a todos los peers
        for peer in peers_copy.values():
            msg = RPCMessage(
                MessageType.LEADER_ELECTED,
                {'leader_id': self.node_id, 'term': self._term}
            )
            try:
                self._rpc_client.call(peer.host, peer.port, msg)
            except Exception as e:
                logger.debug(f"Failed to announce leadership to {peer.node_id}: {e}")
        
        # Callback
        if self.on_become_leader:
            threading.Thread(target=self.on_become_leader, daemon=True).start()
        
        if old_leader != self.node_id and self.on_leader_change:
            threading.Thread(
                target=self.on_leader_change,
                args=(self.node_id,),
                daemon=True
            ).start()
    
    def _election_loop(self):
        """Loop principal que monitorea el estado del líder"""
        while self._running:
            time.sleep(HEARTBEAT_INTERVAL)
            
            with self._lock:
                if self._is_leader:
                    # Somos el líder, enviar heartbeats
                    self._send_leader_heartbeats()
                else:
                    # Verificar si el líder está vivo
                    if self._current_leader:
                        time_since_heartbeat = time.time() - self._last_leader_heartbeat
                        if time_since_heartbeat > LEADER_ELECTION_TIMEOUT:
                            logger.warning(f"Leader {self._current_leader} timeout, starting election")
                            self._start_election()
    
    def _send_leader_heartbeats(self):
        """Envía heartbeats a todos los peers"""
        with self._lock:
            peers_copy = dict(self._peers)
            term = self._term
        
        for peer in peers_copy.values():
            msg = RPCMessage(
                MessageType.HEARTBEAT,
                {'leader_id': self.node_id, 'term': term}
            )
            try:
                self._rpc_client.call(peer.host, peer.port, msg)
            except Exception as e:
                logger.debug(f"Failed to send heartbeat to {peer.node_id}: {e}")
    
    def query_leader(self, peer: NodeInfo) -> Optional[str]:
        """Consulta a un peer quién es el líder"""
        msg = RPCMessage(MessageType.LEADER_QUERY, {})
        try:
            response = self._rpc_client.call(peer.host, peer.port, msg)
            if response:
                return response.payload.get('leader_id')
        except Exception as e:
            logger.debug(f"Failed to query leader from {peer.node_id}: {e}")
        return None

    def _discover_leader(self):
        """Intenta descubrir el líder actual consultando a los peers"""
        with self._lock:
            peers_copy = dict(self._peers)
        
        for peer in peers_copy.values():
            leader_id = self.query_leader(peer)
            if leader_id:
                # Encontramos un líder, actualizar nuestro estado
                with self._lock:
                    self._current_leader = leader_id
                    self._last_leader_heartbeat = time.time()
                    if leader_id == self.node_id:
                        self._is_leader = True
                logger.info(f"Discovered existing leader: {leader_id}")
                return
        
        # No se encontró líder, iniciar elección
        logger.info("No leader found, starting election")
        self._start_election()

    def _delayed_election_check(self):
        """Verifica después de un delay si necesitamos iniciar elección"""
        time.sleep(2)  # Esperar a que otros nodos se registren
        with self._lock:
            if not self._current_leader or self._is_leader:
                # No hay líder o somos el líder, no hacer nada
                return
        
        # Verificar si el líder está vivo
        time_since_hb = time.time() - self._last_leader_heartbeat
        if time_since_hb > LEADER_ELECTION_TIMEOUT:
            logger.info("Leader not responding, starting election")
            self._start_election()

