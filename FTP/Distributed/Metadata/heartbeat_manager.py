"""
Gestor de heartbeats para monitorear el estado de los nodos.
"""
import time
import threading
import logging
from typing import Dict, Set, Callable, Optional
from ..Common.models import NodeInfo
from ..Common.constants import (
    NodeState, NodeType, MessageType,
    HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT
)
from ..Common.rpc_protocol import RPCClient, RPCMessage

logger = logging.getLogger(__name__)


class HeartbeatManager:
    """
    Gestiona los heartbeats de los nodos del clúster.
    Detecta nodos caídos y notifica cambios de estado.
    """
    
    def __init__(self, 
                 on_node_down: Callable[[str], None] = None,
                 on_node_up: Callable[[str], None] = None,
                 on_node_suspect: Callable[[str], None] = None):
        self.on_node_down = on_node_down
        self.on_node_up = on_node_up
        self.on_node_suspect = on_node_suspect
        
        self._lock = threading.RLock()
        self._nodes: Dict[str, NodeInfo] = {}
        self._last_heartbeat: Dict[str, float] = {}
        self._suspect_nodes: Set[str] = set()
        
        self._running = False
        self._check_thread: Optional[threading.Thread] = None
        self._rpc_client = RPCClient()
    
    def register_node(self, node: NodeInfo):
        """Registra un nodo para monitoreo"""
        with self._lock:
            self._nodes[node.node_id] = node
            self._last_heartbeat[node.node_id] = time.time()
            if node.node_id in self._suspect_nodes:
                self._suspect_nodes.remove(node.node_id)
            logger.info(f"Node registered for heartbeat: {node.node_id}")
    
    def unregister_node(self, node_id: str):
        """Elimina un nodo del monitoreo"""
        with self._lock:
            self._nodes.pop(node_id, None)
            self._last_heartbeat.pop(node_id, None)
            self._suspect_nodes.discard(node_id)
            logger.info(f"Node unregistered from heartbeat: {node_id}")
    
    def receive_heartbeat(self, node_id: str):
        """Recibe un heartbeat de un nodo"""
        with self._lock:
            self._last_heartbeat[node_id] = time.time()
            
            # Si el nodo estaba sospechoso o caído, marcarlo como recuperado
            if node_id in self._suspect_nodes:
                self._suspect_nodes.remove(node_id)
                if node_id in self._nodes:
                    self._nodes[node_id].state = NodeState.UP
                    if self.on_node_up:
                        threading.Thread(
                            target=self.on_node_up,
                            args=(node_id,),
                            daemon=True
                        ).start()
                logger.info(f"Node {node_id} recovered")
            elif node_id in self._nodes and self._nodes[node_id].state == NodeState.DOWN:
                self._nodes[node_id].state = NodeState.UP
                if self.on_node_up:
                    threading.Thread(
                        target=self.on_node_up,
                        args=(node_id,),
                        daemon=True
                    ).start()
                logger.info(f"Node {node_id} is back UP")
    
    def start(self):
        """Inicia el servicio de heartbeat"""
        self._running = True
        self._check_thread = threading.Thread(target=self._check_loop, daemon=True)
        self._check_thread.start()
        logger.info("Heartbeat manager started")
    
    def stop(self):
        """Detiene el servicio"""
        self._running = False
    
    def get_node_state(self, node_id: str) -> Optional[NodeState]:
        """Obtiene el estado actual de un nodo"""
        with self._lock:
            if node_id in self._nodes:
                return self._nodes[node_id].state
            return None
    
    def get_active_nodes(self, node_type: NodeType = None) -> list:
        """Obtiene todos los nodos activos, opcionalmente filtrados por tipo"""
        with self._lock:
            nodes = [
                node for node in self._nodes.values()
                if node.state == NodeState.UP
            ]
            if node_type:
                nodes = [n for n in nodes if n.node_type == node_type]
            return nodes
    
    def _check_loop(self):
        """Loop que verifica el estado de los nodos periódicamente"""
        while self._running:
            time.sleep(HEARTBEAT_INTERVAL)
            self._check_nodes()
    
    def _check_nodes(self):
        """Verifica el estado de todos los nodos"""
        now = time.time()
        
        with self._lock:
            nodes_to_check = list(self._nodes.items())
        
        for node_id, node in nodes_to_check:
            with self._lock:
                last_hb = self._last_heartbeat.get(node_id, 0)
                time_since_hb = now - last_hb
            
            if time_since_hb > HEARTBEAT_TIMEOUT:
                # Nodo no responde
                self._handle_node_timeout(node_id, node)
            elif time_since_hb > HEARTBEAT_TIMEOUT / 2:
                # Nodo sospechoso
                self._handle_node_suspect(node_id, node)
    
    def _handle_node_suspect(self, node_id: str, node: NodeInfo):
        """Maneja un nodo sospechoso"""
        with self._lock:
            if node_id not in self._suspect_nodes and node.state == NodeState.UP:
                self._suspect_nodes.add(node_id)
                node.state = NodeState.SUSPECT
                logger.warning(f"Node {node_id} is suspect (no heartbeat)")
                
                if self.on_node_suspect:
                    threading.Thread(
                        target=self.on_node_suspect,
                        args=(node_id,),
                        daemon=True
                    ).start()
    
    def _handle_node_timeout(self, node_id: str, node: NodeInfo):
        """Maneja un nodo que ha dejado de responder"""
        with self._lock:
            if node.state != NodeState.DOWN:
                node.state = NodeState.DOWN
                self._suspect_nodes.discard(node_id)
                logger.error(f"Node {node_id} is DOWN (heartbeat timeout)")
                
                if self.on_node_down:
                    threading.Thread(
                        target=self.on_node_down,
                        args=(node_id,),
                        daemon=True
                    ).start()
    
    def send_heartbeat(self, target_node: NodeInfo) -> bool:
        """Envía un heartbeat a un nodo específico"""
        try:
            msg = RPCMessage(
                MessageType.HEARTBEAT,
                {'timestamp': time.time()}
            )
            response = self._rpc_client.call(target_node.host, target_node.port, msg)
            return response is not None
        except Exception as e:
            logger.debug(f"Failed to send heartbeat to {target_node.node_id}: {e}")
            return False

