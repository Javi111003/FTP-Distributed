"""
Gestor de réplicas para el servicio de Metadata.
Mantiene el mapeo de archivos a nodos de almacenamiento y gestiona la replicación.
"""
import time
import random
import threading
import logging
from typing import Dict, List, Optional, Tuple, Set
from collections import defaultdict
from ..Common.models import NodeInfo, ReplicaInfo, FileMetadata
from ..Common.constants import (
    REPLICATION_FACTOR, MIN_REPLICAS_FOR_WRITE,
    NodeState, NodeType, DistributedResponseCode
)

logger = logging.getLogger(__name__)


class ReplicaManager:
    """
    Gestiona la distribución de réplicas de archivos entre nodos de almacenamiento.
    Implementa políticas de replicación y recuperación.
    """
    
    def __init__(self, replication_factor: int = REPLICATION_FACTOR):
        self.replication_factor = replication_factor
        self._lock = threading.RLock()
        
        # Mapeo de file_id a información de réplicas
        # file_id -> List[ReplicaInfo]
        self._replicas: Dict[str, List[ReplicaInfo]] = {}
        
        # Nodos de almacenamiento registrados
        # node_id -> NodeInfo
        self._storage_nodes: Dict[str, NodeInfo] = {}
        
        # Índice inverso: node_id -> Set[file_id]
        self._node_files: Dict[str, Set[str]] = defaultdict(set)
    
    def register_storage_node(self, node: NodeInfo) -> bool:
        """Registra un nuevo nodo de almacenamiento"""
        with self._lock:
            if node.node_type != NodeType.STORAGE:
                return False
            
            self._storage_nodes[node.node_id] = node
            logger.info(f"Storage node registered: {node.node_id} at {node.host}:{node.port}")
            return True
    
    def unregister_storage_node(self, node_id: str):
        """Elimina un nodo de almacenamiento del registro"""
        with self._lock:
            if node_id in self._storage_nodes:
                del self._storage_nodes[node_id]
                # No eliminamos los archivos del índice, ya que podrían recuperarse
                logger.info(f"Storage node unregistered: {node_id}")
    
    def update_node_state(self, node_id: str, state: NodeState):
        """Actualiza el estado de un nodo"""
        with self._lock:
            if node_id in self._storage_nodes:
                self._storage_nodes[node_id].state = state
                self._storage_nodes[node_id].last_heartbeat = time.time()
    
    def get_storage_node(self, node_id: str) -> Optional[NodeInfo]:
        """Obtiene información de un nodo de almacenamiento"""
        with self._lock:
            return self._storage_nodes.get(node_id)
    
    def get_available_storage_nodes(self) -> List[NodeInfo]:
        """Obtiene todos los nodos de almacenamiento disponibles"""
        with self._lock:
            return [
                node for node in self._storage_nodes.values()
                if node.state == NodeState.UP
            ]
    
    def select_replicas_for_file(self, file_id: str, exclude_nodes: List[str] = None) -> List[NodeInfo]:
        """
        Selecciona nodos para almacenar las réplicas de un archivo.
        Intenta distribuir las réplicas en diferentes hosts físicos.
        """
        with self._lock:
            available_nodes = self.get_available_storage_nodes()
            if exclude_nodes:
                available_nodes = [n for n in available_nodes if n.node_id not in exclude_nodes]
            
            if len(available_nodes) < self.replication_factor:
                logger.warning(f"Not enough storage nodes. Available: {len(available_nodes)}, Required: {self.replication_factor}")
            
            # Ordenar por cantidad de archivos (balanceo de carga)
            available_nodes.sort(key=lambda n: len(self._node_files.get(n.node_id, set())))
            
            # Seleccionar los nodos con menos carga hasta alcanzar el factor de replicación
            selected = available_nodes[:self.replication_factor]
            
            return selected
    
    def assign_replicas(self, file_id: str, nodes: List[NodeInfo], 
                       size: int = 0, primary_node_id: str = None) -> List[ReplicaInfo]:
        """Asigna réplicas de un archivo a nodos específicos"""
        with self._lock:
            replicas = []
            for i, node in enumerate(nodes):
                is_primary = (node.node_id == primary_node_id) if primary_node_id else (i == 0)
                replica = ReplicaInfo(
                    file_id=file_id,
                    node_id=node.node_id,
                    version=1,
                    size=size,
                    is_primary=is_primary
                )
                replicas.append(replica)
                self._node_files[node.node_id].add(file_id)
            
            self._replicas[file_id] = replicas
            logger.debug(f"Assigned {len(replicas)} replicas for file {file_id}")
            return replicas
    
    def get_replicas(self, file_id: str) -> List[ReplicaInfo]:
        """Obtiene todas las réplicas de un archivo"""
        with self._lock:
            return self._replicas.get(file_id, [])
    
    def get_replica_nodes(self, file_id: str) -> List[NodeInfo]:
        """Obtiene los nodos que tienen réplicas de un archivo"""
        with self._lock:
            replicas = self._replicas.get(file_id, [])
            nodes = []
            for replica in replicas:
                node = self._storage_nodes.get(replica.node_id)
                if node and node.state == NodeState.UP:
                    nodes.append(node)
            return nodes
    
    def get_primary_replica(self, file_id: str) -> Optional[Tuple[ReplicaInfo, NodeInfo]]:
        """Obtiene la réplica primaria de un archivo"""
        with self._lock:
            replicas = self._replicas.get(file_id, [])
            for replica in replicas:
                if replica.is_primary:
                    node = self._storage_nodes.get(replica.node_id)
                    if node and node.state == NodeState.UP:
                        return (replica, node)
            
            # Si no hay primaria disponible, elegir cualquier réplica activa
            for replica in replicas:
                node = self._storage_nodes.get(replica.node_id)
                if node and node.state == NodeState.UP:
                    return (replica, node)
            
            return None
    
    def update_replica_version(self, file_id: str, node_id: str, version: int, size: int = None):
        """Actualiza la versión de una réplica"""
        with self._lock:
            replicas = self._replicas.get(file_id, [])
            for replica in replicas:
                if replica.node_id == node_id:
                    replica.version = version
                    if size is not None:
                        replica.size = size
                    replica.last_sync = time.time()
                    break
    
    def remove_replica(self, file_id: str, node_id: str):
        """Elimina una réplica de un archivo"""
        with self._lock:
            if file_id in self._replicas:
                self._replicas[file_id] = [
                    r for r in self._replicas[file_id] if r.node_id != node_id
                ]
                self._node_files[node_id].discard(file_id)
    
    def remove_all_replicas(self, file_id: str):
        """Elimina todas las réplicas de un archivo"""
        with self._lock:
            replicas = self._replicas.pop(file_id, [])
            for replica in replicas:
                self._node_files[replica.node_id].discard(file_id)
    
    def get_under_replicated_files(self) -> List[str]:
        """Obtiene archivos que tienen menos réplicas de las necesarias"""
        with self._lock:
            under_replicated = []
            for file_id, replicas in self._replicas.items():
                # Contar réplicas en nodos activos
                active_replicas = sum(
                    1 for r in replicas
                    if r.node_id in self._storage_nodes
                    and self._storage_nodes[r.node_id].state == NodeState.UP
                )
                if active_replicas < self.replication_factor:
                    under_replicated.append(file_id)
            return under_replicated
    
    def get_orphaned_files_on_node(self, node_id: str, known_files: Set[str]) -> Set[str]:
        """
        Obtiene archivos que un nodo reporta pero que no están en el namespace.
        Usado para sincronización.
        """
        with self._lock:
            tracked_files = self._node_files.get(node_id, set())
            return known_files - tracked_files
    
    def get_missing_files_on_node(self, node_id: str, reported_files: Set[str]) -> Set[str]:
        """
        Obtiene archivos que deberían estar en un nodo pero no reporta.
        Usado para sincronización.
        """
        with self._lock:
            expected_files = set()
            for file_id, replicas in self._replicas.items():
                for replica in replicas:
                    if replica.node_id == node_id:
                        expected_files.add(file_id)
                        break
            return expected_files - reported_files
    
    def check_replication_health(self) -> Dict[str, int]:
        """Verifica el estado de salud de la replicación"""
        with self._lock:
            stats = {
                'total_files': len(self._replicas),
                'under_replicated': 0,
                'fully_replicated': 0,
                'over_replicated': 0,
                'no_replicas': 0
            }
            
            for file_id, replicas in self._replicas.items():
                active_count = sum(
                    1 for r in replicas
                    if r.node_id in self._storage_nodes
                    and self._storage_nodes[r.node_id].state == NodeState.UP
                )
                
                if active_count == 0:
                    stats['no_replicas'] += 1
                elif active_count < self.replication_factor:
                    stats['under_replicated'] += 1
                elif active_count == self.replication_factor:
                    stats['fully_replicated'] += 1
                else:
                    stats['over_replicated'] += 1
            
            return stats
    
    def plan_rebalance(self) -> List[Tuple[str, str, str]]:
        """
        Planifica movimientos de réplicas para rebalancear el clúster.
        Retorna lista de (file_id, from_node, to_node)
        """
        with self._lock:
            moves = []
            available_nodes = self.get_available_storage_nodes()
            
            if len(available_nodes) < 2:
                return moves
            
            # Calcular carga actual de cada nodo
            node_load = {n.node_id: len(self._node_files.get(n.node_id, set())) 
                        for n in available_nodes}
            
            avg_load = sum(node_load.values()) / len(node_load) if node_load else 0
            
            # Identificar nodos sobrecargados y subcargados
            overloaded = [(nid, load) for nid, load in node_load.items() if load > avg_load * 1.2]
            underloaded = [(nid, load) for nid, load in node_load.items() if load < avg_load * 0.8]
            
            # Planificar movimientos
            for over_node, over_load in overloaded:
                files_to_move = int((over_load - avg_load) / 2)
                if files_to_move <= 0:
                    continue
                
                files = list(self._node_files.get(over_node, set()))[:files_to_move]
                for file_id in files:
                    for under_node, _ in underloaded:
                        # Verificar que el archivo no esté ya en el nodo destino
                        replicas = self._replicas.get(file_id, [])
                        if not any(r.node_id == under_node for r in replicas):
                            moves.append((file_id, over_node, under_node))
                            break
            
            return moves

    def get_under_replicated_files(self) -> List[str]:
        """Obtiene archivos que tienen menos réplicas de las necesarias"""
        with self._lock:
            under_replicated = []
            for file_id, replicas in self._replicas.items():
                # Contar réplicas en nodos activos
                active_replicas = sum(
                    1 for r in replicas
                    if r.node_id in self._storage_nodes
                    and self._storage_nodes[r.node_id].state == NodeState.UP
                )
                if active_replicas < self.replication_factor:
                    under_replicated.append((file_id, active_replicas))
            return under_replicated

    def recover_missing_replicas(self) -> List[Tuple[str, str]]:
        """
        Identifica archivos subreplicados y planifica recuperación.
        Retorna lista de (file_id, target_node_id) para nuevas réplicas.
        """
        with self._lock:
            recoveries = []
            available_nodes = self.get_available_storage_nodes()

            if len(available_nodes) < 1:
                return recoveries

            under_replicated = self.get_under_replicated_files()

            for file_id, current_count in under_replicated:
                existing_nodes = {r.node_id for r in self._replicas.get(file_id, [])}
                needed = self.replication_factor - current_count

                for node in available_nodes[:needed]:
                    if node.node_id not in existing_nodes:
                        recoveries.append((file_id, node.node_id))
                        existing_nodes.add(node.node_id)

            return recoveries

    def rebalance_after_storage_failure(self, failed_node_id: str) -> List[Tuple[str, str]]:
        """
        Rebalancea archivos después de que un storage falla.
        Retorna lista de (file_id, new_node_id) para nuevas réplicas.
        """
        with self._lock:
            rebalances = []
            available_nodes = [n for n in self.get_available_storage_nodes()
                             if n.node_id != failed_node_id]

            if len(available_nodes) < 1:
                return rebalances

            # Encontrar archivos que tenían réplicas en el nodo fallido
            affected_files = set()
            for file_id, replicas in self._replicas.items():
                if any(r.node_id == failed_node_id for r in replicas):
                    affected_files.add(file_id)

            for file_id in affected_files:
                # Verificar si necesita rebalanceo
                active_replicas = [
                    r for r in self._replicas.get(file_id, [])
                    if r.node_id in self._storage_nodes
                    and self._storage_nodes[r.node_id].state == NodeState.UP
                ]

                if len(active_replicas) < self.replication_factor:
                    existing_nodes = {r.node_id for r in active_replicas}
                    needed = self.replication_factor - len(active_replicas)

                    for node in available_nodes[:needed]:
                        if node.node_id not in existing_nodes:
                            rebalances.append((file_id, node.node_id))
                            existing_nodes.add(node.node_id)

            return rebalances

    def export_state(self) -> Dict:
        """Exporta el estado para persistencia o sincronización"""
        with self._lock:
            return {
                'replicas': {
                    fid: [r.to_dict() for r in reps]
                    for fid, reps in self._replicas.items()
                },
                'storage_nodes': {
                    nid: node.to_dict() 
                    for nid, node in self._storage_nodes.items()
                }
            }
    
    def import_state(self, state: Dict):
        """Importa estado desde persistencia o sincronización"""
        with self._lock:
            self._replicas.clear()
            self._storage_nodes.clear()
            self._node_files.clear()
            
            for nid, node_dict in state.get('storage_nodes', {}).items():
                self._storage_nodes[nid] = NodeInfo.from_dict(node_dict)
            
            for fid, rep_list in state.get('replicas', {}).items():
                self._replicas[fid] = [ReplicaInfo.from_dict(r) for r in rep_list]
                for rep in self._replicas[fid]:
                    self._node_files[rep.node_id].add(fid)

    def apply_replicas_state(self, file_id: str, replicas_state: List[Dict]):
        """Reemplaza el estado de réplicas de un archivo de forma idempotente"""
        with self._lock:
            replicas = [
                ReplicaInfo.from_dict(r) if isinstance(r, dict) else r
                for r in replicas_state
            ]
            # limpiar índice inverso para este file_id
            for node_id in list(self._node_files.keys()):
                self._node_files[node_id].discard(file_id)
            self._replicas[file_id] = replicas
            for rep in replicas:
                self._node_files[rep.node_id].add(file_id)

