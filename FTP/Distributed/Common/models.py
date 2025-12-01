"""
Modelos de datos para el sistema distribuido FTP
"""
import time
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional, Any
from .constants import NodeState, NodeType, Permission


@dataclass
class NodeInfo:
    """Información de un nodo en el clúster"""
    node_id: str
    node_type: NodeType
    host: str
    port: int
    state: NodeState = NodeState.UP
    last_heartbeat: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'node_id': self.node_id,
            'node_type': self.node_type.value,
            'host': self.host,
            'port': self.port,
            'state': self.state.value,
            'last_heartbeat': self.last_heartbeat,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NodeInfo':
        return cls(
            node_id=data['node_id'],
            node_type=NodeType(data['node_type']),
            host=data['host'],
            port=data['port'],
            state=NodeState(data['state']),
            last_heartbeat=data.get('last_heartbeat', time.time()),
            metadata=data.get('metadata', {})
        )


@dataclass
class FileMetadata:
    """Metadatos de un archivo en el sistema de archivos lógico"""
    file_id: str
    path: str  # Ruta lógica completa (ej: /user1/docs/file.txt)
    name: str
    size: int = 0
    owner: str = "anonymous"
    group: str = "users"
    permissions: int = Permission.READ_WRITE  # Permisos estilo Unix
    version: int = 1
    created_at: float = field(default_factory=time.time)
    modified_at: float = field(default_factory=time.time)
    is_directory: bool = False
    replicas: List[str] = field(default_factory=list)  # Lista de node_ids que tienen réplicas
    checksum: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'file_id': self.file_id,
            'path': self.path,
            'name': self.name,
            'size': self.size,
            'owner': self.owner,
            'group': self.group,
            'permissions': self.permissions,
            'version': self.version,
            'created_at': self.created_at,
            'modified_at': self.modified_at,
            'is_directory': self.is_directory,
            'replicas': self.replicas,
            'checksum': self.checksum
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FileMetadata':
        return cls(
            file_id=data['file_id'],
            path=data['path'],
            name=data['name'],
            size=data.get('size', 0),
            owner=data.get('owner', 'anonymous'),
            group=data.get('group', 'users'),
            permissions=data.get('permissions', Permission.READ_WRITE),
            version=data.get('version', 1),
            created_at=data.get('created_at', time.time()),
            modified_at=data.get('modified_at', time.time()),
            is_directory=data.get('is_directory', False),
            replicas=data.get('replicas', []),
            checksum=data.get('checksum')
        )


@dataclass
class UserInfo:
    """Información de un usuario del sistema"""
    username: str
    password_hash: str
    home_dir: str = "/"
    groups: List[str] = field(default_factory=lambda: ["users"])
    is_admin: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'username': self.username,
            'password_hash': self.password_hash,
            'home_dir': self.home_dir,
            'groups': self.groups,
            'is_admin': self.is_admin
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'UserInfo':
        return cls(
            username=data['username'],
            password_hash=data['password_hash'],
            home_dir=data.get('home_dir', '/'),
            groups=data.get('groups', ['users']),
            is_admin=data.get('is_admin', False)
        )


@dataclass
class LockInfo:
    """Información de un bloqueo sobre un archivo"""
    file_id: str
    lock_type: str  # READ o WRITE
    holder: str  # ID del nodo que tiene el bloqueo
    acquired_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None
    
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at


@dataclass
class ReplicaInfo:
    """Información de una réplica de archivo"""
    file_id: str
    node_id: str
    version: int
    size: int
    checksum: Optional[str] = None
    is_primary: bool = False
    last_sync: float = field(default_factory=time.time)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'file_id': self.file_id,
            'node_id': self.node_id,
            'version': self.version,
            'size': self.size,
            'checksum': self.checksum,
            'is_primary': self.is_primary,
            'last_sync': self.last_sync
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReplicaInfo':
        return cls(
            file_id=data['file_id'],
            node_id=data['node_id'],
            version=data['version'],
            size=data.get('size', 0),
            checksum=data.get('checksum'),
            is_primary=data.get('is_primary', False),
            last_sync=data.get('last_sync', time.time())
        )


@dataclass
class ClusterState:
    """Estado global del clúster"""
    leader_id: Optional[str] = None
    term: int = 0  # Término de elección
    nodes: Dict[str, NodeInfo] = field(default_factory=dict)
    last_updated: float = field(default_factory=time.time)
    
    def get_storage_nodes(self) -> List[NodeInfo]:
        """Obtiene todos los nodos de almacenamiento activos"""
        return [
            node for node in self.nodes.values()
            if node.node_type == NodeType.STORAGE and node.state == NodeState.UP
        ]
    
    def get_metadata_nodes(self) -> List[NodeInfo]:
        """Obtiene todos los nodos de metadata"""
        return [
            node for node in self.nodes.values()
            if node.node_type == NodeType.METADATA
        ]

