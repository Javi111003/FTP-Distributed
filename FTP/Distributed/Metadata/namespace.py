"""
Sistema de archivos lógico (Namespace) para el servicio de Metadata.
Mantiene la estructura de directorios y archivos virtuales.
"""
import os
import json
import threading
import logging
import uuid
from pathlib import PurePosixPath
from typing import Dict, List, Optional, Tuple
from ..Common.models import FileMetadata
from ..Common.constants import Permission, DistributedResponseCode

logger = logging.getLogger(__name__)


class FileSystemNamespace:
    """
    Gestiona el espacio de nombres del sistema de archivos distribuido.
    Mantiene una vista jerárquica de todos los archivos y directorios.
    """
    
    def __init__(self, persist_path: str = "/data/metadata/namespace.json"):
        self.persist_path = persist_path
        self._lock = threading.RLock()
        # Estructura: path -> FileMetadata
        self._namespace: Dict[str, FileMetadata] = {}
        # Índice por file_id para búsquedas rápidas
        self._id_index: Dict[str, str] = {}  # file_id -> path
        
        self._load_from_disk()
        self._ensure_root()
    
    def _ensure_root(self):
        """Asegura que existe el directorio raíz"""
        if "/" not in self._namespace:
            root = FileMetadata(
                file_id=str(uuid.uuid4()),
                path="/",
                name="/",
                is_directory=True,
                owner="root",
                permissions=Permission.ALL
            )
            self._namespace["/"] = root
            self._id_index[root.file_id] = "/"
            self._persist_to_disk()
    
    def _load_from_disk(self):
        """Carga el namespace desde disco"""
        try:
            if os.path.exists(self.persist_path):
                with open(self.persist_path, 'r') as f:
                    data = json.load(f)
                    for path, meta_dict in data.items():
                        meta = FileMetadata.from_dict(meta_dict)
                        self._namespace[path] = meta
                        self._id_index[meta.file_id] = path
                logger.info(f"Loaded {len(self._namespace)} entries from disk")
        except Exception as e:
            logger.error(f"Error loading namespace: {e}")
    
    def _persist_to_disk(self):
        """Persiste el namespace a disco"""
        try:
            os.makedirs(os.path.dirname(self.persist_path), exist_ok=True)
            data = {path: meta.to_dict() for path, meta in self._namespace.items()}
            with open(self.persist_path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Error persisting namespace: {e}")
    
    def export_state(self) -> Dict:
        """Exporta el namespace completo para snapshot"""
        with self._lock:
            return {
                'namespace': {path: meta.to_dict() for path, meta in self._namespace.items()},
                'id_index': dict(self._id_index)
            }
    
    def import_state(self, state: Dict):
        """Importa el namespace desde snapshot"""
        with self._lock:
            self._namespace.clear()
            self._id_index.clear()
            
            for path, meta_dict in state.get('namespace', {}).items():
                meta = FileMetadata.from_dict(meta_dict)
                self._namespace[path] = meta
                self._id_index[meta.file_id] = path
            
            # Persistir para durabilidad
            self._persist_to_disk()
    
    def upsert_entry(self, meta: FileMetadata):
        """
        Inserta o actualiza una entrada exacta del namespace.
        Idempotente: si ya existe, se sobreescribe con los datos recibidos.
        """
        with self._lock:
            self._namespace[meta.path] = meta
            self._id_index[meta.file_id] = meta.path
            self._persist_to_disk()
    
    def _normalize_path(self, path: str) -> str:
        """Normaliza una ruta"""
        if not path:
            return "/"
        # Usar PurePosixPath para normalización
        normalized = str(PurePosixPath("/" + path.strip("/")))
        return normalized if normalized != "//" else "/"
    
    def _get_parent_path(self, path: str) -> str:
        """Obtiene la ruta del directorio padre"""
        path = self._normalize_path(path)
        if path == "/":
            return "/"
        parent = str(PurePosixPath(path).parent)
        return parent if parent else "/"
    
    def create_user_home(self, username: str) -> Tuple[DistributedResponseCode, Optional[FileMetadata]]:
        """Crea el directorio home para un usuario"""
        home_path = f"/{username}"
        return self.create_directory(home_path, username)
    
    def create_directory(self, path: str, owner: str = "anonymous") -> Tuple[DistributedResponseCode, Optional[FileMetadata]]:
        """Crea un nuevo directorio"""
        path = self._normalize_path(path)
        
        with self._lock:
            # Verificar si ya existe
            if path in self._namespace:
                return (DistributedResponseCode.ALREADY_EXISTS, None)
            
            # Verificar que el padre existe y es un directorio
            parent_path = self._get_parent_path(path)
            if parent_path not in self._namespace:
                return (DistributedResponseCode.NOT_FOUND, None)
            
            parent = self._namespace[parent_path]
            if not parent.is_directory:
                return (DistributedResponseCode.ERROR, None)
            
            # Crear el directorio
            name = PurePosixPath(path).name
            dir_meta = FileMetadata(
                file_id=str(uuid.uuid4()),
                path=path,
                name=name,
                is_directory=True,
                owner=owner,
                permissions=Permission.ALL
            )
            
            self._namespace[path] = dir_meta
            self._id_index[dir_meta.file_id] = path
            self._persist_to_disk()
            
            return (DistributedResponseCode.SUCCESS, dir_meta)
    
    def create_file(self, path: str, owner: str = "anonymous", 
                   size: int = 0, replicas: List[str] = None) -> Tuple[DistributedResponseCode, Optional[FileMetadata]]:
        """Crea o actualiza un archivo en el namespace"""
        path = self._normalize_path(path)
        
        with self._lock:
            # Verificar que el padre existe
            parent_path = self._get_parent_path(path)
            if parent_path not in self._namespace:
                return (DistributedResponseCode.NOT_FOUND, None)
            
            parent = self._namespace[parent_path]
            if not parent.is_directory:
                return (DistributedResponseCode.ERROR, None)
            
            name = PurePosixPath(path).name
            
            # Si el archivo ya existe, actualizarlo
            if path in self._namespace:
                existing = self._namespace[path]
                if existing.is_directory:
                    return (DistributedResponseCode.ERROR, None)
                
                existing.size = size
                existing.version += 1
                existing.modified_at = __import__('time').time()
                if replicas:
                    existing.replicas = replicas
                
                self._persist_to_disk()
                return (DistributedResponseCode.SUCCESS, existing)
            
            # Crear nuevo archivo
            file_meta = FileMetadata(
                file_id=str(uuid.uuid4()),
                path=path,
                name=name,
                size=size,
                owner=owner,
                is_directory=False,
                replicas=replicas or []
            )
            
            self._namespace[path] = file_meta
            self._id_index[file_meta.file_id] = path
            self._persist_to_disk()
            
            return (DistributedResponseCode.SUCCESS, file_meta)
    
    def get_file(self, path: str) -> Optional[FileMetadata]:
        """Obtiene los metadatos de un archivo o directorio"""
        path = self._normalize_path(path)
        with self._lock:
            return self._namespace.get(path)
    
    def get_file_by_id(self, file_id: str) -> Optional[FileMetadata]:
        """Obtiene los metadatos por file_id"""
        with self._lock:
            path = self._id_index.get(file_id)
            if path:
                return self._namespace.get(path)
            return None
    
    def list_directory(self, path: str) -> Tuple[DistributedResponseCode, List[FileMetadata]]:
        """Lista el contenido de un directorio"""
        path = self._normalize_path(path)
        
        with self._lock:
            if path not in self._namespace:
                return (DistributedResponseCode.NOT_FOUND, [])
            
            dir_meta = self._namespace[path]
            if not dir_meta.is_directory:
                return (DistributedResponseCode.ERROR, [])
            
            # Buscar todos los hijos directos
            children = []
            prefix = path if path == "/" else path + "/"
            
            for child_path, meta in self._namespace.items():
                if child_path == path:
                    continue
                if child_path.startswith(prefix):
                    # Verificar que es un hijo directo (no un nieto)
                    remaining = child_path[len(prefix):]
                    if "/" not in remaining:
                        children.append(meta)
            
            return (DistributedResponseCode.SUCCESS, children)
    
    def delete_file(self, path: str) -> DistributedResponseCode:
        """Elimina un archivo del namespace"""
        path = self._normalize_path(path)
        
        with self._lock:
            if path not in self._namespace:
                return DistributedResponseCode.NOT_FOUND
            
            meta = self._namespace[path]
            if meta.is_directory:
                return DistributedResponseCode.ERROR
            
            del self._namespace[path]
            del self._id_index[meta.file_id]
            self._persist_to_disk()
            
            return DistributedResponseCode.SUCCESS
    
    def delete_directory(self, path: str, recursive: bool = True) -> DistributedResponseCode:
        """Elimina un directorio del namespace"""
        path = self._normalize_path(path)
        
        if path == "/":
            return DistributedResponseCode.PERMISSION_DENIED
        
        with self._lock:
            if path not in self._namespace:
                return DistributedResponseCode.NOT_FOUND
            
            meta = self._namespace[path]
            if not meta.is_directory:
                return DistributedResponseCode.ERROR
            
            # Buscar hijos
            prefix = path + "/"
            children = [p for p in self._namespace.keys() if p.startswith(prefix)]
            
            if children and not recursive:
                return DistributedResponseCode.ERROR
            
            # Eliminar todos los hijos primero
            for child_path in children:
                child_meta = self._namespace[child_path]
                del self._id_index[child_meta.file_id]
                del self._namespace[child_path]
            
            # Eliminar el directorio
            del self._id_index[meta.file_id]
            del self._namespace[path]
            self._persist_to_disk()
            
            return DistributedResponseCode.SUCCESS
    
    def rename(self, old_path: str, new_path: str) -> DistributedResponseCode:
        """Renombra un archivo o directorio"""
        old_path = self._normalize_path(old_path)
        new_path = self._normalize_path(new_path)
        
        with self._lock:
            if old_path not in self._namespace:
                return DistributedResponseCode.NOT_FOUND
            
            if new_path in self._namespace:
                return DistributedResponseCode.ALREADY_EXISTS
            
            # Verificar que el nuevo padre existe
            new_parent = self._get_parent_path(new_path)
            if new_parent not in self._namespace:
                return DistributedResponseCode.NOT_FOUND
            
            meta = self._namespace[old_path]
            
            if meta.is_directory:
                # Mover todos los hijos también
                old_prefix = old_path + "/"
                moves = []
                for child_path in list(self._namespace.keys()):
                    if child_path.startswith(old_prefix):
                        new_child_path = new_path + child_path[len(old_path):]
                        moves.append((child_path, new_child_path))
                
                for old_cp, new_cp in moves:
                    child_meta = self._namespace[old_cp]
                    child_meta.path = new_cp
                    child_meta.modified_at = __import__('time').time()
                    del self._namespace[old_cp]
                    self._namespace[new_cp] = child_meta
                    self._id_index[child_meta.file_id] = new_cp
            
            # Mover el propio elemento
            meta.path = new_path
            meta.name = PurePosixPath(new_path).name
            meta.modified_at = __import__('time').time()
            del self._namespace[old_path]
            self._namespace[new_path] = meta
            self._id_index[meta.file_id] = new_path
            
            self._persist_to_disk()
            return DistributedResponseCode.SUCCESS
    
    def update_file_replicas(self, path: str, replicas: List[str]) -> DistributedResponseCode:
        """Actualiza la lista de réplicas de un archivo"""
        path = self._normalize_path(path)
        
        with self._lock:
            if path not in self._namespace:
                return DistributedResponseCode.NOT_FOUND
            
            meta = self._namespace[path]
            if meta.is_directory:
                return DistributedResponseCode.ERROR
            
            meta.replicas = replicas
            self._persist_to_disk()
            return DistributedResponseCode.SUCCESS
    
    def update_file_size(self, path: str, size: int, version: int = None) -> DistributedResponseCode:
        """Actualiza el tamaño y versión de un archivo"""
        path = self._normalize_path(path)
        
        with self._lock:
            if path not in self._namespace:
                return DistributedResponseCode.NOT_FOUND
            
            meta = self._namespace[path]
            if meta.is_directory:
                return DistributedResponseCode.ERROR
            
            meta.size = size
            if version:
                meta.version = version
            else:
                meta.version += 1
            meta.modified_at = __import__('time').time()
            
            self._persist_to_disk()
            return DistributedResponseCode.SUCCESS
    
    def check_permission(self, path: str, username: str, required_perm: Permission) -> bool:
        """Verifica si un usuario tiene los permisos requeridos"""
        path = self._normalize_path(path)
        
        with self._lock:
            if path not in self._namespace:
                return False
            
            meta = self._namespace[path]
            
            # El propietario tiene todos los permisos
            if meta.owner == username:
                return True
            
            # Verificar permisos "otros"
            others_perm = meta.permissions & 0o007
            return (others_perm & required_perm) == required_perm
    
    def get_all_files(self) -> List[FileMetadata]:
        """Obtiene todos los archivos (no directorios) del namespace"""
        with self._lock:
            return [meta for meta in self._namespace.values() if not meta.is_directory]
    
    def export_state(self) -> Dict[str, dict]:
        """Exporta el estado completo para sincronización"""
        with self._lock:
            return {path: meta.to_dict() for path, meta in self._namespace.items()}
    
    def import_state(self, state: Dict[str, dict]):
        """Importa estado desde otro nodo (para sincronización de backup)"""
        with self._lock:
            self._namespace.clear()
            self._id_index.clear()
            for path, meta_dict in state.items():
                meta = FileMetadata.from_dict(meta_dict)
                self._namespace[path] = meta
                self._id_index[meta.file_id] = path
            self._persist_to_disk()

