"""
Servicio de autenticación centralizado para el sistema distribuido.
"""
import os
import json
import logging
import threading
from typing import Dict, Optional, Tuple
from passlib.hash import bcrypt
from ..Common.models import UserInfo
from ..Common.constants import DistributedResponseCode

logger = logging.getLogger(__name__)


class AuthService:
    """
    Servicio de autenticación centralizado.
    Gestiona usuarios, credenciales y permisos.
    """
    
    def __init__(self, persist_path: str = "/data/metadata/users.json"):
        self.persist_path = persist_path
        self._lock = threading.RLock()
        self._users: Dict[str, UserInfo] = {}
        
        self._load_from_disk()
        self._ensure_admin_user()
    
    def _load_from_disk(self):
        """Carga usuarios desde disco"""
        try:
            if os.path.exists(self.persist_path):
                with open(self.persist_path, 'r') as f:
                    data = json.load(f)
                    for username, user_dict in data.items():
                        self._users[username] = UserInfo.from_dict(user_dict)
                logger.info(f"Loaded {len(self._users)} users from disk")
        except Exception as e:
            logger.error(f"Error loading users: {e}")
    
    def _persist_to_disk(self):
        """Persiste usuarios a disco"""
        try:
            os.makedirs(os.path.dirname(self.persist_path), exist_ok=True)
            data = {username: user.to_dict() for username, user in self._users.items()}
            with open(self.persist_path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Error persisting users: {e}")
    
    def _ensure_admin_user(self):
        """Asegura que existe un usuario admin inicial"""
        with self._lock:
            if not self._users:
                # Crear usuario admin desde variables de entorno o defaults
                admin_user = os.getenv("ADMIN_USERNAME", "admin")
                admin_pass = os.getenv("ADMIN_PASSWORD", "admin123")
                
                self._users[admin_user] = UserInfo(
                    username=admin_user,
                    password_hash=bcrypt.hash(admin_pass),
                    home_dir=f"/{admin_user}",
                    is_admin=True
                )
                self._persist_to_disk()
                logger.info(f"Created initial admin user: {admin_user}")
    
    def authenticate(self, username: str, password: str) -> Tuple[DistributedResponseCode, Optional[UserInfo]]:
        """Autentica un usuario con username y password"""
        with self._lock:
            user = self._users.get(username)
            if not user:
                return (DistributedResponseCode.NOT_FOUND, None)
            
            if bcrypt.verify(password, user.password_hash):
                return (DistributedResponseCode.SUCCESS, user)
            
            return (DistributedResponseCode.PERMISSION_DENIED, None)
    
    def create_user(self, username: str, password: str, 
                   home_dir: str = None, is_admin: bool = False) -> DistributedResponseCode:
        """Crea un nuevo usuario"""
        with self._lock:
            if username in self._users:
                return DistributedResponseCode.ALREADY_EXISTS
            
            self._users[username] = UserInfo(
                username=username,
                password_hash=bcrypt.hash(password),
                home_dir=home_dir or f"/{username}",
                is_admin=is_admin
            )
            self._persist_to_disk()
            logger.info(f"User created: {username}")
            return DistributedResponseCode.SUCCESS
    
    def delete_user(self, username: str) -> DistributedResponseCode:
        """Elimina un usuario"""
        with self._lock:
            if username not in self._users:
                return DistributedResponseCode.NOT_FOUND
            
            del self._users[username]
            self._persist_to_disk()
            logger.info(f"User deleted: {username}")
            return DistributedResponseCode.SUCCESS
    
    def change_password(self, username: str, new_password: str) -> DistributedResponseCode:
        """Cambia la contraseña de un usuario"""
        with self._lock:
            if username not in self._users:
                return DistributedResponseCode.NOT_FOUND
            
            self._users[username].password_hash = bcrypt.hash(new_password)
            self._persist_to_disk()
            logger.info(f"Password changed for user: {username}")
            return DistributedResponseCode.SUCCESS
    
    def get_user(self, username: str) -> Optional[UserInfo]:
        """Obtiene información de un usuario"""
        with self._lock:
            return self._users.get(username)
    
    def list_users(self) -> list:
        """Lista todos los usuarios"""
        with self._lock:
            return list(self._users.keys())
    
    def is_admin(self, username: str) -> bool:
        """Verifica si un usuario es administrador"""
        with self._lock:
            user = self._users.get(username)
            return user.is_admin if user else False
    
    def get_home_dir(self, username: str) -> str:
        """Obtiene el directorio home de un usuario"""
        with self._lock:
            user = self._users.get(username)
            return user.home_dir if user else f"/{username}"
    
    def export_state(self) -> Dict:
        """Exporta el estado para sincronización"""
        with self._lock:
            return {username: user.to_dict() for username, user in self._users.items()}
    
    def import_state(self, state: Dict):
        """Importa estado desde sincronización"""
        with self._lock:
            self._users.clear()
            for username, user_dict in state.items():
                self._users[username] = UserInfo.from_dict(user_dict)
            self._persist_to_disk()

