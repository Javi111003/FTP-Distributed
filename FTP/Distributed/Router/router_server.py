"""
Servidor Router/Frontend para el sistema FTP distribuido.
Actúa como proxy FTP que redirige operaciones a Metadata y Storage.
"""
import os
import socket
import random
import uuid
import logging
import threading
from pathlib import PurePosixPath
from typing import Dict, Optional, List, Any, Tuple

from ..Common.constants import (
    METADATA_RPC_PORT, ROUTER_FTP_PORT,
    ROUTER_PASV_PORT_START, ROUTER_PASV_PORT_END,
    NodeType, NodeState, DistributedResponseCode
)
from ..Common.models import NodeInfo

from .metadata_client import MetadataClient
from .storage_client import StorageClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FTPSession:
    """Representa una sesión FTP activa"""
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.username: Optional[str] = None
        self.authenticated = False
        self.current_dir = "/"
        self.home_dir = "/"
        
        # Estado de transferencia
        self.transfer_type = 'A'  # ASCII por defecto
        self.passive_mode = False
        self.data_socket: Optional[socket.socket] = None
        self.passive_server: Optional[socket.socket] = None
        self.data_addr: Optional[str] = None
        self.data_port: Optional[int] = None
        
        # Para RNFR/RNTO
        self.rename_from: Optional[str] = None


class RouterServer:
    """
    Servidor FTP que actúa como router/proxy para el sistema distribuido.
    Compatible con RFC 959.
    """
    
    def __init__(self, host: str = '0.0.0.0', port: int = ROUTER_FTP_PORT,
                 public_ip: str = None,
                 metadata_host: str = 'metadata', metadata_port: int = METADATA_RPC_PORT):
        self.host = host
        self.port = port
        self.public_ip = public_ip or os.getenv('PUBLIC_IP', host)
        
        # Generar ID único para este router
        self.node_id = os.getenv('NODE_ID', f"router-{uuid.uuid4().hex[:8]}")
        
        # Clientes para servicios distribuidos
        self.metadata_client = MetadataClient(metadata_host, metadata_port)
        self.storage_client = StorageClient()
        
        # Sesiones activas
        self._sessions: Dict[str, FTPSession] = {}
        self._sessions_lock = threading.Lock()
        
        # Socket del servidor
        self._server_socket: Optional[socket.socket] = None
        self._running = False
    
    def start(self):
        """Inicia el servidor FTP"""
        self._running = True
        
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(50)
        
        logger.info(f"FTP Router started on {self.host}:{self.port}")
        
        try:
            while self._running:
                try:
                    client_socket, client_address = self._server_socket.accept()
                    logger.info(f"Client connected: {client_address}")
                    
                    # Manejar cliente en un nuevo thread
                    thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket, client_address),
                        daemon=True
                    )
                    thread.start()
                except OSError:
                    if self._running:
                        logger.error("Server socket error")
                    break
        except KeyboardInterrupt:
            self.stop()
    
    def stop(self):
        """Detiene el servidor"""
        self._running = False
        if self._server_socket:
            self._server_socket.close()
        logger.info("FTP Router stopped")
    
    def _handle_client(self, client_socket: socket.socket, address: Tuple[str, int]):
        """Maneja la conexión de un cliente FTP"""
        session = FTPSession(str(uuid.uuid4()))
        
        with self._sessions_lock:
            self._sessions[session.session_id] = session
        
        try:
            # Enviar banner de bienvenida
            self._send(client_socket, "220 Welcome to Distributed FTP Server\r\n")
            
            while self._running:
                try:
                    data = client_socket.recv(4096).decode('utf-8', errors='ignore').strip()
                    if not data:
                        break
                    
                    logger.info(f"[session={session.session_id}] cmd={data}")
                    
                    # Parsear comando
                    parts = data.split(None, 1)
                    command = parts[0].upper()
                    args = parts[1] if len(parts) > 1 else ""
                    
                    # Verificar autenticación
                    if command not in ('USER', 'PASS', 'QUIT', 'FEAT', 'SYST') and not session.authenticated:
                        self._send(client_socket, "530 Not logged in\r\n")
                        continue
                    
                    # Procesar comando
                    response = self._process_command(session, client_socket, command, args)
                    if response:
                        logger.info(f"[session={session.session_id}] rsp={response.strip()}")
                        self._send(client_socket, response)
                    
                    if command == 'QUIT':
                        break
                        
                except ConnectionResetError:
                    break
                except Exception as e:
                    logger.error(f"Error processing command: {e}")
                    self._send(client_socket, "500 Internal server error\r\n")
        
        finally:
            self._cleanup_session(session)
            try:
                client_socket.close()
            except:
                pass
            
            with self._sessions_lock:
                self._sessions.pop(session.session_id, None)
    
    def _send(self, sock: socket.socket, data: str):
        """Envía datos al cliente"""
        try:
            sock.sendall(data.encode('utf-8'))
        except Exception as e:
            logger.error(f"Error sending data: {e}")
    
    def _cleanup_session(self, session: FTPSession):
        """Limpia recursos de una sesión"""
        if session.data_socket:
            try:
                session.data_socket.close()
            except:
                pass
        if session.passive_server:
            try:
                session.passive_server.close()
            except:
                pass
    
    def _create_data_connection(self, session: FTPSession) -> bool:
        """Establece la conexión de datos"""
        try:
            if session.passive_mode and session.passive_server:
                session.passive_server.settimeout(30)
                session.data_socket, _ = session.passive_server.accept()
                logger.info(f"[session={session.session_id}] data-conn passive accept ok")
                return True
            elif not session.passive_mode and session.data_addr and session.data_port:
                session.data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                session.data_socket.connect((session.data_addr, session.data_port))
                logger.info(f"[session={session.session_id}] data-conn active connect to {session.data_addr}:{session.data_port} ok")
                return True
            return False
        except Exception as e:
            logger.error(f"[session={session.session_id}] Error creating data connection: {e}")
            return False
    
    def _resolve_path(self, session: FTPSession, path: str) -> str:
        """Resuelve una ruta relativa a absoluta"""
        if not path:
            return session.current_dir
        
        if path.startswith('/'):
            resolved = str(PurePosixPath(path))
        else:
            resolved = str(PurePosixPath(session.current_dir) / path)
        
        # Normalizar
        resolved = str(PurePosixPath(resolved))
        
        return resolved if resolved else "/"
    
    # === Procesamiento de comandos ===
    
    def _process_command(self, session: FTPSession, client_socket: socket.socket,
                        command: str, args: str) -> Optional[str]:
        """Procesa un comando FTP"""
        handlers = {
            'USER': self._cmd_user,
            'PASS': self._cmd_pass,
            'SYST': self._cmd_syst,
            'FEAT': self._cmd_feat,
            'PWD': self._cmd_pwd,
            'CWD': self._cmd_cwd,
            'CDUP': self._cmd_cdup,
            'TYPE': self._cmd_type,
            'PASV': self._cmd_pasv,
            'PORT': self._cmd_port,
            'LIST': self._cmd_list,
            'NLST': self._cmd_nlst,
            'MKD': self._cmd_mkd,
            'RMD': self._cmd_rmd,
            'DELE': self._cmd_dele,
            'RNFR': self._cmd_rnfr,
            'RNTO': self._cmd_rnto,
            'RETR': self._cmd_retr,
            'STOR': self._cmd_stor,
            'STOU': self._cmd_stou,
            'APPE': self._cmd_appe,
            'SIZE': self._cmd_size,
            'NOOP': self._cmd_noop,
            'QUIT': self._cmd_quit,
            'HELP': self._cmd_help,
            'STAT': self._cmd_stat,
            'MODE': self._cmd_mode,
            'STRU': self._cmd_stru,
            'ABOR': self._cmd_abor,
            'SITE': self._cmd_site,
        }
        
        handler = handlers.get(command)
        if handler:
            return handler(session, client_socket, args)
        
        return "502 Command not implemented\r\n"
    
    # === Comandos de autenticación ===
    
    def _cmd_user(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        if not args:
            return "501 Syntax error: USER username\r\n"
        session.username = args
        session.authenticated = False
        return "331 Password required\r\n"
    
    def _cmd_pass(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        if not session.username:
            return "503 Login with USER first\r\n"
        
        success, user_info = self.metadata_client.authenticate(session.username, args)
        
        if success:
            session.authenticated = True
            session.home_dir = user_info.get('home_dir', f"/{session.username}") if user_info else f"/{session.username}"
            session.current_dir = session.home_dir
            
            # Asegurar que existe el directorio home
            self.metadata_client.create_directory(session.home_dir, session.username)
            
            logger.info(f"User {session.username} authenticated")
            return "230 User logged in\r\n"
        
        return "530 Authentication failed\r\n"
    
    # === Comandos de sistema ===
    
    def _cmd_syst(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        return "215 UNIX Type: L8\r\n"
    
    def _cmd_feat(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        return "211-Features:\r\n PASV\r\n SIZE\r\n UTF8\r\n211 End\r\n"
    
    def _cmd_noop(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        return "200 NOOP ok\r\n"
    
    def _cmd_quit(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        return "221 Goodbye\r\n"
    
    def _cmd_help(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        commands = ['USER', 'PASS', 'PWD', 'CWD', 'CDUP', 'LIST', 'NLST', 'MKD', 'RMD',
                   'DELE', 'RETR', 'STOR', 'RNFR', 'RNTO', 'PASV', 'PORT', 'TYPE',
                   'SYST', 'FEAT', 'NOOP', 'QUIT', 'SIZE', 'STAT', 'HELP']
        return f"214-Commands:\r\n {' '.join(commands)}\r\n214 End\r\n"
    
    def _cmd_stat(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        return f"211-Status:\r\n User: {session.username}\r\n Dir: {session.current_dir}\r\n211 End\r\n"
    
    def _cmd_mode(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        if args.upper() == 'S':
            return "200 Mode set to Stream\r\n"
        return "504 Mode not supported\r\n"
    
    def _cmd_stru(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        if args.upper() == 'F':
            return "200 Structure set to File\r\n"
        return "504 Structure not supported\r\n"
    
    def _cmd_abor(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        self._cleanup_session(session)
        return "226 ABOR command successful\r\n"
    
    def _cmd_site(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        return "500 SITE commands not implemented\r\n"
    
    # === Comandos de tipo y conexión ===
    
    def _cmd_type(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        if args.upper() in ('A', 'I', 'L'):
            session.transfer_type = args.upper()
            type_name = {'A': 'ASCII', 'I': 'Binary', 'L': 'Binary'}
            return f"200 Type set to {type_name.get(args.upper(), args.upper())}\r\n"
        return "504 Type not supported\r\n"
    
    def _cmd_pasv(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        try:
            # Cerrar conexión anterior si existe
            if session.passive_server:
                session.passive_server.close()
            
            session.passive_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            session.passive_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Buscar puerto disponible
            for port in range(ROUTER_PASV_PORT_START, ROUTER_PASV_PORT_END):
                try:
                    session.passive_server.bind((self.host, port))
                    session.passive_server.listen(1)
                    session.passive_mode = True
                    
                    # Formatear respuesta
                    ip = self.public_ip if self.public_ip and self.public_ip != '0.0.0.0' else socket.gethostbyname(socket.gethostname())
                    ip_parts = ip.split('.')
                    port_high = port >> 8
                    port_low = port & 0xFF
                    logger.info(f"[session={session.session_id}] PASV listen {ip}:{port}")
                    return f"227 Entering Passive Mode ({','.join(ip_parts)},{port_high},{port_low})\r\n"
                except OSError:
                    continue
            
            return "425 Can't enter passive mode\r\n"
        except Exception as e:
            logger.error(f"PASV error: {e}")
            return "425 Can't enter passive mode\r\n"
    
    def _cmd_port(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        try:
            nums = args.split(',')
            if len(nums) != 6:
                return "501 Invalid PORT command\r\n"
            
            session.data_addr = '.'.join(nums[:4])
            session.data_port = (int(nums[4]) << 8) + int(nums[5])
            session.passive_mode = False
            logger.info(f"[session={session.session_id}] PORT set {session.data_addr}:{session.data_port}")
            return "200 PORT command successful\r\n"
        except:
            return "501 Invalid PORT command\r\n"
    
    # === Comandos de directorio ===
    
    def _cmd_pwd(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        return f'257 "{session.current_dir}"\r\n'
    
    def _cmd_cwd(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        if not args:
            return "501 Syntax error\r\n"
        
        new_path = self._resolve_path(session, args)
        success, meta, _ = self.metadata_client.lookup_file(new_path)
        
        if success and meta and meta.get('is_directory'):
            session.current_dir = new_path
            return "250 Directory changed\r\n"
        
        return "550 Directory not found\r\n"
    
    def _cmd_cdup(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        parent = str(PurePosixPath(session.current_dir).parent)
        if parent and parent != session.current_dir:
            session.current_dir = parent if parent else "/"
            return "200 Directory changed to parent\r\n"
        return "550 Cannot go up\r\n"
    
    def _cmd_mkd(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        if not args:
            return "501 Syntax error\r\n"
        
        path = self._resolve_path(session, args)
        success = self.metadata_client.create_directory(path, session.username)
        
        if success:
            return f'257 "{path}" created\r\n'
        return "550 Could not create directory\r\n"
    
    def _cmd_rmd(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        if not args:
            return "501 Syntax error\r\n"
        
        path = self._resolve_path(session, args)
        success = self.metadata_client.remove_directory(path)
        
        if success:
            return "250 Directory removed\r\n"
        return "550 Could not remove directory\r\n"
    
    def _cmd_list(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        path = self._resolve_path(session, args) if args and not args.startswith('-') else session.current_dir
        
        success, entries = self.metadata_client.list_directory(path)
        
        if not success:
            return "550 Directory not found\r\n"
        
        if not self._create_data_connection(session):
            return "425 No data connection\r\n"
        
        self._send(client_socket, "150 Opening data connection\r\n")
        
        try:
            logger.info(f"[session={session.session_id}] LIST path={path}")
            listing = ""
            for entry in entries:
                # Formato similar a ls -l
                is_dir = 'd' if entry.get('is_directory') else '-'
                size = entry.get('size', 0)
                name = entry.get('name', '')
                listing += f"{is_dir}rw-r--r-- 1 {entry.get('owner', 'ftp')} ftp {size:>10} Jan 01 00:00 {name}\r\n"
            
            session.data_socket.sendall(listing.encode('utf-8'))
            session.data_socket.close()
            session.data_socket = None
            
            return "226 Transfer complete\r\n"
        except Exception as e:
            logger.error(f"LIST error: {e}")
            return "550 Error listing directory\r\n"
    
    def _cmd_nlst(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        path = self._resolve_path(session, args) if args and not args.startswith('-') else session.current_dir
        
        success, entries = self.metadata_client.list_directory(path)
        
        if not success:
            return "550 Directory not found\r\n"
        
        if not self._create_data_connection(session):
            return "425 No data connection\r\n"
        
        self._send(client_socket, "150 Opening data connection\r\n")
        
        try:
            logger.info(f"[session={session.session_id}] NLST path={path}")
            listing = "\r\n".join(entry.get('name', '') for entry in entries) + "\r\n"
            session.data_socket.sendall(listing.encode('utf-8'))
            session.data_socket.close()
            session.data_socket = None
            
            return "226 Transfer complete\r\n"
        except Exception as e:
            logger.error(f"NLST error: {e}")
            return "550 Error listing directory\r\n"
    
    # === Comandos de archivos ===
    
    def _cmd_dele(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        if not args:
            return "501 Syntax error\r\n"
        
        path = self._resolve_path(session, args)
        success, file_id, storage_nodes = self.metadata_client.delete_file(path)
        
        if success:
            # Eliminar de los nodos de storage
            for node in storage_nodes:
                self.storage_client.delete_file(node['host'], node['port'], file_id)
            return "250 File deleted\r\n"
        
        return "550 Could not delete file\r\n"
    
    def _cmd_rnfr(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        if not args:
            return "501 Syntax error\r\n"
        
        path = self._resolve_path(session, args)
        success, meta, _ = self.metadata_client.lookup_file(path)
        
        if success:
            session.rename_from = path
            return "350 Ready for RNTO\r\n"
        
        return "550 File not found\r\n"
    
    def _cmd_rnto(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        if not session.rename_from:
            return "503 RNFR required first\r\n"
        
        if not args:
            session.rename_from = None
            return "501 Syntax error\r\n"
        
        new_path = self._resolve_path(session, args)
        success = self.metadata_client.rename(session.rename_from, new_path)
        session.rename_from = None
        
        if success:
            return "250 File renamed\r\n"
        return "553 Rename failed\r\n"
    
    def _cmd_size(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        if not args:
            return "501 Syntax error\r\n"
        
        path = self._resolve_path(session, args)
        success, meta, _ = self.metadata_client.lookup_file(path)
        
        if success and meta and not meta.get('is_directory'):
            return f"213 {meta.get('size', 0)}\r\n"
        
        return "550 File not found\r\n"
    
    # === Comandos de transferencia ===
    
    def _cmd_retr(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        if not args:
            return "501 Syntax error\r\n"
        
        path = self._resolve_path(session, args)
        
        # Obtener metadatos y nodos de réplica
        success, meta, replica_nodes = self.metadata_client.lookup_file(path)
        
        if not success or not meta:
            return "550 File not found\r\n"
        
        if meta.get('is_directory'):
            return "550 Not a regular file\r\n"
        
        if not replica_nodes:
            return "550 No replicas available\r\n"
        
        # Obtener el archivo de alguna réplica
        file_id = meta.get('file_id')
        data = self.storage_client.retrieve_from_any(replica_nodes, file_id)
        
        if data is None:
            return "550 Could not retrieve file\r\n"
        
        logger.info(f"[session={session.session_id}] RETR path={path} size={len(data)}")
        
        if not self._create_data_connection(session):
            return "425 No data connection\r\n"
        
        self._send(client_socket, "150 Opening data connection\r\n")
        
        try:
            session.data_socket.sendall(data)
            session.data_socket.close()
            session.data_socket = None
            
            return "226 Transfer complete\r\n"
        except Exception as e:
            logger.error(f"RETR error: {e}")
            return "550 Error during transfer\r\n"
    
    def _cmd_stor(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        if not args:
            return "501 Syntax error\r\n"
        
        path = self._resolve_path(session, args)
        
        # Crear entrada en metadata y obtener nodos de storage
        success, meta, storage_nodes = self.metadata_client.create_file(
            path, session.username
        )
        
        if not success:
            return "550 Could not create file\r\n"
        
        if not storage_nodes:
            return "550 No storage nodes available\r\n"
        
        if not self._create_data_connection(session):
            return "425 No data connection\r\n"
        
        self._send(client_socket, "150 Opening data connection\r\n")
        
        try:
            # Recibir datos
            data = b''
            while True:
                chunk = session.data_socket.recv(8192)
                if not chunk:
                    break
                data += chunk
            
            session.data_socket.close()
            session.data_socket = None
            
            # Almacenar en nodos de storage
            file_id = meta.get('file_id')
            stored = self.storage_client.store_with_replication(
                storage_nodes, file_id, data
            )
            
            if stored > 0:
                # Actualizar tamaño en metadata
                self.metadata_client.update_file_meta(path, size=len(data))
                logger.info(f"[session={session.session_id}] STOR path={path} size={len(data)} replicas={stored}")
                return "226 Transfer complete\r\n"
            
            return "550 Could not store file\r\n"
            
        except Exception as e:
            logger.error(f"STOR error: {e}")
            return "550 Error during transfer\r\n"
    
    def _cmd_stou(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        # Generar nombre único
        unique_name = f"file_{uuid.uuid4().hex[:8]}"
        return self._cmd_stor(session, client_socket, unique_name)
    
    def _cmd_appe(self, session: FTPSession, client_socket: socket.socket, args: str) -> str:
        if not args:
            return "501 Syntax error\r\n"
        
        path = self._resolve_path(session, args)
        
        # Verificar si el archivo existe
        success, meta, replica_nodes = self.metadata_client.lookup_file(path)
        
        if not self._create_data_connection(session):
            return "425 No data connection\r\n"
        
        self._send(client_socket, "150 Opening data connection\r\n")
        
        try:
            # Recibir datos nuevos
            new_data = b''
            while True:
                chunk = session.data_socket.recv(8192)
                if not chunk:
                    break
                new_data += chunk
            
            session.data_socket.close()
            session.data_socket = None
            
            if success and meta:
                # Archivo existe, obtener datos existentes
                file_id = meta.get('file_id')
                existing_data = self.storage_client.retrieve_from_any(replica_nodes, file_id)
                if existing_data:
                    new_data = existing_data + new_data
                
                # Actualizar réplicas
                stored = self.storage_client.store_with_replication(
                    replica_nodes, file_id, new_data, version=meta.get('version', 0) + 1
                )
            else:
                # Crear nuevo archivo
                success, meta, storage_nodes = self.metadata_client.create_file(
                    path, session.username
                )
                if success:
                    file_id = meta.get('file_id')
                    stored = self.storage_client.store_with_replication(
                        storage_nodes, file_id, new_data
                    )
                else:
                    return "550 Could not create file\r\n"
            
            if stored > 0:
                self.metadata_client.update_file_meta(path, size=len(new_data))
                logger.info(f"[session={session.session_id}] APPE path={path} size={len(new_data)}")
                return "226 Transfer complete\r\n"
            
            return "550 Could not append to file\r\n"
            
        except Exception as e:
            logger.error(f"APPE error: {e}")
            return "550 Error during transfer\r\n"


def main():
    """Punto de entrada principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description="FTP Router Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host")
    parser.add_argument("--port", type=int, default=ROUTER_FTP_PORT, help="Port")
    parser.add_argument("--public-ip", help="Public IP for PASV mode")
    parser.add_argument("--metadata-host", default="metadata", help="Metadata service host")
    parser.add_argument("--metadata-port", type=int, default=METADATA_RPC_PORT, help="Metadata service port")
    
    args = parser.parse_args()
    
    server = RouterServer(
        args.host, args.port, args.public_ip,
        args.metadata_host, args.metadata_port
    )
    server.start()


if __name__ == "__main__":
    main()

