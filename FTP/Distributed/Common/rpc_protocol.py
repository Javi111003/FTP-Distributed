"""
Protocolo RPC para comunicación entre nodos del sistema distribuido.
Implementa un protocolo simple basado en JSON sobre TCP.
"""
import json
import socket
import struct
import threading
import logging
from typing import Dict, Any, Optional, Callable, Tuple
from concurrent.futures import ThreadPoolExecutor
from .constants import RPC_TIMEOUT, MessageType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RPCMessage:
    """Representa un mensaje RPC"""
    
    def __init__(self, msg_type: MessageType, payload: Dict[str, Any], 
                 request_id: Optional[str] = None):
        self.msg_type = msg_type
        self.payload = payload
        self.request_id = request_id or self._generate_id()
    
    def _generate_id(self) -> str:
        import uuid
        return str(uuid.uuid4())
    
    def to_bytes(self) -> bytes:
        """Serializa el mensaje a bytes"""
        data = {
            'type': self.msg_type.value if isinstance(self.msg_type, MessageType) else self.msg_type,
            'payload': self.payload,
            'request_id': self.request_id
        }
        json_data = json.dumps(data).encode('utf-8')
        # Prefijo de 4 bytes con la longitud del mensaje
        length_prefix = struct.pack('!I', len(json_data))
        return length_prefix + json_data
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'RPCMessage':
        """Deserializa un mensaje desde bytes"""
        json_data = json.loads(data.decode('utf-8'))
        msg_type = json_data['type']
        # Intentar convertir a MessageType si es posible
        try:
            msg_type = MessageType(msg_type)
        except ValueError:
            pass
        return cls(
            msg_type=msg_type,
            payload=json_data['payload'],
            request_id=json_data['request_id']
        )


class RPCClient:
    """Cliente RPC para comunicarse con otros nodos"""
    
    def __init__(self, timeout: int = RPC_TIMEOUT):
        self.timeout = timeout
    
    def call(self, host: str, port: int, message: RPCMessage) -> Optional[RPCMessage]:
        """Realiza una llamada RPC síncrona"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            sock.connect((host, port))
            
            # Enviar mensaje
            sock.sendall(message.to_bytes())
            
            # Recibir respuesta
            response = self._receive_message(sock)
            sock.close()
            return response
            
        except socket.timeout:
            logger.warning(f"RPC timeout connecting to {host}:{port}")
            return None
        except ConnectionRefusedError:
            logger.warning(f"Connection refused to {host}:{port}")
            return None
        except Exception as e:
            logger.error(f"RPC error: {e}")
            return None
    
    def _receive_message(self, sock: socket.socket) -> Optional[RPCMessage]:
        """Recibe un mensaje completo del socket"""
        try:
            # Leer longitud del mensaje (4 bytes)
            length_data = self._recv_exact(sock, 4)
            if not length_data:
                return None
            
            msg_length = struct.unpack('!I', length_data)[0]
            
            # Leer el mensaje completo
            msg_data = self._recv_exact(sock, msg_length)
            if not msg_data:
                return None
            
            return RPCMessage.from_bytes(msg_data)
            
        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            return None
    
    def _recv_exact(self, sock: socket.socket, n: int) -> Optional[bytes]:
        """Recibe exactamente n bytes del socket"""
        data = b''
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                return None
            data += chunk
        return data


class RPCServer:
    """Servidor RPC para recibir llamadas de otros nodos"""
    
    def __init__(self, host: str, port: int, max_workers: int = 10):
        self.host = host
        self.port = port
        self.handlers: Dict[MessageType, Callable] = {}
        self.running = False
        self.server_socket: Optional[socket.socket] = None
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self._lock = threading.Lock()
    
    def register_handler(self, msg_type: MessageType, handler: Callable):
        """Registra un manejador para un tipo de mensaje"""
        with self._lock:
            self.handlers[msg_type] = handler
    
    def start(self):
        """Inicia el servidor RPC en un hilo separado"""
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(50)
        
        logger.info(f"RPC Server started on {self.host}:{self.port}")
        
        thread = threading.Thread(target=self._accept_loop, daemon=True)
        thread.start()
    
    def stop(self):
        """Detiene el servidor RPC"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        self.executor.shutdown(wait=False)
    
    def _accept_loop(self):
        """Loop principal de aceptación de conexiones"""
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                self.executor.submit(self._handle_client, client_socket, address)
            except OSError:
                if self.running:
                    logger.error("Server socket error")
                break
            except Exception as e:
                logger.error(f"Accept error: {e}")
    
    def _handle_client(self, client_socket: socket.socket, address: Tuple[str, int]):
        """Maneja una conexión de cliente"""
        try:
            # Leer longitud del mensaje
            length_data = self._recv_exact(client_socket, 4)
            if not length_data:
                return
            
            msg_length = struct.unpack('!I', length_data)[0]
            
            # Leer mensaje
            msg_data = self._recv_exact(client_socket, msg_length)
            if not msg_data:
                return
            
            message = RPCMessage.from_bytes(msg_data)
            
            # Buscar y ejecutar handler
            handler = self.handlers.get(message.msg_type)
            if handler:
                response = handler(message)
                if response:
                    client_socket.sendall(response.to_bytes())
            else:
                logger.warning(f"No handler for message type: {message.msg_type}")
                
        except Exception as e:
            logger.error(f"Error handling client {address}: {e}")
        finally:
            try:
                client_socket.close()
            except:
                pass
    
    def _recv_exact(self, sock: socket.socket, n: int) -> Optional[bytes]:
        """Recibe exactamente n bytes"""
        data = b''
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                return None
            data += chunk
        return data


class RPCClientPool:
    """Pool de conexiones RPC para mejorar rendimiento"""
    
    def __init__(self, timeout: int = RPC_TIMEOUT):
        self.timeout = timeout
        self._clients: Dict[Tuple[str, int], RPCClient] = {}
        self._lock = threading.Lock()
    
    def get_client(self, host: str, port: int) -> RPCClient:
        """Obtiene un cliente para un destino específico"""
        key = (host, port)
        with self._lock:
            if key not in self._clients:
                self._clients[key] = RPCClient(self.timeout)
            return self._clients[key]
    
    def call(self, host: str, port: int, message: RPCMessage) -> Optional[RPCMessage]:
        """Realiza una llamada RPC usando el pool"""
        client = self.get_client(host, port)
        return client.call(host, port, message)

