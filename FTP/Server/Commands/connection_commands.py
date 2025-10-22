from FTP.Server.Commands.base_command import Command
import socket
import random
import os

class PasvCommand(Command):
    def execute(self, server, client_socket, args):
        try:
            server.passive_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            passive_port = random.randint(30000, 30009)
            server.passive_server.bind((server.host, passive_port))
            server.passive_server.listen(1)
            
             # Usar IP pública configurada o variable de entorno
            public_ip = server.public_ip
            if not public_ip or public_ip == '0.0.0.0':
                # Intentar obtener de variable de entorno
                public_ip = os.getenv('PUBLIC_IP')
                if not public_ip:
                    # Fallback a IP local del contenedor
                    public_ip = socket.gethostbyname(socket.gethostname())
            
            host_parts = public_ip.split('.')
            port_high = passive_port >> 8
            port_low = passive_port & 0xFF
            
            response = f"227 Entering Passive Mode ({','.join(host_parts)},{port_high},{port_low})\r\n"
            server.passive_mode = True
            return response
        except:
            return "425 Can't enter passive mode\r\n"

class PortCommand(Command):
    def execute(self, server, client_socket, args):
        if not args:
            return "501 Syntax error\r\n"
        
        try:
            nums = args[0].split(',')
            if len(nums) != 6:
                raise ValueError
            
            server.data_addr = '.'.join(nums[:4])
            server.data_port = (int(nums[4]) << 8) + int(nums[5])
            server.passive_mode = False
            
            return "200 PORT command successful\r\n"
        except:
            return "501 Invalid PORT command\r\n"
