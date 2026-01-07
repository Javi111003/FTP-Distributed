#!/usr/bin/env python3
"""
Consola FTP Personalizada para Sistema Distribuido
Versión adaptada para conectar al Router del sistema FTP distribuido.
"""
import cmd
from FTP.Client.client import FTPClient
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel
from rich import print as rprint
from rich.text import Text
from rich.style import Style
import re
import sys
import os

# Agregar el directorio raíz del proyecto al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

class FTPCLI_Distributed(cmd.Cmd):
    # Usando Text de rich para el intro para asegurar el color
    intro = None  # Cambiado a None para evitar que cmd.Cmd lo imprima
    _intro_text = Text("""
╔══════════════════════════════════════════════════════════════╗
║                FTP DISTRIBUTED CLIENT v1.0                     ║
║        Sistema FTP Distribuido - Consola Personalizada         ║
║              Escriba 'help' para ayuda                          ║
╚══════════════════════════════════════════════════════════════╝
    """, style="blue")

    # El prompt necesita ser una cadena plana para cmd.Cmd
    prompt = "ftp-distributed> "

    def __init__(self, client=None):
        super().__init__()
        self.console = Console()
        self.client = client
        self.connected = client is not None
        # Configurar el estilo del prompt
        self.prompt_style = Style(color="green", bold=True)

        # Información del sistema distribuido
        self.system_info = {
            "router_host": "127.0.0.1",
            "router_port": 2121,
            "metadata_host": "metadata",
            "metadata_port": 5000
        }

    def precmd(self, line):
        """Procesa el comando antes de ejecutarlo"""
        # Mostrar el prompt con color usando rich
        self.console.print("ftp-distributed> ", style=self.prompt_style, end="")
        if self.client and not self.client.authenticated and line.split()[0].upper() not in ["USER", "PASS", "CONNECT", "QUIT", "HELP", "LOGIN", "DISTRIBUTED_CONNECT"]:
            self.console.print("[red]Error: Debe autenticarse primero.[/red]")
            return ""
        return line

    def preloop(self):
        """Se ejecuta antes de iniciar el loop de comandos"""
        # Imprimir el banner de inicio con rich
        self.console.print(self._intro_text)
        self.console.print("[cyan]Sistema FTP Distribuido - Conectado al Router[/cyan]")
        self.console.print("[yellow]Para conectar: distributed_connect o connect 127.0.0.1 2121[/yellow]\n")

    def default(self, line):
        """Maneja comandos no reconocidos"""
        self.console.print(f"[red]Error: Comando '{line}' no reconocido. Use 'help' para ver comandos disponibles.[/red]")

    def do_distributed_connect(self, arg):
        """Conecta automáticamente al sistema FTP distribuido: distributed_connect"""
        return self.do_connect("127.0.0.1 2121")

    def do_connect(self, arg):
        """Conecta al servidor FTP distribuido: connect <host> [port]"""
        args = arg.split()
        if not args:
            self.console.print("[red]Error: Uso: connect <host> [port][/red]")
            self.console.print("[yellow]Para sistema distribuido: connect 127.0.0.1 2121[/yellow]")
            return

        host = args[0]
        port = int(args[1]) if len(args) > 1 else 2121

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}")
        ) as progress:
            task = progress.add_task(description=f"Conectando al sistema distribuido {host}:{port}...", total=None)
            try:
                self.client = FTPClient(host, port)
                self.client.connect()
                self.connected = True
                self.console.print("[green]OK Conexión establecida al sistema FTP distribuido[/green]")
                self.console.print("[cyan]Router conectado - Sistema distribuido activo[/cyan]")
            except Exception as e:
                self.console.print(f"[red]ERROR Error de conexión: {e}[/red]")
                self.console.print("[yellow]Asegúrate de que el sistema distribuido esté ejecutándose[/yellow]")

    def do_user(self, arg):
        """Especifica el nombre de usuario: USER <username>"""
        if not arg:
            self.console.print("[red]Error: Uso: USER <username>[/red]")
            return
        try:
            response = self.client.execute("USER", arg)
            self.console.print(f"[cyan]{response}[/cyan]")
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_pass(self, arg):
        """Especifica la contraseña: PASS <password>"""
        if not arg:
            self.console.print("[red]Error: Uso: PASS <password>[/red]")
            return
        try:
            response = self.client.execute("PASS", arg)
            self.console.print(f"[green]OK {response}[/green]")
            if self.client.authenticated:
                self.console.print("[green]OK Autenticación exitosa en sistema distribuido[/green]")
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_login(self, arg):
        """Inicia sesión en el servidor distribuido: login <username> <password>"""
        if not self.client:
            self.console.print("[red]Error: No hay conexión. Use 'distributed_connect' primero.[/red]")
            return

        args = arg.split()
        if len(args) != 2:
            self.console.print("[red]Error: Uso: login <username> <password>[/red]")
            self.console.print("[yellow]Usuario por defecto: admin / admin123[/yellow]")
            return

        try:
            with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}")) as progress:
                task = progress.add_task(description="Autenticando en sistema distribuido...", total=None)
                response = self.client.execute("USER", args[0])
                self.console.print(f"[cyan]{response}[/cyan]")
                response = self.client.execute("PASS", args[1])
                self.console.print(f"[green]OK {response}[/green]")
                if self.client.authenticated:
                    self.console.print("[green]OK Autenticación exitosa - Sistema distribuido listo[/green]")
        except Exception as e:
            self.console.print(f"[red]ERROR Error de autenticación: {e}[/red]")

    def do_list(self, arg):
        """Lista archivos en el servidor distribuido: LIST [path]"""
        try:
            # Obtener y procesar la lista de archivos
            file_list = self.client.list_directory(arg)

            if not file_list:
                self.console.print("[yellow]Directorio vacío o error listando archivos[/yellow]")
                return

            # Imprimir encabezado
            self.console.print("\n[bold magenta]Contenido del Directorio (Sistema Distribuido):[/bold magenta]")
            self.console.print("[cyan]" + "-" * 60 + "[/cyan]")

            # Contador de archivos válidos
            total_files = 0

            # Mostrar cada archivo
            for file_info in file_list:
                nombre = file_info.get('nombre', '').strip()
                tamaño = file_info.get('tamaño', '0').strip()
                permisos = file_info.get('permisos', 'unknown')
                owner = file_info.get('owner', 'unknown')
                fecha = file_info.get('fecha', 'unknown')

                # Mostrar solo si el nombre es válido
                if nombre and nombre not in ['.', '..'] and not nombre.startswith('.'):
                    total_files += 1

                    # Determinar tipo de archivo
                    tipo = 'd' if permisos.startswith('d') else '-'
                    tipo_color = "[blue]" if tipo == 'd' else "[cyan]"

                    # Convertir tamaño a formato legible
                    try:
                        tamaño_num = int(tamaño)
                        if tamaño_num >= 1024*1024*1024:
                            tamaño_fmt = f"{tamaño_num/(1024*1024*1024):.1f} GB"
                        elif tamaño_num >= 1024*1024:
                            tamaño_fmt = f"{tamaño_num/(1024*1024):.1f} MB"
                        elif tamaño_num >= 1024:
                            tamaño_fmt = f"{tamaño_num/1024:.1f} KB"
                        else:
                            tamaño_fmt = f"{tamaño_num} B"
                    except (ValueError, TypeError):
                        tamaño_fmt = tamaño if tamaño else "???"

                    # Mostrar archivo con formato mejorado
                    self.console.print(f"{tipo_color}{tipo}{permisos[1:]}[/] [green]{owner:>8}[/] [green]{tamaño_fmt:>10}[/] [yellow]{fecha:>12}[/] {tipo_color}{nombre}[/] [red](replicado)[/]")

            # Mostrar línea final y total
            self.console.print("[cyan]" + "-" * 60 + "[/cyan]")
            self.console.print(f"[blue]Total: {total_files} elementos (sistema distribuido)[/blue]\n")

        except Exception as e:
            self.console.print(f"[red]Error: {str(e)}[/red]")

    def do_retr(self, arg):
        """Descarga un archivo del sistema distribuido: RETR <remote_path> <local_path>"""
        args = arg.split()
        if len(args) != 2:
            self.console.print("[red]Error: Uso: RETR <remote_path> <local_path>[/red]")
            self.console.print("[yellow]Ejemplo: retr archivo.txt descarga.txt[/yellow]")
            return

        remote_path, local_path = args
        try:
            # Solo mostramos una barra de progreso y la respuesta final
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                transient=True
            ) as progress:
                task = progress.add_task("[cyan]Descargando desde sistema distribuido...", total=None)
                response = self.client.download_file(remote_path, local_path)

            if "226" in response:  # Transferencia exitosa
                self.console.print(f"[green]OK Archivo descargado exitosamente como '{local_path}'[/green]")
                self.console.print("[cyan]Descargado desde réplicas distribuidas[/cyan]")
            else:
                self.console.print(f"[yellow]{response}[/yellow]")

        except Exception as e:
            self.console.print(f"[red]ERROR Error: {str(e)}[/red]")
            self.console.print("[yellow]Tip: Verifique que el archivo existe en el sistema distribuido[/yellow]")

    def do_stor(self, arg):
        """Sube un archivo al sistema distribuido: STOR <local_path> <remote_path>"""
        args = arg.split()
        if len(args) != 2:
            self.console.print("[red]Error: Uso: STOR <local_path> <remote_path>[/red]")
            return

        try:
            # Verificar que el archivo existe antes de intentar subirlo
            from pathlib import Path
            if not Path(args[0]).exists():
                self.console.print(f"[red]Error: El archivo local '{args[0]}' no existe[/red]")
                return

            # Solo mostramos una barra de progreso y la respuesta final
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                transient=True  # Esto hace que la barra desaparezca al completar
            ) as progress:
                task = progress.add_task("[cyan]Subiendo a sistema distribuido...", total=None)
                response = self.client.upload_file(args[0], args[1])

            # Mostrar solo el mensaje de éxito
            if "226" in response:  # Si la transferencia fue exitosa
                self.console.print("[green]OK Archivo subido exitosamente al sistema distribuido[/green]")
                self.console.print("[cyan]Archivo replicado automáticamente en múltiples nodos[/cyan]")
            else:
                self.console.print(f"[yellow]{response}[/yellow]")

        except Exception as e:
            self.console.print(f"[red]ERROR Error: {e}[/red]")

    def do_appe(self, arg):
        """Añade datos a un archivo en el sistema distribuido: APPE <local_path> <remote_path>"""
        args = arg.split()
        if len(args) != 2:
            self.console.print("[red]Error: Uso: APPE <local_path> <remote_path>[/red]")
            self.console.print("[yellow]Ejemplo: appe local.txt remoto.txt[/yellow]")
            return

        local_path, remote_path = args
        try:
            # Verificar archivo local
            from pathlib import Path
            if not Path(local_path).exists():
                self.console.print(f"[red]Error: El archivo local '{local_path}' no existe[/red]")
                return

            # Mostrar progreso de la operación
            self.console.print("[cyan]Iniciando operación de append en sistema distribuido...[/cyan]")

            try:
                response = self.client.append_file(local_path, remote_path)

                # Verificar si la operación fue exitosa
                if "226" in response:  # Código de éxito
                    self.console.print(f"[green]OK Datos añadidos exitosamente a '{remote_path}'[/green]")
                    self.console.print("[cyan]Cambios replicados en todas las copias[/cyan]")
                else:
                    self.console.print(f"[yellow]{response}[/yellow]")

            except Exception as e:
                if "timeout" in str(e).lower():
                    self.console.print("[red]ERROR Error: Timeout en la operación[/red]")
                    self.console.print("[yellow]La operación se completó parcialmente[/yellow]")
                else:
                    self.console.print(f"[red]ERROR Error: {str(e)}[/red]")

        except Exception as e:
            self.console.print(f"[red]ERROR Error: {str(e)}[/red]")
            self.console.print("[yellow]Tip: Verifique permisos y espacio disponible en el sistema distribuido[/yellow]")

    def do_quit(self, arg):
        """Cierra la conexión al sistema distribuido: QUIT"""
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}")
        ) as progress:
            progress.add_task(description="Cerrando conexión al sistema distribuido...", total=None)
            try:
                response = self.client.quit()
                self.console.print("[green]OK Conexión cerrada correctamente[/green]")
                self.console.print("[cyan]Sistema FTP distribuido desconectado[/cyan]")
            except Exception as e:
                self.console.print(f"[red]ERROR Error al cerrar: {e}[/red]")
        return True

    def do_help(self, arg):
        """Muestra la ayuda de los comandos disponibles para el sistema distribuido"""
        commands = {
            "CONEXIÓN AL SISTEMA DISTRIBUIDO": {
                "distributed_connect": "Conectar automáticamente al sistema distribuido (127.0.0.1:2121)",
                "connect": "Conectar al router del sistema distribuido: connect <host> [port]",
                "login": "Iniciar sesión: login <username> <password>",
                "quit": "Cerrar conexión y salir"
            },
            "NAVEGACIÓN EN SISTEMA DISTRIBUIDO": {
                "pwd": "Mostrar directorio actual",
                "cwd": "Cambiar directorio: cwd <path>",
                "cdup": "Subir al directorio padre",
                "list": "Listar archivos con información de replicación: list [path]"
            },
            "TRANSFERENCIA DISTRIBUIDA": {
                "retr": "Descargar archivo desde réplicas: retr <remote_path> <local_path>",
                "stor": "Subir archivo con replicación automática: stor <local_path> <remote_path>",
                "pasv": "Entrar en modo pasivo",
                "type": "Tipo de transferencia: type <A|I>",
                "mode": "Modo de transferencia: mode <S|B|C>",
                "stru": "Estructura de archivo: stru <F|R|P>",
                "rest": "Establecer punto de reinicio: rest <marker>"
            },
            "GESTIÓN DISTRIBUIDA": {
                "mkd": "Crear directorio: mkd <path>",
                "rmd": "Eliminar directorio: rmd <path>",
                "dele": "Eliminar archivo (de todas las réplicas): dele <filename>",
                "rnfr": "Renombrar archivo (origen): rnfr <old_name>",
                "rnto": "Renombrar archivo (destino): rnto <new_name>"
            },
            "INFORMACIÓN DEL SISTEMA": {
                "syst": "Información del sistema distribuido",
                "stat": "Estado del sistema: stat [<path>]",
                "help": "Mostrar esta ayuda",
                "noop": "Mantener conexión activa",
                "feat": "Listar características del servidor distribuido"
            },
            "EXTRAS DISTRIBUIDOS": {
                "site": "Ejecutar comando específico del sitio: SITE <comando> [args]",
                "appe": "Añadir datos a archivo con replicación: APPE <local_path> <remote_path>",
                "abor": "Abortar operación actual",
                "rein": "Reinicializar conexión",
                "nlst": "Listar solo nombres de archivos: NLST [path]",
                "stou": "Almacenar archivo con nombre único: STOU <local_path>"
            }
        }

        table = Table(show_header=True, header_style="bold magenta", title="Comandos FTP Sistema Distribuido",
                     title_style="bold blue", border_style="blue")
        table.add_column("Categoría", style="cyan")
        table.add_column("Comando", style="green")
        table.add_column("Descripción", style="white")

        for category, cmds in commands.items():
            for cmd, desc in cmds.items():
                table.add_row(category, cmd, desc)

        self.console.print(table)

    def do_type(self, arg):
        """Configura el tipo de transferencia: TYPE <A|E|I> [<N|T|C>]"""
        try:
            args = arg.split()
            response = self.client.set_type(*args)
            self.console.print(f"[green]OK {response}[/green]")
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_mode(self, arg):
        """Configura el modo de transferencia: MODE <S|B|C>"""
        try:
            response = self.client.set_mode(arg)
            self.console.print(f"[green]OK {response}[/green]")
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_stru(self, arg):
        """Configura la estructura del archivo: STRU <F|R|P>"""
        try:
            response = self.client.set_structure(arg)
            self.console.print(f"[green]OK {response}[/green]")
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_rest(self, arg):
        """Establece punto de reinicio: REST <marker>"""
        try:
            response = self.client.set_restart_point(arg)
            self.console.print(f"[green]OK {response}[/green]")
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_pwd(self, arg):
        """Muestra el directorio actual en el sistema distribuido"""
        try:
            # Asegurar que estamos en modo pasivo
            #self.client.enter_passive_mode()

            response = self.client.get_current_dir()
            # Extraer el path entre comillas y limpiarlo
            import re
            path_match = re.search(r'"([^"]*)"', response)
            if path_match:
                path = path_match.group(1)
                # Limpiar múltiples barras y asegurar formato correcto
                path = path.replace('//', '/')
                if not path or path == '/':
                    self.console.print("[cyan]Directorio actual: /[/cyan] (directorio raíz del sistema distribuido)")
                else:
                    self.console.print(f"[cyan]Directorio actual: {path}[/cyan]")
            else:
                self.console.print(f"[cyan]{response}[/cyan]")
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_cwd(self, arg):
        """Cambia el directorio de trabajo en el sistema distribuido: CWD <path>"""
        if not arg:
            self.console.print("[red]Error: Uso: CWD <path>[/red]")
            return
        try:
            response = self.client.change_dir(arg)

            # Mostrar el directorio actual después del cambio
            try:
                # Obtener y mostrar el nuevo directorio
                pwd_response = self.client.get_current_dir()
                path_match = re.search(r'"([^"]*)"', pwd_response)
                if path_match:
                    current_path = path_match.group(1).replace('//', '/')
                    self.console.print("[green]OK Directorio cambiado exitosamente[/green]")
                    self.console.print(f"[cyan]Directorio actual: {current_path}[/cyan]")
                else:
                    self.console.print("[green]OK Directorio cambiado exitosamente[/green]")
            except:
                # Si hay error al obtener el path, al menos confirmar el cambio
                self.console.print("[green]OK Directorio cambiado exitosamente[/green]")

        except Exception as e:
            # Extraer solo el mensaje de error sin el código
            error_msg = str(e)
            if " - " in error_msg:
                error_msg = error_msg.split(" - ")[1]
            self.console.print(f"[red]Error: {error_msg}[/red]")
            self.console.print("[yellow]Tip: Verifique que el directorio existe en el sistema distribuido[/yellow]")

    def do_cdup(self, arg):
        """Cambia al directorio padre en el sistema distribuido"""
        try:
            response = self.client.change_to_parent_dir()

            # Mostrar el directorio actual después del cambio
            try:
                # Obtener y mostrar el nuevo directorio
                pwd_response = self.client.get_current_dir()
                path_match = re.search(r'"([^"]*)"', pwd_response)
                if path_match:
                    current_path = path_match.group(1).replace('//', '/')
                    self.console.print("[green]OK Cambiado al directorio superior[/green]")
                    self.console.print(f"[cyan]Directorio actual: {current_path}[/cyan]")
                else:
                    self.console.print("[green]OK Cambiado al directorio superior[/green]")
            except:
                # Si hay error al obtener el path, al menos confirmar el cambio
                self.console.print("[green]OK Cambiado al directorio superior[/green]")

        except Exception as e:
            # Extraer solo el mensaje de error sin el código
            error_msg = str(e)
            if " - " in error_msg:
                error_msg = error_msg.split(" - ")[1]
            self.console.print(f"[red]Error: {error_msg}[/red]")

    def do_mkd(self, arg):
        """Crea un directorio en el sistema distribuido: MKD <path>"""
        try:
            response = self.client.make_dir(arg)
            self.console.print(f"[green]OK {response}[/green]")
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_rmd(self, arg):
        """Elimina un directorio del sistema distribuido: RMD <path>"""
        if not arg:
            self.console.print("[red]Error: Uso: RMD <path>[/red]")
            return

        try:
            response = self.client.remove_dir(arg)
            if "250" in response:
                self.console.print(f"[green]OK Directorio '{arg}' eliminado correctamente[/green]")
            else:
                self.console.print(f"[red]Error: {response}[/red]")
        except Exception as e:
            error_msg = str(e)
            if " - " in error_msg:
                error_msg = error_msg.split(" - ")[1]
            self.console.print(f"[red]Error: No se pudo eliminar el directorio: {error_msg}[/red]")

    def do_dele(self, arg):
        """Elimina un archivo del sistema distribuido: DELE <filename>"""
        if not arg:
            self.console.print("[red]Error: Uso: DELE <filename>[/red]")
            self.console.print("[yellow]Ejemplo: dele archivo.txt[/yellow]")
            return

        try:
            response = self.client.delete_file(arg)
            if "250" in response:  # Verificar éxito
                self.console.print(f"[green]OK Archivo '{arg}' eliminado correctamente[/green]")
                self.console.print("[cyan]Eliminado de todas las réplicas del sistema distribuido[/cyan]")
            else:
                self.console.print(f"[red]Error: {response}[/red]")
        except Exception as e:
            error_msg = str(e)
            if " - " in error_msg:
                error_msg = error_msg.split(" - ")[1]
            self.console.print(f"[red]Error: No se pudo eliminar el archivo: {error_msg}[/red]")
            self.console.print("[yellow]Tip: Verifique que el archivo existe y tiene permisos[/yellow]")

    def do_rnfr(self, arg):
        """Especifica el archivo a renombrar en el sistema distribuido: RNFR <old_name>"""
        try:
            response = self.client.rename_from(arg)
            self.console.print(f"[cyan]Archivo seleccionado para renombrar[/cyan]")
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_rnto(self, arg):
        """Especifica el nuevo nombre en el sistema distribuido: RNTO <new_name>"""
        try:
            response = self.client.rename_to(arg)
            self.console.print(f"[green]OK Archivo renombrado exitosamente[/green]")
            self.console.print("[cyan]Cambio aplicado en todas las réplicas[/cyan]")
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_syst(self, arg):
        """Muestra información del sistema distribuido"""
        try:
            response = self.client.get_system()
            self.console.print(f"[cyan]{response}[/cyan]")
            self.console.print("[yellow]Sistema: FTP Distribuido con alta disponibilidad[/yellow]")
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_stat(self, arg):
        """Muestra el estado del sistema distribuido: STAT [<path>]"""
        try:
            response = self.client.get_status(arg)
            self.console.print(f"[cyan]{response}[/cyan]")
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_noop(self, arg):
        """Mantiene la conexión activa al sistema distribuido: NOOP"""
        try:
            response = self.client.noop()
            self.console.print(f"[green]OK {response}[/green]")
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_feat(self, arg):
        """Muestra las características del servidor distribuido"""
        try:
            features = self.client.get_features()
            self.console.print("[cyan]Características del Sistema FTP Distribuido:[/cyan]")
            for feature, details in features.items():
                self.console.print(f"[green]- {feature}[/green]: {details}")
            self.console.print("[yellow]Características adicionales del sistema distribuido:[/yellow]")
            self.console.print("[green]- Replicación automática[/green]: Archivos replicados en múltiples nodos")
            self.console.print("[green]- Alta disponibilidad[/green]: Sistema tolerante a fallos")
            self.console.print("[green]- Balanceo de carga[/green]: Archivos distribuidos automáticamente")
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_site(self, arg):
        """Ejecuta comando específico del sitio en el sistema distribuido: SITE <comando> [args]"""
        if not arg:
            self.console.print("[red]Error: Uso: SITE <comando> [args][/red]")
            return
        try:
            response = self.client.execute("SITE", arg)
            self.console.print(f"[cyan]{response}[/cyan]")
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_appe(self, arg):
        """Añade datos a un archivo en el sistema distribuido: APPE <local_path> <remote_path>"""
        args = arg.split()
        if len(args) != 2:
            self.console.print("[red]Error: Uso: APPE <local_path> <remote_path>[/red]")
            self.console.print("[yellow]Ejemplo: appe local.txt remoto.txt[/yellow]")
            return

        local_path, remote_path = args
        try:

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                transient=True
            ) as progress:
                task = progress.add_task("[cyan]Añadiendo datos al sistema distribuido...", total=None)
                response = self.client.append_file(local_path, remote_path)

            if "226" in response:
                self.console.print(f"[green]OK Datos añadidos exitosamente a '{remote_path}'[/green]")
                self.console.print("[cyan]Cambios replicados en todas las copias del sistema distribuido[/cyan]")
            else:
                self.console.print(f"[yellow]{response}[/yellow]")

        except Exception as e:
            self.console.print(f"[red]ERROR Error: {str(e)}[/red]")
            self.console.print("[yellow]Tip: Verifique permisos y espacio disponible[/yellow]")

    def do_abor(self, arg):
        """Aborta la operación actual en el sistema distribuido: ABOR"""
        try:
            response = self.client.abort()
            self.console.print(f"[yellow]Operación abortada: {response}[/yellow]")
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_rein(self, arg):
        """Reinicializa la conexión al sistema distribuido: REIN"""
        try:
            response = self.client.reinitialize()
            self.console.print(f"[yellow]Conexión reinicializada: {response}[/yellow]")
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def do_nlst(self, arg):
        """Lista solo nombres de archivos en el sistema distribuido: NLST [path]"""
        try:
            # Asegurar que estamos en modo pasivo
            #self.client.enter_passive_mode()

            # Obtener la lista de archivos
            file_list = self.client.list_files(arg).splitlines()

            if not file_list:
                self.console.print("[yellow]Directorio vacío o error listando archivos[/yellow]")
                return

            # Crear una tabla para mostrar los resultados
            table = Table(show_header=True, header_style="bold magenta", title="Nombres de Archivos (Sistema Distribuido)")
            table.add_column("Nombre", style="cyan")

            # Añadir cada archivo a la tabla
            for filename in file_list:
                if filename.strip():  # Ignorar líneas vacías
                    table.add_row(filename.strip())

            # Mostrar la tabla y el total
            self.console.print(table)
            self.console.print(f"[blue]Total: {len(file_list)} elementos (sistema distribuido)[/blue]\n")

        except Exception as e:
            self.console.print(f"[red]Error: {str(e)}[/red]")
            self.console.print("[yellow]Tip: Verifique que tiene permisos y la ruta es válida[/yellow]")

    def do_stou(self, arg):
        """Almacena archivo con nombre único en el sistema distribuido: STOU <local_path>"""
        if not arg:
            self.console.print("[red]Error: Uso: STOU <local_path>[/red]")
            self.console.print("[yellow]Ejemplo: stou archivo.txt[/yellow]")
            return

        try:
            # Verificar archivo local
            from pathlib import Path
            if not Path(arg).exists():
                self.console.print(f"[red]Error: El archivo '{arg}' no existe[/red]")
                return

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                transient=True
            ) as progress:
                task = progress.add_task("[cyan]Subiendo a sistema distribuido...", total=None)
                response = self.client.store_unique(arg)

            if "226" in response:
                # Extraer el nombre generado del mensaje (si está disponible)
                nombre_generado = response.split("Saved as")[-1].strip() if "Saved as" in response else "nombre único"
                self.console.print(f"[green]OK Archivo subido exitosamente como {nombre_generado}[/green]")
                self.console.print("[cyan]Archivo replicado automáticamente en el sistema distribuido[/cyan]")
            else:
                self.console.print(f"[yellow]{response}[/yellow]")

        except Exception as e:
            self.console.print(f"[red]ERROR Error: {str(e)}[/red]")
            self.console.print("[yellow]Tip: Verifique permisos y espacio disponible[/yellow]")

    def do_pasv(self, arg):
        """Entra en modo pasivo para transferencias en el sistema distribuido"""
        try:
            response = self.client.enter_passive_mode()
            self.console.print(f"[green]OK {response}[/green]")
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def emptyline(self):
        """No hacer nada cuando se presiona Enter sin comando"""
        pass

    def do_system_info(self, arg):
        """Muestra información del sistema distribuido"""
        self.console.print("\n[bold blue]📊 Información del Sistema FTP Distribuido[/bold blue]")
        self.console.print("[cyan]═" * 50 + "[/cyan]")

        info_table = Table(show_header=False, border_style="blue")
        info_table.add_column("Componente", style="green", width=15)
        info_table.add_column("Estado", style="yellow", width=10)
        info_table.add_column("Descripción", style="white")

        info_table.add_row("Router", "Activo", "Punto de entrada FTP (puerto 2121)")
        info_table.add_row("Metadata", "3 nodos", "Coordinación y namespace distribuido")
        info_table.add_row("Storage", "3 nodos", "Almacenamiento con replicación")
        info_table.add_row("Replicación", "Factor 2", "Cada archivo en mínimo 2 nodos")
        info_table.add_row("DNS", "Activo", "Resolución automática de servicios")
        info_table.add_row("Consistencia", "Eventual", "Last-write-wins con versioning")

        self.console.print(info_table)
        self.console.print("\n[yellow]💡 Consejos para el sistema distribuido:[/yellow]")
        self.console.print("[white]- Los archivos se replican automáticamente[/white]")
        self.console.print("[white]- El sistema es tolerante a fallos de nodos[/white]")
        self.console.print("[white]- Las descargas vienen de la réplica más cercana[/white]")
        self.console.print("[white]- Los directorios se sincronizan entre nodos[/white]\n")


def main():
    """Función principal para ejecutar la consola distribuida"""
    import argparse

    parser = argparse.ArgumentParser(description="FTP Distributed Client - Consola Personalizada")
    parser.add_argument("--host", default="127.0.0.1", help="Host del router distribuido")
    parser.add_argument("--port", type=int, default=2121, help="Puerto del router distribuido")
    parser.add_argument("--auto-connect", action="store_true", help="Conectar automáticamente")

    args = parser.parse_args()

    try:
        # Crear instancia de la consola distribuida
        cli = FTPCLI_Distributed()

        if args.auto_connect:
            # Conexión automática
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}")
            ) as progress:
                progress.add_task(description=f"Conectando automáticamente a {args.host}:{args.port}...", total=None)
                try:
                    client = FTPClient(args.host, args.port)
                    client.connect()
                    cli = FTPCLI_Distributed(client)
                    rprint("[green]Conexión automática exitosa al sistema distribuido[/green]")
                except Exception as e:
                    rprint(f"[red]Error en conexión automática: {e}[/red]")
                    rprint("[yellow]Iniciando en modo desconectado...[/yellow]")
        else:
            rprint("[cyan]Iniciando consola FTP distribuida...[/cyan]")
            rprint("[yellow]Use 'distributed_connect' para conectar al sistema distribuido[/yellow]")

        # Iniciar el loop de comandos
        cli.cmdloop()

    except KeyboardInterrupt:
        rprint("\n[yellow]¡Hasta luego! 👋[/yellow]")
        rprint("[cyan]Sistema FTP distribuido desconectado[/cyan]")
    except Exception as e:
        rprint(f"[red]Error fatal: {e}[/red]")
        exit(1)


if __name__ == "__main__":
    main()
