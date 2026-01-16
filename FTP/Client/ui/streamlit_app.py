#!/usr/bin/env python3
"""
Interfaz de Usuario Streamlit para el Sistema FTP Distribuido.
Reemplaza la CLI proporcionando una interfaz gráfica moderna.
"""
import os
import sys
import tempfile
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime

import streamlit as st

# Agregar el directorio raíz del proyecto al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

from FTP.Client.client import FTPClient


# ==================== Configuración de la página ====================
st.set_page_config(
    page_title="FTP Distribuido",
    page_icon="📁",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== Estilos CSS personalizados ====================
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .status-connected {
        background-color: #4CAF50;
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-weight: bold;
    }
    .status-disconnected {
        background-color: #f44336;
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-weight: bold;
    }
    .file-card {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 1rem;
        margin: 0.5rem 0;
        border-left: 4px solid #1E88E5;
    }
    .folder-card {
        background-color: #fff3e0;
        border-radius: 10px;
        padding: 1rem;
        margin: 0.5rem 0;
        border-left: 4px solid #FF9800;
    }
    .info-box {
        background-color: #e3f2fd;
        border-radius: 10px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .stButton>button {
        width: 100%;
    }
</style>
""", unsafe_allow_html=True)


# ==================== Funciones de Estado de Sesión ====================
def init_session_state():
    """Inicializa el estado de la sesión de Streamlit."""
    if 'client' not in st.session_state:
        st.session_state.client = None
    if 'connected' not in st.session_state:
        st.session_state.connected = False
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'current_dir' not in st.session_state:
        st.session_state.current_dir = "/"
    if 'username' not in st.session_state:
        st.session_state.username = ""
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    if 'file_list' not in st.session_state:
        st.session_state.file_list = []


def add_message(msg_type: str, message: str):
    """Agrega un mensaje al historial."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.messages.append({
        "type": msg_type,
        "message": message,
        "timestamp": timestamp
    })
    # Mantener solo los últimos 50 mensajes
    if len(st.session_state.messages) > 50:
        st.session_state.messages = st.session_state.messages[-50:]


def get_current_dir() -> str:
    """Obtiene el directorio actual del servidor."""
    if st.session_state.client and st.session_state.authenticated:
        try:
            response = st.session_state.client.get_current_dir()
            # Extraer el path entre comillas
            import re
            match = re.search(r'"([^"]*)"', response)
            if match:
                return match.group(1)
        except Exception:
            pass
    return st.session_state.current_dir


# ==================== Funciones de Conexión ====================
def connect_to_server(host: str, port: int) -> bool:
    """Conecta al servidor FTP distribuido."""
    try:
        client = FTPClient(host, port)
        client.connect()
        st.session_state.client = client
        st.session_state.connected = True
        add_message("success", f"Conectado a {host}:{port}")
        return True
    except Exception as e:
        add_message("error", f"Error de conexión: {str(e)}")
        return False


def authenticate(username: str, password: str) -> bool:
    """Autentica al usuario en el servidor."""
    if not st.session_state.client:
        add_message("error", "No hay conexión al servidor")
        return False
    
    try:
        st.session_state.client.execute("USER", username)
        st.session_state.client.execute("PASS", password)
        
        if st.session_state.client.authenticated:
            st.session_state.authenticated = True
            st.session_state.username = username
            st.session_state.current_dir = get_current_dir()
            add_message("success", f"Autenticado como {username}")
            return True
        else:
            add_message("error", "Autenticación fallida")
            return False
    except Exception as e:
        add_message("error", f"Error de autenticación: {str(e)}")
        return False


def disconnect():
    """Desconecta del servidor."""
    if st.session_state.client:
        try:
            st.session_state.client.quit()
        except Exception:
            pass
        
    st.session_state.client = None
    st.session_state.connected = False
    st.session_state.authenticated = False
    st.session_state.username = ""
    st.session_state.current_dir = "/"
    st.session_state.file_list = []
    add_message("info", "Desconectado del servidor")


# ==================== Funciones de Navegación ====================
def refresh_file_list():
    """Actualiza la lista de archivos del directorio actual."""
    if not st.session_state.authenticated:
        return []
    
    try:
        file_list = st.session_state.client.list_directory(st.session_state.current_dir)
        st.session_state.file_list = file_list if file_list else []
        return st.session_state.file_list
    except Exception as e:
        error_msg = str(e)
        # Si el error contiene 226 es realmente un éxito (bug en parsing)
        if "226" in error_msg:
            # Intentar de nuevo sin path específico
            try:
                file_list = st.session_state.client.list_directory("")
                st.session_state.file_list = file_list if file_list else []
                return st.session_state.file_list
            except:
                pass
        add_message("error", f"Error listando directorio: {error_msg}")
        return []


def change_directory(path: str):
    """Cambia al directorio especificado."""
    if not st.session_state.authenticated:
        return
    
    try:
        st.session_state.client.change_dir(path)
        st.session_state.current_dir = get_current_dir()
        add_message("success", f"Directorio cambiado a: {st.session_state.current_dir}")
        refresh_file_list()
    except Exception as e:
        add_message("error", f"Error cambiando directorio: {str(e)}")


def go_to_parent():
    """Navega al directorio padre."""
    if not st.session_state.authenticated:
        return
    
    try:
        st.session_state.client.change_to_parent_dir()
        st.session_state.current_dir = get_current_dir()
        add_message("success", f"Directorio: {st.session_state.current_dir}")
        refresh_file_list()
    except Exception as e:
        add_message("error", f"Error: {str(e)}")


# ==================== Funciones de Archivos ====================
def upload_file(uploaded_file, remote_name: str):
    """Sube un archivo al servidor."""
    if not st.session_state.authenticated:
        add_message("error", "No autenticado")
        return False
    
    try:
        # Guardar archivo temporal
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(uploaded_file.name).suffix) as tmp:
            tmp.write(uploaded_file.getvalue())
            tmp_path = tmp.name
        
        # Subir archivo
        response = st.session_state.client.upload_file(tmp_path, remote_name)
        
        # Limpiar archivo temporal
        os.unlink(tmp_path)
        
        if "226" in response:
            add_message("success", f"Archivo '{remote_name}' subido correctamente")
            refresh_file_list()
            return True
        else:
            add_message("warning", response)
            return False
    except Exception as e:
        add_message("error", f"Error subiendo archivo: {str(e)}")
        return False


def download_file(remote_path: str) -> Optional[bytes]:
    """Descarga un archivo del servidor."""
    if not st.session_state.authenticated:
        add_message("error", "No autenticado")
        return None
    
    try:
        # Crear archivo temporal para la descarga
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        
        # Descargar archivo
        response = st.session_state.client.download_file(remote_path, tmp_path)
        
        if "226" in response:
            # Leer contenido del archivo
            with open(tmp_path, 'rb') as f:
                data = f.read()
            
            # Limpiar archivo temporal
            os.unlink(tmp_path)
            
            add_message("success", f"Archivo '{remote_path}' descargado")
            return data
        else:
            add_message("warning", response)
            return None
    except Exception as e:
        add_message("error", f"Error descargando archivo: {str(e)}")
        return None


def delete_file(filename: str):
    """Elimina un archivo del servidor."""
    if not st.session_state.authenticated:
        return
    
    try:
        st.session_state.client.delete_file(filename)
        add_message("success", f"Archivo '{filename}' eliminado")
        refresh_file_list()
    except Exception as e:
        add_message("error", f"Error eliminando archivo: {str(e)}")


def create_directory(dirname: str):
    """Crea un nuevo directorio."""
    if not st.session_state.authenticated:
        return
    
    try:
        st.session_state.client.make_dir(dirname)
        add_message("success", f"Directorio '{dirname}' creado")
        refresh_file_list()
    except Exception as e:
        add_message("error", f"Error creando directorio: {str(e)}")


def remove_directory(dirname: str):
    """Elimina un directorio."""
    if not st.session_state.authenticated:
        return
    
    try:
        st.session_state.client.remove_dir(dirname)
        add_message("success", f"Directorio '{dirname}' eliminado")
        refresh_file_list()
    except Exception as e:
        add_message("error", f"Error eliminando directorio: {str(e)}")


def rename_item(old_name: str, new_name: str):
    """Renombra un archivo o directorio."""
    if not st.session_state.authenticated:
        return
    
    try:
        st.session_state.client.rename_from(old_name)
        st.session_state.client.rename_to(new_name)
        add_message("success", f"'{old_name}' renombrado a '{new_name}'")
        refresh_file_list()
    except Exception as e:
        add_message("error", f"Error renombrando: {str(e)}")


# ==================== Componentes de UI ====================
def render_sidebar():
    """Renderiza la barra lateral con conexión y estado."""
    with st.sidebar:
        st.markdown("### 🔌 Conexión")
        
        # Estado de conexión
        if st.session_state.connected:
            if st.session_state.authenticated:
                st.success(f"✅ Conectado como: {st.session_state.username}")
            else:
                st.warning("⚠️ Conectado - No autenticado")
            
            if st.button("🔴 Desconectar", use_container_width=True):
                disconnect()
                st.rerun()
        else:
            st.error("❌ Desconectado")
        
        st.divider()
        
        # Formulario de conexión
        if not st.session_state.connected:
            st.markdown("### 🌐 Conectar al Sistema")
            
            with st.form("connection_form"):
                host = st.text_input("Host", value="127.0.0.1")
                port = st.number_input("Puerto", value=2121, min_value=1, max_value=65535)
                
                if st.form_submit_button("🔗 Conectar", use_container_width=True):
                    if connect_to_server(host, int(port)):
                        st.rerun()
        
        # Formulario de autenticación
        if st.session_state.connected and not st.session_state.authenticated:
            st.markdown("### 🔐 Autenticación")
            
            with st.form("auth_form"):
                username = st.text_input("Usuario", value="admin")
                password = st.text_input("Contraseña", type="password", value="admin123")
                
                if st.form_submit_button("🔓 Iniciar Sesión", use_container_width=True):
                    if authenticate(username, password):
                        refresh_file_list()
                        st.rerun()
        
        st.divider()
        
        # Información del sistema
        if st.session_state.authenticated:
            st.markdown("### 📊 Estado del Sistema")
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Router", "Activo", delta="OK")
            with col2:
                st.metric("Replicación", "Factor 2")
            
            st.info("""
            **Sistema Distribuido:**
            - 3 nodos de metadata
            - 3 nodos de storage
            - Alta disponibilidad
            """)
        
        st.divider()
        
        # Log de mensajes
        st.markdown("### 📝 Log de Actividad")
        
        if st.button("🗑️ Limpiar Log", use_container_width=True):
            st.session_state.messages = []
            st.rerun()
        
        message_container = st.container(height=200)
        with message_container:
            for msg in reversed(st.session_state.messages[-10:]):
                if msg["type"] == "success":
                    st.success(f"[{msg['timestamp']}] {msg['message']}")
                elif msg["type"] == "error":
                    st.error(f"[{msg['timestamp']}] {msg['message']}")
                elif msg["type"] == "warning":
                    st.warning(f"[{msg['timestamp']}] {msg['message']}")
                else:
                    st.info(f"[{msg['timestamp']}] {msg['message']}")


def render_breadcrumbs():
    """Renderiza breadcrumbs clicables para navegación."""
    current_path = st.session_state.current_dir
    
    # Dividir el path en partes
    if current_path == "/":
        parts = []
    else:
        parts = [p for p in current_path.split("/") if p]
    
    # Crear breadcrumbs con botones
    st.markdown("#### 📍 Ubicación actual")
    
    cols = st.columns([1] + [1] * min(len(parts), 6) + [4])
    
    # Botón raíz
    with cols[0]:
        if st.button("🏠 /", key="bc_root", use_container_width=True):
            change_directory("/")
            st.rerun()
    
    # Botones para cada parte del path
    accumulated_path = ""
    for idx, part in enumerate(parts[:6]):  # Limitar a 6 niveles visibles
        accumulated_path += f"/{part}"
        with cols[idx + 1]:
            # Mostrar solo los últimos caracteres si el nombre es muy largo
            display_name = part if len(part) <= 12 else f"...{part[-9:]}"
            if st.button(f"📁 {display_name}", key=f"bc_{idx}", use_container_width=True, help=part):
                change_directory(accumulated_path)
                st.rerun()
    
    # Mostrar path completo si hay más de 6 niveles
    if len(parts) > 6:
        with cols[-1]:
            st.caption(f"... {current_path}")


def render_file_browser():
    """Renderiza el explorador de archivos principal."""
    st.markdown('<p class="main-header">📁 FTP Distribuido</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Sistema de Archivos Distribuido con Alta Disponibilidad</p>', unsafe_allow_html=True)
    
    if not st.session_state.authenticated:
        st.info("👈 Conecta y autentícate usando la barra lateral para comenzar")
        
        # Mostrar información del sistema
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            ### 🔄 Replicación Automática
            Los archivos se replican automáticamente en múltiples nodos 
            para garantizar la disponibilidad de datos.
            """)
        
        with col2:
            st.markdown("""
            ### 🛡️ Alta Disponibilidad
            El sistema continúa funcionando incluso si algunos 
            nodos fallan, manteniendo el acceso a los datos.
            """)
        
        with col3:
            st.markdown("""
            ### ⚡ Balanceo de Carga
            Las solicitudes se distribuyen entre los nodos disponibles
            para optimizar el rendimiento.
            """)
        
        return
    
    # === BREADCRUMBS - Navegación por ruta ===
    render_breadcrumbs()
    
    # === Barra de navegación con acciones ===
    col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 5])
    
    with col1:
        if st.button("⬆️ Subir", use_container_width=True, help="Ir al directorio padre"):
            go_to_parent()
            st.rerun()
    
    with col2:
        if st.button("🏠 Home", use_container_width=True, help="Ir a tu directorio personal"):
            change_directory(f"/{st.session_state.username}")
            st.rerun()
    
    with col3:
        if st.button("📁 Raíz", use_container_width=True, help="Ir al directorio raíz"):
            change_directory("/")
            st.rerun()
    
    with col4:
        if st.button("🔄 Refrescar", use_container_width=True):
            refresh_file_list()
            st.rerun()
    
    with col5:
        # Campo para ir a una ruta específica
        with st.form("goto_form", clear_on_submit=True):
            cols = st.columns([5, 1])
            with cols[0]:
                goto_path = st.text_input(
                    "Ir a ruta",
                    placeholder="/ruta/al/directorio",
                    label_visibility="collapsed"
                )
            with cols[1]:
                if st.form_submit_button("➡️", use_container_width=True):
                    if goto_path:
                        change_directory(goto_path)
                        st.rerun()
    
    st.divider()
    
    # Pestañas principales
    tab1, tab2, tab3, tab4 = st.tabs(["📂 Explorador", "⬆️ Subir", "📁 Nuevo Directorio", "⚙️ Acciones"])
    
    with tab1:
        render_file_list()
    
    with tab2:
        render_upload_section()
    
    with tab3:
        render_create_directory()
    
    with tab4:
        render_actions_section()


def render_file_list():
    """Renderiza la lista de archivos y carpetas."""
    file_list = st.session_state.file_list
    
    if not file_list:
        st.info("📭 Directorio vacío o presiona 'Refrescar' para cargar archivos")
        return
    
    # Separar directorios y archivos
    directories = [f for f in file_list if f.get('permisos', '').startswith('d')]
    files = [f for f in file_list if not f.get('permisos', '').startswith('d')]
    
    # Mostrar estadísticas
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📁 Carpetas", len(directories))
    with col2:
        st.metric("📄 Archivos", len(files))
    with col3:
        total_size = sum(int(f.get('tamaño', 0) or 0) for f in files)
        st.metric("💾 Tamaño Total", format_size(total_size))
    
    st.divider()
    
    # Mostrar directorios primero con mejor diseño
    if directories:
        st.markdown("### 📁 Carpetas")
        
        # Filtrar directorios válidos
        valid_dirs = [d for d in directories if d.get('nombre', '').strip() not in ['', '.', '..']]
        
        # Usar 3 columnas para mejor visualización
        num_cols = 3
        rows = [valid_dirs[i:i + num_cols] for i in range(0, len(valid_dirs), num_cols)]
        
        for row in rows:
            cols = st.columns(num_cols)
            for idx, dir_info in enumerate(row):
                name = dir_info.get('nombre', '').strip()
                
                with cols[idx]:
                    with st.container(border=True):
                        # Nombre de carpeta como botón principal (más grande)
                        if st.button(f"📁 {name}", key=f"enter_{name}", use_container_width=True, type="primary"):
                            change_directory(name)
                            st.rerun()
                        
                        # Botón pequeño para eliminar
                        if st.button("🗑️ Eliminar", key=f"del_dir_{name}", use_container_width=True):
                            remove_directory(name)
                            st.rerun()
    
    # Mostrar archivos
    if files:
        st.markdown("### 📄 Archivos")
        
        for file_info in files:
            name = file_info.get('nombre', '').strip()
            size = file_info.get('tamaño', '0')
            owner = file_info.get('owner', 'unknown')
            permisos = file_info.get('permisos', '')
            
            if not name or name.startswith('.'):
                continue
            
            with st.container(border=True):
                col1, col2, col3, col4, col5 = st.columns([4, 2, 2, 1, 1])
                
                with col1:
                    icon = get_file_icon(name)
                    st.markdown(f"**{icon} {name}**")
                
                with col2:
                    st.caption(f"📏 {format_size(int(size or 0))}")
                
                with col3:
                    st.caption(f"👤 {owner}")
                
                with col4:
                    if st.button("⬇️", key=f"download_{name}", help="Descargar"):
                        data = download_file(name)
                        if data:
                            st.download_button(
                                label="💾 Guardar",
                                data=data,
                                file_name=name,
                                key=f"save_{name}"
                            )
                
                with col5:
                    if st.button("🗑️", key=f"delete_{name}", help="Eliminar"):
                        delete_file(name)
                        st.rerun()


def render_upload_section():
    """Renderiza la sección de subida de archivos."""
    st.markdown("### ⬆️ Subir Archivo al Sistema Distribuido")
    
    st.info("""
    📌 **Los archivos subidos se replican automáticamente** en múltiples nodos de almacenamiento
    para garantizar la disponibilidad y redundancia de datos.
    """)
    
    uploaded_file = st.file_uploader(
        "Selecciona un archivo",
        type=None,
        help="Arrastra y suelta o haz clic para seleccionar"
    )
    
    if uploaded_file:
        col1, col2 = st.columns([3, 1])
        
        with col1:
            remote_name = st.text_input(
                "Nombre en el servidor",
                value=uploaded_file.name,
                help="Nombre con el que se guardará en el servidor"
            )
        
        with col2:
            st.metric("Tamaño", format_size(uploaded_file.size))
        
        if st.button("🚀 Subir Archivo", type="primary", use_container_width=True):
            with st.spinner("Subiendo archivo al sistema distribuido..."):
                if upload_file(uploaded_file, remote_name):
                    st.success("✅ Archivo subido y replicado correctamente")
                    st.balloons()
                    st.rerun()


def render_create_directory():
    """Renderiza la sección para crear directorios."""
    st.markdown("### 📁 Crear Nuevo Directorio")
    
    new_dir_name = st.text_input(
        "Nombre del directorio",
        placeholder="nuevo_directorio",
        help="Ingresa el nombre para el nuevo directorio"
    )
    
    if st.button("➕ Crear Directorio", type="primary", use_container_width=True, disabled=not new_dir_name):
        create_directory(new_dir_name)
        st.rerun()


def render_actions_section():
    """Renderiza la sección de acciones adicionales."""
    st.markdown("### ⚙️ Acciones Adicionales")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📝 Renombrar")
        with st.form("rename_form"):
            old_name = st.text_input("Nombre actual")
            new_name = st.text_input("Nuevo nombre")
            
            if st.form_submit_button("✏️ Renombrar", use_container_width=True):
                if old_name and new_name:
                    rename_item(old_name, new_name)
                    st.rerun()
    
    with col2:
        st.markdown("#### 📋 Información del Sistema")
        
        if st.button("📊 Ver Estado", use_container_width=True):
            try:
                response = st.session_state.client.get_system()
                st.code(response)
            except Exception as e:
                st.error(f"Error: {e}")
        
        if st.button("🔍 Ver Características", use_container_width=True):
            try:
                features = st.session_state.client.get_features()
                for feature, details in features.items():
                    st.write(f"- **{feature}**: {details}")
            except Exception as e:
                st.error(f"Error: {e}")
        
        if st.button("💓 Test Conexión (NOOP)", use_container_width=True):
            try:
                response = st.session_state.client.noop()
                st.success(f"Conexión OK: {response}")
            except Exception as e:
                st.error(f"Error: {e}")


# ==================== Utilidades ====================
def format_size(size_bytes: int) -> str:
    """Formatea el tamaño en bytes a una representación legible."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def get_file_icon(filename: str) -> str:
    """Retorna un icono según la extensión del archivo."""
    ext = Path(filename).suffix.lower()
    
    icons = {
        '.txt': '📝',
        '.pdf': '📕',
        '.doc': '📘', '.docx': '📘',
        '.xls': '📗', '.xlsx': '📗',
        '.ppt': '📙', '.pptx': '📙',
        '.jpg': '🖼️', '.jpeg': '🖼️', '.png': '🖼️', '.gif': '🖼️', '.bmp': '🖼️',
        '.mp3': '🎵', '.wav': '🎵', '.flac': '🎵',
        '.mp4': '🎬', '.avi': '🎬', '.mkv': '🎬', '.mov': '🎬',
        '.zip': '🗜️', '.rar': '🗜️', '.7z': '🗜️', '.tar': '🗜️', '.gz': '🗜️',
        '.py': '🐍',
        '.js': '📜', '.ts': '📜',
        '.html': '🌐', '.css': '🎨',
        '.json': '📋', '.xml': '📋', '.yaml': '📋', '.yml': '📋',
        '.md': '📖',
        '.exe': '⚙️', '.sh': '⚙️', '.bat': '⚙️',
        '.sql': '🗃️', '.db': '🗃️',
    }
    
    return icons.get(ext, '📄')


# ==================== Main ====================
def main():
    """Función principal de la aplicación."""
    init_session_state()
    render_sidebar()
    render_file_browser()


if __name__ == "__main__":
    main()
