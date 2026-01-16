# 🚀 Sistema FTP Distribuido - Comandos para Funcionar

**Secuencia completa de comandos para montar y probar el sistema FTP distribuido con 1 Router, 3 Metadata y 3 Storage.**

---

## 📋 **PASO 1: Preparación**

```bash
# Entrar al directorio del proyecto
cd /home/kendry/Downloads/FTP-Distributed

# Limpiar contenedores existentes
docker rm -f $(docker ps -aq 2>/dev/null) 2>/dev/null || true

# Limpiar red existente
docker network rm ftp-distributed-net 2>/dev/null || true

# Crear nueva red Docker
docker network create --driver overlay ftp-distributed-net --subnet=172.20.0.0/16 --attachable
```

---

## 🔨 **PASO 2: Construir Imágenes**

```bash
# Construir imagen Metadata
docker build -t ftp-metadata -f FTP/Distributed/Dockerfile.metadata .

# Construir imagen Storage
docker build -t ftp-storage -f FTP/Distributed/Dockerfile.storage .

# Construir imagen Router
docker build -t ftp-router -f FTP/Distributed/Dockerfile.router .
```

---

## 🏛️ **PASO 3: Iniciar Nodos Metadata (3 nodos)**

```bash
# Metadata 1 (líder principal)
docker run -d --name metadata1 --hostname metadata1 --network ftp-distributed-net --network-alias metadata --ip 172.20.0.11 -e NODE_ID=metadata-1 -v $(pwd)/data/metadata1:/data/metadata ftp-metadata python3 -m FTP.Distributed.Metadata.metadata_server --port 5000 --data-dir /data/metadata

# Metadata 2 (backup)
docker run -d --name metadata2 --hostname metadata2 --network ftp-distributed-net --network-alias metadata -e NODE_ID=metadata-2 -v $(pwd)/data/metadata2:/data/metadata ftp-metadata python3 -m FTP.Distributed.Metadata.metadata_server --port 5000 --data-dir /data/metadata

# Metadata 3 (backup)
docker run -d --name metadata3 --hostname metadata3 --network ftp-distributed-net --network-alias metadata --ip 172.20.0.13 -e NODE_ID=metadata-3 -v $(pwd)/data/metadata3:/data/metadata ftp-metadata python3 -m FTP.Distributed.Metadata.metadata_server --port 5000 --data-dir /data/metadata

# Esperar inicialización (15 segundos)
echo "⏳ Esperando inicialización de Metadata (15 segundos)..." && sleep 15 && echo "✅ Metadata listo"
```

---

## 💾 **PASO 4: Iniciar Nodos Storage (3 nodos)**

```bash
# Storage 1
docker run -d --name storage1 --hostname storage1 --network ftp-distributed-net --ip 172.20.0.21 -e NODE_ID=storage-1 -v $(pwd)/data/storage1:/data/storage ftp-storage python3 -m FTP.Distributed.Storage.storage_server --port 5001 --data-dir /data/storage --metadata-host metadata --metadata-port 5000

# Storage 2
docker run -d --name storage2 --hostname storage2 --network ftp-distributed-net --ip 172.20.0.22 -e NODE_ID=storage-2 -v $(pwd)/data/storage2:/data/storage ftp-storage python3 -m FTP.Distributed.Storage.storage_server --port 5001 --data-dir /data/storage --metadata-host metadata --metadata-port 5000

# Storage 3
docker run -d --name storage3 --hostname storage3 --network ftp-distributed-net --ip 172.20.0.23 -e NODE_ID=storage-3 -v $(pwd)/data/storage3:/data/storage ftp-storage python3 -m FTP.Distributed.Storage.storage_server --port 5001 --data-dir /data/storage --metadata-host metadata --metadata-port 5000

# Esperar inicialización (15 segundos)
echo "⏳ Esperando inicialización de Storage (15 segundos)..." && sleep 15 && echo "✅ Storage listo"
```

---

## 🌐 **PASO 5: Iniciar Router (1 nodo)**

```bash
# Router (punto de entrada FTP)
docker run -d --name router1 --hostname router1 --network ftp-distributed-net --ip 172.20.0.31 -p 2121:21 -p 30000-30100:30000-30100 ftp-router python3 -m FTP.Distributed.Router.router_server --port 21 --metadata-host metadata --metadata-port 5000 --public-ip 127.0.0.1

# Esperar inicialización (10 segundos)
echo "⏳ Esperando inicialización del Router (10 segundos)..." && sleep 10 && echo "✅ Router listo"
```

---

## 🔍 **PASO 6: Verificar Estado del Sistema**

```bash
# Ver todos los contenedores ejecutándose
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Verificar resolución DNS
docker exec router1 getent hosts metadata

# Ver logs del router (últimas líneas)
docker logs router1 | tail -5
```

---

## 💾 **PASO 6.5: Verificar Estado de los Storages**

```bash
# Verificar estado completo de los nodos de storage
./check_storages.sh

# Verificar qué storages reconoce metadata como activos
./check_metadata_storages.sh

# O usando el comando directo:
docker exec router1 python3 -c "
import socket
storages = [('storage1', 5001), ('storage2', 5001), ('storage3', 5001)]
for name, port in storages:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1.0)
        result = sock.connect_ex((name, port))
        sock.close()
        status = '✅ UP' if result == 0 else '❌ DOWN'
        print(f'{name}: {status}')
    except:
        print(f'{name}: ❌ DOWN')
"
```

---

## 🖥️ **PASO 8: Ejecutar Interfaz Streamlit (Alternativa Moderna)**

**Esta es la forma moderna y recomendada de acceder al sistema. Ejecuta ESTA opción O la consola CLI.**

```bash
# En una nueva terminal, configurar variables de entorno
cd /home/kendry/Downloads/FTP-Distributed
export PYTHONPATH="$(pwd):$(pwd)/FTP:$PYTHONPATH"
export PYTHONIOENCODING=utf-8

# Instalar Streamlit si no lo tienes
pip install streamlit

# Ejecutar la interfaz web
streamlit run FTP/Client/ui/streamlit_app.py
```

**Resultado esperado:**
```
  You can now view your Streamlit app in your browser.

  Local URL: http://localhost:8501
  Network URL: http://192.168.x.x:8501
```

**Luego:**
1. Abre tu navegador en `http://localhost:8501`
2. En la barra lateral, ingresa:
   - **Host:** `127.0.0.1`
   - **Puerto:** `2121`
   - **Usuario:** `admin`
   - **Contraseña:** `admin123`
3. Haz clic en "🔗 Conectar" y "🔓 Iniciar Sesión"
4. ¡Disfruta de la interfaz web completa!

**Características de la UI Streamlit:**
- ✅ Explorador de archivos visual
- ✅ Subida y descarga de archivos
- ✅ Creación y eliminación de directorios
- ✅ Renombrado de archivos
- ✅ Log en tiempo real de actividades
- ✅ Estado del sistema distribuido
- ✅ Soporte para múltiples formatos de archivo

---

## 🖥️ **PASO 8 (Alternativa): Probar con Consola Personalizada**

**Si prefieres la interfaz de línea de comandos, usa esta opción en lugar de Streamlit.**

```bash
# Configurar variables de entorno
cd /home/kendry/Downloads/FTP-Distributed
export PYTHONPATH="$(pwd):$(pwd)/FTP:$PYTHONPATH"
export PYTHONIOENCODING=utf-8

# Ejecutar consola personalizada
python3 FTP/Client/cli/FTPCLI_distributed.py
```

**Comandos dentro de la consola:**
```
ftp-distributed> distributed_connect
ftp-distributed> login admin admin123
ftp-distributed> pwd
ftp-distributed> list
ftp-distributed> quit
```

---

## 🧪 **PASO 9: Prueba Rápida por Código**

```bash
# Prueba de funcionamiento básico
cd /home/kendry/Downloads/FTP-Distributed
export PYTHONPATH="$(pwd):$(pwd)/FTP:$PYTHONPATH"
export PYTHONIOENCODING=utf-8

python3 -c "
from FTP.Client.client import FTPClient
client = FTPClient('127.0.0.1', 2121)
client.connect()
client.execute('USER', 'admin')
client.execute('PASS', 'admin123')
print('✅ Autenticación exitosa!')
files = client.list_directory()
print(f'✅ {len(files)} archivos en el sistema distribuido')
"
```

---

## 🧹 **PASO 11: Detener la Interfaz Streamlit**

**Cuando hayas terminado de usar la UI web:**

```bash
# En la terminal donde está ejecutándose Streamlit, presiona:
# Ctrl + C

# O mata el proceso desde otra terminal:
pkill -f "streamlit run"
```

---

## 🧹 **PASO 12: Limpieza Completa**

```bash
# Detener todos los contenedores
docker stop metadata1 metadata2 metadata3 storage1 storage2 storage3 router1

# Eliminar todos los contenedores
docker rm metadata1 metadata2 metadata3 storage1 storage2 storage3 router1

# Eliminar red
docker network rm ftp-distributed-net

# Eliminar imágenes (opcional)
docker rmi ftp-metadata ftp-storage ftp-router 2>/dev/null || true

echo "✅ Limpieza completada - Sistema completamente removido"
```

---

## 📋 **Resumen del Sistema**

| Componente | Cantidad | Puerto | Acceso |
|------------|----------|--------|--------|
| **Router** | 1 | 2121 | FTP (127.0.0.1:2121) |
| **Metadata** | 3 | 5000 | Interno (172.20.0.x) |
| **Storage** | 3 | 5001 | Interno (172.20.0.x) |
| **Streamlit UI** | 1 | 8501 | Web (http://localhost:8501) |

**Usuario por defecto:** `admin` / `admin123`

---

## 🌐 **Opciones de Acceso al Sistema**

### Opción 1: Interfaz Web Streamlit ⭐ **RECOMENDADO**
```bash
streamlit run FTP/Client/ui/streamlit_app.py
# → http://localhost:8501
```
Perfecta para uso interactivo con interfaz visual moderna.

### Opción 2: Consola CLI
```bash
python3 FTP/Client/cli/FTPCLI_distributed.py
```
Perfecta para scripting y acceso rápido.

### Opción 3: Cliente FTP Estándar
```bash
ftp 127.0.0.1 2121
```
Compatible con cualquier cliente FTP estándar (FileZilla, WinSCP, etc.)

---

## 🚨 **Notas Importantes**

1. **Ejecutar en orden:** Seguir los pasos exactamente en secuencia
2. **Tiempos de espera:** No saltar los `sleep` - son necesarios para estabilización
3. **DNS:** Los `--network-alias metadata` son críticos para funcionamiento
4. **Puertos:** El router usa rango 30000-30100 para conexiones pasivas
5. **Persistencia:** Los datos se guardan en `./data/` localmente

---

## 🎯 **Flujo Recomendado Completo**

**Para una experiencia completa, sigue este orden en diferentes terminales:**

### Terminal 1: Sistema Distribuido
```bash
cd /home/kendry/Downloads/FTP-Distributed

# PASO 1-5: Inicializar infraestructura
# (ver pasos anteriores - preparación, imágenes, metadata, storage, router)
```

### Terminal 2: Interfaz Web (RECOMENDADO)
```bash
cd /home/kendry/Downloads/FTP-Distributed
export PYTHONPATH="$(pwd):$(pwd)/FTP:$PYTHONPATH"
export PYTHONIOENCODING=utf-8

pip install streamlit  # Primera vez solamente

streamlit run FTP/Client/ui/streamlit_app.py
# Abre: http://localhost:8501
```

### Terminal 3: Monitoreo (Opcional)
```bash
cd /home/kendry/Downloads/FTP-Distributed

# Monitorear logs en tiempo real
watch -n 2 'docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"'
```

---

## 🚨 **Checklist de Ejecución**

- [ ] **Terminal 1:** Sistema distribuido iniciado (pasos 1-5)
- [ ] **Terminal 1:** Verificar que los 7 contenedores estén corriendo (paso 6)
- [ ] **Terminal 2:** Instalar Streamlit: `pip install streamlit`
- [ ] **Terminal 2:** Ejecutar: `streamlit run FTP/Client/ui/streamlit_app.py`
- [ ] **Navegador:** Abrir http://localhost:8501
- [ ] **Conectar:** Host `127.0.0.1`, puerto `2121`
- [ ] **Autenticar:** Usuario `admin`, contraseña `admin123`
- [ ] ✅ **¡Sistema listo para usar!**

---

## 🎯 **Resultado Esperado (Versión 2.0 con Streamlit)**

Después de completar todos los pasos, tendrás:
- ✅ Sistema FTP distribuido completamente funcional
- ✅ 7 contenedores ejecutándose (3 metadata + 3 storage + 1 router)
- ✅ Autenticación funcionando
- ✅ Replicación automática de archivos
- ✅ **Interfaz Web Moderna (Streamlit)** con:
  - 📁 Explorador de archivos visual
  - ⬆️ Subida/descarga de archivos
  - 📂 Gestión de directorios
  - ✏️ Renombramiento de archivos
  - 📊 Monitoreo del sistema
  - 💾 Soporte para múltiples formatos

**¡Ejecuta estos comandos en orden y tendrás un sistema FTP distribuido con UI moderna funcionando!** 🚀
