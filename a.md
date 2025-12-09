# Reporte de Cambios: Transformación de Servidor FTP Centralizado a Sistema Distribuido

**Proyecto:** Sistema FTP Distribuido  
**Fecha:** 2025  
**Autor:** Transformación del sistema centralizado a arquitectura distribuida

---

## Índice

1. [Resumen Ejecutivo](#resumen-ejecutivo)
2. [Cambios Arquitectónicos](#cambios-arquitectónicos)
3. [Nuevos Componentes Creados](#nuevos-componentes-creados)
4. [Modificaciones a Componentes Existentes](#modificaciones-a-componentes-existentes)
5. [Cambios en la Infraestructura](#cambios-en-la-infraestructura)
6. [Razones Técnicas de los Cambios](#razones-técnicas-de-los-cambios)
7. [Comparación Antes/Después](#comparación-antesdespués)
8. [Impacto en Funcionalidad](#impacto-en-funcionalidad)

---

## 1. Resumen Ejecutivo

### Transformación Realizada

El servidor FTP centralizado original ha sido transformado en un **sistema distribuido** compuesto por tres tipos de nodos especializados que operan sobre Docker Swarm:

- **Antes:** Un único servidor FTP monolítico que manejaba todo (autenticación, almacenamiento, coordinación)
- **Después:** Sistema distribuido con separación de responsabilidades en Router, Metadata y Storage

### Estadísticas de Cambios

| Métrica | Valor |
|---------|-------|
| **Archivos nuevos creados** | 25+ archivos |
| **Líneas de código nuevas** | ~5,000+ líneas |
| **Componentes nuevos** | 3 servicios principales + 8 módulos de soporte |
| **Protocolos nuevos** | Protocolo RPC interno |
| **Arquitectura** | Monolítica → Microservicios distribuidos |

---

## 2. Cambios Arquitectónicos

### 2.1 Arquitectura Original (Centralizada)

```
┌─────────────────────────────────────────┐
│         Cliente FTP                      │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│      FTPServer (Monolítico)              │
│  ┌───────────────────────────────────┐  │
│  │  • Autenticación                  │  │
│  │  • Gestión de archivos            │  │
│  │  • Almacenamiento directo         │  │
│  │  • Sistema de archivos local       │  │
│  │  • Sesiones FTP                   │  │
│  └───────────────────────────────────┘  │
│           │                              │
│           ▼                              │
│    Sistema de Archivos Local             │
│    (Path: FTP/FTPRoot)                   │
└──────────────────────────────────────────┘
```

**Características:**
- Todo en un solo proceso
- Almacenamiento directo en sistema de archivos local
- Sin replicación
- Sin tolerancia a fallos
- Sin escalabilidad horizontal

### 2.2 Arquitectura Nueva (Distribuida)

```
                    ┌─────────────────────────────────────────┐
                    │              Clientes FTP               │
                    └────────────────────┬────────────────────┘
                                         │
                    ┌────────────────────▼────────────────────┐
                    │         frontend_net (Overlay)          │
                    └────────────────────┬────────────────────┘
                                         │
            ┌────────────────────────────┼────────────────────────────┐
            │                            │                            │
    ┌───────▼───────┐            ┌───────▼───────┐                    │
    │   Router 1    │            │   Router 2    │                    │
    │  (FTP Proxy)  │            │  (FTP Proxy)  │                    │
    └───────┬───────┘            └───────┬───────┘                    │
            │                            │                            │
            └────────────────┬───────────┘                            │
                             │                                        │
    ┌────────────────────────▼────────────────────────────────────────┘
    │                  backend_net (Overlay)                            │
    └───┬─────────────────────┬─────────────────────┬─────────────────┘
        │                     │                     │
┌───────▼───────┐     ┌───────▼───────┐     ┌───────▼───────┐
│  Metadata 1   │◄───►│  Metadata 2   │     │               │
│  (Líder)      │     │  (Backup)     │     │               │
│  • Namespace  │     │  • Namespace  │     │               │
│  • Réplicas   │     │  • Réplicas   │     │               │
│  • Locks      │     │  • Locks      │     │               │
│  • Auth       │     │  • Auth       │     │               │
└───────┬───────┘     └───────┬───────┘     │               │
        │                     │             │               │
        └──────────┬──────────┘             │               │
                   │                        │               │
        ┌──────────▼──────────┐             │               │
        │                     │             │               │
┌───────▼───────┐     ┌───────▼───────┐     ┌───────▼───────┐
│  Storage 1    │     │  Storage 2    │     │  Storage 3    │
│  (Réplicas)   │     │  (Réplicas)   │     │  (Réplicas)   │
│  • Archivos   │     │  • Archivos   │     │  • Archivos   │
└───────────────┘     └───────────────┘     └───────────────┘
```

**Características:**
- Separación de responsabilidades
- Replicación de datos (factor 3)
- Tolerancia a fallos
- Escalabilidad horizontal
- Coordinación distribuida

---

## 3. Nuevos Componentes Creados

### 3.1 Capa Común (`FTP/Distributed/Common/`)

#### 3.1.1 `constants.py` - Constantes del Sistema

**¿Qué es?**  
Definiciones centralizadas de constantes, enums y configuraciones.

**¿Por qué se creó?**  
- **Centralización:** Evitar valores mágicos dispersos en el código
- **Mantenibilidad:** Un solo lugar para cambiar configuraciones
- **Consistencia:** Asegurar que todos los servicios usen los mismos valores

**Contenido principal:**
```python
- Puertos de servicios (METADATA_RPC_PORT, STORAGE_RPC_PORT)
- Configuración de replicación (REPLICATION_FACTOR = 3)
- Timeouts (HEARTBEAT_INTERVAL, RPC_TIMEOUT)
- Estados de nodos (NodeState: UP, DOWN, RECOVERING, SUSPECT)
- Tipos de mensajes RPC (MessageType: 30+ tipos)
- Códigos de respuesta distribuidos
```

**Razón técnica:**  
En un sistema distribuido, múltiples procesos necesitan compartir las mismas constantes. Centralizarlas evita inconsistencias y facilita el mantenimiento.

---

#### 3.1.2 `models.py` - Modelos de Datos

**¿Qué es?**  
Clases de datos (dataclasses) que representan entidades del sistema distribuido.

**¿Por qué se creó?**  
- **Serialización:** Necesario para enviar datos entre nodos vía RPC
- **Tipado:** Mejor validación y autocompletado en el IDE
- **Consistencia:** Mismo formato de datos en todos los servicios

**Modelos principales:**
```python
- NodeInfo: Información de un nodo (ID, tipo, host, puerto, estado)
- FileMetadata: Metadatos de archivos (path, tamaño, propietario, réplicas)
- UserInfo: Información de usuarios
- LockInfo: Información de bloqueos
- ReplicaInfo: Información de réplicas
- ClusterState: Estado global del clúster
```

**Razón técnica:**  
En sistemas distribuidos, los datos deben ser serializables para transmitirse por red. Los dataclasses facilitan la conversión a/desde JSON.

---

#### 3.1.3 `rpc_protocol.py` - Protocolo RPC

**¿Qué es?**  
Implementación de un protocolo RPC (Remote Procedure Call) sobre TCP usando JSON.

**¿Por qué se creó?**  
- **Comunicación entre servicios:** Los nodos necesitan comunicarse
- **Abstracción:** Ocultar detalles de red (sockets, serialización)
- **Confiabilidad:** Manejo de timeouts, reconexiones, errores

**Componentes:**
```python
- RPCMessage: Clase para mensajes RPC serializables
- RPCClient: Cliente para hacer llamadas RPC
- RPCServer: Servidor para recibir llamadas RPC
- RPCClientPool: Pool de conexiones para mejor rendimiento
```

**Razón técnica:**  
El sistema original no tenía comunicación entre procesos. En un sistema distribuido, los servicios deben comunicarse. RPC es un patrón estándar para esto.

**Protocolo implementado:**
```
[4 bytes: longitud] + [JSON: {type, payload, request_id}]
```

---

### 3.2 Servicio Metadata (`FTP/Distributed/Metadata/`)

#### 3.2.1 `metadata_server.py` - Servidor Principal

**¿Qué es?**  
Servidor que coordina todo el sistema: namespace, réplicas, bloqueos, autenticación.

**¿Por qué se creó?**  
- **Coordinación centralizada:** Necesario un punto de verdad único
- **Gestión de estado global:** Namespace, réplicas, usuarios
- **API RPC:** Expone operaciones para Router y Storage

**Funcionalidades:**
- Manejo de 30+ tipos de mensajes RPC
- Integración de todos los subcomponentes
- Elección de líder y failover
- Heartbeats y monitoreo

**Razón técnica:**  
En sistemas distribuidos, se necesita un servicio de coordinación para mantener consistencia. Metadata actúa como el "cerebro" del sistema.

---

#### 3.2.2 `namespace.py` - Sistema de Archivos Lógico

**¿Qué es?**  
Gestión del espacio de nombres virtual del sistema de archivos (no archivos físicos).

**¿Por qué se creó?**  
- **Separación lógica/física:** Los usuarios ven rutas lógicas (`/user1/docs/file.txt`), no ubicaciones físicas
- **Abstracción:** Ocultar detalles de replicación y ubicación
- **Persistencia:** Guardar estructura de directorios en disco

**Operaciones:**
```python
- create_file() / create_directory()
- get_file() / list_directory()
- delete_file() / delete_directory()
- rename()
- update_file_replicas()
- check_permission()
```

**Razón técnica:**  
En el sistema original, las rutas eran físicas (`FTPRoot/user1/file.txt`). En el distribuido, necesitamos un mapeo lógico → físico, ya que un archivo puede estar en múltiples nodos.

**Comparación:**
```python
# ORIGINAL: Ruta física directa
file_path = server.base_dir / "user1" / "file.txt"
open(file_path, 'wb')

# DISTRIBUIDO: Ruta lógica → metadatos → nodos físicos
path = "/user1/file.txt"
meta = namespace.get_file(path)  # Obtiene metadatos
replicas = replica_manager.get_replicas(meta.file_id)  # Obtiene nodos
storage_client.retrieve_file(replicas[0], meta.file_id)  # Lee de nodo
```

---

#### 3.2.3 `replica_manager.py` - Gestor de Réplicas

**¿Qué es?**  
Gestiona qué archivos están en qué nodos de storage y mantiene el factor de replicación.

**¿Por qué se creó?**  
- **Distribución:** Decidir dónde almacenar cada archivo
- **Balanceo:** Distribuir carga entre nodos
- **Recuperación:** Detectar archivos sub-replicados y planificar re-replicación

**Funcionalidades:**
```python
- select_replicas_for_file(): Selecciona nodos para nuevas réplicas
- assign_replicas(): Asigna réplicas a nodos
- get_replicas(): Obtiene nodos que tienen un archivo
- get_under_replicated_files(): Detecta archivos con pocas réplicas
- plan_rebalance(): Planifica movimientos para balancear carga
```

**Razón técnica:**  
El sistema original no tenía replicación. En el distribuido, cada archivo debe estar en 3 nodos para tolerancia a fallos. Este módulo gestiona esa distribución.

**Ejemplo:**
```python
# Al crear un archivo nuevo:
selected_nodes = replica_manager.select_replicas_for_file(file_id)
# → [Storage-1, Storage-2, Storage-3]

replica_manager.assign_replicas(file_id, selected_nodes)
# → Archivo se almacenará en los 3 nodos
```

---

#### 3.2.4 `lock_manager.py` - Gestor de Bloqueos

**¿Qué es?**  
Sistema de bloqueos distribuidos para controlar acceso concurrente a archivos.

**¿Por qué se creó?**  
- **Consistencia:** Evitar escrituras concurrentes que corrompan datos
- **Coordinación:** Múltiples lectores, un escritor
- **Prevención de condiciones de carrera**

**Modelo implementado:**
- **Lectura:** Múltiples lectores simultáneos permitidos
- **Escritura:** Solo un escritor, sin lectores

**Razón técnica:**  
En el sistema original, un solo proceso manejaba todo, así que no había concurrencia entre procesos. En el distribuido, múltiples Routers pueden intentar escribir el mismo archivo simultáneamente. Los bloqueos previenen corrupción.

**Ejemplo:**
```python
# Router 1 intenta escribir:
lock_manager.acquire_write_lock(file_id, "router-1")  # ✅ OK

# Router 2 intenta escribir el mismo archivo:
lock_manager.acquire_write_lock(file_id, "router-2")  # ❌ BLOQUEADO

# Router 1 termina:
lock_manager.release_lock(file_id, "router-1")  # Libera

# Ahora Router 2 puede escribir:
lock_manager.acquire_write_lock(file_id, "router-2")  # ✅ OK
```

---

#### 3.2.5 `leader_election.py` - Elección de Líder

**¿Qué es?**  
Algoritmo de elección de líder para el servicio Metadata (algoritmo Bully simplificado).

**¿Por qué se creó?**  
- **Alta disponibilidad:** Si el líder cae, otro nodo toma el control
- **Consistencia:** Solo el líder procesa escrituras
- **Failover automático**

**Algoritmo:**
1. Nodo con ID más alto se convierte en líder
2. Líder envía heartbeats periódicos
3. Si el líder no responde, se inicia nueva elección

**Razón técnica:**  
En el sistema original, había un solo servidor. En el distribuido, hay múltiples réplicas de Metadata. Necesitamos un líder para evitar conflictos en escrituras (dos nodos escribiendo simultáneamente causaría inconsistencias).

**Flujo:**
```
Metadata-1 (ID: 10) → Líder
Metadata-2 (ID: 5)  → Backup

Si Metadata-1 cae:
  → Metadata-2 detecta timeout
  → Inicia elección
  → Metadata-2 se convierte en nuevo líder
```

---

#### 3.2.6 `heartbeat_manager.py` - Monitoreo de Nodos

**¿Qué es?**  
Sistema de heartbeats para detectar nodos caídos.

**¿Por qué se creó?**  
- **Detección de fallos:** Saber qué nodos están activos
- **Recuperación:** Marcar nodos como DOWN y planificar re-replicación
- **Estado del clúster:** Mantener vista actualizada de nodos disponibles

**Funcionalidades:**
```python
- register_node(): Registra nodo para monitoreo
- receive_heartbeat(): Recibe señal de vida
- get_active_nodes(): Obtiene nodos activos
- _check_nodes(): Verifica nodos periódicamente
```

**Razón técnica:**  
En el sistema original, si el servidor caía, simplemente no respondía. En el distribuido, necesitamos detectar qué nodos están caídos para:
1. No intentar escribir en nodos muertos
2. Re-replicar archivos que estaban solo en nodos caídos
3. Rebalancear carga

**Estados:**
```
UP → SUSPECT (no heartbeat por 7.5s) → DOWN (no heartbeat por 15s)
DOWN → UP (recibe heartbeat de nuevo)
```

---

#### 3.2.7 `auth_service.py` - Autenticación Centralizada

**¿Qué es?**  
Servicio de autenticación que reemplaza al `CredentialsManager` original.

**¿Por qué se creó?**  
- **Centralización:** Todos los Routers consultan el mismo servicio
- **Consistencia:** Mismos usuarios en todos los nodos
- **Persistencia:** Guardar usuarios en disco

**Diferencias con el original:**
- **Original:** `CredentialsManager` local en cada servidor
- **Nuevo:** Servicio centralizado compartido por todos los Routers

**Razón técnica:**  
En el sistema original, cada servidor tenía su propia copia de credenciales. En el distribuido, múltiples Routers deben autenticar contra la misma base de usuarios. Centralizar evita inconsistencias.

---

### 3.3 Servicio Storage (`FTP/Distributed/Storage/`)

#### 3.3.1 `storage_server.py` - Servidor de Almacenamiento

**¿Qué es?**  
Servidor que almacena archivos físicamente en disco.

**¿Por qué se creó?**  
- **Separación de almacenamiento:** Los Routers no almacenan directamente
- **Replicación:** Recibe y almacena réplicas de archivos
- **API RPC:** Expone operaciones de lectura/escritura

**Operaciones:**
```python
- store_file(): Almacena archivo localmente
- retrieve_file(): Recupera archivo del disco
- delete_file(): Elimina archivo local
- replicate_to_peer(): Replica archivo a otro nodo
```

**Razón técnica:**  
En el sistema original, el servidor FTP escribía directamente en el sistema de archivos. En el distribuido, separamos el almacenamiento físico de la lógica FTP para:
1. Escalar storage independientemente
2. Replicar archivos entre múltiples nodos
3. Tolerar fallos de nodos individuales

**Comparación:**
```python
# ORIGINAL:
with open(file_path, 'wb') as f:
    f.write(data)

# DISTRIBUIDO:
# Router → Metadata (obtiene nodos) → Storage (almacena)
storage_client.store_file(node['host'], node['port'], file_id, data)
```

---

### 3.4 Servicio Router (`FTP/Distributed/Router/`)

#### 3.4.1 `router_server.py` - Servidor FTP Distribuido

**¿Qué es?**  
Servidor FTP que actúa como proxy, redirigiendo operaciones a Metadata y Storage.

**¿Por qué se creó?**  
- **Compatibilidad:** Mantener protocolo FTP estándar (RFC 959)
- **Proxy:** Router no almacena, solo coordina
- **Sesiones:** Maneja sesiones FTP de clientes

**Diferencias clave con el original:**

| Aspecto | Original | Distribuido |
|---------|----------|-------------|
| **Almacenamiento** | Directo en disco | Vía Storage nodes |
| **Autenticación** | Local (`CredentialsManager`) | Remota (Metadata) |
| **Namespace** | Sistema de archivos local | Namespace lógico (Metadata) |
| **Réplicas** | No aplica | Consulta Metadata para obtener nodos |

**Razón técnica:**  
El Router mantiene la interfaz FTP estándar para los clientes, pero internamente delega todo a los servicios distribuidos. Esto permite:
1. Escalar Routers horizontalmente
2. Mantener compatibilidad con clientes FTP existentes
3. Aislar la lógica de almacenamiento

**Flujo de operación (ejemplo RETR):**
```
Cliente → Router: RETR /user1/file.txt
Router → Metadata: lookup_file("/user1/file.txt")
Metadata → Router: {file_id, replica_nodes: [S1, S2, S3]}
Router → Storage (S1): retrieve_file(file_id)
Storage → Router: datos del archivo
Router → Cliente: datos del archivo
```

---

#### 3.4.2 `metadata_client.py` - Cliente RPC para Metadata

**¿Qué es?**  
Cliente de alto nivel para interactuar con el servicio Metadata.

**¿Por qué se creó?**  
- **Abstracción:** Ocultar detalles de RPC del Router
- **Redirección al líder:** Maneja automáticamente cambios de líder
- **API simple:** Métodos fáciles de usar (`authenticate()`, `lookup_file()`, etc.)

**Métodos principales:**
```python
- authenticate(username, password)
- lookup_file(path)
- create_file(path, owner)
- delete_file(path)
- list_directory(path)
- get_replicas(file_id)
```

**Razón técnica:**  
El Router necesita hacer muchas operaciones con Metadata. Este cliente simplifica el código del Router y maneja automáticamente detalles como redirección al líder.

---

#### 3.4.3 `storage_client.py` - Cliente RPC para Storage

**¿Qué es?**  
Cliente para interactuar con nodos de Storage.

**¿Por qué se creó?**  
- **Transferencia 