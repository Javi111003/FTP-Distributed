# Sistema FTP Distribuido

Este documento describe la implementación del sistema FTP distribuido basado en Docker Swarm.

## Arquitectura

El sistema está compuesto por tres tipos de nodos:

| Componente | Rol | Puerto | Réplicas |
|------------|-----|--------|----------|
| **Router** | Punto de entrada FTP, proxy de datos | 21, 30000-30100 | 2 |
| **Metadata** | Coordinación, namespace, gestión de réplicas | 5000 | 2 |
| **Storage** | Almacenamiento físico con replicación | 5001 | 3 |

### Diagrama de Arquitectura

```
                    ┌─────────────────────────────────────────┐
                    │              Clientes FTP               │
                    └────────────────────┬────────────────────┘
                                         │
                    ┌────────────────────▼────────────────────┐
                    │            frontend_net                  │
                    │     (Red overlay para clientes)          │
                    └────────────────────┬────────────────────┘
                                         │
            ┌────────────────────────────┼────────────────────────────┐
            │                            │                            │
    ┌───────▼───────┐            ┌───────▼───────┐                    │
    │   Router 1    │            │   Router 2    │                    │
    │   (FTP:21)    │            │   (FTP:21)    │                    │
    └───────┬───────┘            └───────┬───────┘                    │
            │                            │                            │
            └────────────────┬───────────┘                            │
                             │                                        │
    ┌────────────────────────▼────────────────────────────────────────┘
    │                     backend_net                                  │
    │               (Red overlay interna)                              │
    └───┬─────────────────────┬─────────────────────┬─────────────────┘
        │                     │                     │
┌───────▼───────┐     ┌───────▼───────┐     ┌───────▼───────┐
│  Metadata 1   │◄───►│  Metadata 2   │     │               │
│  (Líder)      │     │  (Backup)     │     │               │
└───────┬───────┘     └───────┬───────┘     │               │
        │                     │             │               │
        └──────────┬──────────┘             │               │
                   │                        │               │
        ┌──────────▼──────────┐             │               │
        │                     │             │               │
┌───────▼───────┐     ┌───────▼───────┐     ┌───────▼───────┐
│  Storage 1    │     │  Storage 2    │     │  Storage 3    │
│  (Réplicas)   │     │  (Réplicas)   │     │  (Réplicas)   │
└───────────────┘     └───────────────┘     └───────────────┘
```

## Estructura del Proyecto

```
FTP-Distributed/
├── FTP/
│   ├── Distributed/           # Sistema distribuido
│   │   ├── Common/            # Código compartido
│   │   │   ├── constants.py   # Constantes y enums
│   │   │   ├── models.py      # Modelos de datos
│   │   │   └── rpc_protocol.py # Protocolo RPC
│   │   ├── Metadata/          # Servicio de Metadata
│   │   │   ├── metadata_server.py
│   │   │   ├── namespace.py   # Sistema de archivos lógico
│   │   │   ├── replica_manager.py
│   │   │   ├── lock_manager.py
│   │   │   ├── leader_election.py
│   │   │   ├── heartbeat_manager.py
│   │   │   └── auth_service.py
│   │   ├── Storage/           # Servicio de Storage
│   │   │   └── storage_server.py
│   │   ├── Router/            # Servicio Router (FTP Frontend)
│   │   │   ├── router_server.py
│   │   │   ├── metadata_client.py
│   │   │   └── storage_client.py
│   │   ├── Dockerfile.metadata
│   │   ├── Dockerfile.storage
│   │   └── Dockerfile.router
│   └── Server/                # Servidor FTP centralizado (original)
├── docker-compose.distributed.yml  # Para Docker Swarm
├── docker-compose.dev.yml          # Para desarrollo local
├── scripts/                        # Scripts de despliegue
└── .env.example                    # Ejemplo de configuración
```

## Despliegue

### Requisitos Previos

- Docker 20.10+
- Docker Compose 2.0+
- Docker Swarm inicializado (para producción)
- 2 máquinas físicas (para alta disponibilidad)

### Desarrollo Local

```bash
# Windows (PowerShell)
.\scripts\dev-start.ps1

# Linux/Mac
./scripts/dev-start.sh
```

### Producción (Docker Swarm)

1. **Inicializar Swarm en el nodo manager:**
```bash
docker swarm init --advertise-addr <IP_MANAGER>
```

2. **Unir nodos worker:**
```bash
# En el nodo manager, obtener el token:
docker swarm join-token worker

# En los nodos worker, ejecutar el comando resultante
docker swarm join --token <TOKEN> <IP_MANAGER>:2377
```

3. **Desplegar el sistema:**
```bash
# Windows (PowerShell)
.\scripts\deploy-swarm.ps1

# Linux/Mac
./scripts/deploy-swarm.sh
```

## Uso

### Conexión FTP

```bash
ftp <IP_DEL_SERVIDOR> 21
```

- **Usuario:** admin
- **Contraseña:** admin123

### Comandos FTP Soportados

| Comando | Descripción |
|---------|-------------|
| USER/PASS | Autenticación |
| PWD | Directorio actual |
| CWD/CDUP | Cambiar directorio |
| LIST/NLST | Listar directorio |
| MKD/RMD | Crear/eliminar directorio |
| RETR/STOR | Descargar/subir archivo |
| DELE | Eliminar archivo |
| RNFR/RNTO | Renombrar |
| PASV/PORT | Modo de transferencia |
| TYPE | Tipo de transferencia |
| SIZE | Tamaño de archivo |
| QUIT | Desconectar |

## Características del Sistema Distribuido

### Tolerancia a Fallos

- **Factor de replicación:** 3 copias por archivo
- **Mínimo de réplicas para escritura:** 2
- **Heartbeats:** Cada 5 segundos
- **Timeout de nodo:** 15 segundos

### Consistencia

- **Modelo:** Consistencia eventual con last-write-wins
- **Versionado:** Números de versión incrementales
- **Sincronización:** Automática al recuperar nodos

### Elección de Líder

- **Algoritmo:** Bully simplificado (ID más alto gana)
- **Failover:** Automático al detectar caída del líder

### Seguridad

- **Red interna:** Aislada (backend_net)
- **Autenticación:** Centralizada en Metadata
- **Permisos:** Estilo Unix por archivo/directorio

## Monitoreo

### Ver estado de servicios
```bash
docker stack services ftp-cluster
```

### Ver logs
```bash
# Router
docker service logs ftp-cluster_router

# Metadata
docker service logs ftp-cluster_metadata

# Storage
docker service logs ftp-cluster_storage
```

### Ver tareas
```bash
docker stack ps ftp-cluster
```

## Troubleshooting

### No puedo conectar por FTP
1. Verificar que los servicios están corriendo:
   ```bash
   docker stack services ftp-cluster
   ```
2. Verificar puertos abiertos en el firewall
3. Verificar logs del Router

### Archivos no se replican
1. Verificar que hay al menos 3 nodos de storage activos
2. Verificar logs del servicio de Metadata
3. Verificar conectividad entre nodos en backend_net

### Líder no responde
1. El sistema debería elegir nuevo líder automáticamente
2. Verificar logs de Metadata para ver proceso de elección
3. Si persiste, reiniciar servicio de Metadata

## Desarrollo

### Ejecutar tests
```bash
# Próximamente
python -m pytest tests/
```

### Agregar nuevos comandos FTP
1. Implementar handler en `router_server.py`
2. Agregar operaciones necesarias en `metadata_client.py`
3. Si requiere storage, agregar en `storage_client.py`

## Licencia

Proyecto académico - Universidad de La Habana, 2025

