# FTP DISTRIBUTED SYSTEM - COMANDOS DE REFERENCIA

## 📋 **RESUMEN EJECUTIVO**
Sistema FTP distribuido que usa **resolución DNS pura** (sin variables de entorno METADATA_SERVICE).

## 🚀 **LEVANTAR SISTEMA COMPLETO**

```bash
# 1. Limpiar y preparar
docker rm -f $(docker ps -aq 2>/dev/null) 2>/dev/null || true
docker network create ftp-distributed-net --subnet=172.20.0.0/16

# 2. Construir imágenes
docker build -t ftp-metadata -f FTP/Distributed/Dockerfile.metadata .
docker build -t ftp-storage -f FTP/Distributed/Dockerfile.storage .
docker build -t ftp-router -f FTP/Distributed/Dockerfile.router .

# 3. Metadata (DNS discovery)
docker run -d --name metadata1 --hostname metadata1 --network ftp-distributed-net --network-alias metadata --ip 172.20.0.11 -e NODE_ID=metadata-1 -v $(pwd)/data/metadata1:/data/metadata ftp-metadata python3 -m FTP.Distributed.Metadata.metadata_server --port 5000 --data-dir /data/metadata

docker run -d --name metadata2 --hostname metadata2 --network ftp-distributed-net --network-alias metadata --ip 172.20.0.12 -e NODE_ID=metadata-2 -v $(pwd)/data/metadata2:/data/metadata ftp-metadata python3 -m FTP.Distributed.Metadata.metadata_server --port 5000 --data-dir /data/metadata

docker run -d --name metadata3 --hostname metadata3 --network ftp-distributed-net --network-alias metadata --ip 172.20.0.13 -e NODE_ID=metadata-3 -v $(pwd)/data/metadata3:/data/metadata ftp-metadata python3 -m FTP.Distributed.Metadata.metadata_server --port 5000 --data-dir /data/metadata

# 4. Storage (DNS connection)
docker run -d --name storage1 --hostname storage1 --network ftp-distributed-net --ip 172.20.0.21 -e NODE_ID=storage-1 -v $(pwd)/data/storage1:/data/storage ftp-storage python3 -m FTP.Distributed.Storage.storage_server --port 5001 --data-dir /data/storage --metadata-host metadata --metadata-port 5000

docker run -d --name storage2 --hostname storage2 --network ftp-distributed-net --ip 172.20.0.22 -e NODE_ID=storage-2 -v $(pwd)/data/storage2:/data/storage ftp-storage python3 -m FTP.Distributed.Storage.storage_server --port 5001 --data-dir /data/storage --metadata-host metadata --metadata-port 5000

# 5. Router (DNS connection)
docker run -d --name router1 --hostname router1 --network ftp-distributed-net --ip 172.20.0.31 -p 2121:21 -p 30000-30100:30000-30100 ftp-router python3 -m FTP.Distributed.Router.router_server --port 21 --metadata-host metadata --metadata-port 5000 --public-ip 127.0.0.1

# 6. Verificar
sleep 10 && docker ps
```

## 🧪 **PROBAR SISTEMA**

```bash
# Crear archivo de prueba
echo "Contenido de prueba - $(date)" > test.txt

# FTP Upload
echo -e "user admin admin123\nbinary\nput test.txt\nquit" | ftp -n 127.0.0.1 2121

# Verificar almacenamiento físico
docker exec storage1 find /data/storage -name "*" -exec cat {} \; 2>/dev/null || docker exec storage2 find /data/storage -name "*" -exec cat {} \;

# FTP Download
echo -e "user admin admin123\nbinary\nget test.txt downloaded.txt\nquit" | ftp -n 127.0.0.1 2121
cat downloaded.txt
```

## 🔍 **VERIFICACIONES**

```bash
# Estado de containers
docker ps

# DNS resolution desde container
docker exec storage1 getent hosts metadata

# Logs de servicios
docker logs metadata1 | tail -10    # Peer discovery, elections
docker logs router1 | tail -10      # FTP operations
docker logs storage1 | tail -10     # Registrations

# Líder actual
docker exec -i router1 python3 -c "
import sys, time
sys.path.extend(['/app','/app/FTP'])
from FTP.Distributed.Router.metadata_client import MetadataClient
c = MetadataClient('metadata', 5000)
time.sleep(2)
print(f'Leader: {c._leader_host}:{c._leader_port}')
"
```

## 💥 **PRUEBAS DE FAILOVER**

```bash
# Simular caída de storage
docker rm -f storage2

# Ver rebalanceo
sleep 10 && docker logs metadata1 | grep -E "(rebalance|failed)" | tail -5

# Probar continuidad
echo -e "user admin admin123\nbinary\nget test.txt after_failover.txt\nquit" | ftp -n 127.0.0.1 2121
```

## 🧹 **LIMPIEZA**

```bash
# Detener todo
docker stop metadata1 metadata2 metadata3 storage1 storage2 router1

# Remover containers
docker rm metadata1 metadata2 metadata3 storage1 storage2 router1

# Limpiar red (opcional)
docker network rm ftp-distributed-net
```

## 📁 **ARCHIVOS DE REFERENCIA**

- `commands_reference.sh` - Todos los comandos detallados
- `comandos_directos.sh` - **Comandos seguros para terminal** (sin errores de sintaxis)
- `start_system.sh` - Script para levantar todo
- `test_ftp.sh` - Script para pruebas FTP

## ✅ **CHECKLIST FUNCIONAL**

- [x] **DNS Discovery**: Metadata usa DNS alias `metadata`
- [x] **Leader Election**: Funciona sin variables de entorno
- [x] **Storage Registration**: Se conecta via DNS
- [x] **FTP Upload**: Archivos llegan físicamente
- [x] **FTP Download**: Lectura desde réplicas
- [x] **Failover**: Rebalanceo automático
- [x] **Replicación**: Múltiples copias (REPLICATION_FACTOR=2)

## 🚨 **NOTAS IMPORTANTES**

1. **No uses variables de entorno** `METADATA_SERVICE` - todo usa DNS
2. **Espera 10 segundos** después de levantar para estabilización
3. **IP pública del router**: `--public-ip 127.0.0.1` para conexiones desde host
4. **Network aliases**: `--network-alias metadata` para DNS resolution
5. **Puertos mapeados**: `-p 2121:21` y rangos para datos pasivos

---
**Estado del sistema**: ✅ **100% FUNCIONAL** 🚀