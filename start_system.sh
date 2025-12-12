#!/bin/bash
# =============================================================================
# FTP DISTRIBUTED SYSTEM - STARTUP SCRIPT
# =============================================================================
# Levanta todo el sistema distribuido con DNS resolution
# =============================================================================

echo "=== FTP DISTRIBUTED SYSTEM - STARTUP ==="
cd /home/kendry/Downloads/FTP-Distributed

echo "1. Limpiando containers anteriores..."
docker rm -f $(docker ps -aq 2>/dev/null) 2>/dev/null || true

echo "2. Preparando red Docker..."
docker network create ftp-distributed-net --subnet=172.20.0.0/16 2>/dev/null || true

echo "3. Levantando Metadata Services (DNS discovery)..."
# Metadata1 - Bootstrap node
docker run -d --name metadata1 --hostname metadata1 --network ftp-distributed-net --network-alias metadata --ip 172.20.0.11 -e NODE_ID=metadata-1 -v $(pwd)/data/metadata1:/data/metadata ftp-metadata python3 -m FTP.Distributed.Metadata.metadata_server --port 5000 --data-dir /data/metadata

# Metadata2 - DNS discovery
docker run -d --name metadata2 --hostname metadata2 --network ftp-distributed-net --network-alias metadata --ip 172.20.0.12 -e NODE_ID=metadata-2 -v $(pwd)/data/metadata2:/data/metadata ftp-metadata python3 -m FTP.Distributed.Metadata.metadata_server --port 5000 --data-dir /data/metadata

# Metadata3 - DNS discovery
docker run -d --name metadata3 --hostname metadata3 --network ftp-distributed-net --network-alias metadata --ip 172.20.0.13 -e NODE_ID=metadata-3 -v $(pwd)/data/metadata3:/data/metadata ftp-metadata python3 -m FTP.Distributed.Metadata.metadata_server --port 5000 --data-dir /data/metadata

echo "4. Levantando Storage Services (DNS connection)..."
docker run -d --name storage1 --hostname storage1 --network ftp-distributed-net --ip 172.20.0.21 -e NODE_ID=storage-1 -v $(pwd)/data/storage1:/data/storage ftp-storage python3 -m FTP.Distributed.Storage.storage_server --port 5001 --data-dir /data/storage --metadata-host metadata --metadata-port 5000

docker run -d --name storage2 --hostname storage2 --network ftp-distributed-net --ip 172.20.0.22 -e NODE_ID=storage-2 -v $(pwd)/data/storage2:/data/storage ftp-storage python3 -m FTP.Distributed.Storage.storage_server --port 5001 --data-dir /data/storage --metadata-host metadata --metadata-port 5000

echo "5. Levantando Router FTP (DNS connection)..."
docker run -d --name router1 --hostname router1 --network ftp-distributed-net --ip 172.20.0.31 -p 2121:21 -p 30000-30100:30000-30100 ftp-router python3 -m FTP.Distributed.Router.router_server --port 21 --metadata-host metadata --metadata-port 5000 --public-ip 127.0.0.1

echo "6. Esperando estabilización del sistema..."
sleep 10

echo "7. Estado del sistema:"
docker ps

echo ""
echo "=== SISTEMA LISTO ==="
echo "📁 FTP Server: ftp://127.0.0.1:2121"
echo "👤 Usuario: admin / admin123"
echo "🔍 Verificar: docker logs metadata1 | grep 'DNS\|peer\|register'"
echo "🧪 Probar: ./test_ftp.sh"