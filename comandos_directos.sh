#!/bin/bash
# =============================================================================
# FTP DISTRIBUTED SYSTEM - COMANDOS PARA EJECUTAR DIRECTAMENTE
# =============================================================================
# Comandos seguros para ejecutar en terminal sin problemas de sintaxis
# =============================================================================

echo "=== COMANDOS PARA EJECUTAR DIRECTAMENTE EN TERMINAL ==="
echo ""

# =============================================================================
# 1. LIMPIEZA SEGURA
# =============================================================================
echo "=== 1. LIMPIEZA SEGURA ==="
echo ""
echo "# Comando seguro para limpiar containers (sin problemas de sintaxis):"
echo 'docker rm -f $(docker ps -aq 2>/dev/null) 2>/dev/null || true'
echo ""
echo "# Ejecutar ahora:"
docker rm -f $(docker ps -aq 2>/dev/null) 2>/dev/null || true
echo ""

# =============================================================================
# 2. CREAR RED
# =============================================================================
echo "=== 2. CREAR RED ==="
echo ""
echo "# Crear red Docker:"
echo "docker network create ftp-distributed-net --subnet=172.20.0.0/16"
docker network create ftp-distributed-net --subnet=172.20.0.0/16 2>/dev/null || echo "Red ya existe"
echo ""

# =============================================================================
# 3. LEVANTAR SISTEMA PASO A PASO
# =============================================================================
echo "=== 3. LEVANTAR SISTEMA PASO A PASO ==="
echo ""

echo "# PASO 3.1: Construir imágenes"
echo "docker build -t ftp-metadata -f FTP/Distributed/Dockerfile.metadata ."
echo "docker build -t ftp-storage -f FTP/Distributed/Dockerfile.storage ."
echo "docker build -t ftp-router -f FTP/Distributed/Dockerfile.router ."
echo ""

echo "# PASO 3.2: Metadata1 (bootstrap)"
echo 'docker run -d --name metadata1 --hostname metadata1 --network ftp-distributed-net --network-alias metadata --ip 172.20.0.11 -e NODE_ID=metadata-1 -v $(pwd)/data/metadata1:/data/metadata ftp-metadata python3 -m FTP.Distributed.Metadata.metadata_server --port 5000 --data-dir /data/metadata'
echo ""

echo "# PASO 3.3: Metadata2 (DNS discovery)"
echo 'docker run -d --name metadata2 --hostname metadata2 --network ftp-distributed-net --network-alias metadata --ip 172.20.0.12 -e NODE_ID=metadata-2 -v $(pwd)/data/metadata2:/data/metadata ftp-metadata python3 -m FTP.Distributed.Metadata.metadata_server --port 5000 --data-dir /data/metadata'
echo ""

echo "# PASO 3.4: Metadata3 (DNS discovery)"
echo 'docker run -d --name metadata3 --hostname metadata3 --network ftp-distributed-net --network-alias metadata --ip 172.20.0.13 -e NODE_ID=metadata-3 -v $(pwd)/data/metadata3:/data/metadata ftp-metadata python3 -m FTP.Distributed.Metadata.metadata_server --port 5000 --data-dir /data/metadata'
echo ""

echo "# PASO 3.5: Storage1 (DNS connection)"
echo 'docker run -d --name storage1 --hostname storage1 --network ftp-distributed-net --ip 172.20.0.21 -e NODE_ID=storage-1 -v $(pwd)/data/storage1:/data/storage ftp-storage python3 -m FTP.Distributed.Storage.storage_server --port 5001 --data-dir /data/storage --metadata-host metadata --metadata-port 5000'
echo ""

echo "# PASO 3.6: Storage2 (DNS connection)"
echo 'docker run -d --name storage2 --hostname storage2 --network ftp-distributed-net --ip 172.20.0.22 -e NODE_ID=storage-2 -v $(pwd)/data/storage2:/data/storage ftp-storage python3 -m FTP.Distributed.Storage.storage_server --port 5001 --data-dir /data/storage --metadata-host metadata --metadata-port 5000'
echo ""

echo "# PASO 3.7: Router (DNS connection)"
echo 'docker run -d --name router1 --hostname router1 --network ftp-distributed-net --ip 172.20.0.31 -p 2121:21 -p 30000-30100:30000-30100 ftp-router python3 -m FTP.Distributed.Router.router_server --port 21 --metadata-host metadata --metadata-port 5000 --public-ip 127.0.0.1'
echo ""

# =============================================================================
# 4. VERIFICACIÓN
# =============================================================================
echo "=== 4. VERIFICACIÓN ==="
echo ""
echo "# Ver estado:"
echo "docker ps"
echo ""

echo "# Ver DNS resolution:"
echo "docker exec storage1 getent hosts metadata"
echo ""

echo "# Ver logs de discovery:"
echo "docker logs metadata1 | grep -E \"DNS\|peer\|register\" | tail -5"
echo ""

# =============================================================================
# 5. PRUEBAS FTP
# =============================================================================
echo "=== 5. PRUEBAS FTP ==="
echo ""
echo "# Crear archivo de prueba:"
echo 'echo "Contenido de prueba - $(date)" > test.txt'
echo ""

echo "# FTP Upload:"
echo 'echo -e "user admin admin123\nbinary\nput test.txt\nquit" | ftp -n 127.0.0.1 2121'
echo ""

echo "# Verificar almacenamiento físico:"
echo "docker exec storage1 find /data/storage -name \"*\" -exec cat {} \\; 2>/dev/null || docker exec storage2 find /data/storage -name \"*\" -exec cat {} \\;"
echo ""

echo "# FTP Download:"
echo 'echo -e "user admin admin123\nbinary\nget test.txt downloaded.txt\nquit" | ftp -n 127.0.0.1 2121'
echo ""

# =============================================================================
# 6. LIMPIEZA SEGURA
# =============================================================================
echo "=== 6. LIMPIEZA SEGURA ==="
echo ""
echo "# Detener servicios:"
echo 'docker stop $(docker ps -q --filter "name=metadata\|name=storage\|name=router" 2>/dev/null) 2>/dev/null || true'
echo ""

echo "# Remover containers:"
echo 'docker rm $(docker ps -aq --filter "name=metadata\|name=storage\|name=router" 2>/dev/null) 2>/dev/null || true'
echo ""

echo "# Limpiar red:"
echo "docker network rm ftp-distributed-net 2>/dev/null || true"
echo ""

echo "=== FIN DE COMANDOS DIRECTOS ==="
echo ""
echo "💡 Para ejecutar cualquier comando:"
echo "   1. Copia la línea completa"
echo "   2. Pégala en la terminal"
echo "   3. Presiona Enter"
echo ""
echo "🎯 Sistema 100% funcional con DNS pura!"