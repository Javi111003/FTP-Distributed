#!/bin/bash
# =============================================================================
# FTP DISTRIBUTED SYSTEM - COMANDOS DE REFERENCIA
# =============================================================================
# Este archivo contiene todos los comandos usados para levantar y probar
# el sistema FTP distribuido con DNS resolution (sin variables de entorno)
# =============================================================================

echo "=== FTP DISTRIBUTED SYSTEM - COMANDOS DE REFERENCIA ==="
echo ""
echo "IMPORTANTE: Todos los comandos usan resolución DNS pura."
echo "No se necesitan variables de entorno METADATA_SERVICE."
echo ""

# =============================================================================
# 1. LIMPIEZA Y PREPARACIÓN
# =============================================================================
echo "=== 1. LIMPIEZA Y PREPARACIÓN ==="
echo ""
echo "# Limpiar todos los containers anteriores"
echo "docker rm -f \$(docker ps -aq 2>/dev/null) 2>/dev/null || true"
echo ""
echo "# Crear/redimensionar red Docker"
echo "docker network create ftp-distributed-net --subnet=172.20.0.0/16"
echo "docker network create ftp-distributed-net --subnet=172.20.0.0/16  # si ya existe"
echo ""
echo "# Construir todas las imágenes"
echo "docker build -t ftp-metadata -f FTP/Distributed/Dockerfile.metadata ."
echo "docker build -t ftp-storage -f FTP/Distributed/Dockerfile.storage ."
echo "docker build -t ftp-router -f FTP/Distributed/Dockerfile.router ."
echo ""

# =============================================================================
# 2. LEVANTAR METADATA SERVICES (con DNS discovery)
# =============================================================================
echo "=== 2. LEVANTAR METADATA SERVICES (DNS discovery) ==="
echo ""
echo "# Metadata1 - Nodo bootstrap (no necesita variables de entorno)"
echo "docker run -d --name metadata1 --hostname metadata1 --network ftp-distributed-net --network-alias metadata --ip 172.20.0.11 -e NODE_ID=metadata-1 -v \$(pwd)/data/metadata1:/data/metadata ftp-metadata python3 -m FTP.Distributed.Metadata.metadata_server --port 5000 --data-dir /data/metadata"
echo ""
echo "# Metadata2 - Se conecta via DNS al alias 'metadata'"
echo "docker run -d --name metadata2 --hostname metadata2 --network ftp-distributed-net --network-alias metadata --ip 172.20.0.12 -e NODE_ID=metadata-2 -v \$(pwd)/data/metadata2:/data/metadata ftp-metadata python3 -m FTP.Distributed.Metadata.metadata_server --port 5000 --data-dir /data/metadata"
echo ""
echo "# Metadata3 - Se conecta via DNS al alias 'metadata'"
echo "docker run -d --name metadata3 --hostname metadata3 --network ftp-distributed-net --network-alias metadata --ip 172.20.0.13 -e NODE_ID=metadata-3 -v \$(pwd)/data/metadata3:/data/metadata ftp-metadata python3 -m FTP.Distributed.Metadata.metadata_server --port 5000 --data-dir /data/metadata"
echo ""

# =============================================================================
# 3. LEVANTAR STORAGE SERVICES (conectan via DNS)
# =============================================================================
echo "=== 3. LEVANTAR STORAGE SERVICES (DNS connection) ==="
echo ""
echo "# Storage1 - Se conecta usando --metadata-host metadata (DNS)"
echo "docker run -d --name storage1 --hostname storage1 --network ftp-distributed-net --ip 172.20.0.21 -e NODE_ID=storage-1 -v \$(pwd)/data/storage1:/data/storage ftp-storage python3 -m FTP.Distributed.Storage.storage_server --port 5001 --data-dir /data/storage --metadata-host metadata --metadata-port 5000"
echo ""
echo "# Storage2 - Se conecta usando --metadata-host metadata (DNS)"
echo "docker run -d --name storage2 --hostname storage2 --network ftp-distributed-net --ip 172.20.0.22 -e NODE_ID=storage-2 -v \$(pwd)/data/storage2:/data/storage ftp-storage python3 -m FTP.Distributed.Storage.storage_server --port 5001 --data-dir /data/storage --metadata-host metadata --metadata-port 5000"
echo ""

# =============================================================================
# 4. LEVANTAR ROUTER FTP (conecta via DNS)
# =============================================================================
echo "=== 4. LEVANTAR ROUTER FTP (DNS connection) ==="
echo ""
echo "# Router - Se conecta usando --metadata-host metadata (DNS)"
echo "docker run -d --name router1 --hostname router1 --network ftp-distributed-net --ip 172.20.0.31 -p 2121:21 -p 30000-30100:30000-30100 ftp-router python3 -m FTP.Distributed.Router.router_server --port 21 --metadata-host metadata --metadata-port 5000 --public-ip 127.0.0.1"
echo ""

# =============================================================================
# 5. VERIFICACIÓN DEL SISTEMA
# =============================================================================
echo "=== 5. VERIFICACIÓN DEL SISTEMA ==="
echo ""
echo "# Ver estado de todos los containers"
echo "docker ps"
echo ""
echo "# Verificar que los metadata se registraron entre sí (DNS)"
echo "docker logs metadata1 | grep -E \"(DNS|metadata|register|peer)\" | tail -10"
echo ""
echo "# Verificar que los storage nodes se registraron"
echo "docker logs metadata1 | grep \"Storage node registered\" | tail -5"
echo ""
echo "# Verificar resolución DNS desde un container"
echo "docker exec storage1 getent hosts metadata"
echo ""

# =============================================================================
# 6. PRUEBAS FTP FUNCIONALES
# =============================================================================
echo "=== 6. PRUEBAS FTP FUNCIONALES ==="
echo ""
echo "# Crear archivo de prueba"
echo "echo \"Contenido de prueba completo del sistema\" > test_complete_system.txt"
echo ""
echo "# FTP Upload (desde el host)"
echo "echo -e \"user admin admin123\\nbinary\\nput test_complete_system.txt\\nquit\" | ftp -n 127.0.0.1 2121"
echo ""
echo "# Verificar que el archivo llegó físicamente a storage"
echo "docker exec storage1 find /data/storage -name \"*\" -exec cat {} \\; 2>/dev/null || docker exec storage2 find /data/storage -name \"*\" -exec cat {} \\;"
echo ""
echo "# FTP Download (desde el host)"
echo "echo -e \"user admin admin123\\nbinary\\nget test_complete_system.txt downloaded_system_test.txt\\nquit\" | ftp -n 127.0.0.1 2121"
echo ""
echo "# Verificar contenido descargado"
echo "cat downloaded_system_test.txt"
echo ""

# =============================================================================
# 7. PRUEBAS DE FAILOVER Y REBALANCEO
# =============================================================================
echo "=== 7. PRUEBAS DE FAILOVER Y REBALANCEO ==="
echo ""
echo "# Simular caída de storage (para probar rebalanceo)"
echo "docker rm -f storage2"
echo ""
echo "# Verificar que se activó rebalanceo"
echo "sleep 10 && docker logs metadata1 | grep -E \"(rebalance|Rebalance|failed|Failed)\" | tail -5"
echo ""
echo "# Verificar que el sistema sigue operativo (puede leer archivos existentes)"
echo "echo -e \"user admin admin123\\nbinary\\nget test_complete_system.txt downloaded_after_failover.txt\\nquit\" | ftp -n 127.0.0.1 2121"
echo ""
echo "# Recrear storage2 (para restaurar redundancia)"
echo "docker run -d --name storage2 --hostname storage2 --network ftp-distributed-net --ip 172.20.0.22 -e NODE_ID=storage-2 -v \$(pwd)/data/storage2:/data/storage ftp-storage python3 -m FTP.Distributed.Storage.storage_server --port 5001 --data-dir /data/storage --metadata-host metadata --metadata-port 5000"
echo ""

# =============================================================================
# 8. PRUEBAS DE CONECTIVIDAD DIRECTA (sin FTP)
# =============================================================================
echo "=== 8. PRUEBAS DE CONECTIVIDAD DIRECTA ==="
echo ""
echo "# Verificar líder actual desde router"
echo "docker exec -i router1 python3 - <<'PY'"
echo "import sys, time"
echo "sys.path.extend(['/app','/app/FTP'])"
echo "from FTP.Distributed.Router.metadata_client import MetadataClient"
echo "c = MetadataClient(metadata_host='metadata', metadata_port=5000)"
echo "time.sleep(2)"
echo "print(f'Current leader: {c._leader_host}:{c._leader_port}')"
echo "PY"
echo ""
echo "# Crear archivo directamente desde router"
echo "docker exec -i router1 python3 - <<'PY'"
echo "import sys, time"
echo "sys.path.extend(['/app','/app/FTP'])"
echo "from FTP.Distributed.Router.metadata_client import MetadataClient"
echo "c = MetadataClient(metadata_host='metadata', metadata_port=5000)"
echo "time.sleep(2)"
echo "success, meta, storage_nodes = c.create_file('/admin/test_direct.txt', 'admin')"
echo "print(f'Create file result: success={success}, storage_nodes={storage_nodes}')"
echo "PY"
echo ""

# =============================================================================
# 9. MONITOREO Y LOGS
# =============================================================================
echo "=== 9. MONITOREO Y LOGS ==="
echo ""
echo "# Ver logs del router (operaciones FTP)"
echo "docker logs router1 | tail -20"
echo ""
echo "# Ver logs de metadata (registros, elecciones, rebalanceo)"
echo "docker logs metadata1 | tail -20"
echo ""
echo "# Ver logs de storage (registros, operaciones)"
echo "docker logs storage1 | tail -10"
echo ""
echo "# Buscar errores en todos los servicios"
echo "for s in metadata1 metadata2 metadata3 storage1 storage2 router1; do echo \"=== \$s ===\"; docker logs \$s 2>&1 | grep -i error | tail -3; done"
echo ""

# =============================================================================
# 10. LIMPIEZA FINAL
# =============================================================================
echo "=== 10. LIMPIEZA FINAL ==="
echo ""
echo "# Detener todos los servicios"
echo "docker stop metadata1 metadata2 metadata3 storage1 storage2 router1"
echo ""
echo "# Remover todos los containers"
echo "docker rm metadata1 metadata2 metadata3 storage1 storage2 router1"
echo ""
echo "# Limpiar red (opcional)"
echo "docker network rm ftp-distributed-net"
echo ""

# =============================================================================
# 11. SCRIPTS DE PRUEBA COMPLETOS
# =============================================================================
echo "=== 11. SCRIPTS DE PRUEBA COMPLETOS ==="
echo ""
echo "# Script para levantar todo el sistema"
echo "cat > start_system.sh << 'EOF'"
echo "#!/bin/bash"
echo "cd /home/kendry/Downloads/FTP-Distributed"
echo ""
echo "# Limpiar"
echo "docker rm -f \$(docker ps -aq) 2>/dev/null || true"
echo "docker network create ftp-distributed-net --subnet=172.20.0.0/16 2>/dev/null || true"
echo ""
echo "# Metadata"
echo "docker run -d --name metadata1 --hostname metadata1 --network ftp-distributed-net --network-alias metadata --ip 172.20.0.11 -e NODE_ID=metadata-1 -v \$(pwd)/data/metadata1:/data/metadata ftp-metadata python3 -m FTP.Distributed.Metadata.metadata_server --port 5000 --data-dir /data/metadata"
echo "docker run -d --name metadata2 --hostname metadata2 --network ftp-distributed-net --network-alias metadata --ip 172.20.0.12 -e NODE_ID=metadata-2 -v \$(pwd)/data/metadata2:/data/metadata ftp-metadata python3 -m FTP.Distributed.Metadata.metadata_server --port 5000 --data-dir /data/metadata"
echo "docker run -d --name metadata3 --hostname metadata3 --network ftp-distributed-net --network-alias metadata --ip 172.20.0.13 -e NODE_ID=metadata-3 -v \$(pwd)/data/metadata3:/data/metadata ftp-metadata python3 -m FTP.Distributed.Metadata.metadata_server --port 5000 --data-dir /data/metadata"
echo ""
echo "# Storage"
echo "docker run -d --name storage1 --hostname storage1 --network ftp-distributed-net --ip 172.20.0.21 -e NODE_ID=storage-1 -v \$(pwd)/data/storage1:/data/storage ftp-storage python3 -m FTP.Distributed.Storage.storage_server --port 5001 --data-dir /data/storage --metadata-host metadata --metadata-port 5000"
echo "docker run -d --name storage2 --hostname storage2 --network ftp-distributed-net --ip 172.20.0.22 -e NODE_ID=storage-2 -v \$(pwd)/data/storage2:/data/storage ftp-storage python3 -m FTP.Distributed.Storage.storage_server --port 5001 --data-dir /data/storage --metadata-host metadata --metadata-port 5000"
echo ""
echo "# Router"
echo "docker run -d --name router1 --hostname router1 --network ftp-distributed-net --ip 172.20.0.31 -p 2121:21 -p 30000-30100:30000-30100 ftp-router python3 -m FTP.Distributed.Router.router_server --port 21 --metadata-host metadata --metadata-port 5000 --public-ip 127.0.0.1"
echo ""
echo "echo \"Sistema levantado. Espera 10 segundos para estabilización...\""
echo "sleep 10"
echo "docker ps"
echo "EOF"
echo "chmod +x start_system.sh"
echo ""
echo "# Script para probar FTP completo"
echo "cat > test_ftp.sh << 'EOF'"
echo "#!/bin/bash"
echo "cd /home/kendry/Downloads/FTP-Distributed"
echo ""
echo "# Crear archivo de prueba"
echo "echo \"Contenido de prueba FTP completo - \$(date)\" > test_ftp.txt"
echo ""
echo "# FTP Upload"
echo "echo \"=== UPLOAD FTP ===\""
echo "echo -e \"user admin admin123\\nbinary\\nput test_ftp.txt\\nquit\" | ftp -n 127.0.0.1 2121"
echo ""
echo "# Verificar almacenamiento físico"
echo "echo \"=== VERIFICACIÓN FÍSICA ===\""
echo "for s in storage1 storage2; do"
echo "  echo \"--- \$s ---\""
echo "  docker exec \$s find /data/storage -name \"*\" -exec cat {} \\; 2>/dev/null || echo \"No files\""
echo "done"
echo ""
echo "# FTP Download"
echo "echo \"=== DOWNLOAD FTP ===\""
echo "echo -e \"user admin admin123\\nbinary\\nget test_ftp.txt downloaded_ftp.txt\\nquit\" | ftp -n 127.0.0.1 2121"
echo ""
echo "# Verificar download"
echo "echo \"=== CONTENIDO DESCARGADO ===\""
echo "cat downloaded_ftp.txt"
echo "EOF"
echo "chmod +x test_ftp.sh"
echo ""

echo "=== FIN DEL ARCHIVO DE REFERENCIA ==="
echo ""
echo "Para usar estos comandos:"
echo "1. Copia y pega los comandos individuales"
echo "2. O crea los scripts start_system.sh y test_ftp.sh"
echo "3. Ejecuta: ./start_system.sh && ./test_ftp.sh"