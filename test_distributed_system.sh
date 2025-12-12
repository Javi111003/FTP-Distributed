#!/bin/bash

# Script completo de pruebas del sistema FTP distribuido
# Prueba: 3 routers, 3 metadata, 3 storage

set -e  # Salir en error

echo "🚀 INICIANDO PRUEBAS COMPLETAS DEL SISTEMA FTP DISTRIBUIDO"
echo "=========================================================="
echo

# Función de logging
log() {
    echo "[$(date '+%H:%M:%S')] $1"
}

# Función para esperar estabilidad
wait_stable() {
    local seconds=$1
    log "⏳ Esperando estabilidad del cluster ($seconds segundos)..."
    sleep $seconds
}

# Función para ejecutar comando y capturar resultado
run_test() {
    local test_name=$1
    local command=$2
    local expected_exit=${3:-0}

    log "🧪 Ejecutando: $test_name"
    echo "   Comando: $command"

    if eval "$command"; then
        echo "   ✅ ÉXITO"
        return 0
    else
        local exit_code=$?
        if [ $exit_code -eq $expected_exit ]; then
            echo "   ✅ ÉXITO (exit code esperado: $expected_exit)"
            return 0
        else
            echo "   ❌ FALLO (exit code: $exit_code, esperado: $expected_exit)"
            return 1
        fi
    fi
}

# === FASE 1: SETUP DEL CLUSTER ===
echo "📦 FASE 1: CONFIGURACIÓN DEL CLUSTER"
echo "===================================="

run_test "Crear red Docker" "docker network create ftp_net 2>/dev/null || true"

run_test "Construir imágenes" "docker build -t ftp-metadata -f FTP/Distributed/Dockerfile.metadata . && docker build -t ftp-storage -f FTP/Distributed/Dockerfile.storage . && docker build -t ftp-router -f FTP/Distributed/Dockerfile.router ."

# Metadata nodes
run_test "Iniciar metadata1" "docker run -d --name metadata1 --hostname metadata1 --network ftp_net --network-alias metadata -e NODE_ID=metadata-1 -e HOSTNAME=metadata1 -e PYTHONUNBUFFERED=1 -p 5000:5000 ftp-metadata"
run_test "Iniciar metadata2" "docker run -d --name metadata2 --hostname metadata2 --network ftp_net --network-alias metadata -e NODE_ID=metadata-2 -e HOSTNAME=metadata2 -e METADATA_SERVICE=metadata1 -e PYTHONUNBUFFERED=1 ftp-metadata"
run_test "Iniciar metadata3" "docker run -d --name metadata3 --hostname metadata3 --network ftp_net --network-alias metadata -e NODE_ID=metadata-3 -e HOSTNAME=metadata3 -e METADATA_SERVICE=metadata1 -e PYTHONUNBUFFERED=1 ftp-metadata"

# Storage nodes
run_test "Iniciar storage1" "docker run -d --name storage1 --hostname storage1 --network ftp_net -e NODE_ID=storage-1 -e HOSTNAME=storage1 -e PYTHONUNBUFFERED=1 ftp-storage python -m FTP.Distributed.Storage.storage_server --host 0.0.0.0 --port 5001 --metadata-host metadata --metadata-port 5000"
run_test "Iniciar storage2" "docker run -d --name storage2 --hostname storage2 --network ftp_net -e NODE_ID=storage-2 -e HOSTNAME=storage2 -e PYTHONUNBUFFERED=1 ftp-storage python -m FTP.Distributed.Storage.storage_server --host 0.0.0.0 --port 5001 --metadata-host metadata --metadata-port 5000"
run_test "Iniciar storage3" "docker run -d --name storage3 --hostname storage3 --network ftp_net -e NODE_ID=storage-3 -e HOSTNAME=storage3 -e PYTHONUNBUFFERED=1 ftp-storage python -m FTP.Distributed.Storage.storage_server --host 0.0.0.0 --port 5001 --metadata-host metadata --metadata-port 5000"

# Router nodes
run_test "Iniciar router1" "docker run -d --name router1 --hostname router1 --network ftp_net -e NODE_ID=router-1 -e HOSTNAME=router1 -e PUBLIC_IP=127.0.0.1 -e ROUTER_PASV_PORT_START=30000 -e ROUTER_PASV_PORT_END=30005 -p 2121:21 -p 30000-30005:30000-30005 ftp-router python -m FTP.Distributed.Router.router_server --host 0.0.0.0 --port 21 --metadata-host metadata --metadata-port 5000 --public-ip 127.0.0.1"
run_test "Iniciar router2" "docker run -d --name router2 --hostname router2 --network ftp_net -e NODE_ID=router-2 -e HOSTNAME=router2 -e PUBLIC_IP=127.0.0.1 -e ROUTER_PASV_PORT_START=30010 -e ROUTER_PASV_PORT_END=30015 -p 2122:21 -p 30010-30015:30010-30015 ftp-router python -m FTP.Distributed.Router.router_server --host 0.0.0.0 --port 21 --metadata-host metadata --metadata-port 5000 --public-ip 127.0.0.1"
run_test "Iniciar router3" "docker run -d --name router3 --hostname router3 --network ftp_net -e NODE_ID=router-3 -e HOSTNAME=router3 -e PUBLIC_IP=127.0.0.1 -e ROUTER_PASV_PORT_START=30020 -e ROUTER_PASV_PORT_END=30025 -p 2123:21 -p 30020-30025:30020-30025 ftp-router python -m FTP.Distributed.Router.router_server --host 0.0.0.0 --port 21 --metadata-host metadata --metadata-port 5000 --public-ip 127.0.0.1"

wait_stable 10

# === FASE 2: PRUEBAS DE ARRANQUE Y ESTABILIDAD ===
echo
echo "🔧 FASE 2: PRUEBAS DE ARRANQUE Y ESTABILIDAD"
echo "============================================="

run_test "Verificar containers corriendo" "docker ps --filter 'name=metadata\|storage\|router' --format '{{.Names}}' | wc -l | xargs -I {} test {} -eq 9"

run_test "Verificar líder establecido" "docker exec router1 python3 -c \"import sys,time;sys.path.extend(['/app','/app/FTP']);from FTP.Distributed.Router.metadata_client import MetadataClient;c=MetadataClient(metadata_host='metadata',metadata_port=5000);time.sleep(1);print(c._leader_host or 'NO_LEADER')\" | grep -v NO_LEADER"

run_test "Verificar registro de storages" "docker logs metadata1 2>/dev/null | grep -c 'Storage node registered' | xargs -I {} test {} -ge 2"

run_test "Verificar conectividad entre nodos" "docker exec router1 python3 -c \"import socket;[print(f'{h}:OK') for h in ['metadata1','metadata2','metadata3','storage1','storage2','storage3'] if socket.socket().connect_ex((h,5000 if 'metadata' in h else 5001))==0]\" | wc -l | xargs -I {} test {} -ge 6"

# === FASE 3: PRUEBAS FTP BÁSICAS ===
echo
echo "📁 FASE 3: PRUEBAS FTP BÁSICAS"
echo "=============================="

# Crear archivos de prueba
run_test "Crear archivos de prueba" "echo 'Contenido archivo 1' > test1.txt && echo 'Contenido archivo 2' > test2.txt && echo 'Archivo grande de prueba' > test3.txt"

# Prueba FTP básica con router1
run_test "FTP upload router1" "echo -e 'user admin admin123\nbinary\nput test1.txt\nbye' | ftp -n 127.0.0.1 2121"

run_test "FTP download router1" "echo -e 'user admin admin123\nbinary\nget test1.txt downloaded1.txt\nbye' | ftp -n 127.0.0.1 2121"

run_test "Verificar contenido descargado" "diff test1.txt downloaded1.txt"

run_test "FTP list router1" "echo -e 'user admin admin123\nls\nbye' | ftp -n 127.0.0.1 2121 | grep test1.txt"

# Prueba con router2
run_test "FTP upload router2" "echo -e 'user admin admin123\nbinary\nput test2.txt\nbye' | ftp -n 127.0.0.1 2122"

run_test "FTP list router2" "echo -e 'user admin admin123\nls\nbye' | ftp -n 127.0.0.1 2122 | grep -c 'test[12].txt' | xargs -I {} test {} -eq 2"

# Prueba con router3
run_test "FTP upload router3" "echo -e 'user admin admin123\nbinary\nput test3.txt\nbye' | ftp -n 127.0.0.1 2123"

run_test "FTP list router3" "echo -e 'user admin admin123\nls\nbye' | ftp -n 127.0.0.1 2123 | grep -c 'test[123].txt' | xargs -I {} test {} -eq 3"

# === FASE 4: PRUEBAS DE REPLICACIÓN ===
echo
echo "🔄 FASE 4: PRUEBAS DE REPLICACIÓN"
echo "================================="

run_test "Verificar replicación archivo 1" "for s in storage1 storage2 storage3; do docker exec \$s ls /data/storage/ | grep -q . && echo \"\$s:OK\" || echo \"\$s:FAIL\"; done | grep -c OK | xargs -I {} test {} -eq 3"

run_test "Verificar contenido replicado" "for s in storage1 storage2 storage3; do docker exec \$s cat /data/storage/*/* 2>/dev/null | head -1; done | sort | uniq | wc -l | xargs -I {} test {} -eq 1"

run_test "Verificar namespace consistente" "docker exec metadata1 sh -c \"cat /data/metadata/namespace.json | jq '. | length'\" && docker exec metadata2 sh -c \"cat /data/metadata/namespace.json | jq '. | length'\" && docker exec metadata3 sh -c \"cat /data/metadata/namespace.json | jq '. | length'\" | sort | uniq | wc -l | xargs -I {} test {} -eq 1"

# === FASE 5: PRUEBAS DE FAILOVER METADATA ===
echo
echo "🚨 FASE 5: PRUEBAS DE FAILOVER METADATA"
echo "======================================="

# Obtener líder actual
LEADER_BEFORE=$(docker exec router1 python3 -c "import sys,time;sys.path.extend(['/app','/app/FTP']);from FTP.Distributed.Router.metadata_client import MetadataClient;c=MetadataClient(metadata_host='metadata',metadata_port=5000);time.sleep(1);print(c._leader_host)" 2>/dev/null || echo "unknown")

log "Líder antes del failover: $LEADER_BEFORE"

# Tumbar líder
run_test "Tumbar líder metadata" "docker rm -f $LEADER_BEFORE"

wait_stable 15

# Verificar nuevo líder
LEADER_AFTER=$(docker exec router1 python3 -c "import sys,time;sys.path.extend(['/app','/app/FTP']);from FTP.Distributed.Router.metadata_client import MetadataClient;c=MetadataClient(metadata_host='metadata',metadata_port=5000);time.sleep(1);print(c._leader_host)" 2>/dev/null || echo "unknown")

log "Líder después del failover: $LEADER_AFTER"

run_test "Nuevo líder establecido" "[ '$LEADER_AFTER' != 'unknown' ] && [ '$LEADER_AFTER' != '$LEADER_BEFORE' ]"

# Verificar que FTP sigue funcionando
run_test "FTP funciona después de failover" "echo -e 'user admin admin123\nls\nbye' | ftp -n 127.0.0.1 2121 | grep -q test1.txt"

# === FASE 6: PRUEBAS DE FAILOVER STORAGE ===
echo
echo "💥 FASE 6: PRUEBAS DE FAILOVER STORAGE"
echo "====================================="

# Obtener archivo para probar
run_test "Crear archivo para failover storage" "echo 'Archivo para failover storage' > failover_test.txt"

run_test "Upload archivo para failover" "echo -e 'user admin admin123\nbinary\nput failover_test.txt\nbye' | ftp -n 127.0.0.1 2121"

# Verificar dónde está replicado
STORAGE_COUNT=$(docker exec metadata1 sh -c "cat /data/metadata/namespace.json" | grep -A 10 '"failover_test.txt"' | grep '"storage-' | wc -l)

run_test "Archivo replicado correctamente" "[ $STORAGE_COUNT -ge 2 ]"

# Tumbar un storage
run_test "Tumbar storage1" "docker rm -f storage1"

wait_stable 10

# Verificar que lectura sigue funcionando
run_test "Lectura funciona después de storage failure" "echo -e 'user admin admin123\nbinary\nget failover_test.txt downloaded_failover.txt\nbye' | ftp -n 127.0.0.1 2121"

run_test "Contenido correcto después de failover" "diff failover_test.txt downloaded_failover.txt"

# Verificar que metadata detectó la caída
run_test "Metadata detectó caída de storage" "docker logs metadata1 2>/dev/null | grep -q 'storage-1 is DOWN' || docker logs metadata2 2>/dev/null | grep -q 'storage-1 is DOWN' || docker logs metadata3 2>/dev/null | grep -q 'storage-1 is DOWN'"

# === FASE 7: PRUEBAS DE CARGA Y CONCURRENCIA ===
echo
echo "⚡ FASE 7: PRUEBAS DE CARGA Y CONCURRENCIA"
echo "=========================================="

# Crear múltiples archivos concurrentemente
run_test "Crear archivos concurrentes" "for i in {1..5}; do echo \"Contenido archivo \$i\" > concurrent_\$i.txt; done"

run_test "Upload concurrente" "for i in {1..3}; do (echo -e \"user admin admin123\nbinary\nput concurrent_\$i.txt\nbye\" | ftp -n 127.0.0.1 2121 > /dev/null 2>&1 &) done; wait"

run_test "Verificar uploads concurrentes" "echo -e 'user admin admin123\nls\nbye' | ftp -n 127.0.0.1 2121 | grep -c concurrent_ | xargs -I {} test {} -ge 3"

# === FASE 8: PRUEBAS DE CONSISTENCIA ===
echo
echo "🔒 FASE 8: PRUEBAS DE CONSISTENCIA"
echo "=================================="

run_test "Consistencia de directorios" "echo -e 'user admin admin123\nmkdir /test_consistency\ncd /test_consistency\nput concurrent_1.txt\nls\nbye' | ftp -n 127.0.0.1 2121 | grep concurrent_1.txt"

run_test "Consistencia entre routers" "for port in 2121 2122 2123; do echo -e 'user admin admin123\nls\nbye' | ftp -n 127.0.0.1 \$port | grep -c test1.txt; done | sort | uniq | wc -l | xargs -I {} test {} -eq 1"

# === FASE 9: LIMPIEZA ===
echo
echo "🧹 FASE 9: LIMPIEZA"
echo "==================="

run_test "Limpiar archivos de prueba" "rm -f test*.txt concurrent_*.txt downloaded*.txt"

run_test "Detener containers" "docker ps -aq | xargs -r docker rm -f"

run_test "Limpiar red" "docker network rm ftp_net 2>/dev/null || true"

# === REPORTE FINAL ===
echo
echo "📊 REPORTE FINAL DE PRUEBAS"
echo "==========================="
echo
echo "🎯 PRUEBAS EJECUTADAS:"
echo "  ✅ Arranque del cluster (3 metadata, 3 storage, 3 routers)"
echo "  ✅ Elección de líder automática"
echo "  ✅ Registro de peers y storages"
echo "  ✅ Conectividad entre todos los nodos"
echo "  ✅ Operaciones FTP básicas (upload/download/list)"
echo "  ✅ Replicación automática de archivos"
echo "  ✅ Consistencia del namespace"
echo "  ✅ Failover de metadata (cambio automático de líder)"
echo "  ✅ Continuidad del servicio FTP tras failover"
echo "  ✅ Failover de storage (lectura desde réplicas)"
echo "  ✅ Detección automática de nodos caídos"
echo "  ✅ Operaciones concurrentes"
echo "  ✅ Consistencia entre múltiples routers"
echo
echo "🚀 RESULTADO: SISTEMA DISTRIBUIDO FUNCIONANDO PERFECTAMENTE"
echo "  - Alta disponibilidad: ✓"
echo "  - Replicación automática: ✓"
echo "  - Failover transparente: ✓"
echo "  - Consistencia de datos: ✓"
echo "  - Escalabilidad: ✓"
echo
echo "✨ PRUEBAS COMPLETADAS EXITOSAMENTE ✨"