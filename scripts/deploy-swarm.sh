#!/bin/bash
# ============================================================================
# Script para desplegar el sistema en Docker Swarm
# ============================================================================

set -e

# Configuración
STACK_NAME="${STACK_NAME:-ftp-cluster}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.distributed.yml}"

echo "============================================"
echo "Desplegando Sistema FTP Distribuido en Docker Swarm"
echo "============================================"
echo "Stack: $STACK_NAME"
echo "Compose: $COMPOSE_FILE"
echo ""

# Verificar que Docker Swarm está activo
if ! docker info | grep -q "Swarm: active"; then
    echo "ERROR: Docker Swarm no está activo."
    echo "Ejecuta 'docker swarm init' primero."
    exit 1
fi

# Directorio del proyecto
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

# Construir imágenes si no existen
echo "[1/3] Verificando imágenes..."
if ! docker image inspect ftp-metadata:latest > /dev/null 2>&1; then
    echo "Construyendo imágenes..."
    ./scripts/build-images.sh
fi

# Desplegar el stack
echo ""
echo "[2/3] Desplegando stack..."
docker stack deploy -c "$COMPOSE_FILE" "$STACK_NAME"

# Esperar a que los servicios estén listos
echo ""
echo "[3/3] Esperando a que los servicios inicien..."
sleep 10

# Mostrar estado
echo ""
echo "============================================"
echo "Estado del despliegue:"
echo "============================================"
docker stack services "$STACK_NAME"

echo ""
echo "============================================"
echo "Despliegue completado!"
echo ""
echo "Para verificar el estado:"
echo "  docker stack services $STACK_NAME"
echo "  docker stack ps $STACK_NAME"
echo ""
echo "Para ver logs:"
echo "  docker service logs ${STACK_NAME}_router"
echo "  docker service logs ${STACK_NAME}_metadata"
echo "  docker service logs ${STACK_NAME}_storage"
echo ""
echo "Para conectar por FTP:"
echo "  ftp <IP_DEL_NODO> 21"
echo "  Usuario: admin / Contraseña: admin123"
echo "============================================"

