#!/bin/bash
# ============================================================================
# Script para iniciar el sistema en modo desarrollo (sin Swarm)
# ============================================================================

set -e

echo "============================================"
echo "Iniciando Sistema FTP Distribuido (Desarrollo)"
echo "============================================"

# Directorio del proyecto
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

# Crear directorios de datos
mkdir -p data/metadata data/storage1 data/storage2 data/storage3

# Iniciar con docker-compose
docker-compose -f docker-compose.dev.yml up --build -d

echo ""
echo "============================================"
echo "Sistema iniciado en modo desarrollo"
echo ""
echo "Servicios:"
docker-compose -f docker-compose.dev.yml ps

echo ""
echo "Para ver logs:"
echo "  docker-compose -f docker-compose.dev.yml logs -f"
echo ""
echo "Para conectar por FTP:"
echo "  ftp localhost 21"
echo "  Usuario: admin / Contraseña: admin123"
echo "============================================"

