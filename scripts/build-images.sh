#!/bin/bash
# ============================================================================
# Script para construir las imágenes Docker del sistema distribuido
# ============================================================================

set -e

echo "============================================"
echo "Construyendo imágenes Docker del sistema FTP Distribuido"
echo "============================================"

# Directorio del proyecto
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

echo ""
echo "[1/3] Construyendo imagen de Metadata..."
docker build -t ftp-metadata:latest -f FTP/Distributed/Dockerfile.metadata .

echo ""
echo "[2/3] Construyendo imagen de Storage..."
docker build -t ftp-storage:latest -f FTP/Distributed/Dockerfile.storage .

echo ""
echo "[3/3] Construyendo imagen de Router..."
docker build -t ftp-router:latest -f FTP/Distributed/Dockerfile.router .

echo ""
echo "============================================"
echo "Imágenes construidas exitosamente:"
echo "  - ftp-metadata:latest"
echo "  - ftp-storage:latest"
echo "  - ftp-router:latest"
echo "============================================"

