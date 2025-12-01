#!/bin/bash
# ============================================================================
# Script para detener el sistema en modo desarrollo
# ============================================================================

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

echo "Deteniendo Sistema FTP Distribuido (Desarrollo)..."
docker-compose -f docker-compose.dev.yml down

echo "Sistema detenido."

