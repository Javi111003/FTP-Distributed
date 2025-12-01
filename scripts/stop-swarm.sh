#!/bin/bash
# ============================================================================
# Script para detener el sistema en Docker Swarm
# ============================================================================

STACK_NAME="${STACK_NAME:-ftp-cluster}"

echo "============================================"
echo "Deteniendo Sistema FTP Distribuido"
echo "============================================"
echo "Stack: $STACK_NAME"
echo ""

# Eliminar el stack
docker stack rm "$STACK_NAME"

echo ""
echo "Stack eliminado. Los volúmenes persisten."
echo "Para eliminar volúmenes: docker volume prune"

