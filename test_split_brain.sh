#!/bin/bash

# Script de prueba para simular y probar reconciliación de split-brain

set -e

echo "=========================================="
echo "Test de Reconciliación de Split-Brain"
echo "=========================================="
echo ""

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_step() {
    echo -e "${BLUE}>>> $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Función para esperar a que un servicio esté disponible
wait_for_service() {
    local host=$1
    local port=$2
    local max_attempts=30
    local attempt=1
    
    print_step "Esperando a que $host:$port esté disponible..."
    
    while [ $attempt -le $max_attempts ]; do
        if nc -z "$host" "$port" 2>/dev/null; then
            print_success "$host:$port está disponible"
            return 0
        fi
        echo -n "."
        sleep 1
        attempt=$((attempt + 1))
    done
    
    print_error "$host:$port no está disponible después de $max_attempts intentos"
    return 1
}

# Función para verificar líder
check_leader() {
    local metadata=$1
    print_step "Verificando líder en $metadata..."
    
    # Aquí deberías implementar una consulta al metadata para ver quién es el líder
    # Por ejemplo, usando el script get_leader.sh si existe
    if [ -f "./get_leader.sh" ]; then
        ./get_leader.sh "$metadata"
    else
        print_warning "Script get_leader.sh no encontrado"
    fi
}

# Paso 1: Limpiar ambiente anterior
print_step "Limpiando ambiente anterior..."
docker-compose down -v 2>/dev/null || true
rm -rf data/metadata*/
print_success "Ambiente limpiado"
echo ""

# Paso 2: Iniciar metadata1 con sus storages
print_step "Iniciando metadata1 y sus storages..."
docker-compose up -d metadata1 storage1 storage2
sleep 5
wait_for_service localhost 5000
print_success "metadata1 iniciado"
echo ""

# Paso 3: Iniciar metadata2 con sus storages
print_step "Iniciando metadata2 y sus storages..."
docker-compose up -d metadata2 storage3 storage4
sleep 5
wait_for_service localhost 5002
print_success "metadata2 iniciado"
echo ""

# Paso 4: Esperar a que se estabilice el clúster
print_step "Esperando estabilización del clúster (15 segundos)..."
sleep 15
print_success "Clúster estabilizado"
echo ""

# Paso 5: Verificar líder inicial
print_step "Verificando líder inicial del clúster..."
check_leader "metadata1"
check_leader "metadata2"
echo ""

# Paso 6: Simular partición de red (detener comunicación entre metadata)
print_step "Simulando partición de red..."
print_warning "Para simular partición real, desconecta la red entre las máquinas"
print_warning "En este test, simularemos deteniendo metadata2 temporalmente"
docker-compose stop metadata2
sleep 5
print_success "Partición simulada (metadata2 detenido)"
echo ""

# Paso 7: Realizar operaciones en partición 1
print_step "Realizando operaciones en partición 1 (metadata1)..."
print_warning "Aquí deberías subir archivos vía FTP a metadata1"
print_warning "Ejemplo: subir archivo1.txt, archivo2.txt"
echo "Esperando 10 segundos para operaciones manuales..."
sleep 10
echo ""

# Paso 8: Reiniciar metadata2 (simular reconexión)
print_step "Reiniciando metadata2 (simulando reconexión de red)..."
docker-compose start metadata2
sleep 5
wait_for_service localhost 5002
print_success "metadata2 reiniciado"
echo ""

# Paso 9: Realizar operaciones en partición 2
print_step "Realizando operaciones en partición 2 (metadata2)..."
print_warning "Aquí deberías subir archivos vía FTP a metadata2"
print_warning "Ejemplo: subir archivo1.txt (diferente), archivo3.txt"
echo "Esperando 10 segundos para operaciones manuales..."
sleep 10
echo ""

# Paso 10: Forzar reconexión completa
print_step "Forzando reconexión completa del clúster..."
docker-compose restart metadata1 metadata2
sleep 10
wait_for_service localhost 5000
wait_for_service localhost 5002
print_success "Clúster reconectado"
echo ""

# Paso 11: Esperar reconciliación
print_step "Esperando reconciliación de split-brain (30 segundos)..."
sleep 30
print_success "Periodo de reconciliación completado"
echo ""

# Paso 12: Verificar líder final
print_step "Verificando líder final después de reconciliación..."
check_leader "metadata1"
check_leader "metadata2"
echo ""

# Paso 13: Verificar logs
print_step "Verificando logs de reconciliación..."
echo ""
echo "=== Logs de metadata1 ==="
docker-compose logs --tail=50 metadata1 | grep -E "SPLIT-BRAIN|reconciliation|CONFLICT" || print_warning "No se encontraron logs de split-brain en metadata1"
echo ""
echo "=== Logs de metadata2 ==="
docker-compose logs --tail=50 metadata2 | grep -E "SPLIT-BRAIN|reconciliation|CONFLICT" || print_warning "No se encontraron logs de split-brain en metadata2"
echo ""

# Paso 14: Resumen
echo "=========================================="
echo "Resumen del Test"
echo "=========================================="
echo ""
print_success "Test completado"
echo ""
print_step "Verificaciones manuales recomendadas:"
echo "1. Revisar logs completos: docker-compose logs metadata1 metadata2"
echo "2. Conectar con cliente FTP y listar archivos"
echo "3. Verificar que archivos conflictivos tengan sufijos _v1_ y _v2_"
echo "4. Confirmar que solo hay un líder en el clúster"
echo ""
print_step "Comandos útiles:"
echo "- Ver logs en tiempo real: docker-compose logs -f metadata1 metadata2"
echo "- Ver estado de contenedores: docker-compose ps"
echo "- Detener todo: docker-compose down"
echo ""

read -p "¿Deseas ver los logs completos ahora? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker-compose logs --tail=100 metadata1 metadata2
fi

