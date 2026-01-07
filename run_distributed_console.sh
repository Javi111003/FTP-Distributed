#!/bin/bash
# Script para ejecutar la consola personalizada del sistema FTP distribuido

echo "🎯 Ejecutando Consola Personalizada FTP Distribuido"
echo "=================================================="

# Verificar que Python esté disponible
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: Python 3 no está instalado"
    exit 1
fi

# Verificar que existe el archivo de la consola
CONSOLE_FILE="FTP/Client/cli/FTPCLI_distributed.py"
if [ ! -f "$CONSOLE_FILE" ]; then
    echo "❌ Error: No se encuentra el archivo de la consola: $CONSOLE_FILE"
    exit 1
fi

# Verificar dependencias
echo "🔍 Verificando dependencias..."
python3 -c "import rich, cmd, sys; sys.path.insert(0, 'FTP'); from FTP.Client.client import FTPClient" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "❌ Error: Faltan dependencias. Instala con: pip install rich"
    exit 1
fi

echo "✅ Dependencias verificadas"

# Función para verificar si el sistema distribuido está corriendo
check_distributed_system() {
    echo "🌐 Verificando sistema distribuido..."

    # Verificar si hay contenedores corriendo
    RUNNING_CONTAINERS=$(docker ps --filter "name=router\|metadata\|storage" --format "{{.Names}}" | wc -l)

    if [ "$RUNNING_CONTAINERS" -lt 4 ]; then
        echo "⚠️  Advertencia: Sistema distribuido no detectado ($RUNNING_CONTAINERS contenedores)"
        echo "💡 Ejecuta primero: ./test_distributed_custom_cli.sh"
        echo ""
        read -p "¿Deseas continuar de todos modos? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "👋 Saliendo..."
            exit 0
        fi
    else
        echo "✅ Sistema distribuido detectado ($RUNNING_CONTAINERS servicios activos)"
    fi
}

# Verificar sistema distribuido (opcional)
if [ "$1" != "--skip-check" ]; then
    check_distributed_system
fi

echo ""
echo "🚀 Iniciando Consola Personalizada FTP Distribuido"
echo "=================================================="
echo ""
echo "📋 Comandos principales:"
echo "   distributed_connect    Conectar automáticamente"
echo "   connect <host> <port>  Conectar manualmente"
echo "   login <user> <pass>    Iniciar sesión (admin/admin123)"
echo "   help                   Ver todos los comandos"
echo "   quit                   Salir"
echo ""
echo "🌟 Características del sistema distribuido:"
echo "   • Replicación automática de archivos"
echo "   • Alta disponibilidad"
echo "   • Balanceo de carga inteligente"
echo "   • Recuperación automática de fallos"
echo ""

# Configurar PYTHONPATH y ejecutar la consola
cd "$(dirname "$0")"
export PYTHONPATH="$(pwd):$(pwd)/FTP:$PYTHONPATH"
python3 "$CONSOLE_FILE" "$@"

echo ""
echo "👋 ¡Gracias por usar el Sistema FTP Distribuido!"
