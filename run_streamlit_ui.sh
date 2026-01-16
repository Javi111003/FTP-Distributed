#!/bin/bash
# Script para ejecutar la interfaz Streamlit del sistema FTP Distribuido

# Obtener el directorio del script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR" && pwd)"

# Verificar que streamlit está instalado
if ! command -v streamlit &> /dev/null; then
    echo "❌ Streamlit no está instalado. Instalando..."
    pip install streamlit
fi

echo "🚀 Iniciando interfaz FTP Distribuido..."
echo "📁 Directorio del proyecto: $PROJECT_ROOT"
echo ""
echo "🌐 La aplicación se abrirá en tu navegador automáticamente."
echo "   Si no se abre, visita: http://localhost:8501"
echo ""
echo "⚠️  Asegúrate de que el sistema distribuido esté ejecutándose:"
echo "   docker-compose up -d"
echo ""

# Ejecutar streamlit
cd "$PROJECT_ROOT"
PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH" streamlit run FTP/Client/ui/streamlit_app.py \
    --server.headless=false \
    --browser.gatherUsageStats=false \
    --theme.primaryColor="#1E88E5" \
    --theme.backgroundColor="#FFFFFF" \
    --theme.secondaryBackgroundColor="#F0F2F6"
