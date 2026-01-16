#!/bin/bash
# Script para ejecutar la interfaz Streamlit del Sistema FTP Distribuido
# Asegúrate de haber iniciado el sistema distribuido ANTES de ejecutar este script

set -e

# Colores para output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Obtener directorio del script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo -e "${BLUE}🚀 Iniciando Interfaz Streamlit para FTP Distribuido${NC}"
echo ""

# Verificar que Docker está corriendo
echo -e "${YELLOW}📋 Verificando que el sistema distribuido está iniciado...${NC}"
if ! docker ps | grep -q router1; then
    echo -e "${YELLOW}⚠️  No se encontró el contenedor 'router1'${NC}"
    echo -e "${YELLOW}Por favor, inicia el sistema distribuido primero:${NC}"
    echo -e "${BLUE}   - Sigue los pasos 1-5 del README_COMANDOS_FUNCIONANDO.md${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Sistema distribuido encontrado${NC}"
echo ""

# Cambiar al directorio del proyecto
cd "$SCRIPT_DIR"

# Configurar variables de entorno
export PYTHONPATH="$(pwd):$(pwd)/FTP:$PYTHONPATH"
export PYTHONIOENCODING=utf-8

# Verificar e instalar Streamlit si es necesario
echo -e "${YELLOW}📦 Verificando dependencias...${NC}"
if ! python3 -c "import streamlit" 2>/dev/null; then
    echo -e "${YELLOW}Instalando Streamlit...${NC}"
    pip install streamlit
fi
echo -e "${GREEN}✅ Dependencias verificadas${NC}"
echo ""

# Mostrar información de conexión
echo -e "${BLUE}📊 Información del Sistema:${NC}"
echo -e "  ${GREEN}Router FTP${NC}: 127.0.0.1:2121"
echo -e "  ${GREEN}Usuario${NC}: admin"
echo -e "  ${GREEN}Contraseña${NC}: admin123"
echo ""

# Mostrar información de Streamlit
echo -e "${BLUE}🌐 Interfaz Web Streamlit:${NC}"
echo -e "  ${GREEN}Local URL${NC}: http://localhost:8501"
echo ""

# Ejecutar Streamlit
echo -e "${YELLOW}▶️  Iniciando Streamlit...${NC}"
echo -e "${BLUE}(Presiona Ctrl+C para detener)${NC}"
echo ""

streamlit run FTP/Client/ui/streamlit_app.py
