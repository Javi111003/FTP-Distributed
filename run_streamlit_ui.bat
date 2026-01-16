@echo off
REM Script para ejecutar la interfaz Streamlit del sistema FTP Distribuido en Windows

echo 🚀 Iniciando interfaz FTP Distribuido...
echo.

REM Verificar que streamlit está instalado
pip show streamlit >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Streamlit no está instalado. Instalando...
    pip install streamlit
)

echo 🌐 La aplicación se abrirá en tu navegador automáticamente.
echo    Si no se abre, visita: http://localhost:8501
echo.
echo ⚠️  Asegúrate de que el sistema distribuido esté ejecutándose:
echo    docker-compose up -d
echo.

REM Establecer PYTHONPATH y ejecutar
set PYTHONPATH=%~dp0;%PYTHONPATH%
streamlit run FTP/Client/ui/streamlit_app.py ^
    --server.headless=false ^
    --browser.gatherUsageStats=false ^
    --theme.primaryColor="#1E88E5" ^
    --theme.backgroundColor="#FFFFFF" ^
    --theme.secondaryBackgroundColor="#F0F2F6"
