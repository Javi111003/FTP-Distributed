# ============================================================================
# Script para desplegar el sistema en Docker Swarm (Windows)
# ============================================================================

$ErrorActionPreference = "Stop"

# Configuración
$StackName = if ($env:STACK_NAME) { $env:STACK_NAME } else { "ftp-cluster" }
$ComposeFile = if ($env:COMPOSE_FILE) { $env:COMPOSE_FILE } else { "docker-compose.distributed.yml" }

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Desplegando Sistema FTP Distribuido en Docker Swarm" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Stack: $StackName"
Write-Host "Compose: $ComposeFile"
Write-Host ""

# Verificar que Docker Swarm está activo
$swarmInfo = docker info 2>&1 | Select-String "Swarm: active"
if (-not $swarmInfo) {
    Write-Host "ERROR: Docker Swarm no está activo." -ForegroundColor Red
    Write-Host "Ejecuta 'docker swarm init' primero." -ForegroundColor Red
    exit 1
}

# Directorio del proyecto
$ProjectDir = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $ProjectDir) { $ProjectDir = (Get-Location).Path }
Set-Location $ProjectDir

# Construir imágenes si no existen
Write-Host "[1/3] Verificando imágenes..." -ForegroundColor Yellow
$metadataImage = docker image inspect ftp-metadata:latest 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Construyendo imágenes..."
    & "$PSScriptRoot\build-images.ps1"
}

# Desplegar el stack
Write-Host ""
Write-Host "[2/3] Desplegando stack..." -ForegroundColor Yellow
docker stack deploy -c $ComposeFile $StackName

# Esperar a que los servicios estén listos
Write-Host ""
Write-Host "[3/3] Esperando a que los servicios inicien..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

# Mostrar estado
Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host "Estado del despliegue:" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
docker stack services $StackName

Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host "Despliegue completado!" -ForegroundColor Green
Write-Host ""
Write-Host "Para verificar el estado:"
Write-Host "  docker stack services $StackName"
Write-Host "  docker stack ps $StackName"
Write-Host ""
Write-Host "Para ver logs:"
Write-Host "  docker service logs ${StackName}_router"
Write-Host "  docker service logs ${StackName}_metadata"
Write-Host "  docker service logs ${StackName}_storage"
Write-Host ""
Write-Host "Para conectar por FTP:"
Write-Host "  ftp <IP_DEL_NODO> 21"
Write-Host "  Usuario: admin / Contraseña: admin123"
Write-Host "============================================" -ForegroundColor Green

