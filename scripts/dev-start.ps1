# ============================================================================
# Script para iniciar el sistema en modo desarrollo (Windows)
# ============================================================================

$ErrorActionPreference = "Stop"

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Iniciando Sistema FTP Distribuido (Desarrollo)" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# Directorio del proyecto
$ProjectDir = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $ProjectDir) { $ProjectDir = (Get-Location).Path }
Set-Location $ProjectDir

# Crear directorios de datos
New-Item -ItemType Directory -Force -Path "data/metadata" | Out-Null
New-Item -ItemType Directory -Force -Path "data/storage1" | Out-Null
New-Item -ItemType Directory -Force -Path "data/storage2" | Out-Null
New-Item -ItemType Directory -Force -Path "data/storage3" | Out-Null

# Iniciar con docker-compose
docker-compose -f docker-compose.dev.yml up --build -d

Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host "Sistema iniciado en modo desarrollo" -ForegroundColor Green
Write-Host ""
Write-Host "Servicios:"
docker-compose -f docker-compose.dev.yml ps

Write-Host ""
Write-Host "Para ver logs:"
Write-Host "  docker-compose -f docker-compose.dev.yml logs -f"
Write-Host ""
Write-Host "Para conectar por FTP:"
Write-Host "  ftp localhost 21"
Write-Host "  Usuario: admin / Contraseña: admin123"
Write-Host "============================================" -ForegroundColor Green

