# ============================================================================
# Script para construir las imágenes Docker del sistema distribuido (Windows)
# ============================================================================

$ErrorActionPreference = "Stop"

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Construyendo imágenes Docker del sistema FTP Distribuido" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# Directorio del proyecto
$ProjectDir = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $ProjectDir) { $ProjectDir = (Get-Location).Path }
Set-Location $ProjectDir

Write-Host ""
Write-Host "[1/3] Construyendo imagen de Metadata..." -ForegroundColor Yellow
docker build -t ftp-metadata:latest -f FTP/Distributed/Dockerfile.metadata .

Write-Host ""
Write-Host "[2/3] Construyendo imagen de Storage..." -ForegroundColor Yellow
docker build -t ftp-storage:latest -f FTP/Distributed/Dockerfile.storage .

Write-Host ""
Write-Host "[3/3] Construyendo imagen de Router..." -ForegroundColor Yellow
docker build -t ftp-router:latest -f FTP/Distributed/Dockerfile.router .

Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host "Imágenes construidas exitosamente:" -ForegroundColor Green
Write-Host "  - ftp-metadata:latest" -ForegroundColor Green
Write-Host "  - ftp-storage:latest" -ForegroundColor Green
Write-Host "  - ftp-router:latest" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green

