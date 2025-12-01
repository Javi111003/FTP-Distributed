# ============================================================================
# Script para detener el sistema en modo desarrollo (Windows)
# ============================================================================

$ProjectDir = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $ProjectDir) { $ProjectDir = (Get-Location).Path }
Set-Location $ProjectDir

Write-Host "Deteniendo Sistema FTP Distribuido (Desarrollo)..." -ForegroundColor Yellow
docker-compose -f docker-compose.dev.yml down

Write-Host "Sistema detenido." -ForegroundColor Green

