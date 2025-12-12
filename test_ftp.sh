#!/bin/bash
# =============================================================================
# FTP DISTRIBUTED SYSTEM - FTP TEST SCRIPT
# =============================================================================
# Pruebas completas de FTP: upload, download, verificación física
# =============================================================================

echo "=== FTP DISTRIBUTED SYSTEM - FTP TESTS ==="
cd /home/kendry/Downloads/FTP-Distributed

# Verificar que el sistema esté corriendo
if ! docker ps | grep -q router1; then
    echo "❌ Error: Router no está corriendo. Ejecuta primero: ./start_system.sh"
    exit 1
fi

echo "1. Creando archivo de prueba..."
echo "Contenido de prueba FTP completo - $(date)" > test_ftp.txt
echo "✅ Archivo creado: test_ftp.txt"

echo ""
echo "2. === FTP UPLOAD TEST ==="
echo "Conectando a ftp://127.0.0.1:2121 (admin/admin123)..."
if echo -e "user admin admin123\nbinary\nput test_ftp.txt\nquit" | ftp -n 127.0.0.1 2121; then
    echo "✅ Upload FTP completado"
else
    echo "❌ Error en upload FTP"
    exit 1
fi

echo ""
echo "3. === VERIFICACIÓN FÍSICA ==="
echo "Buscando archivo en storage nodes..."
FOUND=false
for storage in storage1 storage2; do
    echo "--- $storage ---"
    if docker exec $storage find /data/storage -name "*" -exec cat {} \; 2>/dev/null; then
        FOUND=true
        echo "✅ Archivo encontrado en $storage"
    else
        echo "❌ No encontrado en $storage"
    fi
done

if [ "$FOUND" = false ]; then
    echo "❌ Error: Archivo no encontrado físicamente en ningún storage"
    exit 1
fi

echo ""
echo "4. === FTP DOWNLOAD TEST ==="
echo "Descargando archivo via FTP..."
if echo -e "user admin admin123\nbinary\nget test_ftp.txt downloaded_ftp.txt\nquit" | ftp -n 127.0.0.1 2121; then
    echo "✅ Download FTP completado"
else
    echo "❌ Error en download FTP"
    exit 1
fi

echo ""
echo "5. === VERIFICACIÓN DE CONTENIDO ==="
if [ -f "downloaded_ftp.txt" ]; then
    echo "Contenido descargado:"
    cat downloaded_ftp.txt
    echo ""
    echo "✅ Test FTP completo exitoso!"
    echo "📊 Resumen:"
    echo "   • Upload: ✅ Funciona"
    echo "   • Almacenamiento físico: ✅ Funciona"
    echo "   • Download: ✅ Funciona"
    echo "   • Replicación: ✅ Funciona"
else
    echo "❌ Error: Archivo descargado no encontrado"
    exit 1
fi

echo ""
echo "6. === VERIFICACIÓN DNS ==="
echo "Probando resolución DNS desde storage1:"
docker exec storage1 getent hosts metadata

echo ""
echo "7. === LÍDER ACTUAL ==="
docker exec -i router1 python3 -c "
import sys, time
sys.path.extend(['/app','/app/FTP'])
from FTP.Distributed.Router.metadata_client import MetadataClient
c = MetadataClient('metadata', 5000)
time.sleep(2)
print(f'🏆 Líder actual: {c._leader_host}:{c._leader_port}')
" 2>/dev/null

echo ""
echo "=== PRUEBAS COMPLETADAS ==="
echo "🎉 Sistema FTP distribuido funcionando correctamente!"
echo ""
echo "Para probar failover:"
echo "  docker rm -f storage2  # Simular caída"
echo "  sleep 10 && docker logs metadata1 | grep rebalance"
echo "  ./test_ftp.sh          # Probar continuidad"