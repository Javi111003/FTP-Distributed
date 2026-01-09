# Guía de Prueba: Reconciliación de Split-Brain

Esta guía te ayudará a probar el sistema de reconciliación de split-brain en un escenario realista con dos máquinas.

## Escenario de Prueba

- **Máquina 1 (PC1)**: metadata1, storage1, storage2
- **Máquina 2 (PC2)**: metadata2, storage3, storage4
- **Red**: Conectadas por red local, luego se desconecta y reconecta

## Preparación

### En ambas máquinas:

1. **Clonar el repositorio**:
```bash
git clone <repo-url>
cd FTP-Distributed
```

2. **Configurar variables de entorno** (crear `.env`):
```bash
# PC1
NODE_ID=metadata1
METADATA_SERVICE=metadata

# PC2
NODE_ID=metadata2
METADATA_SERVICE=metadata
```

3. **Construir imágenes Docker**:
```bash
./scripts/build-images.sh
```

## Paso 1: Iniciar Clúster Completo

### En PC1:
```bash
# Iniciar metadata1 y sus storages
docker-compose up -d metadata1 storage1 storage2

# Verificar que están corriendo
docker-compose ps

# Ver logs
docker-compose logs -f metadata1
```

### En PC2:
```bash
# Iniciar metadata2 y sus storages
docker-compose up -d metadata2 storage3 storage4

# Verificar que están corriendo
docker-compose ps

# Ver logs
docker-compose logs -f metadata2
```

### Verificación:
Espera 15-20 segundos y verifica los logs. Deberías ver:
- Nodos descubriéndose mutuamente
- Un líder elegido (metadata1 o metadata2)
- Storage nodes registrándose con el líder

```bash
# En cualquier PC, verificar líder
./test_split_brain_detailed.py --summary
```

## Paso 2: Desconectar la Red

Simula una partición de red. Hay varias formas:

### Opción A: Desconexión física
- Desconecta el cable de red de una de las máquinas
- Espera 10-15 segundos

### Opción B: Firewall (en PC2)
```bash
# Bloquear tráfico desde/hacia PC1
sudo iptables -A INPUT -s <IP_PC1> -j DROP
sudo iptables -A OUTPUT -d <IP_PC1> -j DROP
```

### Opción C: Simulación (detener metadata2 temporalmente)
```bash
# En PC2
docker-compose stop metadata2
```

### Verificación:
En los logs deberías ver:
- **PC1**: Mensajes de timeout conectando con PC2
- **PC2**: Si está activo, mensajes de timeout conectando con PC1
- **Ambos**: Eventualmente, cada uno elige su propio líder

```
metadata1: Starting election for term 6
metadata1: Node metadata1 became leader (term 6)
```

```
metadata2: Starting election for term 6
metadata2: Node metadata2 became leader (term 6)
```

## Paso 3: Realizar Operaciones en PC1

Ahora cada partición tiene su propio líder. Realiza operaciones en PC1:

```bash
# Conectar con cliente FTP a metadata1 (router en PC1)
ftp localhost 21

# Login
USER testuser
PASS testpass

# Crear directorios y subir archivos
MKD proyecto
CWD proyecto
STOR documento.txt    # Subir archivo1
STOR datos.csv        # Subir archivo2
QUIT
```

**Contenido de ejemplo para `documento.txt`**:
```
Este es el documento de la partición 1.
Modificado en PC1 durante la desconexión.
Versión: 1.0
```

## Paso 4: Realizar Operaciones en PC2

Realiza operaciones similares en PC2:

```bash
# Conectar con cliente FTP a metadata2 (router en PC2)
ftp localhost 21

# Login
USER testuser
PASS testpass

# Crear directorios y subir archivos
MKD proyecto
CWD proyecto
STOR documento.txt    # Subir archivo CON MISMO NOMBRE pero diferente contenido
STOR imagenes.png     # Subir archivo3 (diferente)
QUIT
```

**Contenido de ejemplo para `documento.txt` en PC2**:
```
Este es el documento de la partición 2.
Modificado en PC2 durante la desconexión.
Versión: 2.0
```

## Paso 5: Reconectar la Red

### Opción A: Reconexión física
- Reconecta el cable de red
- Espera 10 segundos

### Opción B: Firewall (en PC2)
```bash
# Eliminar reglas de bloqueo
sudo iptables -D INPUT -s <IP_PC1> -j DROP
sudo iptables -D OUTPUT -d <IP_PC1> -j DROP
```

### Opción C: Simulación (reiniciar metadata2)
```bash
# En PC2
docker-compose start metadata2
```

## Paso 6: Observar la Reconciliación

### Ver logs en tiempo real:

**En PC1**:
```bash
docker-compose logs -f metadata1 | grep -E "SPLIT-BRAIN|reconciliation|CONFLICT|Canonical"
```

**En PC2**:
```bash
docker-compose logs -f metadata2 | grep -E "SPLIT-BRAIN|reconciliation|CONFLICT|Canonical"
```

### Logs esperados:

Deberías ver algo como:

```
metadata1: 🔴 SPLIT-BRAIN DETECTED: Both metadata1 and metadata2 are leaders in term 6
metadata1: 🔄 Starting split-brain reconciliation with 1 peers
metadata1: Collected state from metadata2: term=6, leader=metadata2, files=3
metadata1: ✅ Canonical leader determined: metadata1
metadata2: ⚠️ Stepping down as leader. New canonical leader: metadata1
metadata1: 🔀 Merging states from 1 peers
metadata1: Merging 3 entries from metadata2
metadata1: 🔀 FILE CONFLICT detected for /proyecto/documento.txt. Creating versioned copies.
metadata1: ✅ Created versioned files: /proyecto/documento_v1_metadata1.txt and /proyecto/documento_v2_metadata2.txt
metadata1: Adding new file /proyecto/imagenes.png from metadata2
metadata1: ✅ Split-brain reconciliation completed successfully
metadata2: Synchronizing with canonical leader metadata1
metadata2: ✅ Synchronized with leader successfully
```

## Paso 7: Verificar Resultados

### Verificar estado del clúster:
```bash
./test_split_brain_detailed.py --summary
```

Deberías ver:
- **Un solo líder** (probablemente metadata1)
- **Archivos mergeados** correctamente

### Listar archivos vía FTP:

```bash
ftp localhost 21
USER testuser
PASS testpass
CWD proyecto
LIST
```

**Resultado esperado**:
```
documento_v1_metadata1.txt    # Versión de PC1
documento_v2_metadata2.txt    # Versión de PC2
datos.csv                     # Solo de PC1
imagenes.png                  # Solo de PC2
```

### Verificar contenido de archivos versionados:

```bash
# Descargar y verificar ambas versiones
RETR documento_v1_metadata1.txt
RETR documento_v2_metadata2.txt
```

Deberías ver que cada archivo mantiene su contenido original de cada partición.

## Paso 8: Limpieza

```bash
# En ambas PCs
docker-compose down -v

# Opcional: limpiar datos
rm -rf data/metadata*/
```

## Verificaciones Adicionales

### 1. Logs completos:
```bash
# Ver todos los logs relacionados con split-brain
docker-compose logs metadata1 metadata2 | grep -A 5 -B 5 "SPLIT-BRAIN"
```

### 2. Estado de réplicas:
```bash
# Verificar que los storages están sincronizados
docker-compose exec storage1 ls -la /data/storage/
docker-compose exec storage3 ls -la /data/storage/
```

### 3. Persistencia:
```bash
# Verificar namespace en disco
cat data/metadata1/namespace.json | jq '.'
cat data/metadata2/namespace.json | jq '.'
```

## Casos de Prueba Adicionales

### Caso 1: Mismo archivo, múltiples particiones
- Sube `informe.pdf` en PC1
- Sube `informe.pdf` (diferente) en PC2
- Verifica: `informe_v1_metadata1.pdf` y `informe_v2_metadata2.pdf`

### Caso 2: Solo nuevos archivos (sin conflictos)
- Sube `archivo_a.txt`, `archivo_b.txt` en PC1
- Sube `archivo_c.txt`, `archivo_d.txt` en PC2
- Verifica: Todos los archivos presentes sin versionado

### Caso 3: Directorios y archivos
- Crea `/dir1/subdir1/file.txt` en PC1
- Crea `/dir2/subdir2/file.txt` en PC2
- Verifica: Ambas estructuras de directorios intactas

### Caso 4: Partición larga
- Desconecta por 10 minutos
- Realiza múltiples operaciones en ambas particiones
- Reconecta y verifica merge completo

## Troubleshooting

### Problema: No se detecta split-brain
**Causa**: Timeout muy corto o reconciliación muy rápida
**Solución**: Ajusta `_reconciliation_cooldown` en `split_brain_reconciliation.py`

### Problema: Archivos se pierden
**Causa**: Merge no está funcionando correctamente
**Solución**: Revisa logs, verifica que `_merge_peer_namespace` se ejecuta

### Problema: Múltiples líderes persisten
**Causa**: Nodos no se comunican correctamente
**Solución**: 
- Verifica conectividad de red: `ping <IP_PC2>` desde PC1
- Verifica puertos abiertos: `telnet <IP_PC2> 5000`
- Revisa configuración de Docker networking

### Problema: Versionado no funciona
**Causa**: Archivos no se detectan como conflicto
**Solución**: Verifica que:
- Tienen el mismo path
- Tienen diferente checksum o tamaño
- Timestamps están dentro de 5 minutos

## Métricas de Éxito

✅ **Reconciliación exitosa si**:
1. Después de reconectar, hay solo UN líder
2. Todos los nodos reportan el mismo líder
3. Archivos sin conflicto aparecen una vez
4. Archivos con conflicto tienen versiones _v1_ y _v2_
5. No hay pérdida de datos
6. Los logs muestran el proceso de reconciliación

## Referencia Rápida de Comandos

```bash
# Verificar líder
./test_split_brain_detailed.py --summary

# Ver logs de split-brain
docker-compose logs metadata1 metadata2 | grep "SPLIT-BRAIN"

# Ver logs de reconciliación
docker-compose logs metadata1 metadata2 | grep "reconciliation"

# Ver archivos en namespace
cat data/metadata1/namespace.json | jq 'keys'

# Listar archivos en storage
docker-compose exec storage1 find /data/storage -type f

# Verificar conectividad entre nodos
docker-compose exec metadata1 ping -c 3 metadata2
```

## Notas Importantes

1. **Tiempo de detección**: La detección de split-brain puede tomar hasta 10-15 segundos después de la reconexión.

2. **Cooldown**: Hay un cooldown de 10 segundos entre reconciliaciones para evitar sobrecarga.

3. **Determinismo**: El líder se elige determinísticamente (mayor commit_index o menor ID).

4. **Persistencia**: Los archivos versionados persisten en disco y en los storage nodes.

5. **Sin pérdida de datos**: El sistema garantiza que ningún dato se pierde durante la reconciliación.

