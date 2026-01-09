# Reconciliación de Split-Brain en FTP Distribuido

## Problema

Cuando una red se particiona (split-brain), cada partición puede elegir su propio líder y continuar operando independientemente. Al reconectar la red, el sistema tenía dos problemas:

1. **Múltiples líderes activos**: Cada partición mantenía su propio líder
2. **Datos divergentes**: Las operaciones realizadas en cada partición no se sincronizaban
3. **Conflictos de archivos**: Si se subía el mismo archivo en ambas particiones, uno se perdía

## Solución Implementada

### 1. Detección de Split-Brain

El sistema detecta split-brain en los siguientes escenarios:

- **Múltiples líderes con mismo término**: Ambos nodos piensan que son líderes
- **Líderes diferentes con términos cercanos**: Indica partición reciente
- **Heartbeats de múltiples líderes**: Se reciben heartbeats de diferentes líderes

La detección ocurre cuando:
- Un nodo metadata se registra con otro
- Se reciben heartbeats o anuncios de liderazgo
- Periódicamente durante la verificación de estado del clúster

### 2. Resolución de Líder Canónico

Cuando se detecta split-brain, se inicia un proceso de reconciliación:

1. **Recolección de estado**: Se consulta el estado de todos los peers (término, commit_index, cantidad de archivos)
2. **Determinación del líder canónico**:
   - **Prioridad 1**: Nodo con mayor término
   - **Prioridad 2**: Si hay empate, el que tenga más operaciones (commit_index mayor)
   - **Prioridad 3**: Si aún hay empate, el nodo con menor ID (orden lexicográfico)
3. **Ceder liderazgo**: Los nodos que no son el líder canónico ceden su liderazgo

### 3. Merge de Estados Divergentes

El líder canónico realiza un merge de los estados de todas las particiones:

#### Merge de Namespace (Archivos y Directorios)

Para cada archivo en el namespace de los peers:

- **Archivo no existe localmente**: Se agrega al namespace
- **Archivo existe sin conflicto**: Se usa la versión más reciente (por timestamp)
- **Archivo existe con conflicto**: Se crean versiones múltiples

#### Detección de Conflictos

Un archivo está en conflicto si:
- Tiene el mismo path pero diferente checksum
- Tiene el mismo path pero diferente tamaño y timestamps similares (< 5 minutos)

#### Resolución de Conflictos mediante Versionado

Cuando se detecta un conflicto en `/path/file.txt`:

1. Se crean dos versiones:
   - `/path/file_v1_metadata1.txt` (versión del nodo 1)
   - `/path/file_v2_metadata2.txt` (versión del nodo 2)
2. Se elimina el archivo original conflictivo
3. Ambas versiones quedan disponibles para el usuario

**Ejemplo**:
```
# Antes de la reconciliación (en cada partición)
Partición A: /user1/documento.txt (contenido A, 1024 bytes)
Partición B: /user1/documento.txt (contenido B, 2048 bytes)

# Después de la reconciliación
/user1/documento_v1_metadata1.txt (contenido A, 1024 bytes)
/user1/documento_v2_metadata2.txt (contenido B, 2048 bytes)
```

### 4. Sincronización Final

Después del merge, todos los nodos se sincronizan con el líder canónico:

1. Solicitan un snapshot completo del líder
2. Instalan el snapshot (reemplazando su estado local)
3. Limpian sus oplogs y comienzan a seguir al nuevo líder

### 5. Prevención de Reconciliaciones Frecuentes

Para evitar sobrecarga:
- **Cooldown**: 10 segundos entre reconciliaciones
- **Flag de progreso**: Solo una reconciliación a la vez
- **Verificación antes de iniciar**: No se inicia si el líder está activo

## Archivos Modificados

### Nuevos Archivos

1. **`FTP/Distributed/Metadata/split_brain_reconciliation.py`**
   - Módulo principal de reconciliación
   - Clase `SplitBrainReconciliation` con toda la lógica

### Archivos Modificados

1. **`FTP/Distributed/Metadata/metadata_server.py`**
   - Integración del módulo de reconciliación
   - Detección en `_handle_register_node`
   - Soporte para snapshots en `_handle_repl_snapshot`

2. **`FTP/Distributed/Metadata/leader_election.py`**
   - Detección de split-brain en `handle_leader_announcement`
   - Resolución por ID cuando hay mismo término

## Flujo de Reconciliación

```
1. Red se particiona
   ├─ Partición A: metadata1 es líder (term 5)
   │  └─ Operaciones: crea /file1.txt, /file2.txt
   └─ Partición B: metadata2 es líder (term 5)
      └─ Operaciones: crea /file1.txt (diferente), /file3.txt

2. Red se reconecta
   └─ metadata1 intenta registrarse con metadata2

3. Detección de split-brain
   └─ metadata2 detecta: "Ambos somos líderes con term 5"

4. Inicio de reconciliación
   ├─ Recolectar estado de todos los peers
   ├─ metadata1: term=5, commit=10, files=2
   └─ metadata2: term=5, commit=8, files=2

5. Determinar líder canónico
   └─ metadata1 gana (mayor commit_index: 10 > 8)

6. metadata2 cede liderazgo
   └─ Actualiza: leader = metadata1, is_leader = false

7. metadata1 merge estados
   ├─ /file1.txt → CONFLICTO
   │  ├─ Crea /file1_v1_metadata1.txt
   │  └─ Crea /file1_v2_metadata2.txt
   ├─ /file2.txt → Agrega (no existe en B)
   └─ /file3.txt → Agrega (no existe en A)

8. Sincronización final
   └─ metadata2 solicita y aplica snapshot de metadata1

9. Sistema reconciliado
   └─ Un solo líder, todos los archivos preservados
```

## Logging

El sistema genera logs detallados para diagnosticar split-brain:

```bash
# Detección
🔴 SPLIT-BRAIN DETECTED: Both metadata1 and metadata2 are leaders in term 5

# Inicio de reconciliación
🔄 Starting split-brain reconciliation with 2 peers

# Merge de estados
🔀 Merging states from 2 peers
🔀 FILE CONFLICT detected for /file1.txt. Creating versioned copies.
✅ Created versioned files: /file1_v1_metadata1.txt and /file1_v2_metadata2.txt

# Finalización
✅ Split-brain reconciliation completed successfully
```

## Configuración

### Constantes Ajustables

En `split_brain_reconciliation.py`:

```python
self._reconciliation_cooldown = 10  # segundos entre reconciliaciones
```

En la detección de conflictos:

```python
if time_diff < 300:  # 5 minutos = conflicto
```

## Limitaciones y Consideraciones

### Limitaciones Actuales

1. **Oplog merge simplificado**: El merge de oplogs se hace principalmente via namespace. Un merge completo de oplog requeriría ordenamiento temporal complejo.

2. **Usuarios y locks no se mergean**: Solo se mergean archivos. Los usuarios y locks se toman del líder canónico.

3. **No hay historial de versiones**: Los archivos versionados no tienen historial, son copias estáticas.

### Consideraciones de Diseño

1. **Trade-off disponibilidad vs consistencia**: El sistema prioriza disponibilidad (permite operaciones durante partición) sobre consistencia estricta.

2. **Versionado automático**: Previene pérdida de datos pero requiere intervención manual del usuario para elegir la versión correcta.

3. **Término como detector de split-brain**: Funciona bien para particiones cortas. Particiones largas podrían tener términos muy diferentes.

## Casos de Uso

### Caso 1: Partición de Red Temporal

```
Escenario: Cable de red se desconecta por 5 minutos

1. PC1 tiene metadata1 + storage1 + storage2
2. PC2 tiene metadata2 + storage3 + storage4
3. Se desconecta la red
4. Cada PC elige su líder y continúa operando
5. Se reconecta la red
6. Sistema detecta y reconcilia automáticamente
7. Archivos conflictivos se versionan
```

### Caso 2: Actualización del Mismo Archivo

```
Escenario: Usuario A y Usuario B editan el mismo archivo

PC1 (Usuario A):
- Edita /proyecto/documento.txt
- Sube nueva versión (1000 bytes)

PC2 (Usuario B):
- Edita /proyecto/documento.txt
- Sube nueva versión (1500 bytes)

Al reconectar:
- /proyecto/documento_v1_metadata1.txt (versión de A)
- /proyecto/documento_v2_metadata2.txt (versión de B)
- Usuario puede comparar y elegir versión correcta
```

### Caso 3: Archivos Diferentes en Cada Partición

```
Escenario: Cada usuario trabaja en archivos diferentes

PC1:
- Crea /user1/informe.pdf
- Crea /user1/datos.csv

PC2:
- Crea /user2/presentacion.ppt
- Crea /user2/graficos.png

Al reconectar:
- Todos los archivos se mergean sin conflicto
- Namespace final contiene todos los archivos
```

## Testing Manual

Para probar el sistema de reconciliación:

```bash
# Terminal 1: PC1 - Iniciar metadata1 y storages
docker-compose up metadata1 storage1 storage2

# Terminal 2: PC2 - Iniciar metadata2 y storages
docker-compose up metadata2 storage3 storage4

# Terminal 3: Desconectar red entre PCs
# (simular con firewall o desconexión física)

# Terminal 4: Subir archivos en PC1
# (usar cliente FTP conectado a metadata1)

# Terminal 5: Subir archivos en PC2
# (usar cliente FTP conectado a metadata2)

# Terminal 6: Reconectar red
# Observar logs de reconciliación

# Verificar: Listar archivos debería mostrar
# - Todos los archivos sin conflicto
# - Archivos conflictivos con versiones _v1_ y _v2_
```

## Próximos Pasos (Mejoras Futuras)

1. **Merge de oplogs completo**: Implementar ordenamiento temporal de operaciones
2. **Merge de usuarios**: Sincronizar usuarios creados en ambas particiones
3. **Vector clocks**: Usar vector clocks para mejor detección de causalidad
4. **Compactación de versiones**: Herramienta para mergear versiones manualmente
5. **UI para resolución**: Interfaz gráfica para comparar y elegir versiones
6. **Métricas**: Estadísticas de reconciliaciones (cantidad, duración, conflictos)

## Referencias

- **Split-Brain en Sistemas Distribuidos**: https://en.wikipedia.org/wiki/Split-brain_(computing)
- **Algoritmo Bully**: Usado para elección de líder
- **Estrategias de Merge**: Inspirado en Git y sistemas de control de versiones

