# 📚 ÍNDICE DE ARCHIVOS DE COMANDOS - FTP DISTRIBUTED SYSTEM

## 📋 **ARCHIVOS CREADOS**

### 🚀 **Scripts Ejecutables**
- **`start_system.sh`** - Levanta todo el sistema distribuido
- **`test_ftp.sh`** - Pruebas completas de FTP (upload/download)
- **`commands_reference.sh`** - Todos los comandos detallados
- **`comandos_directos.sh`** - Comandos seguros para terminal (sin errores de sintaxis)

### 📖 **Documentación**
- **`README_COMANDOS.md`** - Guía completa de comandos
- **`INDICE_COMANDOS.md`** - Este archivo (índice)

### 📁 **Archivos Existentes**
- **`test_distributed_system.sh`** - Script de pruebas completo anterior
- **`DISTRIBUTED_README.md`** - Documentación detallada del sistema

## 🎯 **USO RÁPIDO**

```bash
# Levantar sistema completo
./start_system.sh

# Probar FTP
./test_ftp.sh

# Ver todos los comandos
./commands_reference.sh
```

## 📊 **CONTENIDO DE CADA ARCHIVO**

### `start_system.sh`
✅ Levanta metadata (3 nodos con DNS discovery)  
✅ Levanta storage (2 nodos con conexión DNS)  
✅ Levanta router (con conexión DNS)  
✅ Verifica estado del sistema  

### `test_ftp.sh`
✅ FTP upload completo  
✅ Verificación física en storages  
✅ FTP download  
✅ Verificación de contenido  
✅ Test de DNS resolution  
✅ Verificación de líder actual  

### `commands_reference.sh`
✅ Todos los comandos individuales  
✅ Scripts completos para copiar/pegar  
✅ Instrucciones detalladas  
✅ Comandos de monitoreo/logs  
✅ Comandos de limpieza  

### `README_COMANDOS.md`
✅ Guía concisa de comandos  
✅ Checklist funcional  
✅ Notas importantes  
✅ Comandos de troubleshooting  

## 🔍 **COMANDOS MÁS USADOS**

### Levantar Sistema
```bash
./start_system.sh
```

### Probar FTP
```bash
./test_ftp.sh
```

### Verificar Estado
```bash
docker ps
docker logs metadata1 | tail -10
docker exec storage1 getent hosts metadata
```

### FTP Manual
```bash
echo -e "user admin admin123\nbinary\nput archivo.txt\nquit" | ftp -n 127.0.0.1 2121
echo -e "user admin admin123\nbinary\nget archivo.txt\nquit" | ftp -n 127.0.0.1 2121
```

### Limpiar Todo
```bash
docker stop metadata1 metadata2 metadata3 storage1 storage2 router1
docker rm metadata1 metadata2 metadata3 storage1 storage2 router1
```

## ✅ **SISTEMA FUNCIONAL**

- **DNS Resolution**: ✅ Funciona sin variables de entorno
- **Metadata Discovery**: ✅ Peer-to-peer via DNS
- **Storage Registration**: ✅ Automático via DNS
- **FTP Operations**: ✅ Upload/download completo
- **Failover**: ✅ Rebalanceo automático
- **Replicación**: ✅ Múltiples copias

## 📍 **UBICACIÓN**
Todos los archivos están en: `/home/kendry/Downloads/FTP-Distributed/`

---
**Creado**: $(date)  
**Estado**: ✅ **Sistema 100% funcional** 🚀