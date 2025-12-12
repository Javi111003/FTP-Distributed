#!/bin/bash

# Script para consultar quién es el líder actual de metadata
# Uso: ./get_leader.sh

echo "Consultando líder actual desde router1..."

# Ejecutar consulta desde router1 contenedor
docker exec -i router1 python3 <<'EOF'
import sys, time
sys.path.extend(['/app','/app/FTP'])
from FTP.Distributed.Router.metadata_client import MetadataClient

# Crear cliente que descubre líder
c = MetadataClient(metadata_host='metadata', metadata_port=5000)
time.sleep(1)  # Esperar discovery

print("✅ Líder actual:")
print(f"   Host: {c._leader_host}")
print(f"   Port: {c._leader_port}")
print(f"   Nodos conocidos: {len(c._known_metadata_nodes)}")
EOF