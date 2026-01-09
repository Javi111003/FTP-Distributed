# ============================================
# CONFIGURACIÓN DEL NODO WORKER
# ============================================

# 1. UNIRSE AL SWARM
# (Usar el comando mostrado en el manager)
docker swarm join --token <TOKEN> <IP_MANAGER>:<PORT_MANAGER>

# 2. VERIFICAR CONEXIÓN
docker info | grep Swarm

# 3. CONSTRUIR IMAGEN DEL CLIENTE
docker build -t dftp-client:latest

# 4. CONECTARSE A LA RED OVERLAY
# (La red debe haberse creado en el manager con --attachable)
docker network ls
docker network connect ftp_net $(hostname)

# 5. LEVANTAR CONTENEDOR DEL CLIENTE
docker run -d \
  --name ftp_client \
  --network ftp_net \
  -p 8501:8501 \
  dftp-client:latest

# 6. VERIFICAR
docker ps
docker network inspect ftp_net | grep ftp_client
docker logs ftp_client

# 7. SALIR DEL SWARM (si es necesario)
docker swarm leave
