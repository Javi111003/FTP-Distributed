# ============================================
# CONFIGURACIÓN DEL NODO MANAGER
# ============================================

# 1. OBTENER IP DEL HOST
# (Esta IP será usada para anunciar el nodo en el cluster)
hostname -I

# 2. INICIAR SWARM
docker swarm init --advertise-addr <IP_DEL_HOST>

# (Guardar el comando "join" que se imprime, lo usarás en el worker)

# 3. CREAR RED OVERLAY
docker network create --driver overlay --attachable ftp_net

# Verificar subred asignada a la red overlay
docker network inspect ftp_net | grep Subnet

# 4. CREAR VOLUMEN COMPARTIDO
docker volume create ftp_volume

# 5. CONSTRUIR IMAGEN DEL SERVIDOR
docker build -t dftp-server:latest .

# 6. LEVANTAR CONTENEDOR DEL SERVIDOR FTP
docker run -d --rm \
  --name ftp_server \
  --network ftp_net \
  --mount type=volume,source=ftp_volume,destination=/tmp/ftp_root \
  -p 21:21 \
  -p 20:20 \
  -p 50000-50010:50000-50010 \
  -e PUBLIC_IP=<IP_DEL_HOST> \
  -e PRIVATE_SUBNET=$(docker network inspect ftp_net -f '{{range .IPAM.Config}}{{.Subnet}}{{end}}') \
  dftp-server:latest

# 7. VERIFICAR
docker ps
docker network inspect ftp_net | grep ftp_server
docker logs ftp_server

# 8. LIMPIEZA
docker stop ftp_server
docker volume rm ftp_volume
docker network rm ftp_net
docker swarm leave --force
