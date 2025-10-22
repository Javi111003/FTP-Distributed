# Usa una imagen base ligera y compatible
FROM python:3.11-slim

# Establece el directorio de trabajo
WORKDIR /app

# Copia solo lo necesario
COPY FTP /app/FTP

# Instala dependencias
RUN pip install --no-cache-dir -r /app/FTP/requirements.txt

# Asegura que los paquetes se reconozcan como módulos
RUN touch /app/FTP/__init__.py && touch /app/FTP/Server/__init__.py

# Expone los puertos necesarios para FTP
EXPOSE 21
EXPOSE 30000-30009

# Ejecuta el servidor como módulo con argumentos dinámicos
CMD ["python", "-u", "-m", "FTP.Server.server", "--public-ip", "10.40.68.195"]
