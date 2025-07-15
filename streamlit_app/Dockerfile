# Usa Python slim oficial
FROM python:3.9-slim

# Variables de entorno para evitar buffering y mejorar logs
ENV PYTHONUNBUFFERED=1

# Carpeta de trabajo
WORKDIR /app

# Copia requirements (asegúrate que tengas requirements.txt con pymongo, pandas, streamlit, altair)
COPY requirements.txt /app/

# Instala dependencias
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código de la app
COPY app.py /app/

# Expone puerto default Streamlit
EXPOSE 8501

# Comando para correr Streamlit
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
