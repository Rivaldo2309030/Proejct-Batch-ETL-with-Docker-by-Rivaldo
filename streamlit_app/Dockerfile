# Use the official slim Python image
FROM python:3.9-slim

# Environment variables to avoid output buffering and improve logs
ENV PYTHONUNBUFFERED=1

# Set working directory inside the container
WORKDIR /app

# Copy requirements file (make sure you have requirements.txt with pymongo, pandas, streamlit, altair)
COPY requirements.txt /app/

# Update apt-get and install build-essential, then clean up to reduce image size
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

# Install Python dependencies without cache to keep image size small
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app source code
COPY app.py /app/

# Expose the default Streamlit port
EXPOSE 8501

# Command to run Streamlit app, listen on all network interfaces
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
