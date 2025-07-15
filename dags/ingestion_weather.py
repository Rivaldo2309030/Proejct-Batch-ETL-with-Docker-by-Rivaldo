import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.mongo_utils import get_mongo_client

def extract_weather_data(ti):
    client = get_mongo_client()
    db = client["weather_db"]
    raw_collection = db["raw_weather"]

    url = (
        "https://api.open-meteo.com/v1/forecast?"
        "latitude=20.9754&longitude=-89.617&hourly=temperature_2m,rain,weather_code,"
        "cloud_cover,relative_humidity_2m&timezone=America/Mexico_City"
    )
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            raw_collection.update_one(
                {"generationtime_ms": data.get("generationtime_ms")},
                {"$set": data},
                upsert=True
            )
            ti.xcom_push(key="raw_weather_data", value=data)
            logging.info("Datos raw insertados o actualizados en raw_weather")
            return data
        else:
            error_data = {"error": f"API call failed with status {response.status_code}"}
            ti.xcom_push(key="raw_weather_data", value=error_data)
            logging.error(error_data["error"])
    except Exception as e:
        error_data = {"error": str(e)}
        ti.xcom_push(key="raw_weather_data", value=error_data)
        logging.error(f"Error en extracción Weather: {e}")

default_args = {
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': 300,
}

with DAG(
    'ingestion_weather',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_weather_task = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather_data
    )
