import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.mongo_utils import get_mongo_client

def extract_countries_data(ti):
    client = get_mongo_client()
    db = client["countries_db"]
    raw_collection = db["raw_countries"]

    url = "https://restcountries.com/v3.1/all?fields=cca3,name,capital,region,subregion,population,area,languages,currencies"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            logging.info(f"Extracción countries: recibidos {len(data)} registros")
            raw_collection.delete_many({})
            if isinstance(data, list) and data:
                raw_collection.insert_many(data)
                logging.info(f"Insertados {len(data)} registros en raw_countries")
            else:
                logging.warning("Respuesta de countries está vacía o no es lista")

            ti.xcom_push(key="raw_countries_count", value=len(data))
            example = data[0].copy()
            example.pop("_id", None)
            ti.xcom_push(key="example_country", value=example)

            return len(data)
        else:
            logging.error(f"API countries falló con status {response.status_code}")
            ti.xcom_push(key="raw_countries_error", value=f"status {response.status_code}")
    except Exception as e:
        logging.error(f"Error en extracción countries: {e}")
        ti.xcom_push(key="raw_countries_error", value=str(e))

default_args = {
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': 300,
}

with DAG(
    'ingestion_countries',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_countries_data',
        python_callable=extract_countries_data
    )
