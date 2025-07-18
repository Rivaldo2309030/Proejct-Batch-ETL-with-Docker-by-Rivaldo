import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.mongo_utils import get_mongo_client

# Function to extract COVID data from the API and store it in MongoDB
def extract_covid_data(ti):
    client = get_mongo_client()
    db = client["covid_db"]
    raw_collection = db["raw_covid"]

    url = "https://disease.sh/v3/covid-19/countries/Mexico"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            # Upsert data based on country and last updated timestamp
            raw_collection.update_one(
                {"country": data["country"], "updated": data["updated"]},
                {"$set": data},
                upsert=True
            )
            ti.xcom_push(key="raw_covid_data", value=data)
            logging.info("Data inserted or updated in raw_covid")
            return data
        else:
            error_msg = f"API call failed with status {response.status_code}"
            error_data = {"error": error_msg}
            ti.xcom_push(key="raw_covid_data", value=error_data)
            logging.error(error_msg)
            return error_data
    except Exception as e:
        error_msg = f"Error during COVID extraction: {e}"
        error_data = {"error": error_msg}
        ti.xcom_push(key="raw_covid_data", value=error_data)
        logging.error(error_msg)
        return error_data

# Default DAG arguments
default_args = {
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': 300,  # in seconds (5 minutes)
}

# Define the DAG
with DAG(
    'ingestion_covid',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_covid_task = PythonOperator(
        task_id='extract_covid_data',
        python_callable=extract_covid_data
    )
