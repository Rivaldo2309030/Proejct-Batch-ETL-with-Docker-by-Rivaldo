import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.mongo_utils import get_mongo_client

# List countries 
COUNTRIES = ["Mexico", "Brazil", "Argentina", "United States", "France"]

# Función de extracción
def extract_covid_data(ti):
    client = get_mongo_client()
    db = client["covid_db"]
    raw_collection = db["raw_covid"]

    all_data = []

    for country in COUNTRIES:
        url = f"https://disease.sh/v3/covid-19/countries/{country}"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()

                # get date 
                updated_date = datetime.fromtimestamp(data["updated"] / 1000).date()
                data["date"] = str(updated_date)  # guardamos como string

                # Upsert por país y fecha
                raw_collection.update_one(
                    {"country": data["country"], "date": data["date"]},
                    {"$set": data},
                    upsert=True
                )

                logging.info(f"{country}: insertado o actualizado con éxito.")
                all_data.append(data)
            else:
                error_msg = f"{country}: API call failed with status {response.status_code}"
                logging.error(error_msg)
        except Exception as e:
            logging.error(f"{country}: error durante extracción - {e}")

    # save all en XCom to next step
    ti.xcom_push(key="raw_covid_data", value=all_data)
    return all_data

# Default DAG arguments
default_args = {
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': 300,  # 5 minutes
}

# define the DAG
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
