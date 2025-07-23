import logging
from utils.mongo_utils import get_mongo_client

# Load transformed COVID data into MongoDB
def load_covid_data(ti):
    transformed_data = ti.xcom_pull(key="transformed_covid_data", task_ids="transform_covid_task")

    if not transformed_data:
        logging.error("No COVID data loaded: the list is empty or not found in XCom.")
        return

    if isinstance(transformed_data, dict) and transformed_data.get("error"):
        logging.error(f"No COVID data loaded: {transformed_data.get('error')}")
        return

    if not isinstance(transformed_data, list):
        logging.error(f"Unexpected format for transformed data: {type(transformed_data)}")
        return

    client = get_mongo_client()
    db = client["covid_db"]
    processed_collection = db["processed_covid"]

    upserted = 0
    try:
        for record in transformed_data:
            if "country" in record and "date" in record:
                processed_collection.update_one(
                    {"country": record["country"], "date": record["date"]},
                    {"$set": record},
                    upsert=True
                )
                upserted += 1
            else:
                logging.warning(f"Registro omitido por falta de 'country' o 'date': {record}")

        logging.info(f"{upserted} registros transformados cargados/actualizados en processed_covid.")
        return upserted

    except Exception as e:
        logging.error(f"Error al cargar datos COVID en MongoDB: {e}")

# Just a placeholder for COVID reporting logic
def report_covid():
    logging.info("COVID report generated")

# Load transformed country data into MongoDB
def load_countries_data(ti):
    # Retrieve transformed data from XCom
    transformed_data = ti.xcom_pull(key="transformed_countries_data", task_ids="transform_countries_task")

    # Validate the data
    if not transformed_data or not isinstance(transformed_data, list):
        logging.error(f"Invalid or empty transformed data: {type(transformed_data)} - {transformed_data}")
        return

    try:
        client = get_mongo_client()
        db = client["countries_db"]
        processed_collection = db["processed_countries"]

        # Clear previous data
        delete_result = processed_collection.delete_many({})
        logging.info(f"Deleted {delete_result.deleted_count} previous documents in processed_countries")

        # Insert new data
        insert_result = processed_collection.insert_many(transformed_data)
        logging.info(f"{len(insert_result.inserted_ids)} countries loaded into processed_countries.")
    except Exception as e:
        logging.error(f"Error loading countries data into MongoDB: {e}")

# Load transformed weather data into MongoDB
def load_weather_data(ti):
    data = ti.xcom_pull(key="transformed_weather_data", task_ids="transform_weather_task")
    if data and isinstance(data, dict) and not data.get("error"):
        client = get_mongo_client()
        db = client["weather_db"]
        processed_collection = db["processed_weather"]

        # Use upsert to update existing entry or insert if not present
        processed_collection.update_one(
            {"summary_time": data["summary_time"]},
            {"$set": data},
            upsert=True
        )
        logging.info("Weather data inserted or updated in processed_weather")
    else:
        logging.error(f"Weather data not loaded, error or empty: {data}")

# Save the timestamp of the last successful ETL run
def save_last_success_time(ti):
    import logging
    from datetime import datetime
    client = get_mongo_client()
    db = client["airflow_metadata"]
    collection = db["etl_runs"]
    now = datetime.utcnow().isoformat()
    collection.insert_one({"dag": "main_etl_pipeline", "timestamp": now})
    logging.info(f"Saved success timestamp: {now}")
