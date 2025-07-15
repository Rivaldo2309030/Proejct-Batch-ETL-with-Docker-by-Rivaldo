import logging
from utils.mongo_utils import get_mongo_client


def load_covid_data(ti):
    transformed_data = ti.xcom_pull(key="transformed_covid_data", task_ids="transform_covid_task")

    if not transformed_data:
        logging.error("No se cargaron datos COVID: la lista está vacía o no se encontró en XCom.")
        return

    if isinstance(transformed_data, dict) and transformed_data.get("error"):
        logging.error(f"No se cargaron datos COVID: {transformed_data.get('error')}")
        return

    if not isinstance(transformed_data, list):
        logging.error(f"Formato inesperado de datos transformados: {type(transformed_data)}")
        return

    client = get_mongo_client()
    db = client["covid_db"]
    processed_collection = db["processed_covid"]

    try:
        processed_collection.delete_many({})
        processed_collection.insert_many(transformed_data)
        logging.info(f"Se cargaron {len(transformed_data)} registros transformados en processed_covid.")
        return len(transformed_data)
    except Exception as e:
        logging.error(f"Error al cargar datos COVID en MongoDB: {e}")

def report_covid():
    logging.info("Reporte COVID generado")

def load_countries_data(ti):
    # Recuperar datos transformados del XCom
    transformed_data = ti.xcom_pull(key="transformed_countries_data", task_ids="transform_countries_task")

    # Validación
    if not transformed_data or not isinstance(transformed_data, list):
        logging.error(f"Datos transformados inválidos o vacíos: {type(transformed_data)} - {transformed_data}")
        return

    try:
        client = get_mongo_client()
        db = client["countries_db"]
        processed_collection = db["processed_countries"]

        # Limpiar colección previa
        delete_result = processed_collection.delete_many({})
        logging.info(f"Eliminados {delete_result.deleted_count} documentos previos en processed_countries")

        # Insertar nuevos documentos
        insert_result = processed_collection.insert_many(transformed_data)
        logging.info(f"{len(insert_result.inserted_ids)} países cargados en processed_countries.")
    except Exception as e:
        logging.error(f"Error al cargar datos de países en MongoDB: {e}")

def load_weather_data(ti):
    data = ti.xcom_pull(key="transformed_weather_data", task_ids="transform_weather_task")
    if data and isinstance(data, dict) and not data.get("error"):
        client = get_mongo_client()
        db = client["weather_db"]
        processed_collection = db["processed_weather"]

        processed_collection.update_one(
            {"summary_time": data["summary_time"]},
            {"$set": data},
            upsert=True
        )
        logging.info("Datos Weather insertados o actualizados en processed_weather")
    else:
        logging.error(f"No se cargaron datos Weather, error o datos vacíos: {data}")

def save_last_success_time(ti):
    import logging
    from datetime import datetime
    client = get_mongo_client()
    db = client["airflow_metadata"]
    collection = db["etl_runs"]
    now = datetime.utcnow().isoformat()
    collection.insert_one({"dag": "main_etl_pipeline", "timestamp": now})
    logging.info(f"Guardada la hora de éxito: {now}")
