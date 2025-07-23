import logging
from datetime import datetime, timedelta
from utils.mongo_utils import get_mongo_client

# Function to evitate duplicates
def transform_covid_data(ti):
    client = get_mongo_client()
    db = client["covid_db"]
    raw_collection = db["raw_covid"]
    processed_collection = db["processed_covid"]

    
    start_date = datetime.utcnow().date() - timedelta(days=7)
    docs = list(raw_collection.find({"date": {"$gte": str(start_date)}}))

    if not docs:
        logging.error("No se encontraron datos para transformar en los últimos 7 días.")
        return []

    transformed_list = []

    for data in docs:
        if not isinstance(data, dict):
            logging.warning("Registro no es un dict, se omite.")
            continue

        # Verify necessary camps
        if not all(key in data for key in ["country", "cases", "deaths", "recovered", "updated"]):
            logging.warning(f"Registro COVID incompleto omitido: {data.get('country', 'sin país')}")
            continue

        try:
            # Convert into a iso
            updated_at = datetime.utcfromtimestamp(data["updated"] / 1000).isoformat()
        except Exception as e:
            logging.warning(f"Error al parsear fecha en {data.get('country')}: {e}")
            updated_at = None

        transformed = {
            "country": data["country"],
            "date": data.get("date"),  # string in format YYYY-MM-DD
            "cases": data.get("cases", 0),
            "deaths": data.get("deaths", 0),
            "recovered": data.get("recovered", 0),
            "active": data.get("active", 0),
            "critical": data.get("critical", 0),
            "cases_per_million": data.get("casesPerOneMillion"),
            "deaths_per_million": data.get("deathsPerOneMillion"),
            "fatality_rate": round(data.get("deaths", 0) / data.get("cases", 1), 4),
            "recovery_rate": round(data.get("recovered", 0) / data.get("cases", 1), 4),
            "updated_at": updated_at
        }

        # save in the final list
        transformed_list.append(transformed)

        # save in mongo 
        processed_collection.update_one(
            {"country": transformed["country"], "date": transformed["date"]},
            {"$set": transformed},
            upsert=True
        )

    ti.xcom_push(key="transformed_covid_data", value=transformed_list)
    logging.info(f"Transformación completada. Total registros: {len(transformed_list)}")
    return transformed_list

