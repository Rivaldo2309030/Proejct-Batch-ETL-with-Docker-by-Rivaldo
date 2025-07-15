import logging
from datetime import datetime
from utils.mongo_utils import get_mongo_client

def transform_covid_data(ti):
    client = get_mongo_client()
    db = client["covid_db"]
    raw_collection = db["raw_covid"]

    docs = list(raw_collection.find())
    if not docs:
        logging.error("No hay datos raw para transformar en covid_db.raw_covid")
        return []

    transformed_list = []
    for data in docs:
        if not isinstance(data, dict):
            logging.warning("Registro covid no es dict, se omite.")
            continue
        if not all(key in data for key in ["country", "cases", "deaths", "recovered", "updated"]):
            logging.warning(f"Registro covid incompleto omitido: {data.get('country', 'sin país')}")
            continue

        cases = data.get("cases", 0)
        deaths = data.get("deaths", 0)
        recovered = data.get("recovered", 0)

        try:
            updated_at = datetime.utcfromtimestamp(data["updated"] / 1000).isoformat()
        except Exception as e:
            logging.warning(f"Error al parsear fecha para {data.get('country', 'sin país')}: {e}")
            updated_at = None

        transformed = {
            "country": data["country"],
            "cases": cases,
            "deaths": deaths,
            "recovered": recovered,
            "active": data.get("active", 0),
            "critical": data.get("critical", 0),
            "cases_per_million": data.get("casesPerOneMillion"),
            "deaths_per_million": data.get("deathsPerOneMillion"),
            "fatality_rate": round(deaths / cases, 4) if cases else None,
            "recovery_rate": round(recovered / cases, 4) if cases else None,
            "updated_at": updated_at
        }
        transformed_list.append(transformed)

    ti.xcom_push(key="transformed_covid_data", value=transformed_list)
    logging.info(f"Datos COVID transformados enviados correctamente. Total registros: {len(transformed_list)}")
    return transformed_list

