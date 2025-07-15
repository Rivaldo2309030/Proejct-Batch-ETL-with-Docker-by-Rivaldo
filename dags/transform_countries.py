import logging
from utils.mongo_utils import get_mongo_client



def enrich_countries_data(ti):
    transformed_data = ti.xcom_pull(key="transformed_countries_data", task_ids="transform_countries_task")
    if not transformed_data:
        logging.error("No hay datos transformados para enriquecer")
        return []

    enriched_data = []
    for country in transformed_data:
        # Ejemplo simple de enriquecimiento: añadir categoría según densidad
        density = country.get("density")
        if density is not None:
            if density > 100:
                category = "Alta densidad"
            elif density > 50:
                category = "Media densidad"
            else:
                category = "Baja densidad"
        else:
            category = "Sin datos"

        country["density_category"] = category
        enriched_data.append(country)

    ti.xcom_push(key="enriched_countries_data", value=enriched_data)
    logging.info(f"Enriquecimiento Countries completado: {len(enriched_data)} registros.")
    return enriched_data

def transform_countries_data(ti):
    client = get_mongo_client()
    db = client["countries_db"]
    raw_collection = db["raw_countries"]

    docs = list(raw_collection.find())
    if not docs:
        logging.warning("No hay datos en raw_countries para transformar.")
        return []

    transformed_list = []
    for doc in docs:
        try:
            # Validar capital: si es lista y tiene elementos, usar el primero, si no "N/A"
            capital_list = doc.get("capital")
            if isinstance(capital_list, list) and capital_list:
                capital = capital_list[0]
            else:
                capital = "N/A"

            transformed = {
                "country_code": doc.get("cca3"),
                "name": doc.get("name", {}).get("common", "Desconocido"),
                "capital": capital,
                "region": doc.get("region"),
                "subregion": doc.get("subregion"),
                "population": doc.get("population"),
                "area": doc.get("area"),
                "languages": list(doc.get("languages", {}).values()),
                "currencies": list(doc.get("currencies", {}).keys()),
            }
            transformed_list.append(transformed)
        except Exception as e:
            logging.warning(f"Error al transformar país: {doc.get('name', {}).get('common', 'Desconocido')} - {e}")

    ti.xcom_push(key="transformed_countries_data", value=transformed_list)
    logging.info(f"{len(transformed_list)} países transformados correctamente.")
    return transformed_list

def summarize_countries_data(ti):
    enriched_data = ti.xcom_pull(key="enriched_countries_data", task_ids="enrich_countries_task")
    if not enriched_data:
        logging.error("No hay datos enriquecidos para resumir")
        return {}

    # Ejemplo simple de resumen: contar países por región
    summary = {}
    for country in enriched_data:
        region = country.get("region", "Unknown")
        summary[region] = summary.get(region, 0) + 1

    ti.xcom_push(key="countries_summary", value=summary)
    logging.info(f"Resumen Countries completado: {summary}")
    return summary

def report_countries_summary(ti):
    summary = ti.xcom_pull(key="countries_summary", task_ids="summarize_countries_task")
    if summary:
        logging.info(f"Reporte resumen de países: {summary}")
    else:
        logging.warning("No se encontró resumen de países para reportar")
