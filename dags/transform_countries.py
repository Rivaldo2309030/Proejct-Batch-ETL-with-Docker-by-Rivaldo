import logging
from utils.mongo_utils import get_mongo_client

# Enrich countries data by adding a density category
def enrich_countries_data(ti):
    transformed_data = ti.xcom_pull(key="transformed_countries_data", task_ids="transform_countries_task")
    if not transformed_data:
        logging.error("No transformed data available to enrich.")
        return []

    enriched_data = []
    for country in transformed_data:
        # Simple enrichment: classify based on population density
        density = country.get("density")
        if density is not None:
            if density > 100:
                category = "High density"
            elif density > 50:
                category = "Medium density"
            else:
                category = "Low density"
        else:
            category = "No data"

        country["density_category"] = category
        enriched_data.append(country)

    ti.xcom_push(key="enriched_countries_data", value=enriched_data)
    logging.info(f"Countries enrichment completed: {len(enriched_data)} records.")
    return enriched_data

# Transform raw countries data to a more structured format
def transform_countries_data(ti):
    client = get_mongo_client()
    db = client["countries_db"]
    raw_collection = db["raw_countries"]

    docs = list(raw_collection.find())
    if not docs:
        logging.warning("No data found in raw_countries to transform.")
        return []

    transformed_list = []
    for doc in docs:
        try:
            # Capital validation: use first item if it's a list, otherwise "N/A"
            capital_list = doc.get("capital")
            if isinstance(capital_list, list) and capital_list:
                capital = capital_list[0]
            else:
                capital = "N/A"

            transformed = {
                "country_code": doc.get("cca3"),
                "name": doc.get("name", {}).get("common", "Unknown"),
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
            logging.warning(f"Error transforming country: {doc.get('name', {}).get('common', 'Unknown')} - {e}")

    ti.xcom_push(key="transformed_countries_data", value=transformed_list)
    logging.info(f"{len(transformed_list)} countries transformed successfully.")
    return transformed_list

# Summarize enriched countries data by region
def summarize_countries_data(ti):
    enriched_data = ti.xcom_pull(key="enriched_countries_data", task_ids="enrich_countries_task")
    if not enriched_data:
        logging.error("No enriched data available to summarize.")
        return {}

    # Simple summary: count countries by region
    summary = {}
    for country in enriched_data:
        region = country.get("region", "Unknown")
        summary[region] = summary.get(region, 0) + 1

    ti.xcom_push(key="countries_summary", value=summary)
    logging.info(f"Countries summary completed: {summary}")
    return summary

# Log the summarized countries data
def report_countries_summary(ti):
    summary = ti.xcom_pull(key="countries_summary", task_ids="summarize_countries_task")
    if summary:
        logging.info(f"Countries summary report: {summary}")
    else:
        logging.warning("No countries summary found to report.")
