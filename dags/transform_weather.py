import logging
from datetime import datetime

# Transform raw weather data into a structured format
def transform_weather_data(ti):
    data = ti.xcom_pull(key="raw_weather_data", task_ids="extract_weather_task")
    if not data or not isinstance(data, dict) or "hourly" not in data:
        error = {"error": "Invalid or missing data"}
        logging.error(error["error"])
        return error

    hourly = data["hourly"]
    transformed_records = []
    for i, time_str in enumerate(hourly.get("time", [])):
        record = {
            "time": time_str,
            "temperature_c": hourly["temperature_2m"][i],
            "rain_mm": hourly["rain"][i],
            "weather_code": hourly["weather_code"][i],
            "cloud_cover": hourly["cloud_cover"][i],
            "humidity_percent": hourly["relative_humidity_2m"][i],
        }
        # Mark as extreme if heavy rain or low temperature
        record["is_extreme_weather"] = (record["rain_mm"] > 10 or record["temperature_c"] < 10)
        transformed_records.append(record)

    transformed = {
        "summary_time": datetime.utcnow().isoformat(),
        "records": transformed_records,
        "record_count": len(transformed_records)
    }

    ti.xcom_push(key="transformed_weather_data", value=transformed)
    logging.info("Weather data transformed and pushed successfully.")
    return transformed

# Generate a summary from transformed weather data
def summarize_weather_data(ti):
    transformed_data = ti.xcom_pull(key="transformed_weather_data", task_ids="transform_weather_task")
    if not transformed_data:
        logging.warning("No transformed weather data found to summarize")
        return {}

    # Basic summary: count records and calculate average temperature
    records = transformed_data.get("records", [])
    if not records:
        logging.warning("No records found for summary")
        return {}

    avg_temp = sum(r["temperature_c"] for r in records) / len(records)
    total_rain = sum(r["rain_mm"] for r in records)

    summary = {
        "record_count": len(records),
        "average_temperature_c": round(avg_temp, 2),
        "total_rain_mm": round(total_rain, 2),
    }

    logging.info(f"Weather summary generated: {summary}")
    ti.xcom_push(key="weather_summary", value=summary)
    return summary

# Report the summarized weather data
def report_weather_summary(ti):
    summary = ti.xcom_pull(key="weather_summary", task_ids="summarize_weather_task")
    if summary:
        logging.info(f"Weather Summary Report: {summary}")
    else:
        logging.warning("No weather summary found to report")
