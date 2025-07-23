from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# --- Import functions per per module ---
from ingestion_covid import extract_covid_data
from transform_covid import transform_covid_data
from load_mongo import load_covid_data, report_covid

from ingestion_countries import extract_countries_data
from transform_countries import transform_countries_data, enrich_countries_data, summarize_countries_data, report_countries_summary
from load_mongo import load_countries_data

from ingestion_weather import extract_weather_data
from transform_weather import transform_weather_data, summarize_weather_data, report_weather_summary
from load_mongo import load_weather_data, save_last_success_time

# --- DAG principal ---
dag = DAG(
    dag_id="main_etl_pipeline",
    description="Unified ETL pipeline for COVID, Countries, and Weather data",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False
)

# --- COVID tasks ---
extract_covid_task = PythonOperator(
    task_id="extract_covid_task",
    python_callable=extract_covid_data,
    dag=dag
)

transform_covid_task = PythonOperator(
    task_id="transform_covid_task",
    python_callable=transform_covid_data,
    dag=dag
)

load_covid_task = PythonOperator(
    task_id="load_covid_task",
    python_callable=load_covid_data,
    dag=dag
)

report_covid_task = PythonOperator(
    task_id="report_covid_task",
    python_callable=report_covid,
    dag=dag
)

# --- Countries tasks ---
extract_countries_task = PythonOperator(task_id="extract_countries_task", python_callable=extract_countries_data, dag=dag)
transform_countries_task = PythonOperator(task_id="transform_countries_task", python_callable=transform_countries_data, dag=dag)
enrich_countries_task = PythonOperator(task_id="enrich_countries_task", python_callable=enrich_countries_data, dag=dag)
load_countries_task = PythonOperator(task_id="load_countries_task", python_callable=load_countries_data, dag=dag)
summarize_countries_task = PythonOperator(task_id="summarize_countries_task", python_callable=summarize_countries_data, dag=dag)
report_countries_task = PythonOperator(task_id="report_countries_task", python_callable=report_countries_summary, dag=dag)

# --- Weather tasks ---
extract_weather_task = PythonOperator(task_id="extract_weather_task", python_callable=extract_weather_data, dag=dag)
transform_weather_task = PythonOperator(task_id="transform_weather_task", python_callable=transform_weather_data, dag=dag)
load_weather_task = PythonOperator(task_id="load_weather_task", python_callable=load_weather_data, dag=dag)
summarize_weather_task = PythonOperator(task_id="summarize_weather_task", python_callable=summarize_weather_data, dag=dag)
report_weather_task = PythonOperator(task_id="report_weather_task", python_callable=report_weather_summary, dag=dag)

# --- Final y timestamp ---
save_last_success_time_task = PythonOperator(task_id="save_last_success_time_task", python_callable=save_last_success_time, dag=dag)
final_task = DummyOperator(task_id="final_report_task", dag=dag)

# --- dependencies---

# COVID
extract_covid_task >> transform_covid_task >> load_covid_task >> report_covid_task

# Countries
extract_countries_task >> transform_countries_task >> enrich_countries_task >> load_countries_task >> summarize_countries_task >> report_countries_task

# Weather
extract_weather_task >> transform_weather_task >> load_weather_task >> summarize_weather_task >> report_weather_task

# branches
[report_covid_task, report_countries_task, report_weather_task] >> save_last_success_time_task >> final_task
