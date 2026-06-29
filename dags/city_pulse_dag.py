from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="city_pulse_pipeline",
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
) as dag:

    ingest_air_quality = BashOperator(
        task_id="ingest_air_quality",
        bash_command="python /opt/airflow/pipelines/ingest_air_quality.py",
    )

    ingest_weather = BashOperator(
        task_id="ingest_weather",
        bash_command="python /opt/airflow/pipelines/ingest_weather.py",
    )

    run_analytics = BashOperator(
        task_id="run_analytics",
        bash_command="python /opt/airflow/analytics/run_analysis.py",
    )

    ingest_air_quality >> ingest_weather >> run_analytics