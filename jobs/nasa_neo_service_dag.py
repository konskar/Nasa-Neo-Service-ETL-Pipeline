
# Standard library imports
from datetime import datetime, timedelta

# Third party imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

# Local application imports
from nasa_neo_service_etl_dag.jobs import functions as f
from nasa_neo_service_etl_dag.configs import etl_config as cfg

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 4, 30),
    "email": ["kokargag@gmail.com", "konskar93@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="nasa_neo_service_ingestion_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:

    get_nasa_feed_api_data = PythonOperator(
        task_id="get_nasa_feed_api_data",
        python_callable=f.get_nasa_feed_api_data,
        retries=3
    )

    make_nasa_feed_parquet_file = PythonOperator(
        task_id="make_nasa_feed_parquet_file",
        python_callable=f.make_nasa_feed_parquet_file,
        retries=3
    )

    load_parquet_to_mongodb_stage = PythonOperator(
        task_id="load_parquet_to_mongodb_stage",
        python_callable=f.load_parquet_to_mongodb_stage,
        retries=3
    )

    update_mongodb_production = PythonOperator(
        task_id="update_mongodb_production",
        python_callable=f.update_mongodb_production,
        retries=3
    )
    send_run_success_notification = EmailOperator(
        task_id ='send_run_success_notification',
        to = ["kokargag@gmail.com", "konskar93@gmail.com"],
        subject = 'Airflow Notification: "nasa_neo_service_ingestion_dag"',
        html_content = """ <h3>Dag run sucesfully</h3> """
    )

get_nasa_feed_api_data >> make_nasa_feed_parquet_file >> load_parquet_to_mongodb_stage >> update_mongodb_production >> send_run_success_notification
