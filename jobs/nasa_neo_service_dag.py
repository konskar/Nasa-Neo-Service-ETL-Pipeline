
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
    "email": cfg.smtp["receiver_email_list"], # ["kokargag@gmail.com", "konskar93@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),    
    # "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="nasa_neo_service_ingestion_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:

    collect_api_data = PythonOperator(
        task_id="collect_api_data",
        python_callable=f.collect_api_data
    )

    transform_and_write_to_parquet = PythonOperator(
        task_id="transform_and_write_to_parquet",
        python_callable=f.transform_and_write_to_parquet
    )

    load_parquet_to_mongodb_staging = PythonOperator(
        task_id="load_parquet_to_mongodb_stage",
        python_callable=f.load_parquet_to_mongodb_staging,
        retries=3
    )

    populate_mongodb_production = PythonOperator(
        task_id="populate_mongodb_production",
        python_callable=f.populate_mongodb_production,
        retries=1
    )
    # send_run_success_notification = EmailOperator(
    #     task_id ='send_run_success_notification',
    #     to = cfg.smtp["receiver_email_list"],
    #     subject = 'Airflow Notification: "nasa_neo_service_ingestion_dag"',
    #     html_content = """ <h3>Dag run sucesfully</h3> """
    # )

collect_api_data >> transform_and_write_to_parquet >> load_parquet_to_mongodb_staging >> populate_mongodb_production # >> send_run_success_notification
