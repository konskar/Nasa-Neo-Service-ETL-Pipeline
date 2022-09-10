
# Standard library imports
from datetime import datetime, timedelta

# Third party imports
from airflow import DAG
from airflow.operators.python import PythonOperator

# Local application imports
from nasa_neo_service_etl_dag.jobs import functions as f
from nasa_neo_service_etl_dag.configs import etl_config as cfg

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 4, 30),
    "email": cfg.airflow["email_list"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
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

get_nasa_feed_api_data >> make_nasa_feed_parquet_file >> load_parquet_to_mongodb_stage >> update_mongodb_production