
# Standard library imports
from datetime import datetime, timedelta

# Third party imports
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

# Local application imports
from nasa_neo_service_elt_dag.jobs import functions as f
from nasa_neo_service_elt_dag.configs import elt_config as cfg


# Variables
local_tz = pendulum.timezone("Europe/Athens")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 4, 30, tzinfo = local_tz),
    "email": cfg.email["receiver_email_list"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="nasa_neo_service_ingestion_dag",
    schedule_interval="0 7 * * *",
    default_args=default_args,
    catchup=False,
) as dag:

    collect_api_data = PythonOperator(
        task_id="collect_api_data",
        python_callable=f.collect_api_data,
    )

    transform_and_write_to_parquet = PythonOperator(
        task_id="transform_and_write_to_parquet",
        python_callable=f.transform_and_write_to_parquet
    )

    load_parquet_to_mongodb_staging = PythonOperator(
        task_id="load_parquet_to_mongodb_stage",
        python_callable=f.load_parquet_to_mongodb_staging,
    )

    populate_mongodb_production = PythonOperator(
        task_id="populate_mongodb_production",
        python_callable=f.populate_mongodb_production,
    )

    send_success_notification = PythonOperator(
        task_id ='send_success_notification',
        python_callable=f.send_success_notification,
    )

collect_api_data >> transform_and_write_to_parquet >> load_parquet_to_mongodb_staging >> populate_mongodb_production >> send_success_notification
