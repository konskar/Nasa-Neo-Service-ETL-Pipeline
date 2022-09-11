
# Standard library imports
import os

# Third party imports
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path('/c/Users/KonstantinosKaragian/airflowhome/dags/nasa_neo_service_etl_dag/.env')
load_dotenv(dotenv_path=dotenv_path)

airflow = {
    "project_path": os.getenv('PROJECT_ABSOLUTE_PATH'),
    "email_list": os.getenv('AIRFLOW_EMAIL_LIST')
}

nasa_neo_api = {
    "url": os.getenv('API_URL'),
    "api_key": os.getenv('API_KEY'),
    "json_file": os.getenv('API_JSON_FILE'),
    "cache": os.getenv('API_CACHE')
}

spark = {
    "app_name": os.getenv('SPARK_APP_NAME'),
    "parquet_path": os.getenv('PARQUET_PATH')
}

mongo_db = {
    "url": os.getenv('MONGODB_URL'),
    "host": os.getenv('MONGODB_HOST'),
    "port": int(os.getenv('MONGODB_PORT')),
    "database": os.getenv('MONGODB_DATABASE'),
    "staging_collection": os.getenv('MONGODB_STAGING_COLLECTION'),
    "production_collection": os.getenv('MONGODB_PRODUCTION_COLLECTION')
}

smtp = {
    "port" : int(os.getenv('SMTP_PORT')), # For SSL
    "smtp_server" : os.getenv('SMTP_SERVER'),
    "sender_email" : os.getenv('SMTP_SENDER_EMAIL'),
    "receiver_email_list" : [os.getenv('SMTP_RECIEVER_EMAIL_USER_A'), os.getenv('SMTP_RECIEVER_EMAIL_USER_B')],
    "receiver_email" : os.getenv('SMTP_RECIEVER_EMAIL_USER_A'),
    "password" : os.getenv('SMTP_PASSWORD')
}

absolute_paths = {
    "json_abs_path": os.path.join(airflow["project_path"], nasa_neo_api["json_file"]),
    "cache_abs_path": os.path.join(airflow["project_path"], nasa_neo_api["cache"]),
    "parquet_abs_path": os.path.join(airflow["project_path"], spark["parquet_path"])
}
