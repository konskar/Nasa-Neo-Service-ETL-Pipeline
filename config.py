import os

from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path('/c/Users/KonstantinosKaragian/airflowhome/dags/nasa_neo_service_etl_dag/.env')
load_dotenv(dotenv_path=dotenv_path)

airflow = {
    "project_path": os.getenv('PROJECT_ABSOLUTE_PATH'),
    "email_list": os.getenv('AIRFLOW_EMAIL_LIST')
}

nasa_feed_api = {
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
    "port": 27414,
    "database": os.getenv('MONGODB_DATABASE'),
    "staging_collection": os.getenv('MONGODB_STAGING_COLLECTION'),
    "production_collection": os.getenv('MONGODB_PRODUCTION_COLLECTION')
}

absolute_paths = {
    "json_abs_path": os.path.join(airflow["project_path"], nasa_feed_api["json_file"]),
    "cache_abs_path": os.path.join(airflow["project_path"], nasa_feed_api["cache"]),
    "parquet_abs_path": os.path.join(airflow["project_path"], spark["parquet_path"])
}
