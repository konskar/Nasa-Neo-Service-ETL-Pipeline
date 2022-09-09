import os

airflow = {
    "project_path": "/c/Users/KonstantinosKaragian/airflowhome/dags/nasa_neo_service_etl_dag",
    "email_list": ["kokargag@gmail.com", "konskar93@gmail.com"]
}


nasa_feed_api = {
    "url": "https://api.nasa.gov/neo/rest/v1/feed",
    "api_key": 'sHNjNLf1d44OkX8pZj2Dz2eqyrBpmvnjnfEKGTr7',
    # "api_key":'DEMO_KEY',
    "json_file": "nasa_neo_service_api.json",
    "cache": 'http_cache'
}


spark = {
    "app_name": 'Demo Spark App',
    "parquet_path": 'nasa_neo_service_ingestion.parquet'
}


mongo_db = {
    "url": 'mongodb://localhost:27414',
    "host": 'localhost',
    "port": 27414,
    "database": 'nasa_gov',
    "staging_collection": 'nasa_neo_service_staging',
    "production_collection": 'nasa_neo_service_production'
}

absolute_paths = {
    "json_abs_path": os.path.join(airflow["project_path"], nasa_feed_api["json_file"]),
    "cache_abs_path": os.path.join(airflow["project_path"], nasa_feed_api["cache"]),
    "parquet_abs_path": os.path.join(airflow["project_path"], spark["parquet_path"])
}
