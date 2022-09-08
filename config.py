import os

airflow = {
    "project_path": "/c/Users/KonstantinosKaragian/airflowhome/dags/nasa_etl_job",
    "email_list": ["kokargag@gmail.com", "konskar93@gmail.com"]
}


nasa_feed_api = {
    "url": "https://api.nasa.gov/neo/rest/v1/feed",
    "api_key": 'sHNjNLf1d44OkX8pZj2Dz2eqyrBpmvnjnfEKGTr7',
    # "api_key":'DEMO_KEY',
    "json_file": "nasa_feed_data.json",
    "cache": 'http_cache'
}


spark = {
    "app_name": 'Demo Spark App',
    "parquet_path": 'nasa_feed.parquet'
}


mongo_db = {
    "url": 'mongodb://localhost:27414',
    "host": 'localhost',
    "port": 27414,
    "database": 'nasa_feed',
    "collection": 'nasa_feed_stage'
}


absolute_paths = {
    "json_abs_path": os.path.join(airflow["project_path"], nasa_feed_api["json_file"]),
    "cache_abs_path": os.path.join(airflow["project_path"], nasa_feed_api["cache"]),
    "parquet_abs_path": os.path.join(airflow["project_path"], spark["parquet_path"])
}
