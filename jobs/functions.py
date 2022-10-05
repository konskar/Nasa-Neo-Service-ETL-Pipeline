
# Standard library imports
from datetime import datetime, timedelta
import json
import time

# Third party imports
import requests
import requests_cache
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pymongo import MongoClient
from airflow import AirflowException
import smtplib, ssl
import logging

# Local application imports
from nasa_neo_service_elt_dag.configs import elt_config as cfg


# Initialize objects
task_logger = logging.getLogger('airflow.task')


# Functions deceleration
def log_task_duration(start_time: float, end_time: float) -> None:
    """Inject custom log entries to Airflow task log regarding execution time.

    :param start_time: The seconds since epoch when a task started.
    :param end_time: The seconds since epoch when a task ended.
    :return: None
    """  
    log_msg = f"Task Duration: {end_time - start_time} seconds, {(end_time - start_time)/60} minutes"
    task_logger.info(log_msg)


def validate_date_format(date: str) -> None:
    """Validate that date arguments format is YYYY-MM-DD that program expects to properly function. 
       If not, stops execution.

    :param date: The date to be validated.
    :return: None
    """      
    try:
        return datetime.strptime(date, '%Y-%m-%d').date()
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")


def validate_date_ranges(start_date: str, end_date: str) -> None:
    """Validate that end_date is equal or bigger than start_date so API call don't break.

    :param start_time: The start date.
    :param end_time: The end_date.
    :return: None
    """      
    if(start_date > end_date):
        raise Exception(f"end_date (current value: {end_date}) should be bigger than start_date (current value: {start_date})")


def send_email (message: str) -> None:
    """Sends email to configured receiver. 

    :param message: Email subject & body.
    :return: None
    """        
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(cfg.email["smtp_server"], cfg.email["port"], context=context) as server:
        server.login(cfg.email["sender_email"], cfg.email["password"])
        server.sendmail(cfg.email["sender_email"],  cfg.email["receiver_email"], message)


def collect_api_data( \
                        start_date: str = None, 
                        end_date: str = None, 
                        api_response_path : str = cfg.absolute_paths["json_abs_path"], 
                        **kwargs: dict
                    ) -> None:
    """Collect data from NeoWs (Near Earth Object Web Service) of NASA for near earth Asteroid information.

    Links:
        NASA APIs: https://api.nasa.gov/
        NEO Service endpoint:  https://api.nasa.gov/neo/rest/v1/feed?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&api_key=API_KEY

    Functionality:
        Collect data for last 3 days unless function is invoked externally 
        or dag is triggered with date configuration e.g. {"start_date": "2022-07-27", "end_date":"2022-07-29"}
        API response is saved in JSON file.

    :param 'start_date': Start date API request parameter
    :param 'end_date': End date API request parameter
    :param 'api_response_path': Path to save API response in JSON form 
    :param 'kwargs': Dict with Airflow build-in variables and user arguments if provided.
    :return: None
    """        
    try:
        start_time = time.time()

        # Use Airflow config date arguments if provided, function arguments if provided, 
        # or otherwise load last 3 days dynamically
        
        # Airflow config date arguments
        try:
            start_date = kwargs["dag_run"].conf["start_date"]
            end_date = kwargs["dag_run"].conf["end_date"]

        except KeyError:

            # Function arguments, need them to invoke function for unit testing. Both arguments should be provided
            try:
                if (start_date is None or end_date is None):
                    raise ValueError('Not both function arguments are provided')
                
                start_date = start_date
                end_date = end_date

            # If above conditions aren't met, load last 3 days dynamically
            except ValueError:
                execution_date = kwargs["dag_run"].start_date.strftime("%Y-%m-%d")

                # Convert str to date object to calculate dynamic past dates, then convert again to str to pass to application
                execution_date_object = datetime.strptime(execution_date, "%Y-%m-%d")

                start_date_object = execution_date_object - timedelta(days=2)
                start_date = start_date_object.strftime("%Y-%m-%d")

                end_date_object = execution_date_object - timedelta(days=0)
                end_date = end_date_object.strftime("%Y-%m-%d")

        requests_cache.install_cache(cfg.absolute_paths["cache_abs_path"])

        # Validate date ranges and format
        validate_date_format(start_date)
        validate_date_format(end_date)
        validate_date_ranges(start_date, end_date)

        # Prepare api payload
        url = cfg.nasa_neo_api["url"]

        params = {
            "start_date": start_date,
            "end_date": end_date,
            "api_key": cfg.nasa_neo_api["api_key"]
        }

        # Request api data and store them in variable in json form
        api_response = requests.get(url=url, params=params, timeout=120)
        data = api_response.json()

        # print(json.dumps(data, indent=2))

        dict_list = []

        # Retrieve attributes from api response by iterating response objects and store them to list of dictionaries
        for key, value in data["near_earth_objects"].items():

            date = key

            for row in value:
                neo_reference_id = row["neo_reference_id"]
                name = row["name"]
                nasa_jpl_url = row["nasa_jpl_url"]
                estimated_diameter_min_in_km = row["estimated_diameter"]["kilometers"]["estimated_diameter_min"]
                estimated_diameter_max_in_km = row["estimated_diameter"]["kilometers"]["estimated_diameter_max"]
                is_potentially_hazardous_asteroid = row["is_potentially_hazardous_asteroid"]
                velocity_in_km_per_hour = row["close_approach_data"][0]["relative_velocity"]["kilometers_per_hour"]
                lunar_distance = row["close_approach_data"][0]["miss_distance"]["lunar"]

                # Temporary dict that holds current row data, will be appended to dict list and overwritten in next row iteration 
                _dict = {
                    'date': date,
                    'neo_reference_id': neo_reference_id,
                    'name': name,
                    'nasa_jpl_url': nasa_jpl_url,
                    'estimated_diameter_min_in_km': estimated_diameter_min_in_km,
                    'estimated_diameter_max_in_km' : estimated_diameter_max_in_km,
                    'is_potentially_hazardous_asteroid' : is_potentially_hazardous_asteroid,
                    'velocity_in_km_per_hour' : velocity_in_km_per_hour,
                    'lunar_distance' : lunar_distance,
                }

                dict_list.append(_dict)

        # print(json.dumps(dict_list, indent=2))

        # Persist api data dict created above to JSON file, so it can be further processed downstream
        with open(api_response_path, 'w') as file:
            json.dump(dict_list, file, indent=4)

        end_time = time.time()
        log_task_duration(start_time, end_time)

    except Exception as e:

        # Force Airflow task to fail so all downstream tasks won't execute
        raise AirflowException({e})


def transform_and_write_to_parquet( \
                                        api_response_path : str = cfg.absolute_paths["json_abs_path"], 
                                        parquet_path : str = cfg.absolute_paths["parquet_abs_path"]
                                    ) -> None:
    """Process with Spark json file from API response, create field 'velocity_in_miles_per_hour' and store output to parquet file.

    :param 'api_response_path': Path of API response saved as json file
    :param 'parquet_path': Path of parquet file that will store transformed dataset
    :return: None
    """        
    try:
        start_time = time.time()

        spark = SparkSession \
                .builder \
                .appName("nasa_gov_etl | transform_and_write_to_parquet") \
                .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
                .master('local[*]') \
                .getOrCreate()

        nasa_neo_df = spark.read.option("multiline", "true").json(api_response_path)
        cached_nasa_neo_df = nasa_neo_df.cache()

        nasa_neo_transformed_df = cached_nasa_neo_df \
                                .withColumn("lunar_distance", col("lunar_distance").cast("double")) \
                                .withColumn("velocity_in_km_per_hour", col("velocity_in_km_per_hour").cast("double")) \
                                .withColumn("velocity_in_miles_per_hour", col("velocity_in_km_per_hour") * 0.6213712)

        nasa_neo_transformed_df.write.mode('overwrite').partitionBy("date").parquet(parquet_path)

        nasa_neo_df.unpersist()

        spark.stop()

        end_time = time.time()
        log_task_duration(start_time, end_time)

    except Exception as e:

        # Force Airflow task to fail so downstream tasks won't execute 
        raise AirflowException({e})


def load_parquet_to_mongodb_staging( \
                                        database : str = cfg.mongo_db["database"], 
                                        staging_collection : str = cfg.mongo_db["staging_collection"], 
                                        parquet_path : str = cfg.absolute_paths["parquet_abs_path"]
                                    ) -> None:
    """With Spark read parquet file and store it mongodb staging collection.

    :param 'database': Mongodb database that holds staging and production collections
    :param 'staging_collection': Staging collection that will be populated with parquet data 
    :param 'parquet_path': Path of parquet file that transformed dataset is stored
    :return: None
    """     
    try:
        start_time = time.time()

        spark = SparkSession \
                .builder \
                .appName("nasa_gov_etl | load_parquet_to_mongodb_staging") \
                .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
                .master('local[*]') \
                .getOrCreate()

        nasa_neo_df = spark.read.parquet(parquet_path)

        nasa_neo_df.write.format("mongo").mode("overwrite") \
            .option("uri", cfg.mongo_db["url"]) \
            .option("database", database) \
            .option("collection", staging_collection) \
            .save()

        spark.stop()

        end_time = time.time()
        log_task_duration(start_time, end_time)

    except Exception as e:

        # Force Airflow task to fail so all downstream tasks won't execute
        raise AirflowException({e})


def populate_mongodb_production( \
                                    database : str = cfg.mongo_db["database"], 
                                    staging_collection : str = cfg.mongo_db["staging_collection"], 
                                    production_collection : str = cfg.mongo_db["production_collection"]
                                ) -> None:
    """Populate mongodb production collection with staging documents.

    :param 'database': Mongodb database that holds staging and production collections
    :param 'staging_collection': Staging collection loaded with transformed dataset
    :param 'production_collection': Production collection that will be populated with staging collection records    
    :return: None
    """   
    try:
        start_time = time.time()

        with MongoClient(host=cfg.mongo_db["host"], port=cfg.mongo_db["port"]) as client:

            database = getattr(client, database)
            staging_collection = getattr(database, staging_collection)
            production_collection = getattr(database, production_collection)

            staging_documents = []

            # Create list of dictionaries from staging documents
            for doc in staging_collection.find():
                staging_documents.append(doc)

            # Delete production documents that exist on staging to avoid duplicates, since all staging documents will be loaded
            # unique key: date & neo_reference_id
            for row in staging_documents:
                production_collection.delete_one(
                    {"date": row["date"], "neo_reference_id": row["neo_reference_id"]})

            # Load all stage documents to prod
            production_collection.insert_many(staging_documents)

        end_time = time.time()
        log_task_duration(start_time, end_time)

    except Exception as e:

        # Force Airflow task to fail so all downstream tasks won't execute
        raise AirflowException({e})


def send_success_notification(**kwargs: dict) -> None:
    """Prepares email subject & body with dynamic Airflow variables and calls send_email() to deliver it.

    :param kwargs: Dict with Airflow build-in variables and user arguments if provided.
    :return: None
    """   
    try:
        start_time = time.time()

        # Message structure should have this format and indentation for email to be properly rendered
        message = f"""\
Subject: Airflow Success Notification: Dag Run 

dag_run: {kwargs["dag_run"]}

params: {kwargs["params"]}

"""
        send_email(message)

        end_time = time.time()
        log_task_duration(start_time, end_time)

    except Exception as e:

        # Force Airflow task to fail so all downstream tasks won't execute
        raise AirflowException({e})
