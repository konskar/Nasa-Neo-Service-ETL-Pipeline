
# Standard library imports
from datetime import datetime, timedelta
import requests
import requests_cache
import json
import os

# Third party imports
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pymongo import MongoClient

# Local application imports
from nasa_neo_service_etl_dag.configs import etl_config as cfg


spark = SparkSession \
    .builder \
    .appName(cfg.spark["app_name"]) \
    .master('local[*]') \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()


def jprint(obj):
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)


def validate_date_format(input_date):
    try:
        return datetime.strptime(input_date, '%Y-%m-%d').date()
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")


def validate_date_ranges(start_date, end_date):
    if(start_date > end_date):
        raise Exception("enddate should be bigger than startdate")


def get_nasa_feed_api_data(**kwargs):
    """
    get api data from https://api.nasa.gov/neo/rest/v1/feed?start_date=2022-03-08&end_date=2022-03-09&api_key=DEMO_KEY
    for last 4 days
    """

    execution_date = kwargs['ds']

    # convert str to date object to calculate dynamic past dates, then convert again to str to pass to application
    execution_date_object = datetime.strptime(execution_date, "%Y-%m-%d")

    start_date_object = execution_date_object - timedelta(days=1)
    start_date = start_date_object.strftime("%Y-%m-%d")

    end_date_object = execution_date_object - timedelta(days=0)
    end_date = end_date_object.strftime("%Y-%m-%d")

    requests_cache.install_cache(cfg.absolute_paths["cache_abs_path"])

    validate_date_format(start_date)
    validate_date_format(end_date)

    validate_date_ranges(start_date, end_date)

    url = cfg.nasa_feed_api["url"]

    params = {
        "start_date": start_date,
        "end_date": end_date, 
        # "start_date": '2022-07-20',
        # "end_date": '2022-07-23',         
        "api_key": cfg.nasa_feed_api["api_key"]
    }

    res = requests.get(url=url, params=params, timeout=120)
    data = res.json()

    dict_list = []

    for key, value in data["near_earth_objects"].items():

        date = key

        for row in value:
            neo_reference_id = row["neo_reference_id"]
            name = row["name"]
            nasa_jpl_url = row["nasa_jpl_url"]
            estimated_diameter_km_min = row["estimated_diameter"]["kilometers"]["estimated_diameter_min"]

            _dict = {
                'date': date,
                'neo_reference_id': neo_reference_id,
                'name': name,
                'nasa_jpl_url': nasa_jpl_url,
                'estimated_diameter_km_min': estimated_diameter_km_min,
            }

            dict_list.append(_dict)

    # print(json.dumps(dict_list, indent=2))

    with open(cfg.absolute_paths["json_abs_path"], 'w') as file:
        json.dump(dict_list, file, indent=4)


def make_nasa_feed_parquet_file():
    global spark

    df_nasa_feed = spark.read.option("multiline", "true").json(
        cfg.absolute_paths["json_abs_path"])

    # df_nasa_feed.printSchema()
    # df_nasa_feed.show()

    cached_df_nasa_feed = df_nasa_feed.cache()

    # ------------ dataframe api method ------------

    windowSpec = Window.partitionBy("date").orderBy("neo_reference_id")

    df_nasa_feed_updated_dfapi = cached_df_nasa_feed.withColumn(
        "row_number", row_number().over(windowSpec))

    # df_nasa_feed_updated_dfapi.printSchema()
    # df_nasa_feed_updated_dfapi.show()

    # ------------ spark sql method ------------

    # cached_df_nasa_feed.createOrReplaceTempView("NASA_FEED")

    # df_nasa_feed_updated_sparksql = spark.sql("""
    # SELECT *, ROW_NUMBER() OVER(PARTITION BY date ORDER BY neo_reference_id) AS row_number
    # FROM NASA_FEED
    # """)

    # df_nasa_feed_updated_sparksql.printSchema()
    # df_nasa_feed_updated_sparksql.show()

    df_nasa_feed_updated_dfapi.write.mode('overwrite').partitionBy(
        "date").parquet(cfg.absolute_paths["parquet_abs_path"])

    df_nasa_feed.unpersist()  # we don't need it anymore


def load_parquet_to_mongodb_stage():
    global spark

    nasa_feed_df = spark.read.parquet(cfg.absolute_paths["parquet_abs_path"])

    nasa_feed_df.write.format("mongo").mode("overwrite") \
        .option("uri", cfg.mongo_db["url"]) \
        .option("database", cfg.mongo_db["database"]) \
        .option("collection", cfg.mongo_db["staging_collection"]) \
        .save()


def update_mongodb_production():
    with MongoClient(host=cfg.mongo_db["host"], port=cfg.mongo_db["port"]) as client:

        database = getattr(client, cfg.mongo_db["database"])
        staging_collection = getattr(database, cfg.mongo_db["staging_collection"])
        production_collection = getattr(database, cfg.mongo_db["production_collection"]) # equilevant client.nasa_gov.nasa_neo_service_production

        staging_documents = []

        # create dict list with stage rows
        for doc in staging_collection.find():
            staging_documents.append(doc)

        # delete from prod rows that exist on stage
        for row in staging_documents:
            production_collection.delete_one(
                {"date": row["date"], "neo_reference_id": row["neo_reference_id"]})

        # load stage records to prod
        production_collection.insert_many(staging_documents)
