
# Standard library imports
import unittest
import os
import sys

# Third party imports
import hashlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, to_date


def Setup_Paths() -> None:
    """ Define paths and import libraries according to the environment the script is running (WSL or Docker)
    """

    global api_test_dataset_abs_path, api_produced_dataset_abs_path, parquet_produced_dataset_abs_path, cfg, f

    file_path = os.path.dirname( __file__ )

    config_dir =  os.path.abspath(os.path.join(file_path, '..', 'configs'))
    jobs_dir =  os.path.abspath(os.path.join(file_path, '..', 'jobs'))

    sys.path.insert(1, config_dir)
    sys.path.insert(1, jobs_dir)

    import elt_config as cfg
    import functions as f

    api_test_dataset_abs_path = cfg.absolute_paths["api_test_dataset_abs_path"]
    api_produced_dataset_abs_path = cfg.absolute_paths["api_produced_dataset_abs_path"]
    parquet_produced_dataset_abs_path = cfg.absolute_paths["parquet_produced_dataset_abs_path"]


def Produce_Files() -> None:
    """ Produce datasets that will be used in unit/integration tests
    """

    # define global paths
    global api_produced_dataset_abs_path, parquet_produced_dataset_abs_path, api_test_dataset_abs_path

    # Produce json file "api_produced_dataset" retrieving API data for the same period with the same schema as production grade test file
    f.collect_api_data(   start_date= '2022-07-27', \
                          end_date='2022-07-31', \
                          api_response_path = api_produced_dataset_abs_path
                      )
    
    # Produce parquet file "api_produced_dataset" from 'api_produced_dataset' applying transformation
    f.transform_and_write_to_parquet(   api_response_path = api_produced_dataset_abs_path, \
                                        parquet_path = parquet_produced_dataset_abs_path
                                    )

    # # Populate mongodb staging collection from 'api_produced_dataset'
    f.load_parquet_to_mongodb_staging(  database = cfg.testing["test_database"], \
                                        staging_collection = cfg.testing["test_staging_collection"], \
                                        parquet_path = parquet_produced_dataset_abs_path
                                     )

    # # Populate mongodb production collection from mongodb staging collection
    f.populate_mongodb_production(  database = cfg.testing["test_database"], \
                                    staging_collection = cfg.testing["test_staging_collection"], \
                                    production_collection = cfg.testing["test_production_collection"]
                                 )

def Spark_Setup() -> None:
    """Start Spark Session and define dataframes that will be used in unit/integration tests
    """
    global spark, api_produced_df, api_validated_df, transformed_df, mongodb_staging_df, mongodb_production_df, mongodb_production_df_date_filtered

    spark = SparkSession \
        .builder \
        .appName("nasa_gov_etl | test_etl_job") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .master('local[*]') \
        .getOrCreate()

    # Production grade dataset we have tested it's schema and data, will compare the produced datasets with it
    api_validated_df = spark.read.option("multiline", "true").json(api_test_dataset_abs_path)

    # Dataframe of api_produced_dataset, should match api_validated_df
    api_produced_df = spark.read.option("multiline", "true").json(api_produced_dataset_abs_path)

    # Dataframe of parquet file "api_produced_dataset", should include only the additional field 'velocity_in_miles_per_hour'
    transformed_df = spark.read.parquet(parquet_produced_dataset_abs_path)

    # Dataframe of mongodb test staging collection
    mongodb_staging_df = spark.read.format("mongo") \
                        .option("uri", cfg.mongo_db["url"]) \
                        .option("database", cfg.testing["test_database"]) \
                        .option("collection", cfg.testing["test_staging_collection"]) \
                        .load() \
                        .withColumn("date", col("date").cast("string")) \
                        .withColumn("date", to_date(col("date"))) \
                        .drop("_id")

    mongodb_staging_df.createOrReplaceTempView("mongodb_staging_df")

    # Dataframe of mongodb test production collection
    mongodb_production_df = spark.read.format("mongo") \
                        .option("uri", cfg.mongo_db["url"]) \
                        .option("database", cfg.testing["test_database"]) \
                        .option("collection", cfg.testing["test_production_collection"]) \
                        .load() \
                        .withColumn("date", col("date").cast("string")) \
                        .withColumn("date", to_date(col("date"))) \
                        .drop("_id")
    
    mongodb_production_df.createOrReplaceTempView("mongodb_production_df")

    # Dataframe of mongodb test production collection containing only rows for the dates on staging collection
    mongodb_production_df_date_filtered = spark.sql("""
                                                    SELECT * FROM mongodb_production_df
                                                    WHERE date IN (
                                                                    SELECT DISTINCT date FROM mongodb_staging_df
                                                                  )
                                               """)


def Spark_Teardown() -> None:
    """Terminate Spark Session 
    """
    spark.stop()


# Test Suite
class Test_API_Client(unittest.TestCase):
    """Integration tests for API
    
    "api_produced_df" should be like to like with "api_validated_df"
    """

	# Test cases
    def test_schema(self):
        api_produced_df_schema = api_produced_df.schema
        api_validated_df_schema = api_validated_df.schema
        self.assertEqual(api_produced_df_schema, api_validated_df_schema)


    def test_column_count(self):
        api_produced_df_column_count = len(api_produced_df.columns)
        api_validated_df_column_count = len(api_validated_df.columns)
        self.assertEqual(api_produced_df_column_count, api_validated_df_column_count)


    def test_row_count(self):
        api_produced_df_rows = api_produced_df.count()
        api_validated_df_rows = api_validated_df.count()
        self.assertEqual(api_produced_df_rows, api_validated_df_rows)


    def test_column_names(self):
        api_produced_df_column_names = api_produced_df.columns
        api_validated_df_column_names = api_validated_df.columns
        self.assertEqual(api_produced_df_column_names, api_validated_df_column_names)


    def test_average_lunar_distance(self):
        api_produced_df_avg_lunar_distance = (
                                                api_produced_df
                                                .agg(avg('lunar_distance').alias('average_lunar_distance'))
                                                .collect()[0][0]
                                             )

        api_validated_df_avg_lunar_distance = (
                                                api_validated_df
                                                .agg(avg('lunar_distance').alias('average_lunar_distance'))
                                                .collect()[0][0]
                                               )
        
        self.assertEqual(api_produced_df_avg_lunar_distance, api_validated_df_avg_lunar_distance)


    def test_sum_of_velocity_in_km_per_hour(self):
        api_produced_df_velocity_in_km_per_hour = (
                                                    api_produced_df
                                                    .agg(sum('velocity_in_km_per_hour').alias('sum_velocity_in_km_per_hour'))
                                                    .collect()[0][0]
                                                  )

        api_validated_df_velocity_in_km_per_hour = (
                                                    api_validated_df
                                                    .agg(sum('velocity_in_km_per_hour').alias('sum_velocity_in_km_per_hour'))
                                                    .collect()[0][0]
                                                   )
        
        self.assertEqual(api_produced_df_velocity_in_km_per_hour, api_validated_df_velocity_in_km_per_hour)


    def test_sum_of_estimated_diameter_min_in_km(self):
        api_produced_df_sum_of_estimated_diameter_min_in_km = (
                                                                api_produced_df
                                                                .agg(sum('estimated_diameter_min_in_km').alias('sum_estimated_diameter_min_in_km'))
                                                                .collect()[0][0]
                                                              )

        api_validated_df_sum_of_estimated_diameter_min_in_km = (
                                                                api_validated_df
                                                                .agg(sum('estimated_diameter_min_in_km').alias('sum_estimated_diameter_min_in_km'))
                                                                .collect()[0][0]
                                                               )

        self.assertEqual(api_produced_df_sum_of_estimated_diameter_min_in_km, api_validated_df_sum_of_estimated_diameter_min_in_km)
    

    def test_sum_of_estimated_diameter_max_in_km(self):
        api_produced_df_sum_of_estimated_diameter_max_in_km = (
                                                                api_produced_df
                                                                .agg(sum('estimated_diameter_max_in_km').alias('sum_estimated_diameter_max_in_km'))
                                                                .collect()[0][0]
                                                              )

        api_validated_df_sum_of_estimated_diameter_max_in_km = (
                                                                api_validated_df
                                                                .agg(sum('estimated_diameter_max_in_km').alias('sum_estimated_diameter_max_in_km'))
                                                                .collect()[0][0]
                                                               )    

        self.assertEqual(api_produced_df_sum_of_estimated_diameter_max_in_km, api_validated_df_sum_of_estimated_diameter_max_in_km)


    def test_dimension_count(self):
        api_produced_df_agg = api_produced_df.groupBy("date", "neo_reference_id", "name", "nasa_jpl_url", "is_potentially_hazardous_asteroid").count()
        api_produced_df_agg_md5_hash = hashlib.md5(str(api_produced_df_agg.collect()[0:]).encode('utf-8')).hexdigest()

        api_validated_df_agg = api_validated_df.groupBy("date", "neo_reference_id", "name", "nasa_jpl_url", "is_potentially_hazardous_asteroid").count()
        api_validated_df_agg_md5_hash = hashlib.md5(str(api_validated_df_agg.collect()).encode('utf-8')).hexdigest()

        # Compare hashes instead of lists for better performance
        self.assertEqual(api_produced_df_agg_md5_hash, api_validated_df_agg_md5_hash)


# Test Suite
class Test_Transformations(unittest.TestCase):
    """Unit tests for Spark Transformation 

    "transformed_df" should be like "api_validated_df", except having the additional field 'velocity_in_miles_per_hour_double'
    """

    # Test cases
    def test_velocity_conversion_to_miles(self):
        velocity_in_km_per_hour = transformed_df.groupBy().sum("velocity_in_km_per_hour").collect()[0][0]
        velocity_in_miles_per_hour = transformed_df.groupBy().sum("velocity_in_miles_per_hour").collect()[0][0]
        self.assertEqual(float("{:.8f}".format(velocity_in_km_per_hour * 0.621371)), float("{:.8f}".format(velocity_in_miles_per_hour)))


    def test_row_count(self):
        api_validated_df_rows = api_validated_df.count()
        transformed_df_rows = transformed_df.count()
        self.assertEqual(api_validated_df_rows, transformed_df_rows)


    def test_column_count(self):
        api_validated_df_column_count = len(api_validated_df.columns)
        transformed_df_column_count = len(transformed_df.columns)

        # transformed df has one new column        
        self.assertEqual(api_validated_df_column_count, transformed_df_column_count - 1)


    def test_dimension_count(self):
        api_validated_df_agg = api_validated_df.groupBy("date", "neo_reference_id", "name", "nasa_jpl_url", "is_potentially_hazardous_asteroid") \
                                               .count() \
                                               .sort("date", "neo_reference_id", "name", "nasa_jpl_url", "is_potentially_hazardous_asteroid")

        api_validated_df_agg_md5_hash = hashlib.md5(str(api_validated_df_agg.collect()).encode('utf-8')).hexdigest()

        # convert date from date type to string to match schemas, since hash function takes schema into account
        transformed_df_agg =  transformed_df.withColumn("date",col("date").cast("string")) \
                                            .groupBy("date", "neo_reference_id", "name", "nasa_jpl_url", "is_potentially_hazardous_asteroid") \
                                            .count() \
                                            .sort("date", "neo_reference_id", "name", "nasa_jpl_url", "is_potentially_hazardous_asteroid")

        transformed_df_agg_md5_hash = hashlib.md5(str(transformed_df_agg.collect()[0:]).encode('utf-8')).hexdigest()
        
        self.assertEqual(api_validated_df_agg_md5_hash, transformed_df_agg_md5_hash)


    def test_metrics_sum(self):

        api_validated_df_velocity_in_km_per_hour= api_validated_df.agg(sum("velocity_in_km_per_hour")).collect()[0][0]
        api_validated_df_estimated_diameter_min_in_km= api_validated_df.agg(sum("estimated_diameter_min_in_km")).collect()[0][0]
        api_validated_df_estimated_diameter_max_in_km= api_validated_df.agg(sum("estimated_diameter_max_in_km")).collect()[0][0]
        api_validated_df_lunar_distance= api_validated_df.agg(sum("lunar_distance")).collect()[0][0]

        transformed_df_velocity_in_km_per_hour=transformed_df.agg(sum("velocity_in_km_per_hour")).collect()[0][0]
        transformed_df_estimated_diameter_min_in_km= transformed_df.agg(sum("estimated_diameter_min_in_km")).collect()[0][0]
        transformed_df_estimated_diameter_max_in_km= transformed_df.agg(sum("estimated_diameter_max_in_km")).collect()[0][0]
        transformed_df_lunar_distance= transformed_df.agg(sum("lunar_distance")).collect()[0][0]

        self.assertEqual(float("{:.3f}".format(api_validated_df_velocity_in_km_per_hour)), float("{:.3f}".format(transformed_df_velocity_in_km_per_hour)))
        self.assertEqual(float("{:.3f}".format(api_validated_df_estimated_diameter_min_in_km)), float("{:.3f}".format(transformed_df_estimated_diameter_min_in_km)))
        self.assertEqual(float("{:.3f}".format(api_validated_df_estimated_diameter_max_in_km)), float("{:.3f}".format(transformed_df_estimated_diameter_max_in_km)))
        self.assertEqual(float("{:.3f}".format(api_validated_df_lunar_distance)), float("{:.3f}".format(transformed_df_lunar_distance)))


# Test Suite
class Test_Loading_Parquet_to_MongoDB_Staging(unittest.TestCase):
    """Unit tests for loading parquet to mongodb staging collection

    mongodb_staging should be like to like with "transformed_df"
    """

    # Test cases
    def test_row_count(self):
        transformed_df_rows = transformed_df.count()
        mongodb_staging_df_rows = mongodb_staging_df.count()
        self.assertEqual(transformed_df_rows, mongodb_staging_df_rows)


    def test_column_count(self):
        transformed_df_column_count= len(transformed_df.columns)
        mongodb_staging_df_column_count= len(mongodb_staging_df.columns)
        self.assertEqual(transformed_df_column_count, mongodb_staging_df_column_count)


    def test_dimension_count(self):

        transformed_df_agg = transformed_df.groupBy("date", "neo_reference_id", "name", "nasa_jpl_url", "is_potentially_hazardous_asteroid") \
                                           .count() \
                                           .sort("date", "neo_reference_id", "name", "nasa_jpl_url", "is_potentially_hazardous_asteroid")

        transformed_df_agg_md5_hash = hashlib.md5(str(transformed_df_agg.collect()).encode('utf-8')).hexdigest()

        mongodb_staging_df_agg = mongodb_staging_df.groupBy("date", "neo_reference_id", "name", "nasa_jpl_url", "is_potentially_hazardous_asteroid") \
                                                   .count() \
                                                   .sort("date", "neo_reference_id", "name", "nasa_jpl_url", "is_potentially_hazardous_asteroid")

        mongodb_staging_df_agg_md5_hash = hashlib.md5(str(mongodb_staging_df_agg.collect()).encode('utf-8')).hexdigest()
        
        self.assertEqual(transformed_df_agg_md5_hash, mongodb_staging_df_agg_md5_hash)


    def test_metrics_sum(self):

        transformed_df_velocity_in_km_per_hour= transformed_df.agg(sum("velocity_in_km_per_hour")).collect()[0][0]
        transformed_df_estimated_diameter_min_in_km= transformed_df.agg(sum("estimated_diameter_min_in_km")).collect()[0][0]
        transformed_df_estimated_diameter_max_in_km= transformed_df.agg(sum("estimated_diameter_max_in_km")).collect()[0][0]
        transformed_df_lunar_distance= transformed_df.agg(sum("lunar_distance")).collect()[0][0]

        mongodb_staging_df_velocity_in_km_per_hour=mongodb_staging_df.agg(sum("velocity_in_km_per_hour")).collect()[0][0]
        mongodb_staging_df_estimated_diameter_min_in_km= mongodb_staging_df.agg(sum("estimated_diameter_min_in_km")).collect()[0][0]
        mongodb_staging_df_estimated_diameter_max_in_km= mongodb_staging_df.agg(sum("estimated_diameter_max_in_km")).collect()[0][0]
        mongodb_staging_df_lunar_distance= mongodb_staging_df.agg(sum("lunar_distance")).collect()[0][0]

        self.assertEqual(float("{:.3f}".format(transformed_df_velocity_in_km_per_hour)), float("{:.3f}".format(mongodb_staging_df_velocity_in_km_per_hour)))
        self.assertEqual(float("{:.3f}".format(transformed_df_estimated_diameter_min_in_km)), float("{:.3f}".format(mongodb_staging_df_estimated_diameter_min_in_km)))
        self.assertEqual(float("{:.3f}".format(transformed_df_estimated_diameter_max_in_km)), float("{:.3f}".format(mongodb_staging_df_estimated_diameter_max_in_km)))
        self.assertEqual(float("{:.3f}".format(transformed_df_lunar_distance)), float("{:.3f}".format(mongodb_staging_df_lunar_distance)))


# Test Suite
class Test_Populating_MongoDB_Production(unittest.TestCase):
    """Unit tests for populating mongodb production collection

    mongodb_production should match mongodb_staging on dates that are on staging
    mongodb_production has data for other dates that should not be affected 
    """

    # Test cases
    def test_row_count_for_dates_in_staging(self):
        mongodb_staging_df_rows = mongodb_staging_df.count()
        mongodb_production_df_date_filtered_rows = mongodb_production_df_date_filtered.count()
        self.assertEqual(mongodb_staging_df_rows, mongodb_production_df_date_filtered_rows)

    def test_total_row_count(self):
        mongodb_staging_df_rows = mongodb_staging_df.count()
        mongodb_production_df_rows = mongodb_production_df.count()
        self.assertGreater(mongodb_production_df_rows, mongodb_staging_df_rows)

    def test_column_count(self):
        mongodb_staging_df_cols = len(mongodb_staging_df.columns)
        mongodb_production_df_cols = len(mongodb_production_df.columns)
        self.assertEqual(mongodb_production_df_cols, mongodb_staging_df_cols)

    def test_production_rows_not_on_staging_remained_the_same(self):
        
        expected_total_production_rows_not_on_staging = 448
        """ the value is extracted with below mql query:

            db.test_nasa_neo_service_production.find
            ({
                $or: [
                        {date:{$lt:ISODate("2022-07-26T21:00:00.000+00:00")}},
                        {date:{$gte:ISODate("2022-07-31T21:00:00.000+00:00")}}
                    ]
            })
        """

        total_production_rows_not_on_staging = mongodb_production_df.count() - mongodb_production_df_date_filtered.count()

        self.assertEqual(total_production_rows_not_on_staging, expected_total_production_rows_not_on_staging)


    def test_dimension_count_for_dates_in_staging(self):

        mongodb_staging_df_agg = mongodb_staging_df.groupBy("date", "neo_reference_id", "name", "nasa_jpl_url", "is_potentially_hazardous_asteroid") \
                                                   .count() \
                                                   .sort("date", "neo_reference_id", "name", "nasa_jpl_url", "is_potentially_hazardous_asteroid")

        mongodb_staging_df_agg_md5_hash = hashlib.md5(str(mongodb_staging_df_agg.collect()).encode('utf-8')).hexdigest()

        mongodb_production_df_date_filtered_agg = mongodb_production_df_date_filtered.groupBy("date", "neo_reference_id", "name", "nasa_jpl_url", "is_potentially_hazardous_asteroid") \
                                                                                     .count() \
                                                                                     .sort("date", "neo_reference_id", "name", "nasa_jpl_url", "is_potentially_hazardous_asteroid")

        mongodb_production_df_date_filtered_agg_md5_hash = hashlib.md5(str(mongodb_production_df_date_filtered_agg.collect()[0:]).encode('utf-8')).hexdigest()
        
        self.assertEqual(mongodb_staging_df_agg_md5_hash, mongodb_production_df_date_filtered_agg_md5_hash)


    def test_metrics_sum_for_dates_in_staging(self):

        mongodb_staging_df_velocity_in_km_per_hour=mongodb_staging_df.agg(sum("velocity_in_km_per_hour")).collect()[0][0]
        mongodb_staging_df_estimated_diameter_min_in_km= mongodb_staging_df.agg(sum("estimated_diameter_min_in_km")).collect()[0][0]
        mongodb_staging_df_estimated_diameter_max_in_km= mongodb_staging_df.agg(sum("estimated_diameter_max_in_km")).collect()[0][0]
        mongodb_staging_df_lunar_distance= mongodb_staging_df.agg(sum("lunar_distance")).collect()[0][0]

        mongodb_production_df_date_filtered_velocity_in_km_per_hour= mongodb_production_df_date_filtered.agg(sum("velocity_in_km_per_hour")).collect()[0][0]
        mongodb_production_df_date_filtered_estimated_diameter_min_in_km= mongodb_production_df_date_filtered.agg(sum("estimated_diameter_min_in_km")).collect()[0][0]
        mongodb_production_df_date_filtered_estimated_diameter_max_in_km= mongodb_production_df_date_filtered.agg(sum("estimated_diameter_max_in_km")).collect()[0][0]
        mongodb_production_df_date_filtered_lunar_distance= mongodb_production_df_date_filtered.agg(sum("lunar_distance")).collect()[0][0]

        self.assertEqual(float("{:.3f}".format(mongodb_production_df_date_filtered_velocity_in_km_per_hour)), float("{:.3f}".format(mongodb_staging_df_velocity_in_km_per_hour)))
        self.assertEqual(float("{:.3f}".format(mongodb_production_df_date_filtered_estimated_diameter_min_in_km)), float("{:.3f}".format(mongodb_staging_df_estimated_diameter_min_in_km)))
        self.assertEqual(float("{:.3f}".format(mongodb_production_df_date_filtered_estimated_diameter_max_in_km)), float("{:.3f}".format(mongodb_staging_df_estimated_diameter_max_in_km)))
        self.assertEqual(float("{:.3f}".format(mongodb_production_df_date_filtered_lunar_distance)), float("{:.3f}".format(mongodb_staging_df_lunar_distance)))


if __name__ == '__main__':

    Setup_Paths()

    Produce_Files()

    Spark_Setup()

    unittest.main()

    Spark_Teardown()