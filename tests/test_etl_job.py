
from audioop import avg
import unittest
import os
import sys
import hashlib

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg

file_path = os.path.dirname( __file__ )

config_dir =  os.path.abspath(os.path.join(file_path, '..', 'configs'))
jobs_dir =  os.path.abspath(os.path.join(file_path, '..', 'jobs'))

sys.path.insert(1, config_dir)
sys.path.insert(1, jobs_dir)

import etl_config as cfg
import functions as f


def setUp():
    """Start Spark, define config and path to test data
    """
    global spark, input_df, expected_df

    spark = SparkSession \
        .builder \
        .appName(cfg.spark["app_name"]) \
        .master('local[*]') \
        .getOrCreate()

    # expected_df: Data we have tested their schema and content
    expected_df = spark.read.option("multiline", "true").json(cfg.absolute_paths["api_test_dataset_abs_path"])

    # input_df: Retrieve API data for the same period with the same schema as test file
    f.collect_api_data(start_date= '2022-07-27', end_date='2022-07-31')
    input_df = spark.read.option("multiline", "true").json(cfg.absolute_paths["json_abs_path"])

def tearDown():
    """Stop Spark
    """
    spark.stop()


# Test Suite
class TestAPI(unittest.TestCase):
    """Integration tests for API 
    """

	# Test cases
    def test_schema(self):
        expected_df_schema = expected_df.schema
        input_df_schema = input_df.schema

        self.assertEqual(input_df_schema, expected_df_schema)

    def test_column_count(self):
        expected_cols = len(expected_df.columns)
        input_cols = len(input_df.columns)
        self.assertEqual(input_cols, expected_cols)

    def test_row_count(self):
        expected_rows = expected_df.count()
        input_rows = input_df.count()
        self.assertEqual(input_rows, expected_rows)

    def test_column_names(self):
        expected_columns = expected_df.columns
        input_columns = input_df.columns
        self.assertEqual(input_columns, expected_columns)

    def test_average_lunar_distance(self):
        expected_average_lunar_distance = (
            expected_df
            .agg(avg('lunar_distance').alias('average_lunar_distance'))
            .collect()[0]['average_lunar_distance'])
        
        input_average_lunar_distance = (
            input_df
            .agg(avg('lunar_distance').alias('average_lunar_distance'))
            .collect()[0]['average_lunar_distance'])

        self.assertEqual(input_average_lunar_distance, expected_average_lunar_distance)

    def test_sum_of_velocity_in_km_per_hour(self):
        expected_sum_of_velocity_in_km_per_hour = (
            expected_df
            .agg(sum('velocity_in_km_per_hour').alias('sum_velocity_in_km_per_hour'))
            .collect()[0]['sum_velocity_in_km_per_hour'])
        
        input_sum_of_velocity_in_km_per_hour = (
            input_df
            .agg(sum('velocity_in_km_per_hour').alias('sum_velocity_in_km_per_hour'))
            .collect()[0]['sum_velocity_in_km_per_hour'])
        
        self.assertEqual(input_sum_of_velocity_in_km_per_hour, expected_sum_of_velocity_in_km_per_hour)


    def test_sum_of_estimated_diameter_min_in_km(self):

        expected_sum_of_estimated_diameter_min_in_km = (
            expected_df
            .agg(sum('estimated_diameter_min_in_km').alias('sum_estimated_diameter_min_in_km'))
            .collect()[0]['sum_estimated_diameter_min_in_km'])
        
        input_sum_of_estimated_diameter_min_in_km = (
            input_df
            .agg(sum('estimated_diameter_min_in_km').alias('sum_estimated_diameter_min_in_km'))
            .collect()[0]['sum_estimated_diameter_min_in_km'])

        self.assertEqual(input_sum_of_estimated_diameter_min_in_km, expected_sum_of_estimated_diameter_min_in_km)
    

    def test_sum_of_estimated_diameter_max_in_km(self):

        expected_sum_of_estimated_diameter_max_in_km = (
            expected_df
            .agg(sum('estimated_diameter_max_in_km').alias('sum_estimated_diameter_max_in_km'))
            .collect()[0]['sum_estimated_diameter_max_in_km'])
        
        input_sum_of_estimated_diameter_max_in_km = (
            input_df
            .agg(sum('estimated_diameter_max_in_km').alias('sum_estimated_diameter_max_in_km'))
            .collect()[0]['sum_estimated_diameter_max_in_km'])

        self.assertEqual(input_sum_of_estimated_diameter_max_in_km, expected_sum_of_estimated_diameter_max_in_km)


    def test_dimension_count(self):
        expected_df_agg = expected_df.groupBy("date", "neo_reference_id", "name", "nasa_jpl_url", "is_potentially_hazardous_asteroid").count()
        expected_df_agg_md5_hash = hashlib.md5(str(expected_df_agg.collect()).encode('utf-8')).hexdigest()

        input_df_agg = input_df.groupBy("date", "neo_reference_id", "name", "nasa_jpl_url", "is_potentially_hazardous_asteroid").count()
        input_df_agg_md5_hash = hashlib.md5(str(input_df_agg.collect()[0:]).encode('utf-8')).hexdigest()

        self.assertEqual(input_df_agg_md5_hash, expected_df_agg_md5_hash)


if __name__ == '__main__':

    setUp()

    unittest.main()

    tearDown()
