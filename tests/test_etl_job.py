
from audioop import avg
import unittest
import os
import sys

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

    # expected_df: Data we have tested theor schema and content
    expected_df_path = os.path.join(cfg.airflow["project_path"], 'tests/test_data', 'nasa_neo_api_response_27.07.2022-31.07.2022.json')
    expected_df = spark.read.option("multiline", "true").json(expected_df_path)

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
            .collect()[0]
            ['average_lunar_distance'])
        
        input_average_lunar_distance = (
            input_df
            .agg(avg('lunar_distance').alias('average_lunar_distance'))
            .collect()[0]
            ['average_lunar_distance'])

        self.assertEqual(input_average_lunar_distance, expected_average_lunar_distance)

    def test_sum_of_velocity_in_km_per_hour(self):
        expected_sum_of_velocity_in_km_per_hour = (
            expected_df
            .agg(sum('velocity_in_km_per_hour').alias('sum_velocity_in_km_per_hour'))
            .collect()[0]
            ['sum_velocity_in_km_per_hour'])
        
        input_sum_of_velocity_in_km_per_hour = (
            input_df
            .agg(sum('velocity_in_km_per_hour').alias('sum_velocity_in_km_per_hour'))
            .collect()[0]
            ['sum_velocity_in_km_per_hour'])
        
        self.assertEqual(input_sum_of_velocity_in_km_per_hour, expected_sum_of_velocity_in_km_per_hour)


    def test_sum_of_estimated_diameter_min_in_km(self):

        expected_sum_of_estimated_diameter_min_in_km = (
            expected_df
            .agg(sum('estimated_diameter_min_in_km').alias('sum_estimated_diameter_min_in_km'))
            .collect()[0]
            ['sum_estimated_diameter_min_in_km'])
        
        input_sum_of_estimated_diameter_min_in_km = (
            input_df
            .agg(sum('estimated_diameter_min_in_km').alias('sum_estimated_diameter_min_in_km'))
            .collect()[0]
            ['sum_estimated_diameter_min_in_km'])

        self.assertEqual(input_sum_of_estimated_diameter_min_in_km, expected_sum_of_estimated_diameter_min_in_km)
    

    def test_sum_of_estimated_diameter_max_in_km(self):

        expected_sum_of_estimated_diameter_max_in_km = (
            expected_df
            .agg(sum('estimated_diameter_max_in_km').alias('sum_estimated_diameter_max_in_km'))
            .collect()[0]
            ['sum_estimated_diameter_max_in_km'])
        
        input_sum_of_estimated_diameter_max_in_km = (
            input_df
            .agg(sum('estimated_diameter_max_in_km').alias('sum_estimated_diameter_max_in_km'))
            .collect()[0]
            ['sum_estimated_diameter_max_in_km'])

        self.assertEqual(input_sum_of_estimated_diameter_max_in_km, expected_sum_of_estimated_diameter_max_in_km)


if __name__ == '__main__':

    setUp()

    unittest.main()

    tearDown()
