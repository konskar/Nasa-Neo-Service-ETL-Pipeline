
import unittest
import os
import sys

from pyspark.sql import SparkSession

file_path = os.path.dirname( __file__ )

config_dir =  os.path.abspath(os.path.join(file_path, '..', 'configs'))
jobs_dir =  os.path.abspath(os.path.join(file_path, '..', 'jobs'))

sys.path.insert(1, config_dir)
sys.path.insert(1, jobs_dir)

import etl_config as cfg
import functions as f


spark = SparkSession \
    .builder \
    .appName(cfg.spark["app_name"]) \
    .master('local[*]') \
    .getOrCreate()

path = os.path.join(cfg.airflow["project_path"], 'tests/test_data', 'nasa_neo_api_response_27.07.2022-31.07.2022.json')

expected_data_df = spark.read.option("multiline", "true").json(path)

print('expected_data_df rows: ', expected_data_df.count())

f.collect_api_data(start_date= '2022-07-27', end_date='2022-07-31')

input_df = spark.read.option("multiline", "true").json(cfg.absolute_paths["json_abs_path"])

print('input_df rows: ', input_df.count())


# expected_data_df.printSchema()

# expected_data_df.show()

spark.stop()

# # test suite
# class TestAPI(unittest.TestCase):

# 	# test case
#     def test_add_numbers_positive(self):
#         a = 4
#         b = 2
#         expected_result = 6
#         result = fun.add_numbers(a, b)
#         self.assertEqual(result, expected_result, "Should be 6")


# f.collect_api_data(start_date= '2022-07-27', end_date='2022-07-27')
