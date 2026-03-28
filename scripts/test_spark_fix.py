import sys
import os

os.environ["PYSPARK_PYTHON"] = f'"{sys.executable}"'
os.environ["PYSPARK_DRIVER_PYTHON"] = f'"{sys.executable}"'

try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("Test").getOrCreate()
    print("SparkSession created safely!")
except Exception as e:
    print(f"Error: {e}")
