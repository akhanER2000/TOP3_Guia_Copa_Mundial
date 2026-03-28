import sys

try:
    import findspark
    findspark.init()
except Exception as e:
    print(f"Error con findspark.init(): {e}")

try:
    from pyspark.sql import SparkSession
    print("pyspark imported correctly.")
    spark = SparkSession.builder.appName("Test").getOrCreate()
    print("SparkSession created successfully.")
except Exception as e:
    print(f"Error con pyspark: {e}")
