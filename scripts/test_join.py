import os, sys, findspark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = r"J:\DevCodeApps\hadoop"
os.environ['SPARK_LOCAL_IP'] = "127.0.0.1"

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("test").getOrCreate()

schema_jugadores = StructType([
    StructField("jugador_id", IntegerType(), True),
    StructField("nombre_jugador", StringType(), True),
    StructField("apellido", StringType(), True),
    StructField("edad", IntegerType(), True),
    StructField("altura", IntegerType(), True),
    StructField("peso", IntegerType(), True),
    StructField("posicion", StringType(), True),
    StructField("equipo_id", IntegerType(), True)
])

jugadores = spark.read.csv("../data/jugadores.csv", header=False, schema=schema_jugadores).repartition(6)
equipos = spark.read.csv("../data/equipos.csv", header=True, inferSchema=True).withColumnRenamed("nombre", "nombre_equipo")
partidos = spark.read.csv("../data/partidos.csv", header=True, inferSchema=True)
estadios = spark.read.csv("../data/estadios.csv", header=True, inferSchema=True).withColumnRenamed("nombre", "nombre_estadio").withColumnRenamed("pais", "pais_estadio")
torneos = spark.read.json("../data/torneos.json").withColumnRenamed("nombre", "nombre_torneo")

print("Part 4 start")
try:
    condicion_partidos = (F.col("equipo_id") == F.col("equipo_local_id")) | (F.col("equipo_id") == F.col("equipo_visitante_id"))

    mundial_completo_no_partition = jugadores.join(equipos, "equipo_id", "inner") \
        .join(partidos, condicion_partidos, "left") \
        .join(estadios, "estadio_id", "left") \
        .join(torneos, "torneo_id", "left")
    print("OK", mundial_completo_no_partition.count())
except Exception as e:
    import traceback
    traceback.print_exc()
