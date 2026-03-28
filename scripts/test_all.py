import os
import sys
import findspark

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = r"J:\DevCodeApps\hadoop"
os.environ['SPARK_LOCAL_IP'] = "127.0.0.1"

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("MundialAnalysis") \
    .config("spark.sql.shuffle.partitions", "5") \
    .getOrCreate()
sc = spark.sparkContext
print("Parte 1 OK")

# Parte 2
try:
    from rdd_funcs import omit_header, parse_jugador_func
    path_jugadores = "../data/jugadores.csv"
    jugador1 = sc.textFile(path_jugadores, minPartitions=6)
    jugador2 = sc.textFile(path_jugadores, minPartitions=6)
    jugadorTotal_bruto = jugador1.union(jugador2)
    jugadorTotal = jugadorTotal_bruto.filter(omit_header)
    print("Conteo:", jugadorTotal.count())

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

    rdd_jugadores_parsed = jugadorTotal.map(parse_jugador_func)
    jugadores = spark.createDataFrame(rdd_jugadores_parsed, schema_jugadores)
    jugadores.show(5)
    print("Parte 2 OK")
except Exception as e:
    import traceback
    traceback.print_exc()

# Parte 3
try:
    from rdd_funcs import filter_mayor_edad, filter_defensa, a_mayusculas_func
    MayorEdad = jugadorTotal.filter(filter_mayor_edad)
    Jugadores_Defensa = jugadorTotal.filter(filter_defensa)
    jugadorTotal_mayusculas = jugadorTotal.map(a_mayusculas_func)
    print("Muestra 3 registros de MayorEdad:")
    for r in MayorEdad.take(3): print(r)
    print("Parte 3 OK")
except Exception as e:
    import traceback
    traceback.print_exc()

# Parte 4
try:
    equipos = spark.read.csv("../data/equipos.csv", header=True, inferSchema=True)
    partidos = spark.read.csv("../data/partidos.csv", header=True, inferSchema=True)
    estadios = spark.read.csv("../data/estadios.csv", header=True, inferSchema=True)
    torneos = spark.read.json("../data/torneos.json")

    equipos = equipos.withColumnRenamed("nombre", "nombre_equipo")
    estadios = estadios.withColumnRenamed("nombre", "nombre_estadio") \
                       .withColumnRenamed("pais", "pais_estadio")
    torneos = torneos.withColumnRenamed("nombre", "nombre_torneo")

    condicion_partidos = (equipos.equipo_id == partidos.equipo_local_id) | \
                         (equipos.equipo_id == partidos.equipo_visitante_id)

    mundial_completo_no_partition = jugadores.join(equipos, "equipo_id", "inner") \
        .join(partidos, condicion_partidos, "left") \
        .join(estadios, "estadio_id", "left") \
        .join(torneos, "torneo_id", "left")
    
    mundial_completo_no_partition.show(5)
    print("Parte 4 OK")
except Exception as e:
    import traceback
    traceback.print_exc()

# Parte 5 & 6
try:
    mundial_completo = mundial_completo_no_partition.repartition(5)
    print(f"Num partitions: {mundial_completo.rdd.getNumPartitions()}")
    print("Parte 5 OK")
except Exception as e:
    import traceback
    traceback.print_exc()

# Parte 7
try:
    mundial_completo = mundial_completo \
        .withColumn("IMC", F.col("peso") / F.pow(F.col("altura") / 100.0, 2)) \
        .withColumn("Categoria_Edad",
            F.when(F.col("edad") < 25, "Joven")
             .when((F.col("edad") >= 25) & (F.col("edad") <= 32), "Experimentado")
             .otherwise("Veterano")
        ) \
        .withColumn("Resultado_Partido",
            F.when(F.col("goles_local") > F.col("goles_visitante"), "Victoria Local")
             .when(F.col("goles_visitante") > F.col("goles_local"), "Victoria Visitante")
             .otherwise("Empate")
        )
    mundial_completo.select("nombre_jugador", "edad", "IMC", "Categoria_Edad", "Resultado_Partido").show(5)
    print("Parte 7 OK")
except Exception as e:
    import traceback
    traceback.print_exc()

print("Prueba finalizada.")
