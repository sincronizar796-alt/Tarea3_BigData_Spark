from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count


def main():
    spark = SparkSession.builder.appName("ProcesamientoBatchSensores").getOrCreate()

    # Cargar archivo CSV
    df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

    print("Esquema del dataset:")
    df.printSchema()

    print("Primeros registros:")
    df.show(10)

    # Limpieza básica
    df_clean = df.dropna()

    print("Cantidad de registros luego de limpieza:")
    print(df_clean.count())

    # Estadísticas generales
    df_clean.select(
        avg("temperature").alias("promedio_temperatura"),
        max("temperature").alias("max_temperatura"),
        min("temperature").alias("min_temperatura"),
        avg("humidity").alias("promedio_humedad")
    ).show()

    # Agrupación por sensor
    df_clean.groupBy("sensor_id").agg(
        avg("temperature").alias("temp_promedio"),
        avg("humidity").alias("humedad_promedio"),
        count("*").alias("cantidad_registros")
    ).show()

    spark.stop()


if __name__ == "__main__":
    main()
