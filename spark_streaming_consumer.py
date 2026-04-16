from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType


def main():
    spark = SparkSession.builder \
        .appName("SensorDataAnalysis") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Esquema de entrada
    schema = StructType([
        StructField("sensor_id", IntegerType(), True),
        StructField("temperature", FloatType(), True),
        StructField("humidity", FloatType(), True),
        StructField("timestamp", LongType(), True)
    ])

    # Leer desde Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sensor_data") \
        .option("startingOffsets", "latest") \
        .load()

    # Parsear JSON
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Convertir timestamp Unix a timestamp legible
    parsed_df = parsed_df.withColumn(
        "event_time",
        to_timestamp(col("timestamp"))
    )

    # Agrupar por ventana de 1 minuto y sensor
    windowed_stats = parsed_df.groupBy(
        window(col("event_time"), "1 minute"),
        col("sensor_id")
    ).avg("temperature", "humidity")

    # Mostrar resultados por consola
    query = windowed_stats.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
