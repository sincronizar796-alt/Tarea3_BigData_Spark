# Tarea 3 - Procesamiento de Datos con Apache Spark

## Descripción
Este proyecto corresponde a la Tarea 3 del curso Big Data. Presenta una propuesta de procesamiento de datos con Apache Spark en dos enfoques: procesamiento batch y procesamiento en tiempo real con Apache Kafka y Spark Streaming.

## Archivos del proyecto

- `batch_processing.py`: script de procesamiento batch sobre un archivo CSV con datos de sensores.
- `kafka_producer.py`: productor Kafka que genera datos simulados de sensores.
- `spark_streaming_consumer.py`: consumidor con Spark Streaming que procesa los datos recibidos desde Kafka.

## Descripción general del funcionamiento

### Procesamiento batch
El script `batch_processing.py` carga un archivo CSV llamado `sensor_data.csv`, realiza una limpieza básica eliminando valores nulos y calcula estadísticas como promedio, máximo y mínimo de temperatura y humedad. También agrupa los datos por sensor.

### Procesamiento en tiempo real
El script `kafka_producer.py` simula sensores y envía datos continuamente al topic `sensor_data` en Kafka. Luego, el script `spark_streaming_consumer.py` consume esos datos en tiempo real, los agrupa por ventanas de tiempo y calcula promedios por sensor.

## Requisitos
- Python 3
- Apache Spark
- Apache Kafka
- Librería `kafka-python`
- Archivo `sensor_data.csv` para el procesamiento batch

## Ejecución general

### Batch
```bash
spark-submit batch_processing.py
