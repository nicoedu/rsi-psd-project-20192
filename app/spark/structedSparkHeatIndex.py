"""
    
    bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from kafka import KafkaProducer
import json


class ForeachWriter:
    producer = None

    #!! Checar se retorno alternativo funciona
    def open(self, partition_id, epoch_id):
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                      value_serializer=lambda v: str(v).encode('utf-8'), acks=1, retries=3, max_in_flight_requests_per_connection=1)
        # return self.producer.bootstrap_connected()
        return True

    def process(self, row):
        temperatureCelcius = temperatureCelsiusToFahrenheit(
            float(row.temperature))
        heatIndexFahreheint = calcHeatIndex(
            temperatureCelcius, float(row.humidity))
        heatIndexCelcius = temperatureFahrenheitToCelsius(heatIndexFahreheint)
        data = {"timestamp": row.timestamp,
                "stationCode": row.stationCode, "heatIndex": heatIndexCelcius}
        print(json.dumps(data))
        self.producer.send('weatherstation.heatindex', json.dumps(data))

    def close(self, error):
        print(error)
        self.producer.close()


def temperatureFahrenheitToCelsius(temp):
    return (temp - 32)/1.8


def temperatureCelsiusToFahrenheit(temp):
    return 1.8*temp + 32


def calcHeatIndex(temperature, relHumidity):
    hi = (1.1 * temperature) - 10.3 + (0.047 * relHumidity)

    if hi < 80:
        return hi
    hi = (-42.379 + (2.04901523 * temperature) + (10.14333127 * relHumidity)
          - (0.22475541 * temperature * relHumidity)
          - (0.00683783 * (temperature**2))
          - (0.05481717 * (relHumidity**2))
          + (0.00122874 * (temperature**2) * relHumidity)
          + (0.00085282 * temperature * (relHumidity**2))
          - (0.00000199 * (temperature**2) * (relHumidity**2)))

    if temperature <= 112 and temperature >= 80 and relHumidity <= 13:
        return hi - (3.25-(0.25*relHumidity)) * ((17-abs(temperature-95)/17))**0.5
    elif temperature <= 87 and temperature >= 80 and relHumidity > 85:
        return hi + 0.02*(relHumidity - 85)*(87-temperature)
    else:
        return hi


if __name__ == "__main__":

    bootstrapServers = 'localhost:9092'
    subscribeType = 'subscribe'
    topics = 'weatherstation.sensor'

    spark = SparkSession\
        .builder\
        .appName("sparkHeatIndexCalculator")\
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # Create DataSet representing the stream of input lines from kafka
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, topics)\
        .load()

    schema = StructType()\
        .add('timestamp', StringType())\
        .add('stationCode', StringType())\
        .add('temperature', StringType())\
        .add('humidity', StringType())\
        .add('latitude', StringType())\
        .add('longitude', StringType())

    lines = lines.select(from_json(col("value").cast("string"), schema=schema).alias("data"))\
        .select("data.*")\
        .select('timestamp', 'stationCode', 'temperature', 'humidity')

    query = lines\
        .writeStream\
        .foreach(ForeachWriter())\
        .start()

    query.awaitTermination()
