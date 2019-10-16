#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
 Consumes messages from one or more topics in Kafka and does wordcount.
 Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
   <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
   comma-separated list of host:port.
   <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
   'subscribePattern'.
   |- <assign> Specific TopicPartitions to consume. Json string
   |  {"topicA":[0,1],"topicB":[2,4]}.
   |- <subscribe> The topic list to subscribe. A comma-separated list of
   |  topics.
   |- <subscribePattern> The pattern used to subscribe to topic(s).
   |  Java regex string.
   |- Only one of "assign, "subscribe" or "subscribePattern" options can be
   |  specified for Kafka source.
   <topics> Different value format depends on the value of 'subscribe-type'.

 Run the example
    `$ bin/spark-submit examples/src/main/python/sql/streaming/structured_kafka_wordcount.py \
    host1:port1,host2:port2 subscribe topic1,topic2'
    
    bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3
"""
from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split, from_json, col
from pyspark.sql.types import *
from kafka import KafkaProducer
import requests
import json


class ForeachWriter:
    producer = None
    def open(self, partition_id, epoch_id):
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                 value_serializer=lambda v: str(v).encode('utf-8'), acks=1, retries=3, max_in_flight_requests_per_connection=1)
        return True

    def process(self, row):
        temperatureCelcius = temperatureCelsiusToFahrenheit(float(row.temperature))
        heatIndexFahreheint = calcHeatIndex(
        temperatureCelcius, float(row.humidity))
        heatIndexCelcius = temperatureFahrenheitToCelsius(heatIndexFahreheint)
        data = {"timestamp":row.timestamp,"stationCode":row.stationCode,"heatIndex":heatIndexCelcius}
        print(json.dumps(data))
        self.producer.send('weatherstation.heatindex',json.dumps(data))

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
