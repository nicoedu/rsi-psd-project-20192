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
    
    bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 /home/rsi-psd-vm/Documents/rsi-psd-codes/psd/2019-2/pratica-05/structured_kafka_wordcount_tb2.py localhost:9092 subscribe meu-topico-legal
"""
from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split, from_json, col
from pyspark.sql.types import *
import requests
import json


THINGSBOARD_HOST = '127.0.0.1'
THINGSBOARD_PORT = '9090'
# ACCESS_TOKEN = 'V7jlIzhvXp2KcvveaVrz'
ACCESS_TOKEN = 'A1_TEST_TOKEN'
url = 'http://' + THINGSBOARD_HOST + ':' + THINGSBOARD_PORT + '/api/v1/' + ACCESS_TOKEN + '/telemetry'
headers = {}
headers['Content-Type'] = 'application/json'

def processRow(row):
    print(row)
    # row_data = { row.word : row.__getitem__("count")}
    # requests.post(url, json=row_data)

if __name__ == "__main__":

    bootstrapServers = 'localhost:9092'
    subscribeType = 'subscribe'
    topics = 'weatherstation.sensor'

    spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()


    fields = [StructField('timestamp', IntegerType(), True), \
    StructField('stationCode', StringType(), True), \
    StructField('temperature', FloatType(), True), \
    StructField('humidity', FloatType(), True), \
    StructField('latitude', StringType(), True), \
    StructField('longitude', StringType(), True)]
    schema = StructType(fields)
    
    # Create DataSet representing the stream of input lines from kafka
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, topics)\
        .load()
        
    # schema = StructType()\
    #     .add('timestamp', IntegerType())\
    #     .add('stationCode', StringType())\
    #     .add('temperature', FloatType())\
    #     .add('humidity', FloatType())\
    #     .add('latitude', StringType())\
    #     .add('longitude', StringType())
        

        
    lines = lines.select(from_json(col("value").cast("string"),schema=schema))
    
    # Split the lines into words
    # words = lines.select(
    #     # explode turns each item in an array into a separate row
    #     explode(
    #         split(lines.value, ' ')
    #     ).alias('word')
    # )
    

    # Generate running word count
    #wordCounts = words.groupBy('word').count()
    #words = words.groupBy('word')

    # Start running the query that prints the running counts to the console
    # query = wordCounts\
    #     .writeStream\
    #     .outputMode('complete')\
    #     .format('console')\
    #     .start()

    # query = lines\
    #      .writeStream\
    #      .outputMode('complete')\
    #      .foreach(processRow)\
    #      .start()
    
    query = lines.writeStream.format("console").start()


    query.awaitTermination()
