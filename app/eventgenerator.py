#!/usr/bin/env python3
from kafka import KafkaProducer
from time import sleep
from datetime import datetime
import time
import csv
import json

# Create an instance of the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: str(v).encode('utf-8'))


FILE_PATH = ''
DELIMITER = ''


def publishKafka(topic, message):
    global producer
    producer.send(topic, message)
    print('sent: %s : %s' % (topic, message))

# TODO implementar pelo modelo de csv


def readCsvFile(file, speed):
    with open(file, "rb") as source:
        reader = csv.reader(source, delimiter=DELIMITER)
        for row in reader:
            message = {"ts": row[0], }
            publishKafka(row[1]+'.sensor',)
            sleep(3600000 * speed)


# Call the producer.send method with a producer-record
print("ctrl+c to stop...")
