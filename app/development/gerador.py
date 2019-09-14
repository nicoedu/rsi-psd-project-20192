#!/usr/bin/env python
from kafka import KafkaProducer
import json
from time import sleep
from datetime import datetime
from random import randrange

# Create an instance of the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def get_random_sensor_data():
    return {'temperature': randrange(-50, 51), 'humidity': randrange(101)}


while True:
    producer.send('sensor', get_random_sensor_data())
    sleep(5)
