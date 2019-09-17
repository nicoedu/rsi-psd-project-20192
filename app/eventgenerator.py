#!/usr/bin/env python3
from kafka import KafkaProducer
from time import sleep
from datetime import datetime
import time
import csv
import json
import os
import fnmatch
import asyncio


def getStationsFilesNames(path):
    station_files = []
    for file in os.listdir(path):
        if fnmatch.fnmatch(file, 'A*.csv'):
            station_files.append(file)
    return station_files


def connect_kafka_producer():
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                 value_serializer=lambda v: str(v).encode('utf-8'))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(ex)
    finally:
        return producer


def publishKafka(producer, topic, message):
    producer.send(topic, message)
    print('sent: %s : %s' % (topic, message))


async def readCsvFile(producer, file, speed):
    with open(file, "rt") as source:
        reader = csv.DictReader(source, delimiter=',')
        for row in reader:
            message = json.dumps({'ts': row['timestamp'], 'values': {
                                 'temperature': row['temp_inst'], 'humidity': row['umid_inst']}})
            publishKafka(producer, row['stationCode']+'.sensor', message)
            sleep(3600 / speed)


# TODO fazer a leitura dos diferentes csv em paralelo
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    files_patch = './data'
    speed = input("Insira a velocidade desejada: ")
    try:
        speed = int(speed)
    except Exception as ex:
        print('Invalid integer. Setting speed to 1')
        speed = 1
    producer = connect_kafka_producer()
    for file in getStationsFilesNames(files_patch):
        asyncio.ensure_future(readCsvFile(
            producer, files_patch+'/'+file, int(speed)))
    loop.run_forever()
