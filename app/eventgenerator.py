#!/usr/bin/env python3
from kafka import KafkaProducer
from time import sleep
import csv
import json
import os
import fnmatch
import asyncio
import threading
import kafkaController

HOST = 'localhost'


class MessageModel:
    timestamp: int
    stationCode: str
    temperature: float
    humidity: float

    def __init__(self, timestamp, stationCode, temperature, humidity):
        self.timestamp = timestamp
        self.stationCode = stationCode
        self.temperature = temperature
        self.humidity = humidity


def getStationsFilesNames(path):
    station_files = []
    for file in os.listdir(path):
        if fnmatch.fnmatch(file, 'A*.csv'):
            station_files.append(file)
    return station_files


async def publishKafka(producer, topic, message):
    await producer.send(topic, message)
    print('sent: %s : %s' % (topic, message))


def readCsvFile(producer, file, speed):
    with open(file, "rt") as source:
        reader = csv.DictReader(source, delimiter=',')
        loop = asyncio.get_event_loop()
        for row in reader:
            kafkaTopic = row['stationCode']+'.sensor'
            sensorData = MessageModel(row['timestamp'],
                                      row['stationCode'], row['temp_inst'], row['umid_inst'])
            kafkaMessage = json.dumps(sensorData.__dict__)
            loop.run_until_complete(publishKafka(
                producer, kafkaTopic, kafkaMessage))
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
    kakfaProducer = kafkaController.connectKafkaProducer(HOST)

    for file in getStationsFilesNames(files_patch):
        t = threading.Thread(target=readCsvFile,
                             args=(kakfaProducer, file, speed))
        t.start()
