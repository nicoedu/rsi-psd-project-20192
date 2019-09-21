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
TOPIC_NAME = 'weatherstation.sensor'


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


# TODO Obter feedback do envio ou tratar falha
def publishKafka(producer, topic, message):
    temp = producer.send(topic, message)
    print('sent: %s : %s' % (topic, message))


def readCsvFile(producer, file, speed):
    with open(file, "rt") as source:
        reader = csv.DictReader(source, delimiter=',')
        for row in reader:
            kafkaTopic = 'weatherstation.sensor'
            sensorData = MessageModel(row['timestamp'],
                                      row['stationCode'], row['temp_inst'], row['umid_inst'])
            kafkaMessage = json.dumps(sensorData.__dict__)
            publishKafka(
                producer, kafkaTopic, kafkaMessage)
            sleep(3600 / speed)


# TODO Tratar exception de thread
def main(files_patch, speed):
    stationFiles = getStationsFilesNames(files_patch)
    if(not kafkaController.checkTopicExists(TOPIC_NAME)):
        kafkaController.createTopic(TOPIC_NAME, len(stationFiles))
    kakfaProducer = kafkaController.connectKafkaProducer(HOST)

    for file in stationFiles:
        t = threading.Thread(target=readCsvFile,
                             args=(kakfaProducer, files_patch + '/' + file, speed))
        t.start()


if __name__ == "__main__":
    files_patch = './data'
    speed = input("Insira a velocidade desejada: ")
    try:
        speed = int(speed)
    except Exception as ex:
        print('Invalid integer. Setting speed to 1')
        speed = 1
    main(files_patch, speed)
