#!/usr/bin/env python3
from kafka import KafkaProducer
from time import sleep
import csv
import json
import os
import sys
import fnmatch
import asyncio
import threading
import kafkaController
#import LogFormatter
import logging

HOST = 'localhost'
TOPIC_NAME = 'weatherstation.sensor'


class MessageModel:
    timestamp: int
    stationCode: str
    temperature: float
    humidity: float
    latitude: str
    longitude: str

    def __init__(self, timestamp, stationCode, temperature, humidity,latitude,longitude):
        self.timestamp = timestamp
        self.stationCode = stationCode
        self.temperature = temperature
        self.humidity = humidity
        self.latitude = latitude
        self.longitude = longitude


def getStationsFilesNames(path):
    station_files = []
    for file in os.listdir(path):
        if fnmatch.fnmatch(file, 'A*.csv'):
            station_files.append(file)
    return station_files


# TODO Obter feedback do envio ou tratar falha
def publishKafka(producer, topic, message):
    try:
        producer.send(topic, message)
    except Exception as e:
        logging.error('FAILED TO SENT: %s : %s  WITH ERROR %s' % (topic, message, str(e)))
    logging.info('sent: %s : %s' % (topic, message))


def readCsvFile(producer, file, speed):
    with open(file, "rt") as source:
        reader = csv.DictReader(source, delimiter=',')
        for row in reader:
            kafkaTopic = 'weatherstation.sensor'
            sensorData = MessageModel(row['timestamp'],
                                      row['stationCode'], row['temp_inst'], row['umid_inst'],row['latitude'],row['longitude'])
            kafkaMessage = json.dumps(sensorData.__dict__)
            publishKafka(
                producer, kafkaTopic, kafkaMessage)
            sleep(3600 / speed)


def main(files_patch, speed):
    logging.error("teste")
    stationFiles = getStationsFilesNames(files_patch)
    if(not kafkaController.checkTopicExists(TOPIC_NAME)):
        kafkaController.createTopic(TOPIC_NAME, len(stationFiles))
    kakfaProducer = kafkaController.connectKafkaProducer(HOST)

    for file in stationFiles:
        
        t = threading.Thread(target=readCsvFile,
                             args=(kakfaProducer, files_patch + '/' + file, speed))
        t.start()

if __name__ == "__main__":
    #fmt = LogFormatter.LogFormatter()
    hdlr = logging.StreamHandler(sys.stdout)

    #hdlr.setFormatter(fmt)
    logging.root.addHandler(hdlr)
    logging.root.setLevel(logging.INFO)
    files_patch = './data'
    speed = input("Insira a velocidade desejada: ")
    try:
        speed = int(speed)
    except Exception as ex:
        print('Invalid integer. Setting speed to 1')
        speed = 1
    main(files_patch, speed)