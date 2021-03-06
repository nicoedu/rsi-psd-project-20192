#!/usr/bin/env python
from kafka import KafkaProducer, KafkaConsumer
import requests
import json
import nearestAlgorithm as near


# variaveis
topicprefix = 'nearest'
request = topicprefix + '.request'
reply = topicprefix + '.reply'
hostKafka = 'kafka:29092'
hostThingsboard = 'thingsboard:9090'


def getThingsboardAuthToken():
    resp = requests.post('http://'+hostThingsboard+'/api/auth/login', json={
                         "username": "tenant@thingsboard.org", "password": "tenant"}, headers={"Accept": "application/json"})
    responseDict = resp.json()
    return responseDict['token']


def getStationList():
    authToken = getThingsboardAuthToken()
    header = {"Accept": "application/json",
              "X-Authorization": "Bearer "+authToken}
    resp = requests.get('http://'+hostThingsboard +
                        '/api/tenant/devices?type=SENSOR&limit=9999', headers=header)
    responseDict = resp.json()
    return (list(map(lambda x: (x['id']['id'], list(map(float, x['additionalInfo'].split(';')))), responseDict['data'])))


def main():
    consumer = KafkaConsumer(request, bootstrap_servers=hostKafka,
                             value_deserializer=lambda v: json.loads(v.decode('utf-8')), enable_auto_commit=False, auto_offset_reset='latest')
    producer = KafkaProducer(bootstrap_servers=hostKafka,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'), acks=1, retries=3, max_in_flight_requests_per_connection=1, batch_size=1000000)
    for message in consumer:
        latlngdict = message.value
        nearestStations = near.nearest5(latlngdict['latitude'],
                                        latlngdict['longitude'], getStationList())
        print("Estaçoes proximas: ", nearestStations)
        future = producer.send(reply, nearestStations)
        future.get(timeout=10)


if __name__ == '__main__':
    main()
