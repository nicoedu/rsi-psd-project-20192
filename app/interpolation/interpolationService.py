#!/usr/bin/env python
from kafka import KafkaProducer, KafkaConsumer
import requests
import json
import interpolationAlgorithm as idw


# variaveis
topicprefix = 'interpolation'
request = topicprefix + '.request'
reply = topicprefix + '.reply'
hostKafka = 'kafka:29092'
hostThingsboard = 'thingsboard:9090'


def getThingsboardAuthToken():
    resp = requests.post('http://'+hostThingsboard+'/api/auth/login', json={
                         "username": "tenant@thingsboard.org", "password": "tenant"}, headers={"Accept": "application/json"})
    responseDict = resp.json()
    return responseDict['token']


def getHeatIndex(distanceDict):
    authToken = getThingsboardAuthToken()
    header = {"Accept": "application/json",
              "X-Authorization": "Bearer "+authToken}
    heatIndexList = []
    for deviceId in distanceDict.keys():
        resp = requests.get('http://'+hostThingsboard + '/api/plugins/telemetry/DEVICE/' + deviceId + '/values/timeseries?keys=heatIndex',
                            headers=header)
        heatIndexList.append((distanceDict[deviceId], float(
            resp.json()['heatIndex'][0]['value'])))
    return heatIndexList


def main():
    consumer = KafkaConsumer(request, bootstrap_servers=hostKafka,
                             value_deserializer=lambda v: json.loads(v.decode('utf-8')), enable_auto_commit=False, auto_offset_reset='latest')
    producer = KafkaProducer(bootstrap_servers=hostKafka,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'), acks=1, retries=3, max_in_flight_requests_per_connection=1, batch_size=1000000)

    for message in consumer:
        valor = idw.interpolateHI(getHeatIndex(message.value))
        print("Valor obtido na interpolacao: ", valor)
        future = producer.send(reply, valor)
        future.get(timeout=10)


if __name__ == '__main__':
    main()
