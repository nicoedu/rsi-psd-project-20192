#!/usr/bin/env python
#!/usr/bin/env python
from kafka import KafkaProducer, KafkaConsumer
import requests
import json
import interpolationAlgorithm as idw


# variaveis
topicprefix = 'interpolation'
request = topicprefix + '.request'
reply = topicprefix + '.reply'
hostKafka = 'localhost:9092'
hostThingsboard = 'localhost:9090'


def getStationList():
    authToken = getThingsboardAuthToken()
    header = {"Accept": "application/json",
              "X-Authorization": "Bearer "+authToken}
    resp = requests.get('http://'+hostThingsboard +
                        '/api/tenant/devices?limit=9999&textSearch=A', headers=header)
    responseDict = resp.json()
    return (list(map(lambda x: (x['id']['id'], list(map(float, x['additionalInfo'].split(';')))), responseDict['data'])))


def main():
    consumer = KafkaConsumer(request, bootstrap_servers=hostKafka,
                             value_deserializer=lambda v: json.loads(v.decode('utf-8')), enable_auto_commit=False, auto_offset_reset='latest')
    producer = KafkaProducer(bootstrap_servers=hostKafka,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'), acks=1, retries=3, max_in_flight_requests_per_connection=1, batch_size=1000000)
    for message in consumer:
        latlngdict = message.value
        nearestStations = idw.nearest5(latlngdict['latitude'],
                                       latlngdict['longitude'], getStationList())
        producer.send(reply, nearestStations)


if __name__ == '__main__':
    main()
