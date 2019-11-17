#!/usr/bin/env python
from kafka import KafkaProducer, KafkaConsumer
import json
import nearestAlgorithm as near

# variaveis
topicprefix = 'nearest'
request = topicprefix + '.request'
reply = topicprefix + '.reply'
kafkaHost = 'localhost:9092'
kafkaThingsboard = 'localhost:9090'


if __name__ == '__main__':
    consumer = KafkaConsumer(request, bootstrap_servers=kafkaHost,
                             value_deserializer=lambda v: json.loads(v.decode('utf-8')), enable_auto_commit=False, auto_offset_reset='latest')
    producer = KafkaProducer(bootstrap_servers=kafkaHost,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'), acks=1, retries=3, max_in_flight_requests_per_connection=1, batch_size=1000000)
    for message in consumer:
        print(message.value)
        producer.send(reply, {'teste': 'teste'})
        # call producer reply
