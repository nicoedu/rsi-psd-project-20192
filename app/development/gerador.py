#!/usr/bin/env python
from kafka import KafkaProducer
import json
from time import sleep
from datetime import datetime
from random import randrange
import fnmatch

# Create an instance of the Kafka producer
# topic_list = []
# topic_list.append(NewTopic(name='sensor',
#                            num_partitions=1, replication_factor=1))
# fs = admin_client.create_topics(
#     new_topics=topic_list, validate_only=False)
# for topic, f in fs.items():
#     try:
#         f.result()  # O resultado virá vazio.
#         result = "Topico {} criado".format(topic)
#         print(result)
#     except Exception as e:
#         print("Falha ao criar topico {}: {}".format(topic, e))

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: str(v).encode('utf-8'))

producer.send("sensor", {'temp': 0})


def get_random_sensor_data():
    return {'temperature': randrange(-50, 51), 'humidity': randrange(101)}


# while True:
#     data = get_random_sensor_data()
#     producer.send('sensor', data)
#     print('sent: A302.sensor:' + str(data))
#     sleep(5)
