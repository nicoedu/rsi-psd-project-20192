#!/usr/bin/env python3
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.client import KafkaClient, KafkaUnavailableError
from time import sleep
import logging
HOSTPORT = 'localhost:9092'


def connectKafkaProducer():
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=HOSTPORT,
                                 value_serializer=lambda v: str(v).encode('utf-8'), acks=1, retries=3, max_in_flight_requests_per_connection=1)
    except Exception as ex:
        logging.error(ex)
    finally:
        return producer


def createTopic(topic_name, partition_number):
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=HOSTPORT, client_id='test')
        topic_list = []
        topic_list.append(NewTopic(name=topic_name,
                                   num_partitions=partition_number, replication_factor=1))
        fs = admin_client.create_topics(
            new_topics=topic_list, validate_only=False)
        for topic, f in fs.items():
            try:
                f.result()  # O resultado virá vazio.
                result = "Topico {} criado".format(topic)
                logging.info(result)
                print(result)
            except Exception as e:
                logging.error("Falha ao criar topico {}: {}".format(topic, e))
                print("FALHA")
    except Exception as exc:
        logging.error(exc)


def checkTopicExists(topic_name):
    try:
        kafkaClient = KafkaClient(bootstrap_servers=HOSTPORT)
        metadata = kafkaClient.poll()
        server_topics = list(x[1] for x in metadata[0].topics)
        kafkaClient.close()
        return topic_name in server_topics
    except IndexError:
        return False
    except KafkaUnavailableError:
        logging.error("Kafka não está disponivel")
