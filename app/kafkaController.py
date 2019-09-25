from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.client import KafkaClient, KafkaUnavailableError
from time import sleep
import logging


# TODO nomear corretamente a Exception
def connectKafkaProducer(host):
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=host + ':9092',
                                 value_serializer=lambda v: str(v).encode('utf-8'))
    except Exception as ex:
        logging.error(ex)
    finally:
        return producer


# TODO Tratar exception de falha ao conectar
def createTopic(topic_name, partition_number):
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers="localhost:9092", client_id='test')
        topic_list = []
        topic_list.append(NewTopic(name=topic_name,
                                num_partitions=partition_number, replication_factor=1))
        fs = admin_client.create_topics(new_topics=topic_list, validate_only=False)
        for topic, f in fs.items():
            try:
                f.result()  # O resultado virá vazio.
                result = "Topico {} criado".format(topic)
                logging.info(result)
            except Exception as e:
                logging.error("Falha ao criar topico {}: {}".format(topic, e))
    except Exception as exc:
        logging.error(exc)


# TODO Tratar exception de falha ao conectar
def checkTopicExists(topic_name):
    try:
        kafkaClient = KafkaClient(bootstrap_servers='localhost:9092')
        metadata = kafkaClient.poll()
        server_topics = list(x[1] for x in metadata[0].topics)
        kafkaClient.close()
        return topic_name in server_topics
    except KafkaUnavailableError:
        logging.error("Kafka não está disponivel")
