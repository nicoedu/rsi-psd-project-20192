from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.client import KafkaClient
from time import sleep


# TODO nomear corretamente a Exception
def connectKafkaProducer(host):
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=host + ':9092',
                                 value_serializer=lambda v: str(v).encode('utf-8'))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(ex)
    finally:
        return producer


# TODO Tratar exception de falha ao conectar
def createTopic(topic_name, partition_number):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092", client_id='test')
    topic_list = []
    topic_list.append(NewTopic(name=topic_name,
                               num_partitions=partition_number, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)


# TODO Tratar exception de falha ao conectar
def checkTopicExists(topic_name):
    kafkaClient = KafkaClient(bootstrap_servers='localhost:9092')
    metadata = kafkaClient.poll()
    server_topics = list(x[1] for x in metadata[0].topics)
    kafkaClient.close()
    return topic_name in server_topics
