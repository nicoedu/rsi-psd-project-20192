from kafka import KafkaProducer, KafkaClient, KafkaAdminClient, NewTopic
from time import sleep


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


def createTopic(topic_name):

    # Check if topic exists
    kafkaClient = KafkaClient(bootstrap_servers='localhost:9092')
    server_topics = kafkaClient.topic_partitions
    if topic_name not in server_topics:
        admin_client = KafkaAdminClient(
            bootstrap_servers="localhost:9092", client_id='test')
        topic_list = []
        topic_list.append(NewTopic(name="example_topic",
                                   num_partitions=13, replication_factor=2))
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
