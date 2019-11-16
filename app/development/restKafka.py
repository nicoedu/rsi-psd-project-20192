from kafka import KafkaProducer, KafkaConsumer
import json
import logging


class kafkaRest():

    def __init__(self, host, topicPreFix, group='group1'):
        self.host = host
        self.producer = None
        self.consumer = None
        self.topicPreFix = topicPreFix
        self.group = group

    def connectKafkaProducer(self):
        if self.producer is not None and not self.producer.bootstrap_connected():
            return self.producer
        try:
            self.producer = None
            self.producer = KafkaProducer(bootstrap_servers=self.host + ':9092',
                                          value_serializer=lambda v: str(v).encode('utf-8'), acks=1, retries=3, max_in_flight_requests_per_connection=1, batch_size=1000000)
        except Exception as ex:
            logging.error(ex)

    def post(self, data):
        try:
            sender = self.producer.send(
                self.topicPreFix+'.request', value=data)
            record_metadata = sender.get(timeout=10)
        except Exception as e:
            print(e)

    def connectKafkaConsumer(self):
        if self.consumer is not None and not self.consumer.bootstrap_connected():
            return None
        try:
            self.consumer = None
            self.consumer = KafkaConsumer(self.topicPreFix + '.request', bootstrap_servers=self.host + ':9092',
                                          value_deserializer=lambda v: str(v).encode('utf-8'), enable_auto_commit=False, auto_offset_reset='latest', group_id=self.group)
        except Exception as ex:
            logging.error(ex)

    def get(self):
        try:
            for message in self.consumer:
                print(str(message.value))
        except Exception as ex:
            logging.error(ex)
