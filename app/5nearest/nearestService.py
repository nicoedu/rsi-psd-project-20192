#!/usr/bin/env python
from kafka import KafkaProducer, KafkaConsumer
import json
import nearestAlgorithm as near

# variaveis
topicprefix = 'nearest'
request = topicprefix + '.request'
reply = topicprefix + '.reply'
host = ''


if __name__ == '__main__':
    consumer = KafkaConsumer(request, bootstrap_servers=host,
                             value_deserializer=lambda v: json.loads(v.decode('utf-8')), enable_auto_commit=False, auto_offset_reset='latest', group_id=topicprefix)
    try:
        for message in consumer:
            print(message.value)
            near.nearest5()
            # call producer reply
            pass
    except Exception as ex:
        logging.error(ex)
