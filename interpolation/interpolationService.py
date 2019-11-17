#!/usr/bin/env python
from kafka import KafkaProducer, KafkaConsumer
import json
import idw

# variaveis
topicprefix = 'interpolation'
request = topicprefix + '.request'
reply = topicprefix + '.reply'
host = ''


if __name__ == '__main__':
    consumer = KafkaConsumer(request, bootstrap_servers=host,
                             value_deserializer=lambda v: str(v).encode('utf-8'), enable_auto_commit=False, auto_offset_reset='latest', group_id=topicprefix)
    try:
        for message in consumer:
            # call idw
            # call producer reply
    except Exception as ex:
        logging.error(ex)
