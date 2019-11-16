import restKafka as rk


if __name__ == '__main__':
    teste = rk.kafkaRest('localhost', 'teste')

    try:
        teste.connectKafkaConsumer()
    except Exception as e:
        print(e)

    teste.get()
