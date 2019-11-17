import restKafka as rk


if __name__ == '__main__':
    teste = rk.kafkaRest('localhost', 'nearest')

    try:
        teste.connectKafkaProducer()
    except Exception as e:
        print(e)

    teste.post({"teste": "teste"})
