#!/usr/bin/env python3
import faust
import aiohttp
from typing import Union

import json
TB_HOST = 'thingsboard'
KAFKA_HOST = 'kafka'
THINGS_BOARD_ACCESS_TOKEN = "A1_TEST_TOKEN"
POST_TARGET_URL = 'http://'+TB_HOST+':9090/api/v1/' + \
    THINGS_BOARD_ACCESS_TOKEN + '/telemetry'
APP_NAME = 'conectorKafktaThingsboard'

# TODO CRIAR CLASSE MODEL DA LEITURA DE SENSOR
app = faust.App(
    APP_NAME,
    broker='kafka://'+KAFKA_HOST+':29092',
    value_serializer='raw',
)

# Instancia dos topicos do kafka que desejam ser escutados pelo agent
topic_instance = app.topic('A301.sensor', 'A302.sensor')


# Notação @app.agent recebe como argumento um canal, que no nosso caso é uma isntância dos topicos kafka
# A função do tipo agent recebe um argumento Stream, produzido pelo app.agent, conténdo um interador assíncrono das mensagens transportadas pelo canal.
#
# A função executa, para cada mensagém lida, uma requisição do tipo post enviando para uma certa url os dados desta mensagem.
@app.agent(topic_instance, concurrency=3)
async def postAgent(stream):
    session = aiohttp.ClientSession()
    async for data in stream:
        print(stream.current_event)
        print(stream.info())
        resp = await session.post(POST_TARGET_URL, data=data.decode("ascii"))
        print(resp.status)
