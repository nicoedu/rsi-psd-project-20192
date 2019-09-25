#!/usr/bin/env python3
import faust
import aiohttp
from typing import Union
import json
import deviceController
import asyncio

# Variaveis de ambiente

TB_HOST = 'thingsboard'
KAFKA_HOST = 'kafka'
THINGS_BOARD_ACCESS_TOKEN = "A1_TEST_TOKEN"
APP_NAME = 'conectorKafktaThingsboard'
TOPIC_NAME = 'weatherstation.sensor'

# Instância do faust
app = faust.App(
    APP_NAME,
    broker='kafka://'+KAFKA_HOST+':29092'
)


# Modelo da mensagens do Kafka
class MessageModel(faust.Record, serializer='json'):
    timestamp: int
    stationCode: str
    temperature: float
    humidity: float
    latitude: str
    longitude: str

# Função de formatação exigida pelo thingsboard
def thingsBoardFormatter(message: MessageModel):
    formattedMessage = {"ts": message.timestamp, "values": {
        "temperature": str(message.temperature), "humidity": str(message.humidity)}}
    return formattedMessage


# Retorna o acess token de uma estação
async def getAcessTokenByStationCode(message: MessageModel):
    acessToken, isNewDevice = await deviceController.getAcessToken(message.stationCode)
    if isNewDevice:
        await deviceController.setLocationToDevice(acessToken,message.latitude, message.longitude)
    return acessToken


# Retorna a URL para o request post para o acess token passado no parametro
def getTargetPostUrl(acessToken):
    return 'http://'+TB_HOST+':9090/api/v1/' + acessToken + '/telemetry'


# Instancia dos topicos do kafka que desejam ser escutados pelo agent
topic_instance = app.topic('weatherstation.sensor', value_type=MessageModel)


# Função actor que recebe mensagens de uma stream (Mensagens de um ou mais tópicos kafka) e envia para o device correto do thingsboard
@app.agent(topic_instance, concurrency=3)
async def postAgent(stream):
    session = aiohttp.ClientSession()
    async for message in stream:
        acessToken = await getAcessTokenByStationCode(message)
        tbData = thingsBoardFormatter(message)
        resp = await session.post(getTargetPostUrl(acessToken), json=tbData)
        print('Response code ' + str(resp.status) + ' to data: ' + str(tbData))
