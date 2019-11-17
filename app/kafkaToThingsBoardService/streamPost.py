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
APP_NAME = 'conectorKafktaThingsboard'
TOPIC_NAME = 'weatherstation.sensor'

# Instância do faust
app = faust.App(
    APP_NAME,
    broker='kafka://'+KAFKA_HOST+':29092'
)


# Modelo da mensagens do Kafka
class MessageSensorModel(faust.Record, serializer='json'):
    timestamp: int
    stationCode: str
    temperature: float
    humidity: float
    latitude: str
    longitude: str


class MessageHeatModel(faust.Record, serializer='json'):
    timestamp: int
    stationCode: str
    heatIndex: float


# Função de formatação exigida pelo thingsboard
def tbSensorFormatter(message: MessageSensorModel):
    formattedMessage = {"ts": message.timestamp, "values": {
        "temperature": str(message.temperature), "humidity": str(message.humidity)}}
    return formattedMessage


def tbHeatFormatter(message: MessageHeatModel):
    formattedMessage = {"ts": message.timestamp, "values": {
        "heatIndex": str(message.heatIndex)}}
    return formattedMessage


# Retorna o acess token de uma estação
async def getAcessTokenByStationCode(stationCode, latitude=None, longitude=None):
    acessToken = await deviceController.getAcessToken(stationCode, latitude, longitude)
    return acessToken

# Retorna a URL para o request post para o acess token passado no parametro


def getTargetPostUrl(acessToken):
    return 'http://'+TB_HOST+':9090/api/v1/' + acessToken + '/telemetry'


# Instancia dos topicos do kafka que desejam ser escutados pelo agent
topic_sensor = app.topic('weatherstation.sensor',
                         value_type=MessageSensorModel)
topic_heatIndex = app.topic(
    'weatherstation.heatindex', value_type=MessageHeatModel)


# Função actor que recebe mensagens de uma stream (Mensagens de um ou mais tópicos kafka) e envia para o device correto do thingsboard
@app.agent(topic_sensor, concurrency=3)
async def postAgent(stream):
    session = aiohttp.ClientSession()
    async for message in stream:
        acessToken = await getAcessTokenByStationCode(message.stationCode, message.latitude, message.longitude)
        tbData = tbSensorFormatter(message)
        resp = await session.post(getTargetPostUrl(acessToken), json=tbData)
        print('Response code ' + str(resp.status) + ' to data: ' + str(tbData))


@app.agent(topic_heatIndex, concurrency=3)
async def heatIndexAgent(stream):
    session = aiohttp.ClientSession()
    async for message in stream:
        acessToken = await getAcessTokenByStationCode(message.stationCode)
        tbData = tbHeatFormatter(message)
        resp = await session.post(getTargetPostUrl(acessToken), json=tbData)
        print('Response code ' + str(resp.status) + ' to data: ' + str(tbData))
