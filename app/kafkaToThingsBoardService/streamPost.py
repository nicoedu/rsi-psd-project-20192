#!/usr/bin/env python3
import faust
import aiohttp
import os

import json
TB_HOST = 'thingsboard'
KAFKA_HOST = 'kafka'
THINGS_BOARD_ACCESS_TOKEN = "A1_TEST_TOKEN"
POST_TARGET_URL = 'http://'+TB_HOST+':9090/api/v1/' + \
    THINGS_BOARD_ACCESS_TOKEN + '/telemetry'

app = faust.App(
    'telemetry',
    broker='kafka://'+KAFKA_HOST+':29092',
    value_serializer='raw',
)

topic_instance = app.topic(pattern='\w[a-zA-Z0-9]+\w\.sensor')


@app.agent(topic_instance)
async def postAgent(stream):
    async for data in stream:
        async with aiohttp.ClientSession() as session:
            async with session.post(POST_TARGET_URL, data=data.decode("ascii")) as resp:
                print(await resp.text())
