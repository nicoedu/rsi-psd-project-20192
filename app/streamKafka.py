#!/usr/bin/env python3
import faust
import aiohttp

import json
HOST = "localhost"
ACCESS_TOKEN = "A1_TEST_TOKEN"
URL = 'http://localhost:9090/api/v1/' + ACCESS_TOKEN + '/telemetry'

app = faust.App(
    'telemetry',
    broker='kafka://localhost:9092',
    value_serializer='raw',
)

greetings_topic = app.topic('sensor')


@app.agent(greetings_topic)
async def greet(greetings):
    async for greeting in greetings:
        async with aiohttp.ClientSession() as session:
            async with session.post(URL, data=greeting.decode("ascii")) as resp:
                # print(resp.status())
                print(await resp.text())
