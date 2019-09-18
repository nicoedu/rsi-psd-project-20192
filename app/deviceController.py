import aiohttp
import json
import asyncio

HEADER = {"Accept": "application/json","X-Authorization": "Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0ZW5hbnRAdGhpbmdzYm9hcmQub3JnIiwic2NvcGVzIjpbIlRFTkFOVF9BRE1JTiJdLCJ1c2VySWQiOiIzZDM4MTI2MC1kODdjLTExZTktYjVkZS1iOWYxN2JhMTRlYjIiLCJlbmFibGVkIjp0cnVlLCJpc1B1YmxpYyI6ZmFsc2UsInRlbmFudElkIjoiM2NmZjRmMjAtZDg3Yy0xMWU5LWI1ZGUtYjlmMTdiYTE0ZWIyIiwiY3VzdG9tZXJJZCI6IjEzODE0MDAwLTFkZDItMTFiMi04MDgwLTgwODA4MDgwODA4MCIsImlzcyI6InRoaW5nc2JvYXJkLmlvIiwiaWF0IjoxNTY4ODEzOTMyLCJleHAiOjE1Njg4MjI5MzJ9.K_QYbLVNYAynE2eACqVJENprrAkBZbB7iUOm1wqOTwu6usVOwGgj4L0JR5JyXmeh3jyhZqpUMIk6TJi3DIwVbw"}

async def createDevice(name):
    async with aiohttp.ClientSession() as session:
        resp = await session.post('http://localhost:9090/api/device', json={ 'name': name, 'type': 'default'},headers = HEADER)
        responseDict = await resp.json()
        if (resp.status == 200):
            return (responseDict['id']['id'])
        else:
            return False
    
async def getDeviceId(name):
    async with aiohttp.ClientSession() as session:
        resp = await session.get('http://localhost:9090/api/tenant/devices?deviceName='+str(name),headers = HEADER)
        responseDict = await resp.json()
        if (resp.status == 200):
            return (responseDict['id']['id'])
        else:
            return False
    
async def getDeviceAcessToken(id):
    async with aiohttp.ClientSession() as session:
        resp = await session.get('http://localhost:9090/api/device/%s/credentials'%(str(id)),headers = HEADER)
        responseDict = await resp.json()
        if (resp.status == 200):
            return (responseDict['credentialsId'])
        else:
            return False
    
def caller(name):
    loop = asyncio.get_event_loop()
    deviceId = loop.run_until_complete(getDeviceId(name))
    if deviceId:
        return loop.run_until_complete(getDeviceAcessToken(deviceId))
    else:
        deviceId = loop.run_until_complete(createDevice(name))
        if deviceId:
            return loop.run_until_complete(getDeviceAcessToken(deviceId))
        else:
            print("opa")
        
print(caller('test06'))
#asyncio.create_task()
#createDevice('test03')