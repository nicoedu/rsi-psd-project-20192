import aiohttp
import json
import asyncio


HOST = 'thingsboard:9090'


async def getThingsboardAuthToken():
    async with aiohttp.ClientSession() as session:
        resp = await session.post('http://'+HOST+'/api/auth/login', json={"username": "tenant@thingsboard.org", "password": "tenant"}, headers={"Accept": "application/json"})
        responseDict = await resp.json()
        if resp.status == 200:
            return responseDict['token']
        else:
            raise Exception(resp.status + str(responseDict))

# @param name Nome do device a ser criado


async def createDevice(name, header):
    async with aiohttp.ClientSession() as session:
        resp = await session.post('http://'+HOST+'/api/device', json={'name': name, 'type': 'default'}, headers=header)
        responseDict = await resp.json()
        if (resp.status == 200):
            return (responseDict['id']['id'])
        else:
            raise Exception(resp.status + str(responseDict))


async def getDeviceId(name, header):
    async with aiohttp.ClientSession() as session:
        resp = await session.get('http://'+HOST+'/api/tenant/devices?deviceName='+str(name), headers=header)
        responseDict = await resp.json()
        if (resp.status == 200):
            return (responseDict['id']['id'])
        else:
            raise Exception(resp.status + str(responseDict))


async def getDeviceAcessToken(id, header):
    async with aiohttp.ClientSession() as session:
        resp = await session.get('http://'+HOST+'/api/device/%s/credentials' % (str(id)), headers=header)
        responseDict = await resp.json()
        if (resp.status == 200):
            return (responseDict['credentialsId'])
        else:
            raise Exception(resp.status + str(responseDict))


async def getAcessToken(name):
    #    loop = asyncio.get_event_loop()
    authToken = await asyncio.ensure_future(getThingsboardAuthToken())
    header = {"Accept": "application/json",
              "X-Authorization": "Bearer "+authToken}
    try:
        deviceId = await asyncio.ensure_future(getDeviceId(name, header))
        acessToken = await asyncio.ensure_future(getDeviceAcessToken(deviceId, header))
        return acessToken
    except:
        try:
            deviceId = await asyncio.ensure_future((createDevice(name, header)))
            acessToken = await asyncio.ensure_future((getDeviceAcessToken(deviceId, header)))
            return acessToken
        except:
            print("Error")


# if __name__ == '__main__':
#    loop = asyncio.get_event_loop()
#    loop.run_until_complete(getAcessToken("teste"))