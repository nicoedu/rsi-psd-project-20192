import aiohttp
import logging
import json
import asyncio


HOST = 'thingsboard:9090'


async def getThingsboardAuthToken():
    try:
        async with aiohttp.ClientSession() as session:
            resp = await session.post('http://'+HOST+'/api/auth/login', json={"username": "tenant@thingsboard.org", "password": "tenant"}, headers={"Accept": "application/json"})
            responseDict = await resp.json()
            if resp.status == 200:
                return responseDict['token']
            else:
                raise Exception(resp.status + str(responseDict))
    except Exception as exc:
        logging.error(exc)


# @param name Nome do device a ser criado
async def createDevice(name, header):
    try:
        async with aiohttp.ClientSession() as session:
            resp = await session.post('http://'+HOST+'/api/device', json={'name': name, 'type': 'default'}, headers=header)
            responseDict = await resp.json()
            if (resp.status == 200):
                return (responseDict['id']['id'])
            else:
                raise Exception(resp.status + str(responseDict))
    except Exception as exc:
        logging.error(exc)



async def getDeviceId(name, header):
    try:
        async with aiohttp.ClientSession() as session:
            resp = await session.get('http://'+HOST+'/api/tenant/devices?deviceName='+str(name), headers=header)
            responseDict = await resp.json()
            if (resp.status == 200):
                return (responseDict['id']['id'])
            else:
                raise Exception(resp.status + str(responseDict))
    except Exception as exc:
        logging.error(exc)


async def getDeviceAcessToken(id, header):
    try:
        async with aiohttp.ClientSession() as session:
            resp = await session.get('http://'+HOST+'/api/device/%s/credentials' % (str(id)), headers=header)
            responseDict = await resp.json()
            if (resp.status == 200):
                return (responseDict['credentialsId'])
            else:
                raise Exception(resp.status + str(responseDict))
    except Exception as exc:
        logging.error(exc)


async def setLocationToDevice(acessToken, latitude, longitude):
    try:
        async with aiohttp.ClientSession() as session:
            resp = await session.post('http://'+HOST+'/api/v1/%s/attributes' % (str(acessToken)), json={'latitude': latitude, 'longitude': longitude})
            if (resp.status == 200):
                return True
            else: 
                return False
    except Exception as exc:
        logging.error(exc)



# TODO nomear Exception
async def getAcessToken(name) -> tuple:
    isNewDevice = False
    authToken = await asyncio.ensure_future(getThingsboardAuthToken())
    header = {"Accept": "application/json",
              "X-Authorization": "Bearer "+authToken}
    try:
        deviceId = await asyncio.ensure_future(getDeviceId(name, header))
        acessToken = await asyncio.ensure_future(getDeviceAcessToken(deviceId, header))
        return acessToken, isNewDevice
    except:
        try:
            deviceId = await asyncio.ensure_future((createDevice(name, header)))
            acessToken = await asyncio.ensure_future((getDeviceAcessToken(deviceId, header)))
            isNewDevice = True
            return acessToken, isNewDevice
        except Exception as ex:
            logging.error(ex)
