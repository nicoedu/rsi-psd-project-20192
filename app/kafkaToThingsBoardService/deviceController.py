#!/usr/bin/env python3
import aiohttp
import logging
import json
import asyncio
from DevicesExceptions import *


HOST = 'thingsboard:9090'


async def getThingsboardAuthToken():
    async with aiohttp.ClientSession() as session:
        resp = await session.post('http://'+HOST+'/api/auth/login', json={"username": "tenant@thingsboard.org", "password": "tenant"}, headers={"Accept": "application/json"})
        responseDict = await resp.json()
        if resp.status == 200:
            return responseDict['token']
        elif resp.status == 401:
            raise UnauthorizedException(
                resp.status, str(responseDict['message']))


# @param name Nome do device a ser criado
async def createDevice(name, latitude, longitude, header):
    async with aiohttp.ClientSession() as session:
        resp = await session.post('http://'+HOST+'/api/device', json={'additionalInfo': str(latitude)+";"+str(longitude), 'name': name, 'type': 'SENSOR'}, headers=header)
        responseDict = await resp.json()
        if (resp.status == 200):
            return (responseDict['id']['id'])
        else:
            raise httpException(resp.status, str(responseDict['message']))


async def getDeviceId(name, header):
    async with aiohttp.ClientSession() as session:
        resp = await session.get('http://'+HOST+'/api/tenant/devices?deviceName='+str(name), headers=header)
        responseDict = await resp.json()
        if (resp.status == 200):
            return (responseDict['id']['id'])
        elif resp.status == 404:
            raise DeviceNotFound(resp.status, str(responseDict['message']))
        else:
            raise httpException(resp.status, str(responseDict['message']))


async def getDeviceAcessToken(id, header):
    async with aiohttp.ClientSession() as session:
        resp = await session.get('http://'+HOST+'/api/device/%s/credentials' % (str(id)), headers=header)
        responseDict = await resp.json()
        if (resp.status == 200):
            return (responseDict['credentialsId'])
        else:
            raise httpException(resp.status, str(responseDict['message']))


async def setLocationToDevice(acessToken, latitude, longitude):
    try:
        async with aiohttp.ClientSession() as session:
            resp = await session.post('http://'+HOST+'/api/v1/%s/attributes' % (str(acessToken)), json={'latitude': latitude, 'longitude': longitude})
            if (resp.status == 200):
                return True
            else:
                print(resp)

    except Exception as e:
        logging.error(e)


# TODO nomear Exception
async def getAcessToken(name, latitude, longitude) -> tuple:
    try:
        authToken = await asyncio.ensure_future(getThingsboardAuthToken())
        header = {"Accept": "application/json",
                  "X-Authorization": "Bearer "+authToken}
        deviceId = await asyncio.ensure_future(getDeviceId(name, header))
        acessToken = await asyncio.ensure_future(getDeviceAcessToken(deviceId, header))
        return acessToken
    except UnauthorizedException as error:
        logging.error(error)
    except DeviceNotFound as error:
        assert latitude is not None and longitude is not None
        try:
            deviceId = await asyncio.ensure_future((createDevice(name, latitude, longitude, header)))
            acessToken = await asyncio.ensure_future((getDeviceAcessToken(deviceId, header)))
            atributes = await asyncio.ensure_future((setLocationToDevice(acessToken, latitude, longitude)))
            return acessToken
        except Exception as ex:
            logging.error(ex)
