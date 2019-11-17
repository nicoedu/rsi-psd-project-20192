#!/usr/bin/env python3
import json
import math


def nearest5(latitute, longitude, lista):
    distancias = []
    for i in range(len(lista)):
        distancias.append((lista[i][0], calculateDistance(
            latitute, longitude, lista[i][1][0], lista[i][1][1])))
    distancias.sort(key=lambda t: t[1])
    resultado = []
    for i in range(0, 5):
        resultado.append(distancias[i])
    return json.dumps(resultado)


def calculateDistance(lat1, long1, lat2, long2):
    d2r = 0.017453292519943295769236
    dlong = (long1-long2)*d2r
    dlat = (lat1-lat2)*d2r
    tempsin = math.sin(dlat/2.0)
    tempcos = math.cos(lat1 * d2r)
    tempsin2 = math.sin(dlong/2.0)
    a = (tempsin * tempsin)+(tempcos*tempcos)+(tempsin2*tempsin2)
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0-a))
    return 6368.1 * c
