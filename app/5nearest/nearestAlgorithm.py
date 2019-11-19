#!/usr/bin/env python3
import json
from math import radians, sin, cos, asin, sqrt


def nearest5(latitute, longitude, lista):
    distancias = []
    for i in range(len(lista)):
        distancias.append((lista[i][0], calculateDistance(
            latitute, longitude, lista[i][1][0], lista[i][1][1])))
    distancias.sort(key=lambda t: t[1])
    resultado = {}
    for i in range(0, 5):
        resultado[distancias[i][0]] = distancias[i][1]
    return resultado


def calculateDistance(lat1, lon1, lat2, lon2):
    R = 6372.8  # KM
    dLat = radians(lat2 - lat1)
    dLon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)

    a = sin(dLat/2)**2 + cos(lat1)*cos(lat2)*sin(dLon/2)**2
    c = 2*asin(sqrt(a))

    return R * c
