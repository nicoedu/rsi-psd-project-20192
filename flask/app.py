from flask import Flask,request
import json
import math

app = Flask(__name__)

@app.route("/")
def hello_world():
    return "Hello World! <strong>I am learning Flask</strong>", 200

@app.route("/5nearest", methods=['POST'])
def nearest5():
    latitute = request.form.get("latitute")
    longitude = request.form.get("longitute")
    #TODO carregar lista na variavel abaixo
    lista = []
    distancias = []
    for i in range(len(lista)):
        distancias.append([i,calculateDistance(latitute,longitude,lista[0],lista[1])])
    distancias.sort()
    resultado = []
    for i in range(0,5):
        resultado.append(distancias[i])
    return json.dumps(resultado), 200

def calculateDistance(lat1, long1, lat2,long2):
    d2r = 0.017453292519943295769236
    dlong = (long1-long2)*d2r
    dlat = (lat1-lat2)*d2r
    tempsin = math.sin(dlat/2.0)
    tempcos = math.cos(lat1 * d2r)
    tempsin2 = math.sin(dlong/2.0)
    a = (tempsin * tempsin)+(tempcos*tempcos)+(tempsin2*tempsin2)
    c = 2.0 * math.atan2(math.sqrt(a),math.sqrt(1.0-a))
    return 6368.1 * c

app.run()