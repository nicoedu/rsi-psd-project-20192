from math import radians, cos, sin, asin, sqrt

def haversine(lat1, lon1, lat2, lon2):

      R = 6372.8 # KM

      dLat = radians(lat2 - lat1)
      dLon = radians(lon2 - lon1)
      lat1 = radians(lat1)
      lat2 = radians(lat2)

      a = sin(dLat/2)**2 + cos(lat1)*cos(lat2)*sin(dLon/2)**2
      c = 2*asin(sqrt(a))

      return R * c

def interpolateHI(_list):# ((x, y) , [((x1, y1), h1), ((x1, y1), h2), ... , ((xn, yn), hn)])
    origin, knowPoints = _list
    return sum(list(map(lambda i: i[1]/haversine(origin[0], origin[1], i[0][0], i[0][1]), knowPoints)))/sum(list(map(lambda i: 1/haversine(origin[0], origin[1], i[0][0], i[0][1]), knowPoints)))

print(interpolateHI(((2,2), [((1,1), 10),((3,3), 30), ((4,4), 40)])))
print(interpolateHI(((3,3), [((1,1), 10),((2,2), 20), ((4,4), 40)])))
print(interpolateHI(((1,1), [((i,i), i*10) for i in range(2, 50)])))
    
    
    
    
    
