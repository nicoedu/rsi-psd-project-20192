import math
from math import radians, sin, cos, asin, sqrt





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


print(calculateDistance(-8.433544, -37.055477, -7.839628, -35.801056))
print(haversine(-8.433544, -37.055477, -7.839628, -35.801056))

""" [('c2590b80-09a9-11ea-86db-d77482ad5b22', [-8.433544, -37.055477]),
# ('c25ae040-09a9-11ea-86db-d77482ad5b22', [-8.666667, -35.567921]),
# ('c25c66e0-09a9-11ea-86db-d77482ad5b22', [-8.598785, -38.584062]),
# ('c27f0a10-09a9-11ea-86db-d77482ad5b22', [-7.885833, -40.102683]),
# ('c2815400-09a9-11ea-86db-d77482ad5b22', [-8.91095, -36.493381]),
# ('c283ec11-09a9-11ea-86db-d77482ad5b22', [-8.236069, -35.98555]),
# ('c299e510-09a9-11ea-86db-d77482ad5b22', [-8.504, -39.31528]),
# ('c29c5610-09a9-11ea-86db-d77482ad5b22', [-8.05928, -34.959239]),
# ('c29ec710-09a9-11ea-86db-d77482ad5b22', [-9.388323, -40.523262]),
# ('c2b5d180-09a9-11ea-86db-d77482ad5b22', [-7.954277, -38.295082]),
# ('c2b84280-09a9-11ea-86db-d77482ad5b22', [-8.509552, -37.711591]),
# ('c2bc6130-09a9-11ea-86db-d77482ad5b22', [-8.0580556, -39.096111]),
# ('c2d03750-09a9-11ea-86db-d77482ad5b22', [-7.839628, -35.801056])]
"""
