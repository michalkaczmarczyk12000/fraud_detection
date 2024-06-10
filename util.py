import math
import random


def rand_lat_long():
    pi = math.pi
    cf = 180.0 / pi
    gx = random.gauss(0.0, 1.0)
    gy = random.gauss(0.0, 1.0)
    gz = random.gauss(0.0, 1.0)

    norm2 = gx * gx + gy * gy + gz * gz
    norm1 = 1.0 / math.sqrt(norm2)
    x = gx * norm1
    y = gy * norm1
    z = gz * norm1

    radLat = math.asin(z)
    radLon = math.atan2(y, x)

    return round(cf * radLat, 5), round(cf * radLon, 5)


def generate_random_point(latitude, longitude, radius):

    radius_in_degrees = radius / 111320

    u = random.random()
    v = random.random()

    w = radius_in_degrees * math.sqrt(u)
    t = 2 * math.pi * v

    delta_latitude = w * math.cos(t)
    delta_longitude = w * math.sin(t) / math.cos(math.radians(latitude))

    new_latitude = latitude + delta_latitude
    new_longitude = longitude + delta_longitude

    return new_latitude, new_longitude


def haversine(lat1, lon1, lat2, lon2):
    from math import radians, sin, cos, sqrt, atan2

    R = 6371
    d_lat = radians(lat2 - lat1)
    d_lon = radians(lon2 - lon1)
    a = (
        sin(d_lat / 2) ** 2
        + cos(radians(lat1)) * cos(radians(lat2)) * sin(d_lon / 2) ** 2
    )
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c
