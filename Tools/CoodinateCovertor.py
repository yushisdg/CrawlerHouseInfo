import math;
x_pi = 3.14159265358979324 * 3000.0 / 180.0;

#将火星坐标转变成百度坐标
def bd_encrypt(lng,lat):
    x = lng;
    y = lat;
    z = math.sqrt(x * x + y * y) + 0.00002 * math.sin(y * x_pi);
    theta = math.atan2(y, x) + 0.000003 * math.cos(x * x_pi);
    lng=z * math.cos(theta) + 0.0065;
    lat=z * math.sin(theta) + 0.006;
    LngLat={};
    LngLat["lng"]=lng;
    LngLat["lat"]=lat;
    return LngLat;

#将百度坐标转变成火星坐标
def bd_decrypt(lng,lat):
    x = lng - 0.0065;
    y = lat - 0.006;
    z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * x_pi);
    theta = math.atan2(y, x) - 0.000003 * math.cos(x * x_pi);
    lng = z * math.cos(theta);
    lat = z * math.sin(theta);
    LngLat = {};
    LngLat["lng"] = lng;
    LngLat["lat"] = lat;
    return LngLat;



