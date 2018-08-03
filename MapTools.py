import urllib.request
import requests
import json
from CrawlerHouseInfo.BaiduConventor import *;

def queryPointInWhereCity(x,y):
    location=convertLL2MC(x,y);
    lng=location["x"];
    lat=location["y"];
    url = "https://api.map.baidu.com/?qt=rgc&x=" + str(lng) + "&y=" +str(lat) + "&dis_poi=100&poi_num=10&ie=utf-8&oue=1&res=webmap";
    print(url)
    cityName="null"
    try:
        res = requests.get(url=url).content;
        total_json = json.loads(res);
        content = total_json.get('content');
        if(content!=None):
            address_detail=content.get("address_detail");
            if(address_detail!=None):
                cityName=address_detail.get("city");
                return cityName;
    except Exception as e:
        print(e);
    return cityName;