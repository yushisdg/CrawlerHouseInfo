import requests
import json
from CrawlerHouseInfo.Tools.BaiduConventor import *;
import json
import psycopg2
import requests
from CrawlerHouseInfo.config.jdbcConfig import *
from CrawlerHouseInfo.Tools.BaiduConventor import *;


def queryPointInWhereCity(x,y):
    location=convertLL2MC(x,y);
    lng=location["x"];
    lat=location["y"];
    url = "https://api.map.baidu.com/?qt=rgc&x=" + str(lng) + "&y=" +str(lat) + "&dis_poi=100&poi_num=10&ie=utf-8&oue=1&res=webmap";
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

def InserIntoTestPoint(points):
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    for point in points:
        print(point)
        lng=point["lng"];
        lat=point["lat"];
        geomStr = "POINT(" + str(lng) + " " + str(lat) + ")";
        sql="INSERT INTO out_test_point (geom) VALUES ("+"st_geomfromText('"+geomStr+"',4326)"+");"

        try:
            cur.execute(sql);
            conn.commit();
        except Exception as e:
            cur.close();
            conn.close();
            print(e);
    cur.close();
    conn.close();