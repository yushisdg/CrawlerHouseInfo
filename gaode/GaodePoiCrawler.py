import requests
import json
import time
from pandas import DataFrame
import psycopg2
import math
from CrawlerHouseInfo.jdbcConfig import *


pages=1;

def getGaodePOI(key,rectangle,type,page,offset):
    poiUrl="http://restapi.amap.com/v3/place/polygon?polygon="+rectangle+"&types="+type+"&output=json&key="+key+"&page="+page+"&offset="+offset+"&extensions=all"
    print(poiUrl);
    try:
        res = requests.get(url=poiUrl).content;
        afterRequest(key);
        total_json = json.loads(res);
        print(total_json);
        insert_list = [];
        count = total_json.get('count');
        poisJson = total_json.get('pois');
        totalSql = "";
        print(poisJson);
        poiCount = len(poisJson);
        if poiCount > 0:
            if any(poisJson):
                i = 0;
                insertGaoPoiInTODB(poisJson);
                pages = math.ceil(int(count) / int(offset));
                return pages;
        else:
            return 0;
    except Exception as e:
        print(e);
        return 0;


def insertGaoPoiInTODB(poisJson):
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    for poi in poisJson:
        id = poi.get('id');
        name = poi.get('name');
        type = poi.get('type');
        typecode = poi.get('typecode');
        biz_type = poi.get('biz_type');
        address = poi.get('address');
        location = poi.get('location');
        postcode = poi.get('postcode');
        pcode = poi.get('pcode');
        pname = poi.get('pname');
        citycode = poi.get('citycode');
        cityname = poi.get('cityname');
        adcode = poi.get('adcode');
        adname = poi.get('adname');
        alias = poi.get('alias');
        geomStr = "POINT(" + location.replace(',', " ") + ")";
        sql = "INSERT INTO gaode_poi (id, name, type, typecode, biz_type, address, location, postcode, pcode, pname, citycode, cityname, adcode, adname, alias, geom) VALUES ('" + str(id) + "', '" + str(name) + "', '" + str(type) + "', '" + str(typecode) + "', '" + str(biz_type) + "', '" + str(address) + "', '" + str(location) + "', '" + str(postcode) + "', '" + str(pcode) + "', '" + str(pname) + "', '" + str(citycode) + "', '" + str(cityname) + "', '" + str(adcode) + "', '" + str(adname) + "', '" + str(alias )+ "', " + "st_geomfromText('" + geomStr + "',4326)" + ");"
        print(sql);
        try:
            cur.execute(sql);
        except Exception as e:
            print(e);
        conn.commit();
    cur.close();
    conn.close();



type=str(150700);
page=str(1);
offset="50";
def getOneRectangleAllPoi(key,rectangle,type,page,offset):
    pages = getGaodePOI(key, rectangle, type, page, offset);
    if pages > 1:
        for num in range(2, pages + 1):
            getGaodePOI(key, rectangle, type, str(num), offset)
    return None;


# 获取研究区域网格
def getRegionRectangles(cityCode):
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    cur.execute("SELECT minX||','||minY||','||maxX||','||maxY rectangle from (SELECT ST_YMin (T .geom) minY,ST_YMax (T .geom) maxY,ST_XMin (T .geom) minX,ST_XMax (T .geom) maxX,T .code code,T .grid_index gridIndex,T .grid_id gridId FROM geomtools_grid t where t.code='"+cityCode+"') k;");
    rectangleData = cur.fetchall();
    cur.close();
    conn.close();
    return rectangleData;


def getUseableKey():
    currentDate = time.strftime("%Y-%m-%d", time.localtime());
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    cur.execute("select KJ.key from (SELECT k.key, COALESCE((select j.poi_reqcount from gaode_keys j where j.date='" + currentDate + "' and j.key=k.key),0) poi_reqcount from (SELECT t.key from gaode_keys t GROUP BY t.key)  K) KJ  where KJ.poi_reqcount<1000");
    keyData = cur.fetchall();
    key=keyData[0][0];
    cur.close;
    conn.close();
    if key==None:
        print("没有可用key了，需要申请更多的账户");
    return key;

#批量请求研究区域内的一类POI
def batchGetGaodePoi(type):
    page = str(1);
    offset = "50";
    rectangleData=getRegionRectangles("3609");
    for rec in rectangleData:
        print(rec[0]);
        key=getUseableKey();
        if key==None:
            print("没有可用key了，需要申请更多的账户");
        else:
            getOneRectangleAllPoi(key, rec[0], type,page,offset);
        print("一个区域的数据已获取完成");


#请求完成后 key计数+1
def afterRequest(key):
    currentDate = time.strftime("%Y-%m-%d", time.localtime());
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    cur.execute("SELECT count(*) from gaode_keys t where t.key='"+key+"' and t.date='"+currentDate+"'");
    keyData = cur.fetchall();
    if keyData[0][0]==0:
        cur.execute(" INSERT INTO gaode_keys (key, date, poi_reqcount) VALUES('"+key+"','"+currentDate+"'"+",1)");
    else:
        cur.execute("UPDATE gaode_keys t set poi_reqcount=t.poi_reqcount+1 where t.key='"+key+"' and t.date='"+currentDate+"'");
    conn.commit();
    cur.close;
    conn.close();
    print("key :"+key+" +1");


# key='a5e9cfffb5d9a00c14142cdc17742a8d';
# rectangle='120.12924,30.30185;120.16018,30.27150';
# type=str(150700);
# page=str(1);
# offset="50";
# pages=getGaodePOI(key,rectangle,type,page,offset);
# print(pages);
# if pages>1:
#     for num in range(2, pages+1):
#         getGaodePOI(key, rectangle, type, str(num), offset)

type='120000';
batchGetGaodePoi(type);