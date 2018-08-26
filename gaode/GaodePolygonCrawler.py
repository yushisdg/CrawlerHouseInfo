import json
import random
import time

import psycopg2
import requests
from CrawlerHouseInfo.Tools.MapTools import *
from CrawlerHouseInfo.config.jdbcConfig import *


def getOneResidentialDate(id):
    roadUrl ="http://ditu.amap.com/detail/get/detail?id="+id;
    print(roadUrl);
    try:
        # # 使用收费代理
        # proxyHost = "http-dyn.abuyun.com"
        # proxyPort = "9020"
        #
        # # 代理隧道验证信息
        # proxyUser = "HN5861W41O905A0D"
        # proxyPass = "044A1683F60BB0C2"
        #
        # proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
        #     "host": proxyHost,
        #     "port": proxyPort,
        #     "user": proxyUser,
        #     "pass": proxyPass,
        # }
        #
        # proxies = {
        #     "http": proxyMeta,
        #     "https": proxyMeta,
        # }
        #
        # res = requests.get(roadUrl, proxies=proxies)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }
        res = requests.get(url=roadUrl, timeout=3,headers=headers);
        content = res.content;
        print(content);
        total_json = json.loads(content);
        print(total_json);
        status = total_json.get("status");
        print(status);
        if status == '1':
            data = total_json.get("data");
            base="";
            spec = data.get('spec');
            mining_shape = spec.get("mining_shape");

            name="";
            region_id="";
            city_code = "";
            city_name = "";
            tag = "";
            address = "";
            service_parking = "";
            green_rate = "";
            opening_data = "";
            price = "";
            volume_rate = "";
            intro = "";
            property_fee = "";
            area_total = "";
            if mining_shape != None:

                base = data.get('base');
                name = base.get("name");
                region_id = base.get("poiid");
                city_code = base.get("city_adcode");
                city_name = base.get("city_name");

                tag = base.get("tag");

                address = base.get("address");
                residential = data.get("residential");

                if residential != None:
                    # service_parking = residential.get("service_parking");
                    # green_rate = residential.get("green_rate");
                    # opening_data = residential.get("opening_data");
                    # price = residential.get("price");
                    # volume_rate = str(residential.get("volume_rate"));
                    # print("-----------------------------------------")
                    # intro = residential.get("intro");
                    # property_fee = residential.get("property_fee");
                    # area_total = residential.get("area_total");
                    service_parking = "";
                    green_rate = ""
                    opening_data = ""
                    price = ""
                    volume_rate ="";
                    print("-----------------------------------------")
                    intro = "";
                    property_fee = "";
                    area_total = "";
                shape = mining_shape.get("shape");
                # insertIntoShpe(shape);
                geomStr=shape.replace(',',' ').replace(';',',');
                center = mining_shape.get("center");
                area = mining_shape.get("area");
                if service_parking==None:
                    service_parking="";
                if green_rate == None:
                    green_rate = "";
                if opening_data == None:
                    opening_data = "";
                if price == None:
                    price = "";
                if volume_rate == None:
                    volume_rate = "";
                if intro == None:
                    intro = "";
                if property_fee == None:
                    property_fee = "";
                if area_total == None:
                    area_total = "";
                sql = "INSERT INTO gaode_polygon(region_id, city_code, shape, name, address, city_name,geom, area, center, tag, service_parking, volume_rate, area_total, price, intro, opening_data, property_fee) VALUES ('" + region_id + "', '" + city_code + "', '" + shape + "', '" + name + "', '" + address + "', '" + city_name + "', "+ "st_geomfromText('POLYGON((" + geomStr + "))',4326)" +","+ area + ", '" + center + "', '" + tag + "', '" + service_parking + "', '" + volume_rate + "', '" + area_total + "', '" + price + "', '" + intro + "', '" + opening_data + "', '" + property_fee + "');"
                print(sql);
                conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
                cur = conn.cursor();
                try:
                    cur.execute(sql);
                    conn.commit();
                except Exception as e:
                    cur.close();
                    conn.close();
                    print(e);
            else:
                reason="没有空间数据";
                print(reason);
                conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
                cur = conn.cursor();
                sql = "INSERT INTO gaode_poi_disable (region_id,reason) VALUES ('" + id + "','" + reason + "');"
                print(sql)
                cur.execute(sql);
                conn.commit();
        else:
            if status=="8":
                reason = "未找到";
                print(reason);
                conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
                cur = conn.cursor();
                sql = "INSERT INTO gaode_poi_disable (region_id,reason) VALUES ('" + id + "','" + reason + "');"
                cur.execute(sql);
                conn.commit();

    except Exception as e:
        print(e);
        reason="请求失败";
        conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
        cur = conn.cursor();
        sql = "INSERT INTO gaode_poi_disable (region_id,reason) VALUES ('" + id + "','" + reason + "');"
        cur.execute(sql);
        conn.commit();
    cur.close();
    conn.close();


def insertIntoShpe(shpe):
    pointsStr=shpe.split(";");
    points=[];
    for pointStr in pointsStr:
        pointDict={};
        point=pointStr.split(",");
        lng=point[0];
        lat=point[1];
        pointDict["lng"]=lng;
        pointDict["lat"] = lat;
        points.append(pointDict);
    InserIntoTestPoint(points);
    return 0;


def batchGetResidential(typecode,citycode):
    a = 1;
    while a == 1:
        sql = "SELECT id from gaode_poi t where t.typecode like '"+str(typecode)+"%' and citycode='"+str(citycode)+"' and t.id not in (select region_id from gaode_polygon ) and t.id not in (select region_id from gaode_poi_disable ) limit 1;";
        print(sql)
        conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
        cur = conn.cursor();
        cur.execute(sql);
        keyData = cur.fetchall();
        uid = keyData[0][0];
        if uid!=None:
            getOneResidentialDate(uid);
            sleepTime=random.randint(3, 6);

            print(sleepTime);
            time.sleep(sleepTime);
        else:
            break;


batchGetResidential(12,"0795");
