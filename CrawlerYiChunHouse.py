import psycopg2
from CrawlerHouseInfo.CityGridTools import *
from CrawlerHouseInfo.MapTools import *
from CrawlerHouseInfo.jdbcConfig import *
from CrawlerHouseInfo.Tools.CoodinateCovertor import *
import urllib.request
import requests
import json
import random
from CrawlerHouseInfo.CrawlerHouseTools import *
import threading
from bs4 import  BeautifulSoup
import urllib
from urllib import parse


def getComCount():
    url="https://yichun.anjuke.com/community/yuanzhoua/";
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }
        html_doc = requests.get(url, timeout=30,headers=headers).content
        soup = BeautifulSoup(html_doc, 'html.parser', from_encoding='utf-8');
        tit=soup.find(class_="tit").contents;
        countEle=tit[3];
        count=countEle.contents[0];
        return count;
    except Exception as e:
        print(e);


def getComInfo():
    countStr=getComCount();
    count=int(str(countStr));
    pageCount=math.ceil(count/30);
    url="https://yichun.anjuke.com/community/yuanzhoua";
    for pageNum in range(1,pageCount+1):
        reqUrl=url+"/p"+str(pageNum)+"/"

        getComList(reqUrl);
        sleepTime = random.randint(5, 8);
        print(sleepTime);
        time.sleep(sleepTime);


def getComList(url):
    print(url)
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }
        html_doc = requests.get(url, timeout=30, headers=headers).content
        soup = BeautifulSoup(html_doc, 'html.parser', from_encoding='utf-8');
        list = soup.find_all(class_="li-itemmod");
        communityVos = [];
        for comEle in list:
            recordDict = {};
            # address = comEle.find(class_="li-info").find("address").contents[0].replace('袁州-袁州', '').replace(' ',
            #                                                                                                  '').replace(
            #     '［', '').replace("］", '').replace(' ', '');
            if(comEle.find('strong')==None):
                price = 0;
            else:
                price = comEle.find('strong').contents[0];
            mapref = comEle.find(class_="bot-tag").find_all("a")[1].attrs["href"];
            parseResult = parse.urlparse(mapref)

            param_dict = parse.parse_qs(parseResult.fragment)
            lat = param_dict['l1'][0];
            lng = param_dict['l2'][0];
            commname = param_dict['commname'][0];
            commid = param_dict['commid'][0];
            recordDict["id"] = commid;
            recordDict["name"] = commname;
            recordDict["longitude"] = lng;
            recordDict["latitude"] = lat;
            recordDict["fromWeb"] = "anjuke";
            recordDict["communityPrice"] = price;
            recordDict["cityCode"] = '3609';
            print(recordDict)
            communityVos.append(recordDict);

        addOneCommunityIntoDB(communityVos);
    except Exception as e:
        print(e);


#小区数据入库
def addOneCommunityIntoDB(communityVos):
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    for communityVo in communityVos:
        id=communityVo["id"];
        name=communityVo["name"];
        longitude = communityVo["longitude"];
        latitude = communityVo["latitude"];
        communityPrice=communityVo["communityPrice"];
        fromWeb=communityVo["fromWeb"];
        cityCode=communityVo["cityCode"];
        gaodePoint = bd_decrypt(float(longitude),float(latitude));
        geomStr="";
        if(fromWeb=="ishangzu"):
            geomStr = "POINT(" + str(longitude) + " " + str(latitude) + ")";
        else:
            geomStr = "POINT(" + str(gaodePoint["lng"]) + " " + str(gaodePoint["lat"]) + ")";
        sql="INSERT INTO web_community (id, name, longitude, latitude, geom, from_web, community_price,city_code) VALUES ('"+str(id)+"', '"+name+"', '"+str(longitude)+"', '"+str(latitude)+"', "+"st_geomfromText('"+geomStr+"',4326)"+", '"+fromWeb+"', '"+str(communityPrice)+"','"+cityCode+"');";
        print(sql)
        try:
            cur.execute(sql);
            conn.commit();
        except Exception as e:
            print(e);
        finally:
            conn.commit();
    cur.close();
    conn.close();

getComInfo();

