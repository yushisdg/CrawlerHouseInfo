import psycopg2
from CrawlerHouseInfo.CityGridTools import *
from CrawlerHouseInfo.MapTools import *
from CrawlerHouseInfo.jdbcConfig import *
from CrawlerHouseInfo.Tools.CoodinateCovertor import *
import urllib.request
import requests
import json

def CrawlerHouseByCityCode(cityCode):
    indexDict=getCityGridsMinIndexAndMaxIndexByCityCode(cityCode);
    for index in range(indexDict["minIndex"],indexDict["maxIndex"]):
       gridDict=SelectGridByCityCodeAndIndex(cityCode,index);
       if(gridDict!=None):
           print(gridDict)
           xmin=gridDict['xmin'];
           ymin=gridDict['ymin'];
           xmax=gridDict['xmax'] ;
           ymax=gridDict['ymax'];
           queryWhereCityByItem(xmin,ymin,xmax,ymax);
    return 0;

def queryCommunityByEnvelop(xmin,ymin,xmax,ymax):
    return 0;

def queryWhereCityByItem(xmin,ymin,xmax,ymax):
    centX = (xmin + xmax) / 2;
    centY = (ymin + ymax) / 2;
    cityName=queryPointInWhereCity(centX,centY);
    resourceDict=getWebResourceUrl(cityName);
    if(resourceDict!=None):
        for item in resourceDict:
             fromWeb=item["fromWeb"];
             comUrl=item["data"]["comUrl"];
             houseUrl=item["data"]["houseUrl"];
             if(fromWeb=="lianjia"):
                queryLianjiaCommunityByEnvelope(xmin, ymin, xmax, ymax, comUrl, houseUrl, fromWeb);
             elif(fromWeb=="anjuke"):
                print(fromWeb)
             elif (fromWeb == "lianjia"):
                 print(fromWeb)
             elif (fromWeb == "woaiwojia"):
                 print(fromWeb)
             elif (fromWeb == "ishangzu"):
                 print(fromWeb)
             elif (fromWeb == "ziru"):
                 print(fromWeb)
             else:
                 print(fromWeb)
    return cityName;




def getWebResourceUrl(cityName):
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    sql="SELECT * from web_resource_url where city_name='"+cityName+"'";
    try:
        cur.execute(sql);
        keyData = cur.fetchall();
        resourceDict=[];
        if(len(keyData)>0):
            for record in keyData:
                dictItem={};
                dataDict={};
                cityCode=record[0];
                cityName=record[1];
                comUrl=record[2];
                houseUrl=record[3];
                fromWeb=record[4];
                dataDict["cityCode"]=cityCode;
                dataDict["cityName"] = cityName;
                dataDict["comUrl"] = comUrl;
                dataDict["houseUrl"] = houseUrl;
                dataDict["fromWeb"] = fromWeb;
                dictItem["fromWeb"]=fromWeb;
                dictItem["data"] = dataDict;
                resourceDict.append(dictItem);
        return resourceDict;
    except Exception as e:
        print(e);
    finally:
        cur.close();
        conn.close();


def queryLianjiaCommunityByEnvelope(xmin,ymin, xmax, ymax, comUrl,houseTabUrl, fromWeb):
    fromWeb = "lianjia";
    baiduMinPoint = bd_encrypt(xmin,ymin);
    baiduMaxPoint = bd_encrypt(xmax,ymax);
    url = "https://ajax.lianjia.com/ajax/mapsearch/area/communityZufang?min_longitude=" + str(baiduMinPoint["lng"]) + "&max_longitude=" + str(baiduMaxPoint["lng"]) + "&min_latitude=" + str(baiduMinPoint["lat"]) + "&max_latitude=" + str(baiduMaxPoint["lat"]) + "&city_id=" + comUrl;
    houseUrl = "https://ajax.lianjia.com/ajax/housesell/area/communityZufang?city_id=" + houseTabUrl + "&ids=";
    ids = "";
    try:
        res = requests.get(url=url).content;
        total_json = json.loads(res);
        content = total_json.get('data');
        if (content != None):
            for record in content:
                print(record)
                recordDict={};
                id=record.get("id");
                name=record.get("name");
                longitude=record.get("longitude");
                latitude=record.get("latitude");
                recordDict["id"]=id;
                recordDict["name"] = name;
                recordDict["longitude"] = longitude;
                recordDict["latitude"] = latitude;
                recordDict["fromWeb"]=fromWeb;
                recordDict["communityPrice"]=0;
                addOneCommunityIntoDB(recordDict);
    except Exception as e:
        print(e);


def addOneCommunityIntoDB(communityVo):
    print(communityVo)
    id=communityVo["id"];
    name=communityVo["name"];
    longitude = communityVo["longitude"];
    latitude = communityVo["latitude"];
    communityPrice=communityVo["communityPrice"];
    print(communityPrice)
    if(communityPrice==None):
        communityPrice=0;
    if(name==None):
        name="None";
    fromWeb=communityVo["fromWeb"];
    gaodePoint = bd_decrypt(longitude,latitude);
    geomStr = "POINT(" + str(gaodePoint["lng"]) + " " + str(gaodePoint["lat"]) + ")";
    print(geomStr)
    sql="INSERT INTO web_community (id, name, longitude, latitude, geom, from_web, community_price) VALUES ('"+id+"', '"+name+"', '"+str(longitude)+"', '"+str(latitude)+"', "+"st_geomfromText('"+geomStr+"',4326)"+", '"+fromWeb+"', '46375');";
    print(sql)
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    try:
        cur.execute(sql);
        conn.commit();
        cur.close();
        conn.close();
    except Exception as e:
        print(e);
    finally:
        conn.commit();
        cur.close();
        conn.close();




CrawlerHouseByCityCode("3301");