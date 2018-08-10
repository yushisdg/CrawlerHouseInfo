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

def CrawlerHouseByCityCode(cityCode):
    print(cityCode)
    indexDict=getCityGridsMinIndexAndMaxIndexByCityCode(cityCode);
    for index in range(indexDict["minIndex"],indexDict["maxIndex"]):
    # for index in range(1550, indexDict["maxIndex"]):
       gridDict=SelectGridByCityCodeAndIndex(cityCode,index);
       if(gridDict!=None):
           xmin=gridDict['xmin'];
           ymin=gridDict['ymin'];
           xmax=gridDict['xmax'] ;
           ymax=gridDict['ymax'];
           queryWhereCityByItem(xmin,ymin,xmax,ymax);
           sleepTime = random.randint(10, 15);
           print(sleepTime);
           time.sleep(sleepTime);
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
                 print("--------")
                 queryLianjiaCommunityByEnvelope(xmin, ymin, xmax, ymax, comUrl, houseUrl, fromWeb);
             elif(fromWeb=="anjuke"):
                 print("--------")
                 queryAnjuKeCommunityByEnvelope(xmin, ymin, xmax, ymax, comUrl, houseUrl, fromWeb);
             elif (fromWeb == "dankegongyu"):
                 print(fromWeb)
                 queryDanKeGongYuCommunityByEnvelope(xmin, ymin, xmax, ymax, comUrl, houseUrl, fromWeb);
             elif (fromWeb == "woaiwojia"):
                 print("--------")
                 queryWoaiWoJiaCommunityByEnvelope(xmin, ymin, xmax, ymax, comUrl, houseUrl, fromWeb);
             elif (fromWeb == "ishangzu"):
                 print("--------")
                 queryIShangzuCommunityByEnvelope(xmin, ymin, xmax, ymax, comUrl, houseUrl, fromWeb);
             elif (fromWeb == "ziru"):
                 print("----------")
                 queryZiRuCommunityByEnvelope(xmin, ymin, xmax, ymax, comUrl, houseUrl, fromWeb);
             elif (fromWeb == "fangtianxia"):
                 print("--------")
                 queryFangTianxiaCommunityByEnvelope(xmin, ymin, xmax, ymax, comUrl, houseUrl, fromWeb);
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

#获取链家的数据
def queryLianjiaCommunityByEnvelope(xmin,ymin, xmax, ymax, comUrl,houseTabUrl, fromWeb):
    fromWeb = "lianjia";
    baiduMinPoint = bd_encrypt(xmin,ymin);
    baiduMaxPoint = bd_encrypt(xmax,ymax);
    url = "https://ajax.lianjia.com/ajax/mapsearch/area/communityZufang?min_longitude=" + str(baiduMinPoint["lng"]) + "&max_longitude=" + str(baiduMaxPoint["lng"]) + "&min_latitude=" + str(baiduMinPoint["lat"]) + "&max_latitude=" + str(baiduMaxPoint["lat"]) + "&city_id=" + comUrl;
    houseUrl = "https://ajax.lianjia.com/ajax/housesell/area/communityZufang?city_id=" + houseTabUrl + "&ids=";
    ids = "";
    try:
        print(url)
        res = requests.get(url=url).content;
        total_json = json.loads(res);
        content = total_json.get('data');
        if (content != None):
            idArr=[];
            communityVos=[];
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
                recordDict["fromWeb"] = fromWeb;
                recordDict["communityPrice"]=0;
                communityVos.append(recordDict);
                idArr.append(id);
            addOneCommunityIntoDB(communityVos);
            for ids in idArr:
                reqUrl=houseUrl+ids;
                addLianjiaHouseFromWeb(reqUrl,fromWeb);
                time.sleep(1);
    except Exception as e:
        print(e);

def queryAnjuKeCommunityByEnvelope(xmin,ymin, xmax, ymax, comUrl,houseTabUrl, fromWeb):
    fromWeb = "anjuke";
    baiduMinPoint = bd_encrypt(xmin, ymin);
    baiduMaxPoint = bd_encrypt(xmax, ymax);
    url = "https://" + comUrl + ".anjuke.com/v3/ajax/map/sale/2856/facet/?zoom=17&lat=" + str(baiduMinPoint["lat"]) + "_" + str(baiduMaxPoint["lat"]) + "&lng=" + str(baiduMinPoint["lng"]) + "_" + str(baiduMaxPoint["lng"]);
    houseUrl = "https://" + houseTabUrl + ".zu.anjuke.com/v3/ajax/map/rent/63/prop_list/?zoom=17&lat=" + str(baiduMinPoint["lat"]) + "_" + str(baiduMaxPoint["lat"]) + "&lng=" + str(baiduMinPoint["lng"]) + "_" + str(baiduMaxPoint["lng"]);
    try:
        print(url)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }
        res = requests.get(url=url,headers=headers).content;
        total_json = json.loads(res);
        content = total_json.get('val');
        if (content != None):
            list=content.get("comms");
            idArr = [];
            communityVos = [];
            for record in list:
                print(record)
                recordDict = {};
                id = record.get("id");
                name = record.get("truncate_name");
                longitude = record.get("lng");
                latitude = record.get("lat");
                communityPrice = record.get("mid_price");
                recordDict["id"] = id;
                recordDict["name"] = name;
                recordDict["longitude"] = longitude;
                recordDict["latitude"] = latitude;
                recordDict["fromWeb"] = fromWeb;
                recordDict["communityPrice"] = communityPrice;
                communityVos.append(recordDict);
                idArr.append(id);
            addOneCommunityIntoDB(communityVos);
            if(len(idArr)>0):
                pages=getAnjuKeHousePageCount(houseUrl);
                for page in range(1,pages):
                    tempUrl=houseUrl+"&p="+str(page);
                    addAnJuKeHouseFromWeb(tempUrl,fromWeb);
                    sleepTime = random.randint(3, 6);
                    print(sleepTime);
                    time.sleep(sleepTime);
    except Exception as e:
        print(e);

def queryIShangzuCommunityByEnvelope(xmin,ymin, xmax, ymax, comUrl,houseTabUrl, fromWeb):
    centX = (xmin + xmax) / 2;
    centY = (ymin + ymax) / 2;
    radius = 1;
    url = comUrl + "lng=" + str(centX) + "&lat=" + str(centY) + "&radius=" + str(radius);
    houseUrl = houseTabUrl + "lng=" + str(centX)+ "&lat=" + str(centY) + "&radius=" + str(radius) + "&type=house";
    try:
        print(url)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }
        res = requests.get(url=url, headers=headers).content;
        print(res)
        total_json = json.loads(res);
        print(total_json)
        content = total_json.get('items');
        if (content != None):
            idArr = [];
            communityVos = [];
            for record in content:
                print(record)
                recordDict = {};
                id = record.get("id");
                name = record.get("name");
                longitude = record.get("lng");
                latitude = record.get("lat");
                recordDict["id"] = id;
                recordDict["name"] = name;
                recordDict["longitude"] = longitude;
                recordDict["latitude"] = latitude;
                recordDict["fromWeb"] = fromWeb;
                recordDict["communityPrice"] = 0;
                communityVos.append(recordDict);
                idArr.append(id);
            addOneCommunityIntoDB(communityVos);
            addIShangZuHouseFromWeb(houseUrl, fromWeb)
    except Exception as e:
        print(e);


def queryZiRuCommunityByEnvelope(xmin,ymin, xmax, ymax, comUrl,houseTabUrl, fromWeb):
    fromWeb="ziru";
    baiduMinPoint = bd_encrypt(xmin, ymin);
    baiduMaxPoint = bd_encrypt(xmax, ymax);
    url = "http://" + comUrl + ".ziroom.com/map/room/count?min_lng=" + str(baiduMinPoint["lng"]) + "&max_lng=" + str(baiduMaxPoint["lng"]) + "&min_lat=" + str(baiduMinPoint["lat"])  + "&max_lat=" + str(baiduMaxPoint["lat"]) ;
    houseUrl = "http://" + houseTabUrl + ".ziroom.com/map/room/list?min_lng=" + str(baiduMinPoint["lng"]) + "&max_lng=" + str(baiduMaxPoint["lng"]) + "&min_lat=" + str(baiduMinPoint["lat"])  + "&max_lat=" + str(baiduMaxPoint["lat"]) ;
    try:
        print(url)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
       }
        res = requests.get(url=url,headers=headers).content;
        print(res)
        total_json = json.loads(res);
        print(total_json)
        content = total_json.get('data');
        if (content != None):
            idArr = [];
            communityVos = [];
            for record in content:
                print(record)
                recordDict = {};
                id = record.get("id");
                name = record.get("name");
                longitude = record.get("longitude");
                latitude = record.get("latitude");
                recordDict["id"] = id;
                recordDict["name"] = name;
                recordDict["longitude"] = longitude;
                recordDict["latitude"] = latitude;
                recordDict["fromWeb"] = fromWeb;
                recordDict["communityPrice"] = 0;
                communityVos.append(recordDict);
                idArr.append(id);
            addOneCommunityIntoDB(communityVos);
            addZiRuFromWeb(houseUrl,fromWeb)
    except Exception as e:
        print(e);

#获取我爱我家数据
def queryWoaiWoJiaCommunityByEnvelope(xmin,ymin, xmax, ymax, comUrl,houseTabUrl, fromWeb):
    fromWeb = "woaiwojia";
    baiduMinPoint = bd_encrypt(xmin, ymin);
    baiduMaxPoint = bd_encrypt(xmax, ymax);
    url = "https://" + comUrl + ".5i5j.com/map/ajax/location/rent?bounds={%22e%22:" + str(baiduMaxPoint["lng"])  + ",%22w%22:" + str(baiduMinPoint["lng"]) + ",%22s%22:" + str(baiduMinPoint["lat"]) + ",%22n%22:" + str(baiduMaxPoint["lat"]) + "}&boundsLevel=5";
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
            'Cookie': '_ga=GA1.2.1436762322.1532586422; yfx_c_g_u_id_10000001=_ck18072614270312424983055951239; yfx_f_l_v_t_10000001=f_t_1532586423238__r_t_1532586423238__v_t_1532586423238__r_c_0; yfx_mr_n_10000001=baidu%3A%3Amarket_type_ppzq%3A%3A%3A%3A%3A%3A%3A%3A%3A%3A%25E6%25A0%2587%25E9%25A2%2598%3A%3A%3A%3A%3A%3A%3A%3A%25E5%25B7%25A6%25E4%25BE%25A7%25E6%25A0%2587%25E9%25A2%2598%3A%3A%25E6%25A0%2587%25E9%25A2%2598%3A%3A170%3A%3Apmf_from_adv%3A%3Ahz.5i5j.com%2F; yfx_mr_f_n_10000001=baidu%3A%3Amarket_type_ppzq%3A%3A%3A%3A%3A%3A%3A%3A%3A%3A%25E6%25A0%2587%25E9%25A2%2598%3A%3A%3A%3A%3A%3A%3A%3A%25E5%25B7%25A6%25E4%25BE%25A7%25E6%25A0%2587%25E9%25A2%2598%3A%3A%25E6%25A0%2587%25E9%25A2%2598%3A%3A170%3A%3Apmf_from_adv%3A%3Ahz.5i5j.com%2F; yfx_key_10000001=; Hm_lvt_94ed3d23572054a86ed341d64b267ec6=1532586430; _Jo0OQK=578A5ADAC307BEDFA63FF68200394DA90DECD4E03E9F31135822CDBA3DBC524284C87DCDDC8964231AE28224BD99B4C0DD19266E8CB23C67ADFD9B17DC381B85967FFBBE0C390CBD8D402631C467319B15B02631C467319B15B869297F6895F5D91GJ1Z1QQ==; PHPSESSID=giucbr65edajrq13m9ndf6qvvg; domain=hz'
        }
        res = requests.get(url=url,headers=headers).content;
        print(res)
        print(url)
        print("------------------------------------------------------")
        total_json = json.loads(res);
        print(total_json)
        content = total_json.get('data');
        if (content != None):
            resJson=content.get("res");
            if(resJson!=None):
                idArr = [];
                communityVos=[];
                mapArr=resJson.get("map");
                for record in mapArr:
                    recordDict = {};
                    id = record.get("id");
                    name = record.get("name");
                    longitude = record.get("lng");
                    latitude = record.get("lat");
                    communityPrice=record.get("price");
                    recordDict["id"] = id;
                    recordDict["name"] = name;
                    recordDict["longitude"] = longitude;
                    recordDict["latitude"] = latitude;
                    recordDict["fromWeb"] = fromWeb;
                    recordDict["communityPrice"] = communityPrice;
                    communityVos.append(recordDict);
                    idArr.append(str(id));
                addOneCommunityIntoDB(communityVos);
                for ids in idArr:
                    reqUrl = url+"&locationId=" + ids+"&locationLevel=5&pageSize=20&page=1"
                    addWoAiWoJiaHouseFromWeb(reqUrl,fromWeb);
                    sleepTime = random.randint(3, 6);
                    print(sleepTime);
                    time.sleep(sleepTime);
    except Exception as e:
        print(e);
    return 0;

#获取房天下数据
def queryFangTianxiaCommunityByEnvelope(xmin,ymin, xmax, ymax, comUrl,houseTabUrl, fromWeb):
    fromWeb = "fangtianxia";
    baiduMinPoint = bd_encrypt(xmin, ymin);
    baiduMaxPoint = bd_encrypt(xmax, ymax);
    url = "http://esf."+comUrl+".fang.com/map/?x1=" + str(baiduMinPoint["lng"]) + "&y1=" + str(baiduMinPoint["lat"]) + "&x2=" +str(baiduMaxPoint["lng"])  + "&y2=" +str(baiduMaxPoint["lat"])  + "&zoom=16&a=ajaxSearch";
    houseUrl="http://zu."+comUrl+".fang.com/map/?x1=" + str(baiduMinPoint["lng"]) + "&y1=" + str(baiduMinPoint["lat"]) + "&x2=" +str(baiduMaxPoint["lng"])  + "&y2=" +str(baiduMaxPoint["lat"])  + "&zoom=16&a=ajaxSearch";
    try:
        res = requests.get(url=url).content;
        print(url)
        total_json = json.loads(res);
        print(total_json)
        content = total_json.get('loupan');
        if (content != None):
            hit=content.get("hit");
            if(hit!=None):
                idArr = [];
                communityVos=[];
                for record in hit:
                    print(record)
                    recordDict = {};
                    id = record.get("newcode");
                    name = record.get("projname");
                    longitude = record.get("x");
                    latitude = record.get("y");
                    communityPrice=record.get("price");
                    recordDict["id"] = id;
                    recordDict["name"] = name;
                    recordDict["longitude"] = longitude;
                    recordDict["latitude"] = latitude;
                    recordDict["fromWeb"] = fromWeb;
                    recordDict["communityPrice"] = communityPrice;
                    communityVos.append(recordDict);
                    idArr.append(id);
                addOneCommunityIntoDB(communityVos);
                for ids in idArr:
                    reqUrl = houseUrl+"&newCode=" + ids;
                    addFangTianXiaHouseFromWeb(reqUrl,fromWeb);
                    time.sleep(1);
    except Exception as e:
        print(e);

def queryDanKeGongYuCommunityByEnvelope(xmin,ymin, xmax, ymax, comUrl,houseTabUrl, fromWeb):
    fromWeb = "dankegongyu";
    baiduMinPoint = bd_encrypt(xmin, ymin);
    baiduMaxPoint = bd_encrypt(xmax, ymax);
    url = "https://www.dankegongyu.com/map/search-by-params?left_bottom_lng=" + str(baiduMinPoint["lng"])+ "&left_bottom_lat=" + str(baiduMinPoint["lat"]) + "&right_top_lng=" + str(baiduMaxPoint["lng"]) + "&right_top_lat=" + str(baiduMaxPoint["lat"]);
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
        'Cookie':comUrl
    }
    try:
        res = requests.get(url=url,headers=headers).content;
        print(url)
        total_json = json.loads(res);
        print(total_json)
        content = total_json.get('data');
        if (content != None):
            hit = content.get("xiaoquList");
            if (hit != None):
                idArr = [];
                communityVos = [];
                for record in hit:
                    print(record)
                    recordDict = {};
                    id = record.get("id");
                    name = record.get("xiaoquName");
                    longitude = record.get("longitude");
                    latitude = record.get("latitude");
                    recordDict["id"] = id;
                    recordDict["name"] = name;
                    recordDict["longitude"] = longitude;
                    recordDict["latitude"] = latitude;
                    recordDict["fromWeb"] = fromWeb;
                    recordDict["communityPrice"] = 0;
                    communityVos.append(recordDict);
                    idArr.append(id);
                addOneCommunityIntoDB(communityVos);
                addDanKeHouseFromWebCookie(url,fromWeb,houseTabUrl)
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
        gaodePoint = bd_decrypt(float(longitude),float(latitude));
        geomStr="";
        if(fromWeb=="ishangzu"):
            geomStr = "POINT(" + str(longitude) + " " + str(latitude) + ")";
        else:
            geomStr = "POINT(" + str(gaodePoint["lng"]) + " " + str(gaodePoint["lat"]) + ")";
        sql="INSERT INTO web_community (id, name, longitude, latitude, geom, from_web, community_price) VALUES ('"+str(id)+"', '"+name+"', '"+str(longitude)+"', '"+str(latitude)+"', "+"st_geomfromText('"+geomStr+"',4326)"+", '"+fromWeb+"', '"+str(communityPrice)+"');";
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





# CrawlerHouseByCityCode("3101");
# CrawlerHouseByCityCode("3301");

threading.Thread(target=CrawlerHouseByCityCode,args=("4301",)).start()

threading.Thread(target=CrawlerHouseByCityCode,args=("3302",)).start()


threading.Thread(target=CrawlerHouseByCityCode,args=("4401",)).start()

threading.Thread(target=CrawlerHouseByCityCode,args=("4403",)).start()


threading.Thread(target=CrawlerHouseByCityCode,args=("1101",)).start()

threading.Thread(target=CrawlerHouseByCityCode,args=("1201",)).start()


threading.Thread(target=CrawlerHouseByCityCode,args=("3501",)).start()

threading.Thread(target=CrawlerHouseByCityCode,args=("3502",)).start()


# threading.Thread(target=CrawlerHouseByCityCode,args=("3101",)).start()
#
# threading.Thread(target=CrawlerHouseByCityCode,args=("3301",)).start()

threading.Thread(target=CrawlerHouseByCityCode,args=("3601",)).start()

threading.Thread(target=CrawlerHouseByCityCode,args=("5301",)).start()


threading.Thread(target=CrawlerHouseByCityCode,args=("6101",)).start()

threading.Thread(target=CrawlerHouseByCityCode,args=("4101",)).start()


threading.Thread(target=CrawlerHouseByCityCode,args=("3205",)).start()

threading.Thread(target=CrawlerHouseByCityCode,args=("3201",)).start()


threading.Thread(target=CrawlerHouseByCityCode,args=("3202",)).start()

threading.Thread(target=CrawlerHouseByCityCode,args=("3702",)).start()



threading.Thread(target=CrawlerHouseByCityCode,args=("3701",)).start()

threading.Thread(target=CrawlerHouseByCityCode,args=("5101",)).start()


threading.Thread(target=CrawlerHouseByCityCode,args=("5001",)).start()

threading.Thread(target=CrawlerHouseByCityCode,args=("4201",)).start()

