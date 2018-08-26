import random

from CrawlerHouseInfo.Tools.BaiduConventor import *;
from CrawlerHouseInfo.Tools.CoodinateCovertor import *
from CrawlerHouseInfo.community.CrawlerHouseTools import *


def getBaiduCityVO(cityName):
    url="https://map.baidu.com/?qt=s&wd="+str(cityName)
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }
        html_doc = requests.get(url, timeout=30,headers=headers).content
        total_json = json.loads(html_doc);
        content = total_json.get('content');
        cityVo={};
        city_type=content.get("city_type");
        code=content.get("code");
        cname=content.get("cname");
        geo = content.get("geo");
        point=getBaiduPoint(geo);
        print(point)
        uid = content.get("uid");
        cityVo["city_type"]=city_type;
        cityVo["city_code"] = code;
        cityVo["city_name"] = cname;
        cityVo["point"] = point;
        cityVo["uid"] = uid;
        print(cityVo)
    except Exception as e:
        print(e);


def getPoiInfoByUid(uid):
    print(uid);
    url="https://map.baidu.com/?&qt=detailConInfo&compat=1&uid="+uid
    print(url)
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }
        html_doc = requests.get(url, timeout=30, headers=headers).content
        total_json = json.loads(html_doc);
        content = total_json.get('content');
        poiVo = {};
        cityCode = content.get("c");
        addr = content.get("addr");
        name = content.get("name");
        geo = content.get("geo");
        std_tag = content.get("std_tag");
        street_id=content.get("street_id");
        point = getBaiduPoint(geo);
        uid = content.get("uid");
        poiVo["cityCode"]=cityCode;
        poiVo["addr"] = addr;
        poiVo["std_tag"] = std_tag;
        poiVo["street_id"] = street_id;
        poiVo["point"] = point;
        poiVo["uid"] = uid;
        return  poiVo
    except Exception as e:
        print(e);

#根据关键字查询
def getBaiduPoiByKeyWord(keyWord,cityName,sug_forward):
    cityCodeDict = getCityCodeByCityName(cityName);
    cityCode = cityCodeDict["baiduCode"];
    print(cityCode)
    url = "https://map.baidu.com/?qt=s&sug=1&wd=" + str(keyWord)+"&c="+str(cityCode)+"&sug_forward="+sug_forward;
    print(url)
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }
        html_doc = requests.get(url, timeout=30, headers=headers).content
        total_json = json.loads(html_doc);
        content = total_json.get('content');
        place_info=total_json.get("place_info");
        if(content!=None):
            pois=[];
            print("----------------------------------------------")
            for item in content:
                poiVo={};
                area_name=item.get("area_name");
                addr=item.get("addr");
                address_norm=item.get("address_norm");
                catalogID=item.get("catalogID");
                di_tag=item.get("di_tag");
                name=item.get("name");
                geo_type=item.get("geo_type");
                new_catalog_id=item.get("new_catalog_id");
                uid=item.get("uid");
                std_tag=item.get("std_tag");
                geo=item.get("geo");
                point=getBaiduPoint(geo);
                poiVo["area_name"]=area_name;
                poiVo["addr"] = addr;
                poiVo["address_norm"] = address_norm;
                poiVo["catalogID"] = catalogID;
                poiVo["name"] = name;
                poiVo["di_tag"] = di_tag;
                poiVo["geo_type"] = geo_type;
                poiVo["new_catalog_id"] = new_catalog_id;
                poiVo["uid"] = uid;
                poiVo["std_tag"] = std_tag;
                poiVo["point"] = point;
                pois.append(poiVo);
                print(poiVo)
                resultVo=getLineOrPolygonByUid(uid,geo_type);
                print(resultVo)
                if(resultVo["geomType"]!="point"):
                    geomType=resultVo["geomType"];
                    geomStr=resultVo["geomStr"];
                    addOneBaiduGeomIntoDB(uid,geomStr,geomType,name);
            return  pois;
    except Exception as e:
        print(e);

#将线或面插入数据库  type(line,polygon)
def addOneBaiduGeomIntoDB(uid,geomStr,type,name):
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    currentDate = time.strftime("%Y-%m-%d", time.localtime());
    if(type=="line"):
        sql="INSERT INTO baidu_line(uid, geom, insert_time,name)VALUES('"+uid+"', "+"st_geomfromText('LINESTRING("+geomStr+")',4326)"+", '"+currentDate+"','"+name+"');";
    elif (type == 'polygon'):
        sql = "INSERT INTO baidu_polygon(uid, geom, insert_time,name)VALUES('" + uid + "', " + "st_geomfromText('POLYGON((" + geomStr + "))',4326)" + ", '" + currentDate + "','"+name+"');";
    else:
        sql="INSERT INTO baidu_poi_disable (uid) VALUES ('"+uid+"');";
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

#根据uid获取线或面的详细数据
def getLineOrPolygonByUid(uid,geo_type):
    print("000000000000000000000000000000000000000000000000000000000000")
    url="";
    if(geo_type==1):
        url="https://map.baidu.com/?qt=ext&uid="+uid+"&ext_ver=new&l=19"
    else:
        url="https://map.baidu.com/?qt=ext&uid="+uid+"&l=17"
    print(url)
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }
        html_doc = requests.get(url, timeout=30,headers=headers).content
        total_json = json.loads(html_doc);

        content = total_json.get('content');
        print(content)
        geo=content.get("geo");

        geomType="";
        geomstr="";
        if(geo==None):
            geomType="point";
            geomstr = "None";
        else:
            firstChar = geo[0:1];
            if(firstChar=='2'):
                geomType = "line";
                geomstr=getBaiduLinePoints(geo)
            elif(firstChar=='4'):
                geomType="polygon"
                geomstr=getBaiduLinePoints(geo)
            else:
                geomType = "other"
                geomstr="None";
        result={};
        result["geomType"]=geomType;
        result["geomStr"]=geomstr;
        print(result)
        return result;
    except Exception as e:
        print(e);

#获取百度线与面的外边
def getBaiduLinePoints(geoStr):
    firstChar = geoStr[0:1];
    if (firstChar == '2'):
        polyline = geoStr[2:].replace('|', ';').split(";")[2].split(",");
    elif (firstChar == '4'):
        polyline = geoStr.split('|')[2].replace("1-","").replace(";","").split(",");
    else:
        return None;
    index = 1;
    points = [];
    point = [0, 0];
    for it in polyline:
        if index % 2 != 0:
            point = [0, 0];
            point[0] = float(it);
        else:
            point[1] = float(it);
            lnglat=convertMC2LL(point[0],point[1]);
            gaodePoint = bd_decrypt(float(lnglat["lng"]), float(lnglat["lat"]));
            points.append(gaodePoint);
        index = index + 1;
    geomStr = "";
    for point in points:
        geomStr = geomStr + str(point['lng']) + " " + str(point['lat']);
        index = index + 1;
        if index != len(points):
            geomStr = geomStr + ","
    print(geomStr)
    geomStr=geomStr[0:len(geomStr)-1];
    return geomStr;

#根据点point Str获取经纬度坐标
def getBaiduPoint(geoStr):
    strlen = len(geoStr)
    dataStr = geoStr[2: strlen].replace("|", ";");
    dataArr = dataStr.split(";");
    xyArr = dataArr[0].split(",");
    x=float(xyArr[0]);
    y=float(xyArr[1]);
    location=convertMC2LL(x,y);
    return location;



def getOneKeyWord():
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    sql = "SELECT name from web_community where city_code='3609'";
    try:
        cur.execute(sql);
        keyData = cur.fetchall();
        print(keyData)
        return keyData;
    except Exception as e:
        print(e);
    finally:
        cur.close();
        conn.close();



def getOneUid(cityCode,areaName):
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    sql = "SELECT uid,name,geo_type from baidu_poi where city_code='"+str(cityCode)+"' and area_name='"+areaName+"' and  uid not in (SELECT uid from baidu_polygon) and uid not in (SELECT uid from baidu_line) and uid not in (SELECT uid from baidu_poi_disable) LIMIT 1" ;
    try:
        cur.execute(sql);
        keyData = cur.fetchall();
        print(keyData)
        return keyData;
    except Exception as e:
        print(e);
    finally:
        cur.close();
        conn.close();

def batchgetBaiduPoiByKeyWord():
    a = 1;
    while a == 1:
        keywords=getOneUid(278,'宜春市袁州区');
        if(len(keywords)<1):
            break;
        print(keywords)
        uid=keywords[0][0];
        name=keywords[0][1];
        geo_type=keywords[0][2];
        print("---------------------------------------")
        resultVo = getLineOrPolygonByUid(uid, geo_type);
        geomType = resultVo["geomType"];
        geomStr = resultVo["geomStr"];
        addOneBaiduGeomIntoDB(uid, geomStr, geomType, name);
        sleepTime = random.randint(2, 3);
        print(sleepTime);
        time.sleep(sleepTime);

def getComsRow():
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    sql = "SELECT name from web_community where city_code='3609'";
    try:
        cur.execute(sql);
        keyData = cur.fetchall();
        print(keyData)
        return keyData;
    except Exception as e:
        print(e);
    finally:
        cur.close();
        conn.close();

def getSugForward(kw,citycode):
    url = "https://map.baidu.com/?qt=s&wd=" + str(kw) + "&c=" + str(citycode);
    print(url)
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }
        html_doc = requests.get(url, timeout=30, headers=headers).content
        total_json = json.loads(html_doc);
        content = total_json.get('content');
        place_info = total_json.get("place_info");
        if (place_info != None):
            for item in content:
                uid = item.get("uid");
                if(uid!=None):
                    return uid;
            return None;
    except Exception as e:
        print(e);

#获取智能提示
def getSuggest(kw,cityName):
    cityCodeDict=getCityCodeByCityName(cityName);
    baiduCityCode=cityCodeDict["baiduCode"];
    envelope=getCityEnvelopeByCityName(cityName);
    minY=envelope['minY'] ;
    maxY=envelope['maxY'] ;
    minX=envelope['minX'] ;
    maxX=envelope['maxX'];
    minXY=convertLL2MC(minX,minY);
    maxXY=convertLL2MC(maxX,maxY);
    minLng=minXY["x"];
    minLat=minXY["y"];
    maxLng=maxXY["x"];
    maxLat=maxXY["y"]
    url = "https://map.baidu.com/su?wd=" + str(kw) + "&cid=" + str(baiduCityCode) + "&type=0&newmap=1&b=(" + str(minLng) + "%2c" + str(minLat) + "%3B" + str(maxLng) + "%2C" + str(maxLat) + ")" + "&pc_ver=2";
    print(url)
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }
        html_doc = requests.get(url, timeout=30, headers=headers).content
        total_json = json.loads(html_doc);
        sus=total_json.get("s");
        vos=[];
        for s in sus:
            sArr=s.split("$");
            sVo={};
            sVo["name"]=sArr[3];
            sVo["uid"]=sArr[5];
            print(sVo)
            vos.append(sVo);
        return vos;
    except Exception as e:
        print(e);


def crawlerBaiduRegion():
    keywords = getComsRow();
    for keyword in keywords:
        name = "宜春"+keyword[0];
        sug_forward=getSuggest(name,"宜春市");
        name = keyword[0];
        sug_forward=getSugForward(name,278);
        if(sug_forward!=None):
            for item in sug_forward:
                kw=item["name"];
                uid=item["uid"];
                getBaiduPoiByKeyWord(name, "宜春市",uid);
                sleepTime = random.randint(1, 2);
                print(sleepTime);
                time.sleep(sleepTime);

crawlerBaiduRegion();
#batchgetBaiduPoiByKeyWord();
# getBaiduPoiByKeyWord('宜春碧桂园',278);


#getSuggest("宜春碧桂园","宜春市");

