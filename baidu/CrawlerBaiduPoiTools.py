from CrawlerHouseInfo.baidu.BaiduAPITools import *
from CrawlerHouseInfo.baidu.BaiduAPITools import *


def crawlerBaiduPoi(cityName,searchType):
    # cityVo=getBaiduCityVO(cityName)
    cityVo=getCityCodeByCityName(cityName);
    cityCode=cityVo["cityCode"][0:4];
    baiduCode=cityVo["baiduCode"]
    indexDict = getCityGridsMinIndexAndMaxIndexByCityCode(cityCode);
    for index in range(indexDict["minIndex"], indexDict["maxIndex"]):
        gridDict = SelectGridByCityCodeAndIndex(cityCode, index);
        if (gridDict != None):
            xmin = gridDict['xmin'];
            ymin = gridDict['ymin'];
            xmax = gridDict['xmax'];
            ymax = gridDict['ymax'];
            centerX = gridDict['centerX']
            centerY = gridDict['centerY']
            LngLat=bd_encrypt(centerX,centerY)
            surroundPoi=getSurroundPoi(LngLat["lng"],LngLat["lat"]);
            if(surroundPoi!=None):
                sUid=surroundPoi["uid"];
                url=getUrl(xmin,ymin,xmax,ymax,searchType,centerX,centerY,baiduCode,sUid,0);
                pages=getPoiPages(url);
                for pn in range(0,pages+1):
                    url = getUrl(xmin, ymin, xmax, ymax, searchType, centerX, centerY, baiduCode, sUid, pn);
                    count=getPoiByUrl(url,baiduCode);
                    if(count==0):
                        break
            print(gridDict)


def getPoiByUrl(url,cityCode):
    print(url)
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }
        html_doc = requests.get(url, timeout=30).content
        total_json = json.loads(html_doc);
        content = total_json.get('content');
        poiArr=[];
        if(content!=None):
            for poi in content:
                print(poi)
                poiVo = {};
                addr = poi.get("addr");
                name = poi.get("name");
                geo = poi.get("geo");
                std_tag = poi.get("std_tag");
                street_id = poi.get("street_id");
                area_name= poi.get("area_name");
                address_norm = poi.get("address_norm");
                di_tag = poi.get("di_tag");
                new_catalog_id = poi.get("new_catalog_id");
                std_tag_id = poi.get("std_tag_id");
                geo_type=poi.get("geo_type")
                point = getBaiduPoint(geo);
                uid = poi.get("uid");
                poiVo["cityCode"] = cityCode;
                poiVo["addr"] = addr;
                poiVo["std_tag"] = std_tag;
                poiVo["point"] = point;
                poiVo["uid"] = uid;
                poiVo["name"] = name;
                poiVo["street_id"] = street_id;
                poiVo["area_name"] = area_name;
                poiVo["address_norm"] = address_norm;
                poiVo["di_tag"] = di_tag;
                poiVo["new_catalog_id"] = new_catalog_id;
                poiVo["std_tag_id"] = std_tag_id;
                poiVo["geo_type"]=geo_type;
                poiArr.append(poiVo);
            addPoisIntoDB(poiArr);
            return len(poiArr);
        else:
            return 0;
    except Exception as e:
        print(e);

def addPoisIntoDB(poiVos):
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    for poi in poiVos:
        addr = poi["addr"];
        name = poi["name"];
        std_tag = poi["std_tag"];
        street_id = poi["street_id"];
        if(street_id==None):
            street_id="None";
        area_name = poi["area_name"];
        address_norm = poi["address_norm"];
        di_tag = poi["di_tag"];

        new_catalog_id = poi["new_catalog_id"];
        print("----------------------------------")
        print(poi)
        std_tag_id = poi["std_tag_id"];
        point = poi["point"];
        geo_type=poi["geo_type"];

        lng=point["lng"];
        lat=point["lat"];
        cityCode=poi["cityCode"];

        gaodePoint = bd_decrypt(float(lng), float(lat));
        geomStr = "POINT(" + str(gaodePoint["lng"]) + " " + str(gaodePoint["lat"]) + ")";
        uid = poi["uid"];
        currentDate = time.strftime("%Y-%m-%d", time.localtime());
        sql="INSERT INTO baidu_poi (uid, addr, std_tag, street_id, name, area_name, city_code, address_norm, di_tag, new_catalog_id, std_tag_id, geom, insert_time,geo_type) VALUES ('"+uid+"', '"+addr+"', '"+std_tag+"', '"+street_id+"', '"+name+"', '"+area_name+"', '"+str(cityCode)+"', '"+address_norm+"', '"+str(di_tag)+"', '"+str(new_catalog_id)+"', '"+str(std_tag_id)+"', "+"st_geomfromText('"+geomStr+"',4326)"+", '"+currentDate+"','"+str(geo_type)+"');"
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



#获取周边POI
def getSurroundPoi(x,y):
    result=convertLL2MC(x,y);
    print(result)
    url = "https://api.map.baidu.com/?qt=rgc&x=" + str(result["x"]) + "&y=" + str(result["y"]) + "&dis_poi=100&poi_num=10&ie=utf-8&oue=1&res=webmap";
    print(url)
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }
        html_doc = requests.get(url, timeout=30).content
        total_json = json.loads(html_doc);
        content = total_json.get('content');
        surround_poi=content.get("surround_poi");
        if(len(surround_poi)>0):
            item=surround_poi[0];
            uid=item.get("uid");
            name=item.get("name");
            PoiVo={};
            PoiVo["name"]=name;
            PoiVo["uid"]=uid;
            return PoiVo;
        print(content)
    except Exception as e:
        print(e);

def getPoiPages(url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }
        html_doc = requests.get(url, timeout=30).content
        total_json = json.loads(html_doc);
        result = total_json.get('result');
        if (result!=None):
            total=result.get("total");
            pages=math.ceil(total/10);
            return total;
    except Exception as e:
        print(e);


def getUrl(xmin,ymin,xmax,ymax,wd,centerX,centerY,cityCode,seaechUid,pageNum):
    minLngLat = convertLL2MC(xmin, ymin)
    maxLngLat = convertLL2MC(xmax, ymax)
    centerLngLat = convertLL2MC(centerX, centerY)
    xmin=minLngLat["x"];
    ymin=minLngLat["y"];
    xmax=maxLngLat["x"];
    ymax=maxLngLat["y"];
    centerX=centerLngLat["x"];
    centerY=centerLngLat["y"];
    print(xmax)
    url="https://map.baidu.com/?qt=nb&c=" + str(cityCode) + "&wd=" + wd + "&l=16&b="+"(" + str(xmin) + "," + str(ymin) + ";" + str(xmax) + "," + str(ymax) + ")"+"&uid=85f6c2a6ae5cbd00843304a9&nb_x="+ str(centerX) + "&nb_y=" + str(centerY) + "&gr_radius=1095&pn="+str(pageNum)+"&device_ratio=1&u_loc=13366359,3522323"
    print(url)
    return url;


crawlerBaiduPoi("宜春市","小区")