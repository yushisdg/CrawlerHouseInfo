import json
import math
import time

import requests
from bs4 import BeautifulSoup

from CrawlerHouseInfo.Tools.CityGridTools import *
from CrawlerHouseInfo.config.jdbcConfig import *


def addLianjiaHouseFromWeb(url,fromWeb,cityCode):
    try:
        res = requests.get(url=url).content;
        total_json = json.loads(res);
        content = total_json.get('data');
        if (content != None):
            list=content.get("list");
            if(list!=None):
                records=[];
                for record in list:
                    id =record.get("house_code");
                    price =record.get("price_total");
                    area =record.get("rent_area");
                    name =record.get("community_name");
                    communityId =record.get("community_id");
                    recordDict={};
                    recordDict["id"]=id;
                    recordDict["price"]=price;
                    recordDict["name"]=name;
                    recordDict["communityId"]=communityId;
                    recordDict["community_name"]=name;
                    recordDict["area"] = area;
                    recordDict["fromWeb"]=fromWeb;
                    records.append(recordDict);
                addOneHouseIntoDB(records);
    except Exception as e:
        print(e);

def addZiRuFromWeb(url,fromWeb,cityCode):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }
        res = requests.get(url=url,headers=headers).content;
        print(url);
        print(res)
        total_json = json.loads(res);
        content = total_json.get('data');
        if (content != None):
            list=content.get("rooms");
            if(list!=None):
                records=[];
                for record in list:
                    id = record.get("id");
                    room_code = record.get("room_code");
                    house_id = room_code + "_" + id;
                    price =record.get("sell_price");
                    area =record.get("usage_area");
                    name =record.get("resblock_name");
                    communityId =record.get("resblock_id");
                    recordDict={};
                    recordDict["id"]=house_id;
                    recordDict["price"]=price;
                    recordDict["name"]=name;
                    recordDict["communityId"]=communityId;
                    recordDict["community_name"]=name;
                    recordDict["area"] = area;
                    recordDict["fromWeb"]=fromWeb;
                    records.append(recordDict);
                addOneHouseIntoDB(records);
    except Exception as e:
        print(e);


def addIShangZuHouseFromWeb(url,fromWeb,cityCode):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }
        res = requests.get(url=url,headers=headers).content;
        print(url);
        print(res)
        total_json = json.loads(res);
        content = total_json.get('items');
        if (content != None):
            list=content.get("list");
            if(list!=None):
                records=[];
                for record in list:
                    info=record.get("info");
                    areaIndex = info.find("m");
                    emptyIndex = info.find(">");
                    area = info[emptyIndex + 1:areaIndex].replace(" ", "")
                    print(area)
                    houseUrl=record.get("url");
                    print(houseUrl)
                    areaIndex=info.find("/zufang/");
                    emptyIndex = info.find("html");
                    houseId = houseUrl[areaIndex+33:emptyIndex-4].replace(" ", "")
                    print(houseId)
                    price =record.get("price");
                    name =record.get("prem_name");
                    recordDict={};
                    recordDict["id"]=houseId;
                    recordDict["price"]=price;
                    recordDict["communityId"]="None";
                    recordDict["community_name"]=name;
                    recordDict["area"] = area;
                    recordDict["fromWeb"]=fromWeb;
                    records.append(recordDict);
                addOneHouseIntoDB(records);
    except Exception as e:
        print(e);

def addDanKeHouseFromWebCookie(url,fromWeb,houseTabUrl,cityCode):
    print(url)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
        'Cookie': houseTabUrl
    }
    try:
        res = requests.get(url=url,headers=headers).content;
        total_json = json.loads(res);
        content = total_json.get('data');
        if (content != None):
            list = content.get("roomList");
            if (list != None):
                records = [];
                for record in list:
                    id = record.get("id");
                    price = record.get("price");
                    area = record.get("area");
                    communityId = record.get("xiaoquId");
                    recordDict = {};
                    recordDict["id"] = id;
                    recordDict["price"] = price;
                    recordDict["name"] = "None";
                    recordDict["communityId"] = communityId;
                    recordDict["community_name"] = "None";
                    recordDict["area"] = area;
                    recordDict["fromWeb"] = fromWeb;
                    records.append(recordDict);
                addOneHouseIntoDB(records);
    except Exception as e:
        print(e);

def getAnjuKeHousePageCount(url):
    print(url)
    pages=1;
    try:
        res = requests.get(url=url).content;
        total_json = json.loads(res);
        content = total_json.get('val');
        if (content != None):
            pages=content.get("pages");
            if(pages!=None):
                total=pages.get("total");
                pages=math.ceil(total/60)+1;
                return pages;
    except Exception as e:
        print(e);
    return  pages;


def addAnJuKeHouseFromWeb(url,fromWeb,cityCode):
    print(url)
    try:
        print(url)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }
        res = requests.get(url=url, headers=headers).content;
        total_json = json.loads(res);
        content = total_json.get('val');
        if (content != None):
            list=content.get("props");
            if(list!=None):
                records=[];
                for record in list:
                    id =record.get("id");
                    price =record.get("price");
                    area =record.get("area");
                    name =record.get("community_name");
                    communityId =record.get("community_id");
                    recordDict={};
                    recordDict["id"]=id;
                    recordDict["price"]=price;
                    recordDict["name"]=name;
                    recordDict["communityId"]=communityId;
                    recordDict["community_name"]=name;
                    recordDict["area"] = area;
                    recordDict["fromWeb"]=fromWeb;
                    records.append(recordDict);
                addOneHouseIntoDB(records);
    except Exception as e:
        print(e);

def addFangTianXiaHouseFromWeb(url,fromWeb,cityCode):
    try:
        print(url)
        res = requests.get(url=url).content;
        total_json = json.loads(res);
        content = total_json.get('list');
        print(content)
        if (content != None):
            soup = BeautifulSoup(content, 'html.parser', from_encoding='utf-8');
            houseListContent = soup.find(class_="houseList");
            houseList=houseListContent.find_all("dl");
            comNameObject=soup.find(class_="fr");
            comName=comNameObject.find("h3").contents[0];
            records=[];
            for house in houseList:
              a_tab=house.find("a");
              com_id=a_tab.attrs['data_id'];
              href=a_tab.attrs['href'];
              house_id=href.replace(".htm","").replace("http://","").replace("zu.","").replace(".fang.com/chuzu/","")
              print(house_id)
              price=house.find(class_="price").contents
              house_price = price[0].replace("  ", "");
              areaContent=house.find(class_="gray6 mt10 font12").contents
              areaIndex=areaContent[0].find("㎡");
              emptyIndex=areaContent[0].find(" ");
              area=areaContent[0][emptyIndex :areaIndex].replace(" ","")
              recordDict={};
              recordDict["id"]=house_id;
              recordDict["price"]=house_price;
              recordDict["communityId"]=com_id;
              recordDict["community_name"]=comName;
              recordDict["area"] = area;
              recordDict["fromWeb"]=fromWeb;
              records.append(recordDict);
            addOneHouseIntoDB(records);
    except Exception as e:
        print(e);

def addWoAiWoJiaHouseFromWeb(url,fromWeb,cityCode):
    print(url)
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
            'Cookie': '_ga=GA1.2.1436762322.1532586422; yfx_c_g_u_id_10000001=_ck18072614270312424983055951239; yfx_f_l_v_t_10000001=f_t_1532586423238__r_t_1532586423238__v_t_1532586423238__r_c_0; yfx_mr_n_10000001=baidu%3A%3Amarket_type_ppzq%3A%3A%3A%3A%3A%3A%3A%3A%3A%3A%25E6%25A0%2587%25E9%25A2%2598%3A%3A%3A%3A%3A%3A%3A%3A%25E5%25B7%25A6%25E4%25BE%25A7%25E6%25A0%2587%25E9%25A2%2598%3A%3A%25E6%25A0%2587%25E9%25A2%2598%3A%3A170%3A%3Apmf_from_adv%3A%3Ahz.5i5j.com%2F; yfx_mr_f_n_10000001=baidu%3A%3Amarket_type_ppzq%3A%3A%3A%3A%3A%3A%3A%3A%3A%3A%25E6%25A0%2587%25E9%25A2%2598%3A%3A%3A%3A%3A%3A%3A%3A%25E5%25B7%25A6%25E4%25BE%25A7%25E6%25A0%2587%25E9%25A2%2598%3A%3A%25E6%25A0%2587%25E9%25A2%2598%3A%3A170%3A%3Apmf_from_adv%3A%3Ahz.5i5j.com%2F; yfx_key_10000001=; Hm_lvt_94ed3d23572054a86ed341d64b267ec6=1532586430; _Jo0OQK=578A5ADAC307BEDFA63FF68200394DA90DECD4E03E9F31135822CDBA3DBC524284C87DCDDC8964231AE28224BD99B4C0DD19266E8CB23C67ADFD9B17DC381B85967FFBBE0C390CBD8D402631C467319B15B02631C467319B15B869297F6895F5D91GJ1Z1QQ==; PHPSESSID=giucbr65edajrq13m9ndf6qvvg; domain=hz'
        }
        res = requests.get(url=url, headers=headers).content;
        total_json = json.loads(res);
        content = total_json.get('data');
        if (content != None):
            resJson = content.get("res");
            if (resJson != None):
                records = [];
                houses = resJson.get("houses");
                list=houses.get("list");
                if(list!=None):
                    for record in list:
                        id = record.get("id");
                        price = record.get("price");
                        area = record.get("buildarea");
                        name = record.get("community_name");
                        communityId = record.get("community_id");
                        recordDict = {};
                        recordDict["id"] = id;
                        recordDict["price"] = price;
                        recordDict["name"] = name;
                        recordDict["communityId"] = communityId;
                        recordDict["community_name"] = name;
                        recordDict["area"] = area;
                        recordDict["fromWeb"] = fromWeb;
                        recordDict["cityCode"]=cityCode;
                        records.append(recordDict);
                    addOneHouseIntoDB(records);
    except Exception as e:
        print(e);

def addOneHouseIntoDB(records):
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    for houseVo in records:
        print(houseVo)
        hosuse_id = houseVo["id"];
        area = houseVo["area"];
        communityId = houseVo["communityId"];
        community_name = houseVo["community_name"];
        price = houseVo["price"];
        fromWeb=houseVo["fromWeb"];
        cityCode = houseVo["cityCode"];
        currentDate = time.strftime("%Y-%m-%d", time.localtime());
        sql="INSERT INTO web_house (house_id, house_area, community_id, community_name, house_price, frow_web, create_time,city_code) VALUES ('"+str(hosuse_id)+"', '"+str(area)+"', '"+str(communityId)+"', '"+community_name+"', '"+str(price)+"', '"+fromWeb+"', '"+currentDate+"','"+cityCode+"');"
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