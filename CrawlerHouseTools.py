import psycopg2
from CrawlerHouseInfo.CityGridTools import *
from CrawlerHouseInfo.jdbcConfig import *
import requests
import json
import time
from bs4 import  BeautifulSoup

def addLianjiaHouseFromWeb(url,fromWeb):
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

def addFangTianXiaHouseFromWeb(url,fromWeb):
    try:
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
              print(area)

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
        currentDate = time.strftime("%Y-%m-%d", time.localtime());
        sql="INSERT INTO web_house (house_id, house_area, community_id, community_name, house_price, frow_web, create_time) VALUES ('"+hosuse_id+"', '"+str(area)+"', '"+str(communityId)+"', '"+community_name+"', '"+str(price)+"', '"+fromWeb+"', '"+currentDate+"');"
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