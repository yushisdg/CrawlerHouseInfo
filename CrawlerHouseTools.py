import psycopg2
from CrawlerHouseInfo.CityGridTools import *
from CrawlerHouseInfo.MapTools import *
from CrawlerHouseInfo.jdbcConfig import *
from CrawlerHouseInfo.Tools.CoodinateCovertor import *
import urllib.request
import requests
import json

def addLianjiaHouseFromWeb(url,fromWeb):
    try:
        res = requests.get(url=url).content;
        total_json = json.loads(res);
        content = total_json.get('data');
        if (content != None):
            list=content.get("list");
            if(list!=None):
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
    except Exception as e:
        print(e);