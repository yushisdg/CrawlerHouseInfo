import uuid

from CrawlerHouseInfo.community.CrawlerHouseTools import *


def getOneNeedOpeCommunity():
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    sql = "SELECT	wc.id,wc.name,st_astext(geom),wc.from_web,wc.city_code from web_community wc WHERE wc. ID NOT IN (SELECT	community_id	FROM	web_community_operator) LIMIT 1";
    try:
        cur.execute(sql);
        keyData = cur.fetchall();
        if (len(keyData) > 0):
            name=keyData[0][1];
            geomStr=keyData[0][2];
            cityCode=keyData[0][4];
            com_id=keyData[0][0];
            return  keyData;
    except Exception as e:
        print(e);
    finally:
        cur.close();
        conn.close();


def getCommunityByGeom(geomStr,name):
    secodName=name[0:2];
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    sql = "SELECT * from web_community wc where 	ST_Intersects (st_transform (	ST_Buffer (	st_transform(ST_PointFromText ('"+geomStr+"',4326),3857),500),	4326),geom) and name like '%"+secodName+"%'";
    try:
        cur.execute(sql);
        keyData = cur.fetchall();
        if (len(keyData) > 0):
            return  keyData;
    except Exception as e:
        print(e);
    finally:
        cur.close();
        conn.close();


def operatorCommunity():
    needOpeCom=getOneNeedOpeCommunity();
    name = needOpeCom[0][1];
    geomStr = needOpeCom[0][2];
    cityCode = needOpeCom[0][4];
    com_id = needOpeCom[0][0];
    matchComs=getCommunityByGeom(geomStr,name);
    uid=uuid.uuid1();
    print(uid)
    insertIntoInterDB(uid, name, cityCode, geomStr);
    for com in matchComs:
        matchName=com[1];
        matchId=com[0];
        matchFromweb=com[5]
        matchCitycode=com[7];
        if(("期" in name) or ('号' in name)or ('-' in name)):
            if(name==matchName):
                insertIntoRelDB(uid,matchId,matchFromweb);
                inserIntoOptDB(matchId,matchFromweb,matchCitycode);
        else:
            insertIntoRelDB(uid, matchId, matchFromweb);
            inserIntoOptDB(matchId, matchFromweb, matchCitycode);
    return 0;


# def insertDB(uid,matcgId,matchFromweb,matchCitycode):
#     conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
#     cur = conn.cursor();
#
#     return 0

def insertIntoInterDB(uid,name,cityCode,geomStr):
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    sql = "INSERT INTO web_community_intergration (community_id, community_name, city_code, geom) VALUES ('"+str(uid)+"', '"+name+"', '"+cityCode+"', "+"st_geomfromText('"+geomStr+"',4326)"+") ";
    print(sql)
    try:
        cur.execute(sql);
        conn.commit();
        cur.close();
        conn.close();
    except Exception as e:
        print(e);
    finally:
        cur.close();
        conn.close();

def inserIntoOptDB(comId,fromWeb,cityCode):
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    currentDate = time.strftime("%Y-%m-%d", time.localtime());
    sql = "INSERT INTO web_community_operator (community_id, create_time, from_web, city_code) VALUES ('"+comId+"', '"+currentDate+"', '"+fromWeb+"', '"+cityCode+"');";
    print(sql)
    try:
        cur.execute(sql);
        conn.commit();
        cur.close();
        conn.close();
    except Exception as e:
        print(e);
    finally:
        cur.close();
        conn.close();

def insertIntoRelDB(uid,comId,fromWeb):
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    currentDate = time.strftime("%Y-%m-%d", time.localtime());
    sql = "INSERT INTO web_community_rel (com_id, web_id, from_web) VALUES ('"+str(uid)+"', '"+comId+"', '"+fromWeb+"');";
    print(sql)
    try:
        cur.execute(sql);
        conn.commit();
        cur.close();
        conn.close();
    except Exception as e:
        print(e);
    finally:
        cur.close();
        conn.close();





def batchGetBaiduBusLine():
    a=1;
    while a == 1:
        operatorCommunity();
        # time.sleep(2);

batchGetBaiduBusLine();