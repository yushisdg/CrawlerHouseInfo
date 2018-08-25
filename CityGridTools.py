import psycopg2
from CrawlerHouseInfo.jdbcConfig import *



def getCityGridsMinIndexAndMaxIndexByCityCode(cityCode):
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    sql="SELECT min(t.grid_index), max(t.grid_index) from geomtools_grid t where t.code='"+cityCode+"'";
    try:
        cur.execute(sql);
        keyData = cur.fetchall();
        minIndex=keyData[0][0];
        maxIndex=keyData[0][1];
        indexDict={};
        indexDict['minIndex'] = minIndex;
        indexDict['maxIndex'] = maxIndex;
        return indexDict;
    except Exception as e:
        print(e);
    finally:
        cur.close();
        conn.close();

def SelectGridByCityCodeAndIndex(cityCode,gridIndex):
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    sql="SELECT ST_YMin (T.geom) minY, ST_YMax (T.geom) maxY,ST_XMin (T.geom) minX,ST_XMax (T.geom) maxX, T .code code,T .grid_index gridIndex,T .grid_id gridId,st_x (st_centroid(T .geom)) centerX,st_y (st_centroid(T .geom)) centerY FROM geomtools_grid t WHERE code ='"+cityCode+"'"+"AND grid_index = "+str(gridIndex);
    print(sql)
    try:
        cur.execute(sql);
        keyData = cur.fetchall();
        datalength=len(keyData);
        if(datalength>0):
            print(keyData)
            minY = keyData[0][0];
            maxY = keyData[0][1];
            minX = keyData[0][2];
            maxX =keyData[0][3];
            centerX=keyData[0][7];
            centerY = keyData[0][8];
            indexDict = {};
            indexDict['xmin'] = minX;
            indexDict['ymin'] = minY;
            indexDict['xmax'] = maxX;
            indexDict['ymax'] = maxY;
            indexDict['centerX'] = centerX;
            indexDict['centerY'] = centerY;
            cur.close();
            conn.close();
            return indexDict;
        else:
            cur.close();
            conn.close();
            return None;
    except Exception as e:
        print(e);
    finally:
        cur.close();
        conn.close();


def getCityCodeByCityName(cityName):
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    sql = "SELECT * from major_city_code where name='" + cityName + "'";
    try:
        cur.execute(sql);
        keyData = cur.fetchall();
        name = keyData[0][0];
        cityCode = keyData[0][1];
        baiduCode=keyData[0][2];
        gaodeCode=keyData[0][3];
        indexDict = {};
        indexDict['name'] = name;
        indexDict['cityCode'] = cityCode;
        indexDict['baiduCode'] = baiduCode;
        indexDict['gaodeCode'] = gaodeCode;
        return indexDict;
    except Exception as e:
        print(e);
    finally:
        cur.close();
        conn.close();

def getCityEnvelopeByCityName(cityName):
    indexDict=getCityCodeByCityName(cityName);
    cityCode=indexDict["cityCode"][0:4];
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    sql = "SELECT ST_YMin (envelope) minY,ST_YMax(envelope) maxY,ST_XMin (envelope) minX,	ST_XMax (envelope) maxX FROM (SELECT	ST_Envelope (	ST_Collect (	ARRAY (	SELECT	T .geom	FROM	geomtools_grid T WHERE T .code LIKE '"+cityCode+"%'))) envelope) K";
    try:
        cur.execute(sql);
        keyData = cur.fetchall();
        minY = keyData[0][0];
        maxY = keyData[0][1];
        minX = keyData[0][2];
        maxX = keyData[0][3];
        indexDict = {};
        indexDict['minY'] = minY;
        indexDict['maxY'] = maxY;
        indexDict['minX'] = minX;
        indexDict['maxX'] = maxX;
        print(indexDict)
        return indexDict;
    except Exception as e:
        print(e);
    finally:
        cur.close();
        conn.close();
    return;




