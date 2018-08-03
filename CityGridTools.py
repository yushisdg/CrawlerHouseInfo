import psycopg2
from CrawlerHouseInfo.jdbcConfig import *



def getCityGridsMinIndexAndMaxIndexByCityCode(cityCode):
    conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port);
    cur = conn.cursor();
    sql="SELECT min(t.grid_index), max(t.grid_index) from geomtools_grid t where t.code='"+cityCode+"'";
    print(sql)
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
            minY = keyData[0][0];
            maxY = keyData[0][1];
            minX = keyData[0][2];
            maxX =keyData[0][3];
            indexDict = {};
            indexDict['xmin'] = minX;
            indexDict['ymin'] = minY;
            indexDict['xmax'] = maxX;
            indexDict['ymax'] = maxY;
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



