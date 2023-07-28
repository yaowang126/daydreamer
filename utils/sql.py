# -*- coding: utf-8 -*-
"""
Created on Sat Jun 10 15:15:04 2023

@author: YW
"""
import pymysql
import pandas as pd
from pymysql.constants import CLIENT
import os
import json

class SQL():
    
    def __init__(self,host,port,user,password,database):
        self.connect = pymysql.connect(host=host,port=port,user=user,
                                       password=password,database=database,
                                       client_flag=CLIENT.MULTI_STATEMENTS)
         
    def select(self,query):
        
        def myqsqlcate_transfer(col,des):
            '''
            From pymysql.constants.FIELD_TYPE we know that:
            DECIMAL = 0
            TINY = 1
            SHORT = 2
            LONG = 3
            FLOAT = 4
            DOUBLE = 5
            NULL = 6
            TIMESTAMP = 7
            LONGLONG = 8
            INT24 = 9
            DATE = 10
            TIME = 11
            DATETIME = 12
            YEAR = 13
            NEWDATE = 14
            VARCHAR = 15
            BIT = 16
            JSON = 245
            NEWDECIMAL = 246
            ENUM = 247
            SET = 248
            TINY_BLOB = 249
            MEDIUM_BLOB = 250
            LONG_BLOB = 251
            BLOB = 252
            VAR_STRING = 253
            STRING = 254
            GEOMETRY = 255
            
            CHAR = TINY
            INTERVAL = ENUM
            '''
            if des[1] in (0,246):
                return col.astype(float)
            return col
        
        cursor = self.connect.cursor() 
        cursor.execute(query)
        res = cursor.fetchall()
        col = [item[0] for item in cursor.description]
        df = pd.DataFrame(res, columns=col)       
        cursor.close()
        
        for i,col in enumerate(df.columns):
            df[col] = myqsqlcate_transfer(df[col],cursor.description[i])
        
        return df
    
    
    def execute(self,query):
        cursor = self.connect.cursor()
        cursor.execute(query)
        self.connect.commit()
        cursor.close()
            
    def close(self):
        self.connect.close()
        

if __name__ == '__main__':
    with open(f'{os.path.dirname(__file__)}/dbcfg.conf', 'r', encoding='utf-8') as f:
                dbcfg = json.load(f)
    tushare_cfg = dbcfg['tushare']
    sql = SQL(tushare_cfg['DBaddr'],int(tushare_cfg['DBport']),tushare_cfg['DBusername'],
                   tushare_cfg['DBpw'],tushare_cfg['DBname'])
    df = sql.select('select * from daily_2010 limit 10;')