# -*- coding: utf-8 -*-
"""
Created on Sat Jun 10 15:15:04 2023

@author: YW
"""
import pymysql
import pandas as pd
from pymysql.constants import CLIENT

class SQL():
    
    def __init__(self,host,port,user,password,database):
        self.connect = pymysql.connect(host=host,port=port,user=user,
                                       password=password,database=database,
                                       client_flag=CLIENT.MULTI_STATEMENTS)
         
    def select(self,query):
        cursor = self.connect.cursor() 
        cursor.execute(query)
        res = cursor.fetchall()
        col = [item[0] for item in cursor.description]
        df = pd.DataFrame(res, columns=col)
        cursor.close()
        return df
    
    def execute(self,query):
        cursor = self.connect.cursor()
        cursor.execute(query)
        self.connect.commit()
        cursor.close()
            
    def close(self):
        self.connect.close()
        