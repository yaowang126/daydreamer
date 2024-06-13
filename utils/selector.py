# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 15:04:30 2023

@author: YW
"""
import datetime
from .sql import SQL
import pandas as pd
from functools import partial
from collections.abc import Iterable
from collections import defaultdict
import json
import os

class Selector:
    '''
    #用法:
    selector = Selector() #注册连接实例
    stock_pool = ['000001.SZ','000002.SZ'] #或者任何iterable的类型
    
    #行情数据daily/dailybasic/adj_factor/moneyflow四张表用法如下,前2个参数起止日期必须传入
    df0 = selector.daily(start_date = 20101201,end_date = 20111210,stock_pool)
    #或者传入日期列表
    df01 = selector.daily(datelist = [20101201,20101211,20101221],stock_pool)
    #财务数据fina_indicator表
    #第1个第2个参数为报告期起点和终点,如20220330，第3个第4个参数为真实公布日期的起止，
    #前2对至少要传入一对,第5个为stock_pool,第6个为字段名列表
    df1 = selector.fina_indicator(end_date_start=20100330,end_date_end=20111231,stock_pool=stock_pool,field=['roe'])
    df2 = selector.trade_cal(start_date=20100330, end_date=20111231)
    #最后记得把连接实例关了！！！
    selector.close()
    '''
    def __init__(self,sql_config=None):
        if not sql_config:
            with open(f'{os.path.dirname(__file__)}/dbcfg.conf', 'r', encoding='utf-8') as f:
                dbcfg = json.load(f)
                tushare_cfg = dbcfg['tushare']
                tushare_warmin_cfg = dbcfg['tushare_warmin']
            self.sql = SQL(tushare_cfg['DBaddr'],int(tushare_cfg['DBport']),tushare_cfg['DBusername'],
                           tushare_cfg['DBpw'],tushare_cfg['DBname'])
            self.sqlwarmin = SQL(tushare_warmin_cfg['DBaddr'],int(tushare_warmin_cfg['DBport']),tushare_warmin_cfg['DBusername'],
                           tushare_warmin_cfg['DBpw'],tushare_warmin_cfg['DBname'])
        else:
            ...
    
    
    @staticmethod
    def _year_split_continous(start_date,end_date):          
        start_year = int(start_date/10000)
        end_year = int(end_date/10000)
        this_year = datetime.datetime.now().year
        
        param_set = []
        year = start_year
        while year <= min(end_year,this_year):
            param_set.append([year,
                              start_date if year==start_year else int(year*10000+101),
                              end_date if year == end_year else int(year*10000+1231)])
            year += 1 
        return param_set
     
    @staticmethod
    def _year_split_discrete(date_list):
        param_set = defaultdict(list)
        for date in date_list:
            year = int(date/10000)
            param_set[year].append(date)
        return param_set
            
    
    def _querydaily_year_split(self,table_name,start_date=None,end_date=None,date_list=None,stock_pool=None):
        print(start_date,type(start_date),end_date,type(end_date))
        assert isinstance(start_date,int) and isinstance(end_date,int) \
            or isinstance(date_list,Iterable),'起始日期和截止日期必须传入或者转入日期列表'
            
        if isinstance(start_date,int) and isinstance(end_date,int):
            param_set = self._year_split_continous(start_date, end_date)        
            df_cum = pd.DataFrame()
            
            for param in param_set:
                query_select = f'''
                select * from {table_name}_{param[0]} where trade_date between {param[1]} and {param[2]}
                '''
                if isinstance(stock_pool,Iterable):
                    stock_pool_str = "("+ ",".join(["'"+item+"'" for item in stock_pool]) +")"
                    query_select += f''' and ts_code in {stock_pool_str}'''
                query_select += ';'
                df = self.sql.select(query_select)
                df_cum = pd.concat([df_cum,df], ignore_index=True, join='outer')
        
        elif isinstance(date_list,Iterable):

            param_set = self._year_split_discrete(date_list)        
            df_cum = pd.DataFrame()
            
            for year,date_list in param_set.items():
                
                if isinstance(stock_pool,Iterable):
            
                    query_select = f'''
                       select * from {table_name}_{year} where (ts_code,trade_date) in (
                       '''
                    for ts_code in stock_pool:
                        for trade_date in date_list:
                            query_select += f"('{ts_code}',{trade_date}),"
                    query_select = query_select[:-1] + ');'
                else:
                     query_select = f'''
                    select * from {table_name}_{year} where trade_date in ({",".join([str(item) for item in date_list])})
                    '''

                df = self.sql.select(query_select)
                df_cum = pd.concat([df_cum,df], ignore_index=True, join='outer')
                    
                    
        return df_cum.sort_values(by='trade_date').reset_index(drop=True)
    
    def _querydaily(self,table_name,start_date=None,end_date=None,date_list=None,stock_pool=None):
        assert isinstance(start_date,int) and isinstance(end_date,int) \
            or isinstance(date_list,Iterable),'起始日期和截止日期必须传入或者转入日期列表'
        
        if isinstance(start_date,int) and isinstance(end_date,int):     
            query_select = f'''
            select * from {table_name} where trade_date between {start_date} and {end_date}
            '''
            if isinstance(stock_pool,Iterable):
                stock_pool_str = "("+ ",".join(["'"+item+"'" for item in stock_pool]) +")"
                query_select += f''' and ts_code in {stock_pool_str}'''
            query_select += ';'
            df_cum = self.sql.select(query_select)
        
        elif isinstance(date_list,Iterable):
            
            if isinstance(stock_pool,Iterable):
        
                query_select = f'''
                   select * from {table_name} where (ts_code,trade_date) in (
                   '''
                for ts_code in stock_pool:
                    for trade_date in date_list:
                        query_select += f"('{ts_code}',{trade_date}),"
                query_select = query_select[:-1] + ');'
            else:
                 query_select = f'''
                select * from {table_name} where trade_date in ({",".join([str(item) for item in date_list])})
                '''
            df_cum = self.sql.select(query_select)
                    
        return df_cum.sort_values(by='trade_date').reset_index(drop=True)    
            
    def fina_indicator(self,end_date_start=None,end_date_end=None,
                           ann_date_start=None,ann_date_end=None,
                           stock_pool=None,field=None,latest=True):
        assert end_date_start and end_date_end or ann_date_start and ann_date_end,'报告期起止时间和公告日期起止时间至少要传入一对'
        
        if isinstance(field,Iterable):
            query = f'''
            select ts_code,ann_date,end_date,{",".join(field)} from fina_indicator where 1=1'''
        else:
            query = '''
            select * from fina_indicator where 1=1'''
            
        if end_date_start and end_date_end:
            query += f'''
            and end_date between {end_date_start} and {end_date_end}
            '''
        if ann_date_start and ann_date_end:
            query += f'''
            and ann_date between {ann_date_start} and {ann_date_end}
            '''
        if isinstance(stock_pool,Iterable):
            stock_pool_str = "("+ ",".join(["'"+item+"'" for item in stock_pool]) +")"
            query += f'''
            and ts_code in {stock_pool_str}
            '''     
            
        if latest:
            query = f'''
            with tmp as ({query})
            select ts_code,ann_date,end_date,{",".join(field)} from 
            (select *,row_number() over (partition by ts_code,end_date order by ann_date desc) as rn
                   from tmp) as s
            where rn = 1;
            '''          
        df = self.sql.select(query)
        
        return df
    
    def trade_cal(self,start_date,end_date,stock_pool=None):
        query = f'''
        select * from trade_cal where cal_date between {start_date} and {end_date};
        '''
        df = self.sql.select(query)
        return df
    
    def warmin(self,start_date=None,end_date=None,date_list=None):
        assert isinstance(start_date,int) and isinstance(end_date,int) \
    or isinstance(date_list,Iterable),'起始日期和截止日期必须传入或者转入日期列表'
        if isinstance(start_date,int) and isinstance(end_date,int):
            df_tables = self.trade_cal(20000000,30000000)
            df_tables = df_tables[df_tables['is_open']==1]
            date_list = df_tables[(df_tables['cal_date']>=start_date)&(df_tables['cal_date']<=end_date)].cal_date.to_list()
        df_cum = pd.DataFrame()
        
        for tradedate in date_list:
            query_select = f'''
            select * from war_{tradedate}
            '''
            df = self.sqlwarmin.select(query_select)
            df_cum = pd.concat([df_cum,df], ignore_index=True, join='outer')
        return df_cum
    
    def stock_basic(self,stock_pool=None):
        query_select = '''
        select * from stock_basic
        '''
        if stock_pool:
            query_select += 'where ts_code in ('
            for ts_code in stock_pool:
                query_select += f"'{ts_code}',"
            query_select = query_select[:-1] + ');'     
        df = self.sql.select(query_select)
        return df
    
    def namechange(self,stock_pool=None):
        query_select = '''
        select * from namechange
        '''
        if stock_pool:
            query_select += 'where ts_code in ('
            for ts_code in stock_pool:
                query_select += f"'{ts_code}',"
            query_select = query_select[:-1] + ');'     
        df = self.sql.select(query_select)
        return df
    
    def dividend(self,ex_date=None,pay_date=None,div_listdate=None,stock_pool=None):
        checksum = sum([bool(ex_date),bool(pay_date),bool(div_listdate)])
        assert(checksum <=1),'Only one of ex_date,pay_date,div_listdate can be a param'
        if checksum == 1:
            if ex_date:
                colname = 'ex_date'
                colvalue = ex_date
            elif pay_date :
                colname = 'pay_date'
                colvalue = pay_date
            else:
                colname = 'div_listdate'
                colvalue = div_listdate
            if stock_pool:
                query_select = f'''
                select * from dividend where (ts_code,{colname}) in (
                '''
                for ts_code in stock_pool:
                    query_select += f"('{ts_code}',{colvalue}),"
                query_select = query_select[:-1] + ');'
            
            else:
                query_select = f'''
                select * from dividend where {colname} = {colvalue};
                '''
        else:
            if stock_pool:
                query_select = '''
                select * from dividend where ts_code in (
                '''
                for ts_code in stock_pool:
                    query_select +=f"'{ts_code},'"
                query_select = query_select[:-1]+');'
            else:
                query_select = '''select * from dividend;'''
        df = self.sql.select(query_select)
        return df
    

    
    def __getattr__(self,table_name):
        if table_name in ('daily','dailybasic','adj_factor','moneyflow','stk_limit'):
            return partial(self._querydaily_year_split,table_name)
        elif table_name in ('index_daily','index_dailybasic'):
            return partial(self._querydaily,table_name)
        else:
            raise AttributeError(f'Selector has no attribute {table_name}')
        

    def close(self):
        self.sql.close()
        self.sqlwarmin.close()