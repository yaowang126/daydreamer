# -*- coding: utf-8 -*-
"""
Created on Tue Jul  4 14:03:28 2023

@author: YW
"""

import pandas as pd
import datetime
import socket
from .utils.selector import Selector


r = socket.gethostbyname(socket.gethostname())


def get_sampletable(*tablenames,before_days):
    tablename_list = tablenames
    selector = Selector()
    #before_days 不超 240
    start_date = 20140505
    end_date = 20171225
    
    trade_cal= selector.trade_cal(20100101, 30000000)
    trade_cal = trade_cal[trade_cal['is_open']==1]
    trade_cal['lag'] = trade_cal['cal_date'].shift(before_days)
    start_date_before = int(trade_cal[trade_cal['cal_date']==start_date].iloc[0].lag)

    selector = Selector()
    stock_pool = ['000519.SZ','600685.SH','000547.SZ','000561.SZ','600879.SH',
                  '600893.SH','600760.SH','600764.SH','600765.SH','600072.SH',
                  '000738.SZ','000768.SZ','000733.SZ','600118.SH','600150.SH',
                  '600316.SH','600038.SH','600399.SH','600372.SH','600391.SH',
                  '600456.SH','600416.SH','600562.SH','600435.SH','600184.SH',
                  '600967.SH','002013.SZ','600482.SH','002025.SZ','002049.SZ',
                  '002149.SZ','002151.SZ','002179.SZ','002190.SZ','601989.SH',
                  '300034.SZ','002338.SZ','300045.SZ','002414.SZ','300114.SZ',
                  '002465.SZ','300395.SZ','603678.SH','300447.SZ','000687.SZ',
                  '600862.SH','300474.SZ','300527.SZ','002829.SZ','300581.SZ',
                  '300593.SZ','300629.sz','300696.SZ','300699.SZ','300123.SZ',
                  '300711.sz','300719.SZ','300722.SZ','300726.SZ','002625.SZ',
                  '002389.SZ','603712.SH','601606.SH','002933.SZ','002935.SZ',
                  '300629.SZ','300762.SZ','603267.SH','300777.SZ','300775.SZ',
                  '601698.SH','002519.SZ','300810.SZ','002977.SZ','002985.SZ',
                  '300855.SZ','300875.SZ','300900.SZ','300922.SZ','300711.SZ']
    
    sampletable_list=[]
    for tablename in tablename_list:
        sampletable_list.append(getattr(selector,tablename)(start_date=start_date_before, 
                                    end_date=end_date,stock_pool=stock_pool))
    
    return sampletable_list[0] if len(sampletable_list) == 1 else sampletable_list


class Factorbuilder:
    
    def __init__(self,*tablenames,start_date,end_date,before_days,period,stock_pool=None):
        self.start_date = start_date
        self.end_date = end_date
        self.tablename_list = tablenames
        self.before_days = before_days
        self.period = period
        self.stock_pool = stock_pool
        self.stock_num = len(stock_pool) if stock_pool else 4500
        self.factor_df = pd.DataFrame(columns=['ts_code','factor_date','factor'])
        selector = Selector()
        trade_cal = selector.trade_cal(20100101, 30000000)
        trade_cal = trade_cal[trade_cal['is_open']==1]
        trade_cal['cal_or_nexttrade_date_lag'] = trade_cal['cal_or_nexttrade_date'].shift(self.before_days)
        trade_cal = trade_cal.dropna()
        trade_cal['cal_or_nexttrade_date_lag'] = trade_cal['cal_or_nexttrade_date_lag'].astype(int)
        trade_cal = pd.merge(left=selector.trade_cal(20100101, 30000000),
                             right=trade_cal.loc[:,['nexttrade_date','cal_or_nexttrade_date_lag']],
                             on = 'nexttrade_date',how='left')
        self.trade_cal = trade_cal
        factor_date = selector.trade_cal(start_date, end_date)
        factor_date = factor_date[factor_date['is_open']==1]
        factor_date = factor_date.reset_index(drop=True)
        factor_date = factor_date[factor_date.index%period==0]
        factor_date = factor_date.loc[:,['cal_date']]
        factor_date = factor_date.rename(columns={'cal_date':'factor_date'})
        self.factor_date = factor_date.loc[:,['factor_date']]
        
        selector.close()
    
    
    def _year_split_continous(self):       #step=多少？   
        start_year = int(self.start_date/10000)
        end_year = int(self.end_date/10000)
        this_year = datetime.datetime.now().year
        
        param_set = []
        year = start_year
        start_date_first = int(self.trade_cal[self.trade_cal['cal_date']==self.start_date].iloc[0].cal_or_nexttrade_date_lag)
        while year <= min(end_year,this_year):
            start_date_other = int(self.trade_cal[self.trade_cal['cal_date']==int(year*10000+101)].iloc[0].cal_or_nexttrade_date_lag)
            param_set.append([self.start_date if year == start_year else int(year*10000+101),
                              start_date_first if year == start_year else start_date_other,
                              self.end_date if year == end_year else int(year*10000+1231)])
            year += 1
        return param_set
    
    
    def factor_build(self,user_func,code_filter=['beijiaosuo'],in_memory=True):
        selector = Selector()
        if in_memory:
            for param in self._year_split_continous():
                table_list=[]
                for tablename in self.tablename_list:
                    table = getattr(selector,tablename)(start_date=param[1], 
                                            end_date=param[2],stock_pool=self.stock_pool)
                    if 'kechuangban' in code_filter:
                        table = table[table['ts_code'].map(lambda x:x[:2]!='68')]
                    if 'beijiaosuo' in code_filter:   
                        table = table[table['ts_code'].map(lambda x:x[-2:]!='BJ')]
                    table_list.append(table)
                factor_df = user_func(*table_list)
                factor_df = pd.merge(left = self.factor_date,right=factor_df,on=['factor_date'],how='inner')
                factor_df = factor_df[factor_df['factor_date']>=param[0]]
                self.factor_df = pd.concat([self.factor_df,factor_df.loc[:,['ts_code','factor_date','factor']]],ignore_index=True)
                #如果返回的不是Df,或者三列不全，就报错
            return self.factor_df
        else:
            ...
            #还没写，可能放到路径下的文件里
        selector.close()
        
        
if __name__ == '__main__':
    
    def my_func(daily_df):
    #17天涨跌幅，11天调仓，两个质数   
        def cal_ret17(df):
            df = df.reset_index(drop=True)
            df['pct_res'] = 1 + df['pct_chg']/100
            df['cum'] = df['pct_res'].cumprod()
            df['cum_lag20'] = df['cum'].shift(17)
            
            df['factor'] = df['cum']/df['cum_lag20'] -1
            df = df.dropna()
            return df
        
        factor_df = daily_df.groupby(by='ts_code').apply(cal_ret17)
        factor_df = factor_df.reset_index(drop=True) 
        factor_df.rename(columns={'trade_date':'factor_date'})
        return factor_df

    factorbuilder = Factorbuilder(start_date=20110501,end_date=20221231,tablename_list=['daily'],
                                  before_days=17,period=11)
    factor_df = factorbuilder.factor_build(my_func)

    print(factor_df.memory_usage(index=True).sum())
        
        
    