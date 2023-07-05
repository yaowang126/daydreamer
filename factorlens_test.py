# -*- coding: utf-8 -*-
"""
Created on Wed Jun 28 18:16:36 2023

@author: YW
"""

import tushare as ts
from utils.selector import Selector
import pandas as pd
from factorlens_oneread import Factorlens
import time

def get_stock_pool():
    pro = ts.pro_api()
    stock_list_df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    stock_list_df = stock_list_df[stock_list_df['ts_code'].map(lambda x:x[:2] in ('60','00','30'))] #选沪深主板
    stock_pool = stock_list_df['ts_code'].unique()
    return stock_pool.tolist()


def make_factor(stock_pool):
    selector = Selector()
    df = selector.fina_indicator(end_date_start=20100330,end_date_end=20211231,stock_pool=stock_pool,field=['roe'])
    df = df[df['end_date'].map(lambda x:x%10000==1231)]
    df_factor = pd.DataFrame({'ts_code':df.ts_code,
                              'date':df.end_date.map(lambda x:int(int(x/10000)+1)*10000+430),
                              'factor':df.roe})
    selector.close()
    return df_factor.sort_values(by='date').reset_index(drop=True)


if __name__ == '__main__':
    t1 = time.time()
    factor_df = make_factor(get_stock_pool())
    lens = Factorlens('roe',factor_df,last_date=20230504)
    lens.backtest(method = 'buyonlysellable',layer_num=10,keep_null=True,
                  in_memory=True,step_size=12)
    data=lens.draw()
    t2 = time.time()
    print(t2-t1)
