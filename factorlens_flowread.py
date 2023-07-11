# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 09:29:42 2023

@author: YW
"""
import pandas as pd
from .utils.selector import Selector
import matplotlib
from matplotlib import pyplot as plt
matplotlib.rcParams['font.sans-serif'] = ['SimHei']
from collections import Iterable
import datetime
import warnings
warnings.filterwarnings('ignore')#warning from draw() 懒得管了

class Factorlens:
    
    def __init__(self,factor_name,factor_df,stock_pool=None,last_date=None):
        for col in ('ts_code','factor_date','factor'):
            assert col in factor_df.columns,f'missing column {col} in factor_df'
        selector = Selector()
        factor_df = factor_df.reset_index(drop=True)
        self.factor_name = factor_name
        #build for rt_df calculation
        self.factor_date_list = factor_df.factor_date.unique().tolist()
        self.trade_cal = selector.trade_cal(start_date=20000000, 
                                                 end_date=30000000) 
        if isinstance(stock_pool,Iterable):
            self.stock_pool = stock_pool
        else:
            self.stock_pool = factor_df['ts_code'].unique().tolist()

        trade_date_df = pd.merge(left=pd.DataFrame({'factor_date':self.factor_date_list}),
                                 right=self.trade_cal,left_on='factor_date',
                                 right_on='cal_date',how='left')
        self.date_list = trade_date_df.nexttrade_date.unique().tolist()
        
        if not last_date:
            index_1 = self.trade_cal[self.trade_cal['nexttrade_date']==self.date_list[-2]].index
            index_0 = self.trade_cal[self.trade_cal['nexttrade_date']==self.date_list[-1]].index
            last_date = self.trade_cal.loc[2*index_0-index_1,'nexttrade_date'].iloc[0]
            self.date_list.append(last_date)
            
        # self.daily_df = self.selector.daily(date_list = self.date_list,stock_pool=self.stock_pool)
        # self.adj_factor_df = self.selector.adj_factor(date_list = self.date_list,stock_pool=self.stock_pool)
        # 延迟到backtest循环里再读表
        self.factor_df = pd.merge(left=factor_df,right=trade_date_df.loc[:,['factor_date','nexttrade_date']],
                         on='factor_date',how='left')
            
        # self.daily_df.set_index('trade_date',inplace=True)
        # self.adj_factor_df.set_index('trade_date',inplace=True)
        # 延迟到backtest循环里再读表
        self.factor_df.set_index('nexttrade_date',inplace=True)
        
        
        #metrics and layer return record
        self.metrics_df = pd.DataFrame(columns=['trade_date','ic','rankic'])
        self.layerrt_df = pd.DataFrame(columns=['trade_date','layer','rt','nv'])
        selector.close()

            
    def _cal_rt_buyonlysellable(self,trade_date,trade_date_next):
        
        buy_df = pd.merge(left = self.daily_df.loc[trade_date,['ts_code','close']],
                          right = self.daily_df.loc[trade_date_next,'ts_code'],
                          on = 'ts_code',how = 'inner')
        adj_df = pd.merge(left=self.adj_factor_df.loc[trade_date,:],
                          right = buy_df.loc[:,'ts_code'],
                          on='ts_code',how='inner')
        adj_df = pd.merge(left=adj_df,
                          right=self.adj_factor_df.loc[trade_date_next,['ts_code','adj_factor']],
                          on='ts_code',
                          how='inner',suffixes=('_0','_1'))
        
        sell_df = pd.merge(left=self.daily_df.loc[trade_date_next,['ts_code','close']],
                           right = adj_df,
                           on='ts_code',how='inner')
        
        sell_df['adj_close'] = sell_df['close']*sell_df['adj_factor_1']/sell_df['adj_factor_0']
        
        rt_df = pd.merge(left=buy_df,
                         right=sell_df,
                         on='ts_code',how='inner',suffixes=('_0','_1'))
        rt_df['nv'] = rt_df['adj_close']/rt_df['close_0']
        rt_df['rt'] = rt_df['nv'] - 1
        
        rt_df = pd.merge(left = rt_df,right = self.factor_df.loc[trade_date,['ts_code','factor']],
                         on = 'ts_code', how='left')
        
        rt_df['factor'] = rt_df['factor'].astype(float)
        rt_df['rt'] = rt_df['rt'].astype(float)
        rt_df['nv'] = rt_df['nv'].astype(float)
        
        return rt_df

    @staticmethod
    def _cal_ic(rt_df):
        rt_df = rt_df[pd.notnull(rt_df['factor'])]
        ic = rt_df['rt'].corr(rt_df['factor'],method="pearson")
        rankic = rt_df['rt'].corr(rt_df['factor'],method="spearman")
        return ic,rankic
    
    @staticmethod
    def _cal_layerrt(rt_df,layer_num,keep_null,cal_layer_func):
        if cal_layer_func:#不是lambda x的函数，是直接传入一个seriresz，再自己返回一个series，操作空间大
            layer_series = cal_layer_func(rt_df['factor'])
            if len(layer_series) == len(rt_df):
                rt_df['layer'] = cal_layer_func(rt_df['factor'])
            else:
                raise Exception('length of user defined layer series does not match length of factor dataframe')
        else:
            rt_df['layer'] = pd.qcut(rt_df['factor'],q=layer_num,labels=False)
        if keep_null:
            rt_df['layer'] = rt_df['layer'].fillna(-1)
        else:
            rt_df = rt_df[pd.notnull(rt_df['factor'])]
        layer_rt = rt_df.groupby(by='layer').agg({'rt':'mean','nv':'mean'}).reset_index()
        return layer_rt
    
    def backtest(self,method='buyonlysellable',layer_num=10,keep_null=True,cal_layer_func=None,in_memory=True,step_size=12):
        assert method in ('buyonlysellable','bieshouli'),'invalid method' #加上憋手里的回测方式
        selector = Selector()
        if in_memory:
            for i in range(0,len(self.date_list),step_size):
                date_list = self.date_list[i:i+step_size]
                self.daily_df = selector.daily(date_list = date_list,stock_pool=self.stock_pool)
                self.adj_factor_df = selector.adj_factor(date_list = date_list,stock_pool=self.stock_pool)
                self.daily_df.set_index('trade_date',inplace=True)
                self.adj_factor_df.set_index('trade_date',inplace=True)
                if method == 'buyonlysellable':
                    for i in range(len(date_list)-1):             
                        trade_date = date_list[i]
                        trade_date_next = date_list[i+1]
                        rt_df = self._cal_rt_buyonlysellable(trade_date,trade_date_next)
                        ic,rankic = self._cal_ic(rt_df)
                        layerrt = self._cal_layerrt(rt_df,layer_num,keep_null,cal_layer_func)
                        layerrt['trade_date'] = trade_date_next
                        self.metrics_df = self.metrics_df.append({'trade_date':trade_date_next,'ic':ic,'rankic':rankic},ignore_index=True)
                        self.layerrt_df = self.layerrt_df.append(layerrt,ignore_index=True)
        else:
            ...#本地 or 数据库临时存储?
        
        selector.close()
        
        def cal_cumnv(df):
            df = df.sort_values(by='trade_date')
            df['cumnv'] = df['nv'].cumprod()
            return df
        self.layerrt_df = self.layerrt_df.groupby(by='layer').apply(cal_cumnv)
        
    def draw(self,path=None):
        # return self.layerrt_df
        figure = plt.figure(figsize=(10,10))
        axes1 = plt.subplot(3,1,1)
        axes2 = plt.subplot(3,1,2)
        axes3 = plt.subplot(3,1,3)
        axes1.bar(self.metrics_df.trade_date.astype(int).astype(str),self.metrics_df.ic)
        axes1.set_xticklabels(self.metrics_df.trade_date.astype(int).astype(str),rotation=45,size=5)
        ic_mean = round(self.metrics_df.ic.mean(),4)
        ir = round(self.metrics_df.ic.mean()/self.metrics_df.ic.std(),4)
        axes1.set_title(f'ic={ic_mean},ir={ir}')
        axes2.bar(self.metrics_df.trade_date.astype(int).astype(str),self.metrics_df.rankic)
        axes2.set_xticklabels(self.metrics_df.trade_date.astype(int).astype(str),rotation=45,size=5)
        axes2.set_title(f'rankic={round(self.metrics_df.rankic.mean(),4)}')
        
        figure.subplots_adjust(hspace=0.5)
        
        for group_num in self.layerrt_df.layer.unique():
            axes3.plot(self.layerrt_df.trade_date.unique().astype(int).astype(str),
                       self.layerrt_df[self.layerrt_df['layer']==group_num]['cumnv'],label=f'group_{group_num}')
        axes3.set_xticklabels(self.layerrt_df.trade_date.unique().astype(int).astype(str),rotation=45,size=5)
        axes3.legend(loc=2,prop = {'size':5})
        
        plt.title(f'Factor:{self.factor_name}')
        if not path:
            path = f'./{self.factor_name}.png'
        plt.savefig(path,dpi=300)
        plt.show()
        return self.metrics_df,self.layerrt_df