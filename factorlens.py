# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 09:29:42 2023

@author: YW
"""
import pandas as pd
import numpy as np
from .utils.selector import Selector
import matplotlib
from matplotlib import pyplot as plt
matplotlib.rcParams['font.sans-serif'] = ['SimHei']
from collections.abc import Iterable
from functools import partial
import datetime
import warnings
warnings.filterwarnings('ignore')#warning from draw() 懒得管了

class Factorlens:
    
    def __init__(self,factor_name,factor_df,stock_pool=None,newlist_delay=180,
                 ignore_level ='*ST',last_date=None,continuousrotation=None):
        for col in ('ts_code','factor_date','factor'):
            assert col in factor_df.columns,f'missing column {col} in factor_df'
        selector = Selector()
        if isinstance(stock_pool,Iterable):
            self.stock_pool = stock_pool
        else:
            self.stock_pool = factor_df['ts_code'].unique().tolist()
        self.stock_basic = selector.stock_basic(stock_pool=self.stock_pool)
        
        self.stock_delist = self.stock_basic[self.stock_basic['list_status'] =='D'][['ts_code','delist_date']]
        factor_df = factor_df.reset_index(drop=True)
        factor_df = factor_df.sort_values(by='factor_date')
        factor_df = factor_df.reset_index(drop=True)
        factor_df = factor_df.sort_values(by='factor_date')
        self.factor_name = factor_name
        #build for rt_df calculation
        self.factor_date_list = factor_df.factor_date.unique().tolist()
        self.trade_cal = selector.trade_cal(start_date=0, 
                                                 end_date=30000000).iloc[1:-2]
        self.trade_cal[['pretrade_date','nexttrade_date']] = self.trade_cal[['pretrade_date','nexttrade_date']].astype(int)
        #此部分为用所有因子日的下一个交易日算出所有调仓日

        trade_date_df = pd.merge(left=pd.DataFrame({'factor_date':self.factor_date_list}),
                                 right=self.trade_cal,left_on='factor_date',
                                 right_on='cal_date',how='left')
        self.date_list = trade_date_df.nexttrade_date.unique().tolist()
        #此为加上新股可被交易的日期
        if newlist_delay:
            def cal_newlist_startdate(newlist_date,newlist_delay):
                newlist_datetime = datetime.datetime.strptime(str(newlist_date), '%Y%m%d')
                newlist_startdate = newlist_datetime + datetime.timedelta(days=newlist_delay)
                newlist_startdate = int(newlist_startdate.strftime('%Y%m%d'))
                return newlist_startdate
            
            self.stock_basic['newlist_startdate'] = self.stock_basic['list_date'].map(lambda x:cal_newlist_startdate(x,newlist_delay))
            self.stock_basic['newlist_startdate'] = self.stock_basic['newlist_startdate'].astype(int)
            self.stock_list = self.stock_basic[['ts_code','newlist_startdate']] 
        else:
            self.stock_list = pd.DataFrame()
            
        #此为定义ST筛选函数
        assert ignore_level in ('ST','*ST','退'),'invalid ignore_level'
        def ingore_filter(ignore_level,stock_name):
            if ignore_level == 'ST':
                if 'ST' in stock_name or '退' in stock_name:
                    return False
                else:
                    return True
            elif ignore_level == '*ST':
                if '*ST' in stock_name or '退' in stock_name:
                    return False
                else:
                    return True
            elif ignore_level == '退':
                if  '退' in stock_name:
                    return False
                else:
                    return True
                
        self.ignore_filter = partial(ingore_filter,ignore_level)
        self.namechange =  selector.namechange(stock_pool=self.stock_pool)
        #把名字结束日空值填成今天
        self.namechange = pd.merge(left=self.namechange,right=self.stock_basic[['ts_code','list_date']],
                                   on='ts_code',how='left')
        self.namechange = self.namechange.query('start_date>=list_date')
        self.namechange['end_date'] = self.namechange['end_date'].fillna(datetime.datetime.now().strftime('%Y%m%d'))
        self.namechange['end_date'] = self.namechange['end_date'].astype(int)
        #此部分为自动加上最后一个调仓日
        if not last_date:
            
            trade_cal_open = self.trade_cal[self.trade_cal['is_open']==1].reset_index()
            index_1 = trade_cal_open[trade_cal_open['nexttrade_date']==self.date_list[-2]].index
            index_0 = trade_cal_open[trade_cal_open['nexttrade_date']==self.date_list[-1]].index
            last_date = trade_cal_open.loc[2*index_0-index_1,'nexttrade_date'].iloc[0]
            self.date_list.append(last_date)
        
        #此为给factor_df添加一列调仓日
        self.factor_df = pd.merge(left=factor_df,right=trade_date_df.loc[:,['factor_date','nexttrade_date']],
                         on='factor_date',how='left')
            
        self.factor_df.set_index('nexttrade_date',inplace=True)
        
        #记录如果憋手里的话憋了哪些票/持仓占比/憋在了第几层
        self.continuousrotation = continuousrotation
        if self.continuousrotation:
            self.passivehold = [pd.DataFrame(columns=['ts_code','buy_price','adj_factor','layer','ratio'])\
                                   for i in range(self.continuousrotation)]
        else:
            self.passivehold = pd.DataFrame(columns=['ts_code','buy_price','adj_factor','layer','ratio'])
        
        #metrics and layer return record
        self.metrics_df = pd.DataFrame(columns=['time','trade_date','ic','rankic'])
        self.layerrt_df = pd.DataFrame(columns=['time','trade_date','layer','rt','nv'])
        selector.close()     
    
    
    def _cal_layer(self,trade_date,cal_layer_func,layer_num,trade_method,rotation_point=None):
        buy_df = self.daily_df.loc[trade_date,['ts_code','close','low','vol','amount']]
        buy_adj_df = self.adj_factor_df.loc[trade_date,['ts_code','adj_factor']]
        buy_df = pd.merge(left=buy_df,right=buy_adj_df,on='ts_code',how='inner')
        
        
        factor_df = self.factor_df.loc[trade_date,['ts_code','factor']]
        buy_df = pd.merge(left=buy_df,right=factor_df,on='ts_code',how='left')
        
        #此为踢出上市未满newlist_delay时间的
        if len(self.stock_list)>0:
            listtradable_df = self.stock_list[self.stock_list['newlist_startdate']<trade_date]
            buy_df = pd.merge(left=buy_df,right=listtradable_df,on='ts_code',how='left')
            buy_df = buy_df[pd.notnull(buy_df['newlist_startdate'])]
        
        #此为踢出ST等ignore_level的
        
        namechange_df = self.namechange.query(f'{trade_date}>=start_date & {trade_date}<=end_date')[['ts_code','stock_name']]
        buy_df = pd.merge(left=buy_df,right=namechange_df,on='ts_code',how='left')
        buy_df = buy_df[buy_df['stock_name'].map(self.ignore_filter)]
        #先分层再踢出涨停不能买入，因为是前天晚上做策略,第二天收盘才知道涨停没涨停，涨停是超前信息
        if cal_layer_func:#不是lambda x的函数，是直接传入一个seriresz，再自己返回一个series，操作空间大
            layer_series = cal_layer_func(buy_df['factor'])
            if len(layer_series) == len(buy_df):
                buy_df['layer'] = cal_layer_func(buy_df['factor'])
                buy_df['layer'] = buy_df['layer'].fillna('null')
                buy_df['layer'] = buy_df['layer'].astype(str)
            else:
                raise Exception('length of user defined layer series does not match length of factor dataframe')
        else:
            buy_df['layer'] = pd.qcut(buy_df['factor'].rank(method='first'),q=layer_num,labels=False)#[ts_code,close,adj_factor,factor,layer]
            buy_df['layer'] = buy_df['layer'].fillna('null')

        
        #加涨停，这里用收盘价是否涨停，因为收盘调仓
        up_limit_df = self.stk_limit_df.loc[trade_date,['ts_code','up_limit']]  
        buy_df = pd.merge(left=buy_df,right=up_limit_df,on='ts_code',how='left')
        if trade_method == 'weighted_mean':
            buy_df = buy_df[buy_df.apply(lambda x:x.low!=x.up_limit,axis=1)]
            buy_df['buy_price'] = buy_df.apply(lambda x:x.amount/x.vol*10,axis=1)
        elif trade_method == 'open_close':
            buy_df = buy_df[buy_df['close']!=buy_df['up_limit']]#踢出收盘价=涨停价
            buy_df['buy_price'] = buy_df['close']
        #以下为加入憋手里的部分，并且为每个票附上持仓比例(憋手里的可能和这期新来的平均分的数值不一样)
        if self.continuousrotation:
            buy_df = pd.concat([buy_df,self.passivehold[rotation_point]],join='outer',ignore_index=True)
        else:
            buy_df = pd.concat([buy_df,self.passivehold],join='outer',ignore_index=True)
        def cal_ratio(df):
            ratio_sum = 1 - df['ratio'].sum()
            null_cnt = pd.isnull(df['ratio']).sum()
            df['ratio'] = df['ratio'].map(lambda x: ratio_sum/null_cnt if pd.isnull(x) else x)
            return df       
        buy_df = buy_df.groupby(by='layer').apply(cal_ratio).reset_index(drop=True)
        

        self.buy_df = buy_df
        return buy_df
    
    def _cal_rt_passivehold(self,trade_date,trade_date_next,trade_method,rotation_point=None):
        sell_df = self.daily_df.loc[trade_date_next,['ts_code','open','high','vol','amount']]
        sell_adj_df = self.adj_factor_df.loc[trade_date_next,['ts_code','adj_factor']]
        sell_df = pd.merge(left=sell_df,right=sell_adj_df,on='ts_code',how='inner')
        #加跌停,下一期的收盘价=跌停价的不能在sell_df里
        down_limit_df = self.stk_limit_df.loc[trade_date_next,['ts_code','down_limit']]  
        sell_df = pd.merge(left=sell_df,right=down_limit_df,on='ts_code',how='left')
        if trade_method == 'weighted_mean':
            sell_df = sell_df[sell_df.apply(lambda x:x.high!=x.down_limit,axis=1)]
            sell_df['sell_price'] = sell_df.apply(lambda x:x.amount/x.vol*10,axis=1)
        elif trade_method == 'open_close':
            sell_df = sell_df[sell_df['open']!=sell_df['down_limit']]#踢出收盘价=跌停价
            sell_df['sell_price'] = sell_df['open']
        self.sell_df = sell_df

        cal_df = pd.merge(left=self.buy_df,right=self.sell_df,on='ts_code',how='left',suffixes=('','_sell'))
        
        if self.continuousrotation:
            self.passivehold[rotation_point] = cal_df[pd.isnull(cal_df['sell_price'])][['ts_code','buy_price','adj_factor','layer','ratio']]
        else:
            self.passivehold = cal_df[pd.isnull(cal_df['sell_price'])][['ts_code','buy_price','adj_factor','layer','ratio']]
            
        cal_df['sell_price_adj'] = cal_df.apply(lambda x: x['sell_price']*x['adj_factor_sell']/x['adj_factor']\
                                                if pd.notnull(x['sell_price']) else x['buy_price'], axis=1)
        #加退市,退市的close_sell_adj=0 
        cal_df = pd.merge(left = cal_df,
                          right = self.stock_delist.query(f'{trade_date}<=delist_date<={trade_date_next}')[['ts_code','delist_date']],
                          how='left',on='ts_code')
        cal_df['sell_price_adj'] = cal_df.apply(lambda x:x.sell_price_adj if pd.isnull(x.delist_date) else 0.0,axis=1)
        # if len(cal_df[cal_df['sell_price_adj']==0]) > 0:
        #     print('delist-----------------',cal_df)    
        cal_df['nv'] = cal_df['sell_price_adj']/cal_df['buy_price']
        cal_df['rt'] = cal_df['nv'] - 1
        self.cal_df = cal_df
        return cal_df


    @staticmethod
    def _cal_ic(cal_df):
        cal_df = cal_df[pd.notnull(cal_df['factor'])]#np.nan算corr是略过还是当0？
        ic = cal_df['rt'].corr(cal_df['factor'],method="pearson")
        rankic = cal_df['rt'].corr(cal_df['factor'],method="spearman")
        return ic,rankic
    
    @staticmethod
    def _cal_layerrt(cal_df,keep_null):
        
        if not keep_null:
            cal_df = cal_df[cal_df['layer'] != 'null']
            
        cal_df['rt_ratio'] = cal_df['rt'] * cal_df['ratio']
        cal_df['nv_ratio'] = cal_df['nv'] * cal_df['ratio']  
        layer_rt = cal_df.groupby(by='layer').agg({'rt_ratio':'sum','nv_ratio':'sum'}).reset_index()
        return layer_rt.rename(columns={'rt_ratio':'rt','nv_ratio':'nv'})

    
    def backtest(self,method='buyonlysellable',trade_method ='weighted_mean',
                 cal_layer_func=None,layer_num=10,keep_null=False,step_size=12):    
        assert method in ('buyonlysellable','holdinlayer'),'invalid method' #加上憋手里的回测方式
        assert trade_method in ('weighted_mean','open_close'),'invalid method' #加上憋手里的回测方式
        selector = Selector()
        if self.continuousrotation == None:
            for i in range(0,len(self.date_list)-1,step_size):
                #以下为mysql to memory读取流
                date_list = self.date_list[i:i+step_size+1]
                self.daily_df = selector.daily(date_list = date_list,stock_pool=self.stock_pool)
                self.adj_factor_df = selector.adj_factor(date_list = date_list,stock_pool=self.stock_pool)
                self.stk_limit_df = selector.stk_limit(date_list = date_list,stock_pool=self.stock_pool)
                self.daily_df.set_index('trade_date',inplace=True)
                self.adj_factor_df.set_index('trade_date',inplace=True)
                self.stk_limit_df.set_index('trade_date',inplace=True)
                #以下为每个调仓日分层并结算下一期收益and计算憋手里等等琐事
                for i in range(len(date_list)-1):             
                    trade_date = date_list[i]
                    trade_date_next = date_list[i+1]
                    if method == 'buyonlysellable':
                        ...
                    
                    elif method == 'holdinlayer':
                        
                        self._cal_layer(trade_date,cal_layer_func,layer_num,trade_method)
                        cal_df = self._cal_rt_passivehold(trade_date,trade_date_next,trade_method)
                        ic,rankic = self._cal_ic(cal_df)
                        layerrt = self._cal_layerrt(cal_df,keep_null)
                        layerrt['trade_date'] = trade_date_next
                        self.metrics_df = pd.concat([self.metrics_df,pd.DataFrame({'trade_date':[trade_date_next],
                                                   'ic':[ic],'rankic':[rankic]})],ignore_index=True)
    
                        self.layerrt_df = pd.concat([self.layerrt_df,layerrt],ignore_index=True)
        else:
            date_list_len = int(len(self.date_list)/self.continuousrotation)*self.continuousrotation
            date_list_int = self.date_list[:date_list_len]
            for query_date_point in range(0,len(date_list_int)-self.continuousrotation,step_size*self.continuousrotation):
                
                date_list = date_list_int[query_date_point:query_date_point+(step_size+1)*self.continuousrotation]
                #以下为mysql to memory读取流
                self.daily_df = selector.daily(start_date=date_list[0],end_date=date_list[-1],stock_pool=self.stock_pool)
                self.adj_factor_df = selector.adj_factor(start_date=date_list[0],end_date=date_list[-1],stock_pool=self.stock_pool)
                self.stk_limit_df = selector.stk_limit(start_date=date_list[0],end_date=date_list[-1],stock_pool=self.stock_pool)
                self.daily_df.set_index('trade_date',inplace=True)
                self.adj_factor_df.set_index('trade_date',inplace=True)
                self.stk_limit_df.set_index('trade_date',inplace=True)
                #以下为每个调仓日分层并结算下一期收益and计算憋手里等等琐事
                for rotation_point in range(self.continuousrotation):
                    date_list_rotation = [date for i,date in enumerate(date_list) if i%self.continuousrotation == rotation_point]
                    for adjust_point in range(len(date_list_rotation)-1):   
                        trade_date = date_list_rotation[adjust_point]
                        trade_date_next = date_list_rotation[adjust_point+1]
                        if method == 'buyonlysellable':
                            ...
                        
                        elif method == 'holdinlayer':
                            
                            self._cal_layer(trade_date,cal_layer_func,layer_num,trade_method,rotation_point)
                            cal_df = self._cal_rt_passivehold(trade_date,trade_date_next,trade_method,rotation_point)
                            ic,rankic = self._cal_ic(cal_df)
                            layerrt = self._cal_layerrt(cal_df,keep_null)
                            layerrt['trade_date'] = trade_date_next
                            layerrt['time'] = query_date_point+adjust_point
                            self.metrics_df = pd.concat([self.metrics_df,pd.DataFrame({'time':[query_date_point+adjust_point],
                                                       'trade_date':[trade_date_next],'ic':[ic],'rankic':[rankic]})],ignore_index=True)
        
                            self.layerrt_df = pd.concat([self.layerrt_df,layerrt],ignore_index=True)

        def cal_cumnv(df):
            df = df.sort_values(by='trade_date')
            df['cumnv'] = df['nv'].cumprod()
            return df
        
        if self.continuousrotation:
            self.layerrt_df_draw = self.layerrt_df.groupby(by=['time','layer'])\
                .agg({'nv':'mean','trade_date':'min'}).reset_index()
        else:

            self.layerrt_df_draw = self.layerrt_df
        
        trade_date_unique = pd.DataFrame({'trade_date':self.layerrt_df_draw.trade_date.unique()})
        trade_date_unique['Cartesian'] = 1
        layer_unique = pd.DataFrame({'layer':self.layerrt_df_draw.layer.unique()})
        layer_unique['Cartesian'] = 1
        full_points_draw = pd.merge(left=trade_date_unique,right=layer_unique,on='Cartesian',how='left')
        self.layerrt_df_draw = pd.merge(left=full_points_draw,right=self.layerrt_df_draw,
                                       on=['trade_date','layer'],how='left')
        self.layerrt_df_draw['nv'] = self.layerrt_df_draw['nv'].fillna(1.0)

        self.layerrt_df_draw = self.layerrt_df_draw.groupby(by='layer').apply(cal_cumnv).reset_index(drop=True)

        
        selector.close()
        return self.metrics_df,self.layerrt_df
        
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
        
        for group_num in self.layerrt_df_draw.layer.unique():
            axes3.plot(self.layerrt_df_draw.trade_date.unique().astype(int).astype(str),
                        self.layerrt_df_draw[self.layerrt_df_draw['layer']==group_num]['cumnv'],label=f'group_{group_num}')
        axes3.set_xticklabels(self.layerrt_df_draw.trade_date.unique().astype(int).astype(str),rotation=45,size=5)
        axes3.legend(loc=2,prop = {'size':5})
        
        plt.title(f'Factor:{self.factor_name}')
        if not path:
            path = f'./{self.factor_name}.png'
        plt.savefig(path,dpi=300)
        return self.metrics_df,self.layerrt_df_draw