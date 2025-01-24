# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 09:29:42 2023

@author: YW
"""
import pandas as pd
import numpy as np
from .utils.selector import Selector
from .fundevaluation import cal_max_percent_drawdown,cal_annrt,cal_sharp
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
        factor_df = factor_df.sort_values(by='factor_date')
        factor_df = factor_df.reset_index(drop=True)
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
        self.namechangesee0 = self.namechange
        self.namechangesee3 = self.namechange.query('start_date<list_date')
        # self.namechange = self.namechange.query('start_date>=list_date')
        self.namechange['end_date'] = self.namechange['end_date'].fillna(datetime.datetime.now().strftime('%Y%m%d'))
        self.namechange['end_date'] = self.namechange['end_date'].astype(int)
        #此部分为自动加上最后一个调仓日
        if not last_date:
            
            trade_cal_open = self.trade_cal[self.trade_cal['is_open']==1].reset_index()
            index_1 = trade_cal_open[trade_cal_open['nexttrade_date']==self.date_list[-2]].index
            index_0 = trade_cal_open[trade_cal_open['nexttrade_date']==self.date_list[-1]].index
            last_date = trade_cal_open.loc[2*index_0-index_1,'nexttrade_date'].iloc[0]
            self.date_list.append(int(last_date))
        
        #此为给factor_df添加一列调仓日
        self.factor_df = pd.merge(left=factor_df,right=trade_date_df.loc[:,['factor_date','nexttrade_date']],
                         on='factor_date',how='left')
            
        self.factor_df.set_index('nexttrade_date',inplace=True)
        
        #记录如果憋手里的话憋了哪些票/持仓占比/憋在了第几层
        self.continuousrotation = continuousrotation
        if self.continuousrotation:
            self.passivehold = [pd.DataFrame(columns=['ts_code','buy_price','adj_factor',
                                                      'layer','ratio'])\
                                   for i in range(self.continuousrotation)]
        else:
            self.passivehold = pd.DataFrame(columns=['ts_code','buy_price','adj_factor',
                                                     'layer','ratio'])
        if self.continuousrotation:
            self.passivehold_priceinfo = [pd.DataFrame(columns=['ts_code','last_trade_date','layer',
                                                            'last_price','last_adj_factor'])\
                                      for i in range(self.continuousrotation)]
        else:
            self.passivehold = pd.DataFrame(columns=['ts_code','last_trade_date','layer',
                                                     'last_price','last_adj_factor'])
        #metrics and layer return record
        self.metrics_df = pd.DataFrame(columns=['time','trade_date','ic','rankic'])
        self.layerrt_df = pd.DataFrame(columns=['time','trade_date','layer','rt','nv'])
        self.layer_daily_nv_df = pd.DataFrame(columns=['trade_date','layer','nv','rotationpoint'])
        selector.close()     
        '''
        check
        '''
        self.layer_stock_df_cum1 = pd.DataFrame()
        self.layer_stock_df_cum2 = pd.DataFrame()
    
    def _cal_layer_buydf(self,trade_date,cal_layer_func,layer_num,trade_method,rotation_point=None):
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
        self.namechangesee1 = self.namechange
        self.namechangesee2 = namechange_df
        buy_df = pd.merge(left=buy_df,right=namechange_df,on='ts_code',how='left')
        
        buy_dfsee = buy_df[pd.isnull(buy_df['stock_name'])]
        if len(buy_dfsee)>0:
            print(buy_dfsee)
        
        buy_df = buy_df[buy_df['stock_name'].map(self.ignore_filter)]
        #先分层再踢出涨停不能买入，因为是前天晚上做策略,第二天收盘才知道涨停没涨停，涨停是超前信息
        if cal_layer_func:#不是lambda x的函数，是直接传入一个serires，再自己返回一个series，操作空间大
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

        
        #加涨停，两种涨停不能买入判断方法做选择入参
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
    
        cal_df = pd.merge(left=self.buy_df,right=self.sell_df,on='ts_code',
                          how='left',suffixes=('','_sell'))#连接了buy_df和
        
        if self.continuousrotation:
            self.passivehold[rotation_point] = cal_df[pd.isnull(cal_df['sell_price'])][['ts_code',
                                                'buy_price','adj_factor','layer','ratio']]
        else:
            self.passivehold = cal_df[pd.isnull(cal_df['sell_price'])][['ts_code',
                                                'buy_price','adj_factor','layer','ratio']]
        #如果是上期留下了一个票，这期也有这个票(不管两个票在)    
        cal_df['sell_price_adj'] = cal_df.apply(lambda x: x['sell_price']*x['adj_factor_sell']/x['adj_factor']\
                                                if pd.notnull(x['sell_price']) else x['buy_price'], axis=1)
        
        #加退市,退市的close_sell_adj=0 
        delist_df = self.stock_delist.query(f'{trade_date}<=delist_date<={trade_date_next}')[['ts_code','delist_date']]
        cal_df = pd.merge(left = cal_df, right = delist_df, how='left', on='ts_code')
        cal_df['sell_price_adj'] = cal_df.apply(lambda x:x.sell_price_adj if pd.isnull(x.delist_date) else 0.0,axis=1)
        # if len(cal_df[cal_df['sell_price_adj']==0]) > 0:
        #     print('delist-----------------',cal_df)    
        cal_df['nv'] = cal_df['sell_price_adj']/cal_df['buy_price']
        cal_df['rt'] = cal_df['nv'] - 1
        self.cal_df = cal_df
        return cal_df    
    
    @staticmethod
    def _calc_stock_layer_lastprice(stock_df):
        #这个stock_df里可能混了一个ts_code的两个layer
        stock_df = stock_df.dropna()
        if len(stock_df)>0:
            stock_df = stock_df.sort_values(by='trade_date').reset_index(drop=True)
            last_price = stock_df.iloc[-1].price
            last_adj_factor = stock_df.iloc[-1].adj_factor
            last_trade_date = stock_df.iloc[-1].trade_date
            return pd.Series({'last_trade_date':last_trade_date,
                              'last_price':last_price,
                              'last_adj_factor':last_adj_factor})
        else:#注释掉?
            return pd.Series({'last_trade_date':None,
                              'last_price':None,
                              'last_adj_factor':None})
        
    @staticmethod
    def _ffill_nacols(stock_df):
        stock_df = stock_df.sort_values(by='trade_date').reset_index(drop=True)
        stock_df['ratio'] = stock_df['ratio'].ffill()
        stock_df['price'] = stock_df['price'].ffill()
        stock_df['adj_factor'] = stock_df['adj_factor'].ffill()
        return stock_df
    
    @staticmethod
    def _calc_stock_cumnv(stock_df,trade_date,trade_date_next):
        
        stock_df = stock_df.sort_values(by='trade_date').reset_index(drop=True)
        stock_df['price'] = stock_df['price'].ffill()
        stock_df['adj_factor'] = stock_df['adj_factor'].ffill()
        stock_df = stock_df[(stock_df['trade_date']>=trade_date)&(stock_df['trade_date']<=trade_date_next)]
        stock_df['nv'] = stock_df['price'] * stock_df['adj_factor'] \
                        /(stock_df['price'].shift(1)*stock_df['adj_factor'].shift(1))
        stock_df['cumnv'] = stock_df['nv'].cumprod()
        stock_df['cumnv'] = stock_df['cumnv'].fillna(1.0)
        stock_df['ratio_cumnv'] = stock_df['ratio'] * stock_df['cumnv']
        return stock_df
    
    
    def _calc_layer_dailynv(self,layer_stock_df,trade_date,trade_date_next,rotation_point):
        
        layer_stock_df_copy = layer_stock_df
        self.layer_stock_df_cum1 = pd.concat([self.layer_stock_df_cum1,layer_stock_df_copy])
        
        
        
        layer_stock_df = layer_stock_df.groupby(by='ts_code').apply(lambda x:self._calc_stock_cumnv(x,trade_date,trade_date_next))
        
        '''
        check rotation_point没用
        '''
        layer_stock_df_copy = layer_stock_df
        layer_stock_df_copy['rotation_point'] = rotation_point
        self.layer_stock_df_cum2 = pd.concat([self.layer_stock_df_cum2,layer_stock_df_copy])
        '''
        '''
        
        
        layer_df = layer_stock_df.groupby('trade_date').agg({'ratio_cumnv':'sum'}).reset_index()\
            .rename(columns={'ratio_cumnv':'cumnv'})
        layer_df['nv'] = layer_df['cumnv']/layer_df['cumnv'].shift(1)
        layer_df = layer_df.iloc[1:]#第一期的最后一天和第二期的第一天会是同一天，也就是说被重复计算
        #而每一期的第一天cum_nv是1，所以把每一期的第一天删掉
        return layer_df
    
    
    def _calc_layernv_lastprice_daily(self,trade_date,trade_date_next,trade_method,rotation_point=None):
        #按照只能给weighted_mean写,截取长度为21天,Open_close的截取长度为20天
        if trade_method == 'weighted_mean':
            daily_df = self.daily_df.loc[trade_date:trade_date_next,['ts_code','vol','amount']].reset_index()#改为loc20天出来
            daily_adj_df = self.adj_factor_df.loc[trade_date:trade_date_next,['ts_code','adj_factor']].reset_index()
            rt_df = pd.merge(left=daily_df,right=daily_adj_df,on=['ts_code','trade_date'],how='inner')
            trade_date_unique = pd.DataFrame({'trade_date':rt_df.trade_date.unique()})#这里默认至少没有一天所有待测票池票都停牌
            trade_date_unique['Cartesian'] = 1
            
            
            hold_df = self.buy_df[['ts_code','layer','ratio']].groupby(['ts_code','layer'])\
                .agg({'ratio':'sum'}).reset_index()
            stock_layer_unique = hold_df[['ts_code','layer','ratio']].drop_duplicates()
            stock_layer_unique['Cartesian'] = 1
            full_points = pd.merge(left=trade_date_unique,right=stock_layer_unique,on='Cartesian',how='left')
            rt_df = pd.merge(left=full_points,right=rt_df,on=['ts_code','trade_date'],how='left')

            
            rt_df['price'] = rt_df['amount']/rt_df['vol']*10

            if self.continuousrotation:
                
                #用self.passivehold_priceinfo中最的last_price填充进rt_df中来自passive_hold部分的price空的部分
                rt_df = pd.merge(left=rt_df,right=self.passivehold_priceinfo[rotation_point]\
                                 [['ts_code','last_price','last_adj_factor']],
                                 how='left',on='ts_code')
                rt_df['price'] = rt_df.apply(lambda x:x.price if pd.notnull(x.price)\
                                             else x.last_price,axis=1)   
                rt_df['adj_factor'] = rt_df.apply(lambda x:x.adj_factor if pd.notnull(x.adj_factor)\
                                             else x.last_adj_factor,axis=1) 
                rt_df = rt_df.drop(columns=['last_price','last_adj_factor'])    
                stock_price_df = rt_df[['ts_code','trade_date','price','adj_factor']]\
                    .drop_duplicates(subset=['ts_code','trade_date'])
                #更新存储pasivehold_daily为本期Passivehold票的最后一个价格

                #因为rt_df里一个ts_code可能多个Layer都有，制作一个一个ts_code只有一个的df用来计算最后一个有交易的日期的价格
                last_date = stock_price_df.groupby(by='ts_code')\
                    .apply(self._calc_stock_layer_lastprice).reset_index()
                
                
                last_date_passivehold = pd.merge(left=self.passivehold[rotation_point]\
                         .drop_duplicates(subset=['ts_code'])[['ts_code']],
                                                 right=last_date,how='left',on='ts_code')
                self.passivehold_priceinfo[rotation_point] = last_date_passivehold

            
            
            
            
            
            else:
                #用self.passivehold_priceinfo中最的last_price填充进rt_df中来自passive_hold部分的price空的部分
                rt_df = pd.merge(left=rt_df,right=self.passivehold_priceinfo\
                                 [['ts_code','last_price','last_adj_factor']],
                                 how='left',on='ts_code')
                rt_df['price'] = rt_df.apply(lambda x:x.price if pd.notnull(x.price)\
                                             else x.last_price,axis=1)   
                rt_df['adj_factor'] = rt_df.apply(lambda x:x.adj_factor if pd.notnull(x.adj_factor)\
                                             else x.last_adj_factor,axis=1) 
                rt_df = rt_df.drop(columns=['last_price','last_adj_factor'])    
                stock_price_df = rt_df[['ts_code','trade_date','price','adj_factor']]\
                    .drop_duplicates(subset=['ts_code','trade_date'])
                #更新存储pasivehold_daily为本期Passivehold票的最后一个价格

                #因为rt_df里一个ts_code可能多个Layer都有，制作一个一个ts_code只有一个的df用来计算最后一个有交易的日期的价格
                last_date = stock_price_df.groupby(by='ts_code')\
                    .apply(self._calc_stock_layer_lastprice).reset_index()
                
                
                last_date_passivehold = pd.merge(left=self.passivehold\
                         .drop_duplicates(subset=['ts_code'])[['ts_code']],
                                                 right=last_date,how='left',on='ts_code')
                self.passivehold_priceinfo = last_date_passivehold
                  
            
            
            #计算每层每日的收益(nv)
            #没有daily行情的也没Layer/ratio，需要向下填充
            
            
            
            rt_df = rt_df.groupby(by=['ts_code','layer']).apply(self._ffill_nacols).reset_index(drop=True)
            layer_nv_df = rt_df.groupby(by=['layer']).apply(lambda x:\
                self._calc_layer_dailynv(x,trade_date,trade_date_next,rotation_point))\
                .reset_index()[['layer','trade_date','cumnv','nv']]
            
        
        return layer_nv_df
                    
            
        
        

    
    def backtest(self,method='buyonlysellable',trade_method ='weighted_mean',
                 cal_layer_func=None,layer_num=10,keep_null=False,step_size=12):    
        assert method in ('buyonlysellable','holdinlayer'),'invalid method' #加上憋手里的回测方式
        assert trade_method in ('weighted_mean','open_close'),'invalid method' #加上憋手里的回测方式
        selector = Selector()
        self.layer_num = layer_num
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
                        
                        self._cal_layer_buydf(trade_date,cal_layer_func,layer_num,trade_method)
                        cal_df = self._cal_rt_passivehold(trade_date,trade_date_next,trade_method)
#                        #在这累加layer_daily_rt_df
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
                            
                            self._cal_layer_buydf(trade_date,cal_layer_func,layer_num,trade_method,rotation_point)
                            cal_df = self._cal_rt_passivehold(trade_date,trade_date_next,trade_method,rotation_point)
                            #在这累加layer_daily_rt_df
                            layer_daily_nv = self._calc_layernv_lastprice_daily(trade_date,trade_date_next,trade_method,rotation_point) #改成开关模式
                            layer_daily_nv['rotationpoint'] = rotation_point
                            self.layer_daily_nv_df = pd.concat([self.layer_daily_nv_df,
                                                                layer_daily_nv[['trade_date','layer','nv','rotationpoint']]],
                                                               ignore_index=True)
                            ic,rankic = self._cal_ic(cal_df)
                            layerrt = self._cal_layerrt(cal_df,keep_null)
                            layerrt['trade_date'] = trade_date_next
                            layerrt['time'] = query_date_point/self.continuousrotation + adjust_point
                            layerrt['rotationpoint'] = rotation_point
                            self.metrics_df = pd.concat([self.metrics_df,pd.DataFrame({'time':[query_date_point+adjust_point],
                                                       'trade_date':[trade_date_next],'ic':[ic],'rankic':[rankic]})],ignore_index=True)
        
                            self.layerrt_df = pd.concat([self.layerrt_df,layerrt],ignore_index=True)
        
        selector.close()
        return self.layerrt_df
    
    
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
    
    
    
    def stat(self):
        
        def cal_cumnv(df):
            df = df.sort_values(by='trade_date').reset_index(drop=True)
            df['cumnv'] = df['nv'].cumprod()
            return df
        
        
        #填充应该有的所有标准日期与层数
        trade_date_unique = pd.DataFrame({'trade_date':self.layerrt_df.trade_date.unique()})
        trade_date_unique['Cartesian'] = 1
        layer_unique = pd.DataFrame({'layer':self.layerrt_df.layer.unique()})
        layer_unique['Cartesian'] = 1
        full_points = pd.merge(left=trade_date_unique,right=layer_unique,on='Cartesian',how='left')
        self.layerrt_df = pd.merge(left=full_points,right=self.layerrt_df,
                                       on=['trade_date','layer'],how='left')
        self.layerrt_df['nv'] = self.layerrt_df['nv'].fillna(1.0) #如果那层没有数据就填充一个不赔不赚
        
        self.layerrt_df = self.layerrt_df.groupby(by=['rotationpoint','layer']).apply(cal_cumnv).reset_index(drop=True)
        self.layerrt_df_draw= self.layerrt_df.groupby(by=['time','layer']).agg({'trade_date':'min',
                                                                                'cumnv':'mean'}).reset_index()
        
        self.ic_mean = self.metrics_df.ic.mean()
        self.icir = self.metrics_df.ic.mean()/self.metrics_df.ic.std()
        
        self.rankic_mean = self.metrics_df.rankic.mean()
        self.rankicir = self.metrics_df.rankic.mean()/self.metrics_df.rankic.std()
        
        
        self.layer_daily_nv_df = self.layer_daily_nv_df.groupby(by=['rotationpoint','layer'])\
            .apply(cal_cumnv).reset_index(drop=True)
        
        
        #计算每层的日频净值曲线
        for layer in range(self.layer_num):
            daily_nv_layer = self.layer_daily_nv_df[self.layer_daily_nv_df['layer']==layer]
            
            daily_nv_layer = daily_nv_layer.pivot(columns='rotationpoint',index='trade_date',values='cumnv')
            daily_nv_layer = daily_nv_layer.ffill()
            daily_nv_layer = daily_nv_layer.fillna(1.0)
            daily_nv_layer['cumnv_mean'] = daily_nv_layer.mean(axis=1)
            setattr(self,f'daily_nv_layer_{layer}',daily_nv_layer)  
            
        
        
        
        if self.ic_mean > 0:
            layermax = self.layerrt_df.layer.max()
            layermin = self.layerrt_df.layer.min()
        else:
            layermax = self.layerrt_df.layer.min()
            layermin = self.layerrt_df.layer.max()
        
        def calc_longshort_dailyrt(rotation_df):
            positive_layer = rotation_df[rotation_df['layer']==layermax].reset_index(drop=True)
            negative_layer = rotation_df[rotation_df['layer']==layermin].reset_index(drop=True)
            longshort_df = pd.DataFrame({'trade_date':positive_layer.trade_date,
                                         'rt':positive_layer['nv']-negative_layer['nv']})
            longshort_df['nv'] = longshort_df['rt'] + 1
            longshort_df['cumnv'] = longshort_df['nv'].cumprod()
            return longshort_df
            
        
        self.longshort_daily_nv = self.layer_daily_nv_df.groupby(by='rotationpoint')\
            .apply(calc_longshort_dailyrt).reset_index().drop(columns=['level_1'])
        
        #多空日频曲线
        self.longshort_nv_daily_mean = self.longshort_daily_nv.pivot(values='cumnv',
                                        index='trade_date',columns='rotationpoint')
        self.longshort_nv_daily_mean = self.longshort_nv_daily_mean.ffill()
        self.longshort_nv_daily_mean = self.longshort_nv_daily_mean.fillna(1.0)
        self.longshort_nv_daily_mean['cumnv_mean'] = self.longshort_nv_daily_mean.mean(axis=1)
        
        #最大回撤
        def cal_maxdrawdown(df):
            return pd.Series({'maxdrawdown':cal_max_percent_drawdown(df.cumnv)})
        
        maxdrawdown = self.longshort_daily_nv.groupby(by='rotationpoint').apply(cal_maxdrawdown)
        self.maxdrawdown = round(maxdrawdown.maxdrawdown.mean(),4)
                
        
        #月度胜率
        def calc_iswin(rotation_time_df):
            return pd.Series({'iswin':rotation_time_df[rotation_time_df['layer']==layermax].rt.iloc[0] > \
                rotation_time_df[rotation_time_df['layer']==layermin].rt.iloc[0]})
        win_df = self.layerrt_df.groupby(by=['rotationpoint','time']).apply(calc_iswin)
        
        self.winrate = win_df.iswin.sum()/len(win_df)
        
        #多空年化收益
        def calc_annrt(rotation_time_df):
            return pd.Series({'annrt':cal_annrt(rotation_time_df.cumnv.tolist(),rotation_time_df.trade_date.astype(int).tolist())})
        annrt = self.longshort_daily_nv.groupby(by='rotationpoint').apply(calc_annrt)
        self.annrt = annrt.annrt.mean()
        
        def calc_sharp(rotation_time_df):
            return pd.Series({'sharp':cal_sharp(rotation_time_df.cumnv.tolist())})
        sharp = self.longshort_daily_nv.groupby(by='rotationpoint').apply(calc_sharp)
        self.sharp = sharp.sharp.mean()
        
        return {'ic':self.ic_mean,'icir':self.icir,
                'rankic':self.rankic_mean,'rankicir':self.rankicir,
                'winrate':self.winrate,'annrt':self.annrt,
                'sharp':self.sharp,'maxdrawdown':self.maxdrawdown}
    
    def draw(self,path=None):
        # return self.layerrt_df
        figure = plt.figure(figsize=(10,10))
        axes1 = plt.subplot(5,1,1)
        axes2 = plt.subplot(5,1,2)
        axes3 = plt.subplot(5,1,3)
        axes4 = plt.subplot(5,1,4)
        table = plt.subplot(5,1,5)
        

        
        #ic时序图
        x_positions = range(len(self.metrics_df.trade_date))
        selected_labels = self.metrics_df.trade_date.astype(int).astype(str)[::20].tolist()
        axes1.bar(x_positions,self.metrics_df.ic)
        axes1.set_xticks(x_positions[::20], selected_labels, rotation=45,size=5)
        axes1.set_title(f'IC时序图')
        
        
        x_positions = range(len(self.metrics_df.trade_date))
        selected_labels = self.metrics_df.trade_date.astype(int).astype(str)[::20].tolist()
        axes2.bar(x_positions,self.metrics_df.rankic)
        axes2.set_xticks(x_positions[::20], selected_labels,rotation=45,size=5)
        axes2.set_title(f'RANKIC时序图')
        
        figure.subplots_adjust(hspace=0.5)
        
        for layer in range(self.layer_num):
            daily_nv_layer = getattr(self,f'daily_nv_layer_{layer}')
            x_positions = range(len(daily_nv_layer.index)) 
            selected_labels = daily_nv_layer.index.astype(int).astype(str)[::20].tolist()
            axes3.plot(x_positions,daily_nv_layer.cumnv_mean,label=f'group_{layer}')
        axes3.legend(loc=2,prop = {'size':5})
        axes3.set_xticks(x_positions[::20],selected_labels,rotation=45,size=5)
        axes3.set_title(f'分层回测曲线')
        
        #多空收益曲线图
        x_positions = range(len(self.longshort_nv_daily_mean.index))
        selected_labels = self.longshort_nv_daily_mean.index.astype(int).astype(str)[::20].tolist()
        axes4.plot(x_positions,self.longshort_nv_daily_mean.cumnv_mean)
        axes4.set_xticks(x_positions[::20], selected_labels, rotation=45,size=5)
        axes4.set_title(f'多空收益曲线')
        
        #统计表格
        data = {'ic': [round(self.ic_mean,4)], 
                'ir': [round(self.icir,4)],
                'rankic':[round(self.rankic_mean,4)],
                'rankicir':[round(self.rankicir,4)],
                '多空胜率':[round(self.winrate,2)],
                '多空年化收益':[round(self.annrt,2)],
                '多空夏普比率':[round(self.sharp,2)],
                '最大回撤均值':[round(self.maxdrawdown,2)]}
        

        table = plt.table(cellText=np.array(list(data.values())).T, 
                          colLabels=list(data.keys()), loc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 1.5)
        plt.axis('off')  # 关闭坐标轴
        plt.title(f'Factor:{self.factor_name}')
        
        if not path:
            path = f'./{self.factor_name}.png'
        plt.savefig(path,dpi=300)
        
        self.layer_daily_nv_df['trade_date'] = self.layer_daily_nv_df['trade_date'].astype(int)
        return self.metrics_df,self.layerrt_df