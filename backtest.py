# -*- coding: utf-8 -*-
"""
Created on Fri Jul 28 10:59:30 2023

@author: YW
"""
from .utils.selector import Selector
import pandas as pd
from abc import ABC,abstractclassmethod
from matplotlib import pyplot as plt

class Recorder:
    def __init__(self,ts_code,share,direction,indate,inprice,outdate,outprice):
        self.ts_code = ts_code
        self.share = share
        assert direction in ('in','out'),"direction must be 'in' or 'out'"
        self.direction = direction
        if self.direction == 'in':
            self.indate = indate
            self.inprice = inprice
        else:
            self.outdate = outdate
            self.outprice = outprice

class Position:

    def __init__(self,ts_code,share):
        #init的过程就是原来持仓没这个票，持仓append这个票
        self.ts_code = ts_code
        self.share = share
        self.divshare = 0
        self.divcash = 0
        self.allshare = self.share + self.divshare
    
    def updateprice(self,price):
        self.lastprice = price
    
    def div_exdate(self,stk_div,cash_div):
        self.divshare = self.share * stk_div
        self.allshare = self.share + self.divshare
        self.divcash = self.share * cash_div
        
    def div_listdate(self):
        self.share += self.divshare
        self.divshare = 0
        self.allshare = self.share + self.divshare
        
    def div_paydate(self):
        divcash = self.divcash
        self.divcash = 0
        return divcash

            
class Account:
    
    def __init__(self,startcash,tax,fee):
        self.cash = startcash
        self.portfolio = {} #{'ts_code':Position,}
        self.tax = tax
        self.fee = fee
        self.portfoliovalue = 0
        self.netvalue = self.cash + self.portfoliovalue

    
    #盘中
    def order_percent(self,ts_code,ratio):
        if ts_code not in self.today_pool.index:
            return
        avgprice = self.today_pool.loc[ts_code].amount/self.today_pool.loc[ts_code].vol*10
        targetshare = self.netvalue * ratio / avgprice
        if ts_code in self.portfolio:
            diffshare = targetshare - self.portfolio[ts_code].allshare
            if diffshare > 0.0 and self.today_pool.loc[ts_code].up_limit_allday==0:
                buyshare = diffshare
                buycost = buyshare * avgprice * (1+self.fee)
                if self.cash>=buycost:
                    self.portfolio[ts_code].share += buyshare
                    self.portfolio[ts_code].allshare += buyshare
                    self.cash -= buycost
                else:
                    buyshare = self.cash/avgprice/(1+self.fee)
                    self.portfolio[ts_code].share += buyshare
                    self.portfolio[ts_code].allshare += buyshare
                    self.cash = 0.0
                    
            elif diffshare <0.0 and self.today_pool.loc[ts_code].down_limit_allday==0:
                sellshare = -diffshare
                if sellshare > self.portfolio[ts_code].share:
                    #只能卖可售股分大小,还有没到手的红股，不删这个position
                    sellshare = self.portfolio[ts_code].share
                    sellrevenue = self.portfolio[ts_code].share * avgprice
                    self.portfolio[ts_code].share -= sellshare
                    self.portfolio[ts_code].allshare -= sellshare
                    self.cash += sellrevenue*(1-self.tax-self.fee)
                else:
                
                    sellrevenue = sellshare *avgprice
                    self.cash += sellrevenue*(1-self.tax-self.fee)
                    self.portfolio[ts_code].share -= sellshare
                    self.portfolio[ts_code].allshare -= sellshare

        else:
            if self.today_pool.loc[ts_code].up_limit_allday==0:
                buyshare= targetshare
                buycost = buyshare * avgprice * (1+self.fee)
                if self.cash>=buycost:
                    self.portfolio[ts_code] = Position(ts_code,buyshare)
                    self.cash -= buycost
                else:
                    buyshare = self.cash/avgprice/(1+self.fee)
                    self.portfolio[ts_code] = Position(ts_code,buyshare)
                    self.cash = 0.0
     
                    
    def order_money(self,ts_code,money):
        if ts_code not in self.today_pool.index:
            return
        avgprice = self.today_pool.loc[ts_code].amount/self.today_pool.loc[ts_code].vol*10
        if ts_code in self.portfolio:
            if money>0.0 and self.today_pool.loc[ts_code].up_limit_allday==0:
                buyshare = money/avgprice
                buycost = money * (1+self.fee)
                if self.cash>= buycost:
                    self.portfolio[ts_code].share += buyshare
                    self.portfolio[ts_code].allshare += buyshare
                    self.cash -= buycost
                else:
                    buyshare = self.cash/avgprice/(1+self.fee)
                    self.portfolio[ts_code].share += buyshare
                    self.portfolio[ts_code].allshare += buyshare
                    self.cash = 0.0
            elif money<0.0 and self.today_pool.loc[ts_code].down_limit_allday==0:
                sellshare = -money/avgprice
                sellrevenue = -money
                if sellshare > self.portfolio[ts_code].share:
                    sellshare = self.portfolio[ts_code].share
                    sellrevenue = self.portfolio[ts_code].share * avgprice
                    self.portfolio[ts_code].share -= sellshare
                    self.portfolio[ts_code].allshare -= sellshare
                    self.cash += sellrevenue*(1-self.tax-self.fee)
                else:
                    self.cash += sellrevenue*(1-self.tax-self.fee)
                    self.portfolio[ts_code].share -= sellshare
                    self.portfolio[ts_code].allshare -= sellshare
        else:
            if self.today_pool.loc[ts_code].up_limit_allday==0:
                buyshare = money/avgprice
                buycost = money * (1+self.fee)
                if self.cash>=buycost:
                    self.portfolio[ts_code] = Position(ts_code,buyshare)
                    self.cash -= buycost
                else:
                    buyshare = self.cash.avgprice/(1+self.fee)
                    self.portfolio[ts_code].share += buyshare
                    self.portfolio[ts_code].allshare += buyshare
                    self.cash = 0.0
                
                
                
                    
                    
                    
    #盘后
    def updateprice(self):
        for ts_code in self.portfolio:
            if ts_code in self.today_pool.index:
                self.portfolio[ts_code].updateprice(self.today_pool.loc[ts_code].close)
        self.portfoliovalue = sum([position.share*position.lastprice for ts_code,position in self.portfolio.items()])
        self.netvalue = self.cash + self.portfoliovalue
        
        portfolio_list = list(self.portfolio.keys())
        for ts_code in portfolio_list:
            if self.portfolio[ts_code].allshare == 0 and self.portfolio[ts_code].divcash==0:
                #如果能卖光就删除这个position
                self.portfolio.pop(ts_code)
        
    #盘前
    def nextday(self,nextday,today_pool,exdate_df,div_listdate_df,pay_date_df):
        self.date = nextday
        self.today_pool = today_pool
        self.exdate_df = exdate_df
        self.div_listdate_df = div_listdate_df
        self.pay_date_df = pay_date_df

        
        
    def dividend(self):
        portfolio_list = list(self.portfolio.keys())
        if portfolio_list:
            if len(self.exdate_df)>=0:
                exdate_df = pd.merge(left=self.exdate_df,right=pd.DataFrame({'ts_code':portfolio_list}),
                                        on='ts_code',how='inner')
                for index,row in exdate_df.iterrows():
                    self.portfolio[row.ts_code].div_exdate(row.stk_div,row.cash_div)
            
            if len(self.div_listdate_df)>=0:
                div_listdate_df = pd.merge(left=self.div_listdate_df,right=pd.DataFrame({'ts_code':portfolio_list}),
                                        on='ts_code',how='inner')
                for index,row in div_listdate_df.iterrows():
                    self.portfolio[row.ts_code].div_listdate()
                    
            if len(self.pay_date_df)>=0:
                pay_date_df = pd.merge(left=self.pay_date_df,right=pd.DataFrame({'ts_code':portfolio_list}),
                                        on='ts_code',how='inner')
                for index,row in pay_date_df.iterrows():
                    self.cash += self.portfolio[row.ts_code].div_paydate()
        else:
            pass
                


        
class Context(ABC):

    def __init__(self,tax=1/1000,fee=2.5/10000):
        self.selector = Selector()
        self.tax = tax
        self.fee = fee
        self.trade_cal = self.selector.trade_cal(start_date=20000000, 
                                                 end_date=30000000)
        self.netvaluerecorder = {}
    
    @abstractclassmethod
    def initialize(self):
        pass
    
    @abstractclassmethod   
    def preparedata(self):
        pass

    @abstractclassmethod   
    def beforeopen(self):
        '''
        此为已经变成除权价格后的盘前
        '''
        pass

    @abstractclassmethod   
    def handlebar(self):
        pass 
    
    @abstractclassmethod   
    def afterclose(self):
        '''
        此为按收盘价结算后的持仓统计
        '''
        pass 
    
            
    def backtest(self,startdate,enddate,startcash):

        trade_cal = self.trade_cal[self.trade_cal['is_open']==1]
        trade_cal = trade_cal.query(f'{startdate}<=cal_date<={enddate}')
        self.tradedate_list = trade_cal.cal_date.to_list()
        self.account = Account(startcash,self.tax,self.fee)
        self.initialize()
        self.preparedata()
        
        for self.date in self.tradedate_list:
            print(self.date)
            self.today_daily = self.selector.daily(date_list=[self.date])
            self.today_stk_limit = self.selector.stk_limit(date_list=[self.date])
            self.today_pool = pd.merge(left=self.today_daily,
                                       right=self.today_stk_limit,
                                       how='left',on='ts_code')
            self.today_pool.set_index('ts_code',inplace=True)
            self.exdate_df = self.selector.dividend(ex_date=self.date)
            self.div_listdate_df = self.selector.dividend(div_listdate=self.date)
            self.pay_date_df = self.selector.dividend(pay_date=self.date)
            

            
            self.account.nextday(self.date,self.today_pool,
                                 self.exdate_df,self.div_listdate_df,self.pay_date_df)
            self.account.dividend()
            self.handlebar()
            self.account.updateprice()
            self.afterclose()
            
            self.netvaluerecorder[self.date] = self.account.netvalue
        self.selector.close()
        
    def draw(self,path=None):
        figure = plt.figure(figsize=(10,10))
        axes1 = plt.subplot(1,1,1)
        axes1.plot(datelist:=[str(int(date)) for date,nevtalue in self.netvaluerecorder.items()],
                    netvaluelist:=[nevtalue for date,nevtalue in self.netvaluerecorder.items()],label='netvalue')
        axes1.set_xticklabels(datelist,rotation=45,size=5)
        axes1.legend(loc=2,prop = {'size':5})
        plt.title('Backtest')
        if not path:
            path = './Backtest.png'
        plt.savefig(path,dpi=300)
        return datelist,netvaluelist
    
        
        
    
            
    
    
    
            