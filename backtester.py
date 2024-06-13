# -*- coding: utf-8 -*-
"""
Created on Fri Jul 28 10:59:30 2023

@author: YW
"""
from .utils.selector import Selector
from .utils.sql import SQL
from .fundevaluation import cal_annrt,cal_ann_excessrt,\
    cal_max_percent_drawdown,cal_sharp,cal_sortino
import pandas as pd
import numpy as np
import datetime
from abc import ABC,abstractclassmethod
from matplotlib import pyplot as plt

plt.rcParams["font.sans-serif"]=["SimHei"]
plt.rcParams["axes.unicode_minus"]=False


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

    def __init__(self,ts_code,share,avgprice):
        #init的过程就是原来持仓没这个票，持仓append这个票
        self._ts_code = ts_code
        self._share = share
        self._divshare = 0
        self._divcash = 0
        self._allshare = self._share + self._divshare
        self._lastprice = avgprice
        self._netvalue = self._allshare * self._lastprice + self._divcash
        
    def updateprice(self,price):
        self._lastprice = price
        self._netvalue = self._allshare * self._lastprice + self._divcash
        
    def div_exdate(self,stk_div,cash_div):
        self._divshare = self._share * stk_div
        self._allshare = self._share + self.divshare
        self._divcash = self._share * cash_div
        
    def div_listdate(self):
        self._share += self._divshare
        self._divshare = 0
        
    def div_paydate(self):
        divcash = self._divcash
        self._divcash = 0
        return divcash
    
    @property
    def share(self):
        return self._share
    
    @property
    def allshare(self):
        return self._allshare
    
    @property 
    def divshare(self):
        return self._divshare
    
    @property
    def divcash(self):
        return self._divcash
    
    @property
    def lastprice(self):
        return self._lastprice 
    
    @property
    def netvalue(self):
        return self._netvalue
       
    @share.setter
    def share(self,new_value):
        diff = new_value - self._share
        self._share += diff
        self._allshare += diff
    
    def __str__(self):
        description = f'''
ts_code:{self._ts_code}|share:{self._share}
divshare:{self._divshare}|divcash:{self._divcash}
allshare:{self._allshare}|lastprice:{self._lastprice}
netvalue:{self._netvalue}'''
        return description
    
    def __repr__(self):
        description = f'''
ts_code:{self._ts_code}|share:{self._share}
divshare:{self._divshare}|divcash:{self._divcash}
allshare:{self._allshare}|lastprice:{self._lastprice}
netvalue:{self._netvalue}
'''
        return description

           
class Account:
    
    def __init__(self,startcash,tax,fee):
        self.cash = startcash
        self.portfolio = {} #{'ts_code':Position,}
        self.tax = tax
        self.fee = fee
        self.portfoliovalue = 0
        self.netvalue = self.cash + self.portfoliovalue

    
    #----------------------------------------------------------------------盘中
    def order_percent(self,ts_code,ratio):
        if ts_code not in self.today_pool.index:
            return
        avgprice = self.today_pool.loc[ts_code].amount/self.today_pool.loc[ts_code].vol*10
        targetshare = self.netvalue * ratio / avgprice
        if ts_code in self.portfolio:
            diffshare = targetshare - self.portfolio[ts_code].allshare
            if diffshare > 0.0 and self.today_pool.loc[ts_code].low!=self.today_pool.loc[ts_code].up_limit:
                buyshare = diffshare
                buycost = buyshare * avgprice * (1+self.fee)
                if self.cash>=buycost:
                    self.portfolio[ts_code].share += buyshare
                    self.cash -= buycost
                else:
                    buyshare = self.cash/avgprice/(1+self.fee)
                    self.portfolio[ts_code].share += buyshare
                    self.cash = 0.0
                    
            elif diffshare <0.0 and self.today_pool.loc[ts_code].high!=self.today_pool.loc[ts_code].down_limit:
                sellshare = -diffshare
                if sellshare > self.portfolio[ts_code].share:
                    #只能卖可售股分大小,还有没到手的红股，不删这个position
                    sellshare = self.portfolio[ts_code].share
                    sellrevenue = self.portfolio[ts_code].share * avgprice
                    self.portfolio[ts_code].share -= sellshare
                    self.cash += sellrevenue*(1-self.tax-self.fee)
                else:
                
                    sellrevenue = sellshare *avgprice
                    self.cash += sellrevenue*(1-self.tax-self.fee)
                    self.portfolio[ts_code].share -= sellshare

        else:
            if self.today_pool.loc[ts_code].low!= self.today_pool.loc[ts_code].up_limit:
                buyshare= targetshare
                buycost = buyshare * avgprice * (1+self.fee)
                if self.cash>=buycost:
                    self.portfolio[ts_code] = Position(ts_code,buyshare,avgprice)
                    self.cash -= buycost
                else:
                    buyshare = self.cash/avgprice/(1+self.fee)
                    self.portfolio[ts_code] = Position(ts_code,buyshare,avgprice)
                    self.cash = 0.0
     
                    
    def order_money(self,ts_code,money):
        if ts_code not in self.today_pool.index:
            return
        avgprice = self.today_pool.loc[ts_code].amount/self.today_pool.loc[ts_code].vol*10
        if ts_code in self.portfolio:
            if money>0.0 and self.today_pool.loc[ts_code].low!= self.today_pool.loc[ts_code].up_limit:
                buyshare = money/avgprice
                buycost = money * (1+self.fee)
                if self.cash>= buycost:
                    self.portfolio[ts_code].share += buyshare
                    self.cash -= buycost
                else:
                    buyshare = self.cash/avgprice/(1+self.fee)
                    self.portfolio[ts_code].share += buyshare
                    self.cash = 0.0
            elif money<0.0 and self.today_pool.loc[ts_code].high!=self.today_pool.loc[ts_code].down_limit:
                sellshare = -money/avgprice
                sellrevenue = -money
                if sellshare > self.portfolio[ts_code].share:
                    sellshare = self.portfolio[ts_code].share
                    sellrevenue = self.portfolio[ts_code].share * avgprice
                    self.portfolio[ts_code].share -= sellshare
                    self.cash += sellrevenue*(1-self.tax-self.fee)
                else:
                    self.cash += sellrevenue*(1-self.tax-self.fee)
                    self.portfolio[ts_code].share -= sellshare
        else:
            if self.today_pool.loc[ts_code].low!= self.today_pool.loc[ts_code].up_limit:
                buyshare = money/avgprice
                buycost = money * (1+self.fee)
                if self.cash>=buycost:
                    self.portfolio[ts_code] = Position(ts_code,buyshare,avgprice)
                    self.cash -= buycost
                else:
                    buyshare = self.cash.avgprice/(1+self.fee)
                    self.portfolio[ts_code].share += buyshare
                    self.cash = 0.0
                
                    
    #----------------------------------------------------------------------盘后
    def updateprice(self):
        for ts_code in self.portfolio:
            if ts_code in self.today_pool.index:
                self.portfolio[ts_code].updateprice(self.today_pool.loc[ts_code].close)
        self.portfoliovalue = sum([position.netvalue for ts_code,position in self.portfolio.items()])
        self.netvalue = self.cash + self.portfoliovalue
        
        portfolio_list = list(self.portfolio.keys())
        for ts_code in portfolio_list:
            if self.portfolio[ts_code].allshare == 0 and self.portfolio[ts_code].divcash==0:
                #如果能卖光就删除这个position
                self.portfolio.pop(ts_code)
        
    #----------------------------------------------------------------------盘前
    def nextday(self,nextday,today_pool,today_exdate_df,today_div_listdate_df,
                today_pay_date_df,today_delist):
        self.date = nextday
        self.today_pool = today_pool
        self.today_exdate_df = today_exdate_df
        self.today_div_listdate_df = today_div_listdate_df
        self.today_pay_date_df = today_pay_date_df
        self.today_delist = today_delist

    
    def delist(self):
        portfolio_list = list(self.portfolio.keys())
        if portfolio_list:
            if len(self.today_delist)>0:
                for ts_code in portfolio_list:
                    if ts_code in self.today_delist.ts_code:
                        self.netvalue -= self.portfolio[ts_code].netvalue
                        self.portfolio.pop(ts_code)

        
    def dividend(self):
        portfolio_list = list(self.portfolio.keys())
        if portfolio_list:
            if len(self.today_exdate_df)>=0:
                exdate_df = pd.merge(left=self.today_exdate_df,right=pd.DataFrame({'ts_code':portfolio_list}),
                                        on='ts_code',how='inner')
                for index,row in exdate_df.iterrows():
                    self.portfolio[row.ts_code].div_exdate(row.stk_div,row.cash_div)

            
            if len(self.today_div_listdate_df)>=0:
                div_listdate_df = pd.merge(left=self.today_div_listdate_df,right=pd.DataFrame({'ts_code':portfolio_list}),
                                        on='ts_code',how='inner')
                for index,row in div_listdate_df.iterrows():
                    self.portfolio[row.ts_code].div_listdate()
                    
            if len(self.today_pay_date_df)>=0:
                pay_date_df = pd.merge(left=self.today_pay_date_df,right=pd.DataFrame({'ts_code':portfolio_list}),
                                        on='ts_code',how='inner')
                for index,row in pay_date_df.iterrows():
                    self.cash += self.portfolio[row.ts_code].div_paydate()
        else:
            pass
                


        
class Context(ABC):

    def __init__(self,tax=1/1000,fee=2.5/10000,stock_pool=None):
        self.selector = Selector()
        self.tax = tax
        self.fee = fee
        self.stock_pool = stock_pool
        self.trade_cal = self.selector.trade_cal(start_date=20000000, 
                                                 end_date=30000000)
        self.stock_basic = self.selector.stock_basic()
        self.namechange =  self.selector.namechange()
        self.namechange = pd.merge(left=self.namechange,right=self.stock_basic[['ts_code','list_date']],
                                   on='ts_code',how='left')
        self.namechange = self.namechange.query('start_date>=list_date')
        self.namechange['end_date'] = self.namechange['end_date'].fillna(datetime.datetime.now().strftime('%Y%m%d'))
        self.namechange['end_date'] = self.namechange['end_date'].astype(int)
        
        self.dividend = self.selector.dividend()
        
        self.netvaluerecorder = {}
        self.selector.close()
    
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
        self.selector = Selector()
        self.startdate = startdate
        self.enddate = enddate
        self.startcash = startcash
        trade_cal = self.trade_cal[self.trade_cal['is_open']==1]
        trade_cal = trade_cal.query(f'{startdate}<=cal_date<={enddate}')
        self.tradedate_list = trade_cal.cal_date.to_list()
        self.account = Account(self.startcash,self.tax,self.fee)
        self.initialize()
        self.preparedata()
        
        lastdate=00000000
        for self.date in self.tradedate_list:
            if int(self.date/10000)!=int(lastdate/10000): 
                self.daily = self.selector.daily(start_date=int(self.date/10000)*10000+101, 
                              end_date=int(self.date/10000)*10000+1231,stock_pool=self.stock_pool)
                self.stk_limit = self.selector.stk_limit(start_date=int(self.date/10000)*10000+101, 
                              end_date=int(self.date/10000)*10000+1231,stock_pool=self.stock_pool)

            lastdate = self.date
            

            self.today_daily = self.daily.query(f"trade_date=={self.date}")
            self.today_stk_limit = self.stk_limit.query(f"trade_date=={self.date}")
            self.today_pool = pd.merge(left=self.today_daily,
                                       right=self.today_stk_limit,
                                       how='left',on='ts_code')

            self.today_delist = self.stock_basic.query(f'delist_date=={self.date}')[['ts_code','delist_date']]
            self.today_name = self.namechange.query(f'{self.date}>=start_date & {self.date}<=end_date')[['ts_code','stock_name']]
            
            self.today_pool = pd.merge(left=self.today_pool,right=self.today_name,
                                       how ='left',on='ts_code')
            self.today_pool.set_index('ts_code',inplace=True)
            
            
            self.today_exdate_df = self.dividend.query(f'ex_date=={self.date}')
            self.today_div_listdate_df = self.dividend.query(f'div_listdate=={self.date}')
            self.today_pay_date_df = self.dividend.query(f'pay_date=={self.date}')

            self.account.nextday(self.date,self.today_pool,
                                 self.today_exdate_df,self.today_div_listdate_df,self.today_pay_date_df,
                                 self.today_delist)
            self.account.delist()
            self.account.dividend()
            self.beforeopen()
            self.handlebar()
            self.account.updateprice()
            
            self.afterclose()


            
            self.netvaluerecorder[self.date] = self.account.netvalue

        self.selector.close()
        
        
    def draw(self,title=None,path=None,compindex=None,compindextype=None):
        datelist=[int(date) for date,nevtalue in self.netvaluerecorder.items()]
        dateliststr=[str(date) for date in datelist]
        netvaluelist=[nevtalue/self.startcash for date,nevtalue in self.netvaluerecorder.items()]
        
        
        
        grid = plt.GridSpec(3, 3, wspace=0.5, hspace=0.5)
        axes1 = plt.subplot(grid[0:2,0:3])
        axes1.plot(dateliststr,netvaluelist,label='netvalue')
        if compindex:
            if compindextype == 'public':
                self.selector = Selector()
                index_daily = self.selector.index_daily(date_list=self.tradedate_list,stock_pool=[compindex])
                self.selector.close()
            elif compindextype == 'homemade':
                self.sql = SQL('192.168.10.2',3306,'waralternative_dev','dongyuandev','waralternative')
                index_query = f'''
                select trade_date,close from index_daily_homemade 
                where trade_date in (
                    {','.join(["'"+str(tradedate)+"'" for tradedate in self.tradedate_list])}
                    )
                and ts_code = '{compindex}';
                '''
                index_daily = self.sql.select(index_query)
                self.sql.close()
            index_daily = index_daily.sort_values(by='trade_date')
            axes1.plot(dateliststr,index_daily.close/index_daily.close[0],label=compindex)
                
            axes1.set_xticklabels([date if i%20==0 else '' for i,date in enumerate(dateliststr)],rotation=45,size=5)
            axes1.legend(loc=2,prop = {'size':5})
            
            
        axes2 = plt.subplot(grid[2,0:3])
        axes2.axis('off')
        axes2.axis('tight')
        cellText = [[f'{round(cal_annrt(netvaluelist,datelist),2)}%',
                    f'{round(cal_max_percent_drawdown(netvaluelist))}%',
                    round(cal_sharp(netvaluelist),2),
                    round(cal_sortino(netvaluelist),2)]]
        colLabels = ['年化收益','最大回撤','夏普比率','索提诺比率']
        if compindex:
            cellText[0] = cellText[0][:1] +\
                [f'{round(cal_ann_excessrt(netvaluelist,(index_daily.close/index_daily.close[0]).to_list(),datelist),2)}%']\
                    + cellText[0][1:]
            colLabels = colLabels[:1] + ['超额收益(几何)'] + colLabels[1:]
        axes2.table(cellText=cellText,colLabels=colLabels,cellLoc='center',loc='center')
        if title:
            axes1.set_title(title)
        else:
            axes1.set_title('Backtest')
        if not path:
            path = './Backtest.png'
        plt.savefig(path,dpi=300)
        
        return datelist,netvaluelist
    

    

        
    
            
    
    
    
            