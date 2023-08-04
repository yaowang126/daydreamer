# Daydreamer
## Discription
factor analysis and strategy backtest tool with tushare data

## Introduction
### Clone to your runtime sys.path
```
cd YourPythonPath/Lib/site-packages
git clone https://github.com/yaowang126/daydreamer.git
```

### Usage of backtest
Import class Context from backtest module 
```
from daydreamer.backtest import Context
```

-You must reload these 5 methods that are restricted by abstractmethod:
-initialize
-preparedata
-beforeopen
-handlebar
-afterclose
-You can follow this example

```
from daydreamer.backtest import Context
import pandas as pd
import numpy as np



class Mystrategy(Context):
    
    def initialize(self):
        pass
    
    def preparedata(self):
        factordf = pd.read_excel('./data.xlsx')
        # factordf['factor'] = factordf['factor_peg'] - factordf['factor_preroe90_median'] - factordf['factor_rating_upminusdown']\
        #     + factordf['turnover_vol_f'] + factordf['ret20']
        factordf['factor'] = factordf['factor_peg'] - factordf['factor_preroe90_median'] - factordf['factor_rating_upminusdown']\
        #     + factordf['turnover_vol_f'] + factordf['ret20']
        self.factordf = factordf

    def beforeopen(self):
        '''
        此为已经变成除权价格后的盘前
        '''
        pass
 
    def handlebar(self):
        if len(factortoday := self.factordf[self.factordf['nexttrade_date']==self.date])>0:
            print(self.date)
            today_daily_buy = self.today_pool.query("'*ST' not in stock_name & '退' not in stock_name ")
            factortoday = pd.merge(left=factortoday,right=today_daily_buy,
                                on='ts_code',how='inner')
            factortoday = factortoday.sort_values(by='factor').dropna()
            tobuy = factortoday.ts_code.iloc[:10].to_list()
            
            for ts_code in self.account.portfolio:
                if ts_code not in tobuy:
                    self.account.order_percent(ts_code, 0.0)
                    
            eachratio = 1/10
            for ts_code in tobuy:
                if ts_code in self.account.portfolio:
                    self.account.order_percent(ts_code,eachratio)
            for ts_code in tobuy:
                if ts_code not in self.account.portfolio:
                    self.account.order_percent(ts_code,eachratio)
            if self.account.cash>0:
                self.account.order_money(tobuy[0],self.account.cash)
     
    def afterclose(self):
        '''
        此为按收盘价结算后的持仓统计
        '''
        pass 



if __name__ == '__main__':
    
    mystrategy = Mystrategy()
    mystrategy.backtest(20210601,20221231,1e8)
    date,netvalue = mystrategy.draw(compindex='399967.SZ',
                                    title ='yourtitle',
                                    path='yourtitle')


```





