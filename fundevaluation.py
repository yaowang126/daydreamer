# -*- coding: utf-8 -*-
"""
Created on Mon Aug  7 14:59:40 2023

@author: YW
"""

import datetime
import pandas as pd
import numpy as np

def cal_annrt(netvaluelist,datelist):
    length = len(datelist)
    startdatetime = datetime.datetime.strptime(str(datelist[0]), '%Y%m%d')
    enddatetime = datetime.datetime.strptime(str(datelist[length-1]), '%Y%m%d')
    years = (enddatetime-startdatetime).days/365
    annrt = netvaluelist[length-1]**(1/years)-1
    return annrt * 100


def cal_ann_excessrt(netvaluelist,indexvaluelist,datelist):
    length = len(datelist)
    startdatetime = datetime.datetime.strptime(str(datelist[0]), '%Y%m%d')
    enddatetime = datetime.datetime.strptime(str(datelist[length-1]), '%Y%m%d')
    years = (enddatetime-startdatetime).days/365
    ann_excessrt = ((netvaluelist[length-1]/netvaluelist[0])/(indexvaluelist[length-1]/indexvaluelist[0]))**(1/years)-1
    return ann_excessrt *100


def cal_max_percent_drawdown(netvaluelist):
    price_array = np.array(netvaluelist)
    max_value_before = np.maximum.accumulate(price_array)
    drawdown = 1 - price_array / max_value_before
    max_percent_drawdown = np.max(drawdown)
    return max_percent_drawdown * 100


def cal_sharp(netvaluelist,rf_rate=0.025):
    df = pd.DataFrame({'net':netvaluelist})
    df['rt'] = df['net']/df['net'].shift(1)-1
    ann_rt = df['rt'].mean() * 250
    ann_std = df['rt'].std() * (250**(1/2))
    sharp = (ann_rt - rf_rate)/ann_std
    return sharp


def cal_sortino(netvaluelist,rf_rate=0.025):
    df = pd.DataFrame({'net':netvaluelist})
    df['rt'] = df['net']/df['net'].shift(1)-1
    negrt = df.query('rt<0')['rt']
    ann_rt = df['rt'].mean() * 250
    ann_std = negrt.std() * (250**(1/2))
    sortino = (ann_rt - rf_rate)/ann_std
    return sortino