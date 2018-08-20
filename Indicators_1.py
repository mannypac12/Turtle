# -*- coding: utf-8 -*-
"""
Created on Wed Apr 25 16:11:23 2018

@author: fnguide
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 09:13:06 2018

@author: fnguide
"""

import cx_Oracle as cxo
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt

conn = cxo.connect(user = '******', password = '******', dsn = '******')

sql_kspi = """
select trd_dt, strt_prc, high_prc, low_prc, cls_prc 
from FNS_UD
where trd_dt between '20150101' and '20170731' and 
u_cd in ('I.001')
"""

sql_ksdq = """
select trd_dt, strt_prc, high_prc, low_prc, cls_prc 
from FNS_UD
where trd_dt between '20150101' and '20170731' and 
u_cd in ('I.201')
"""

def data_opener(sql, st_dt = '2015-01-01'):
    
    data = pd.read_sql(sql, conn, index_col = 'TRD_DT')
    data.index = pd.to_datetime(data.index)

    return data.loc[st_dt:]


def mov_average(data, win = 5, price = 'CLS_PRC', met = 'SMA'):
    
    if met == 'SMA':
        
        mn_avg = data[price].rolling(window = win).mean()
        
    elif met == 'EMA':
        
        mn_avg = data[price].ewm(span = win, min_periods = win).mean()
        
    return mn_avg


def DataGuideReader(data):
    dt = pd.read_excel(data, skiprows = 9, 
                       index_col = 10)

    dt.drop(dt.index[0:4], inplace = True)
    dt.columns[0] = 'TRD_DT'
    
    return dt

## Bollinger Band

def Bol_Band(data, win = 5, price = 'CLS_PRC', met = 'SMA'):
    
    dt = mov_average(data, win = win, price = price, met = met).rename('mv')
    dt_ptstd = (dt + 2 * data[price].rolling(win).std()).rename('ptstd')
    dt_mtstd = (dt - 2 * data[price].rolling(win).std()).rename('mtstd')
    
    return pd.concat([dt, dt_ptstd, dt_mtstd], axis = 1)

## Effective Ratio
    

## Bollinger Band    

kspi = data_opener(sql_kspi)
ksdq = data_opener(sql_ksdq)

