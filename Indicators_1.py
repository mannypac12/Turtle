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
import re
import numpy as np
import matplotlib.pyplot as plt

date = '20180820'

class data_provide:

    def __init__(self, st_date, ed_date):

        self.conn = cxo.connect(user = '******', password = '******', dsn = '******')
        self.st_date = st_date
        self.ed_date = ed_date

    def date_change(self):

        return 'between ' + self.st_date + ' and ' + self.ed_date

    def sql_date_change(self, sql):
        chg_dt = self.date_change()
        return re.sub(pattern = "between '20150101' and '20170731'", repl = chg_dt, string = sql)

    def u_cd_changer(self, sql, u_cd = "'I.001'"):

        ## Only for the index
        qry = self.sql_date_change(sql)
        return re.sub(pattern = "'I.001'", repl = u_cd, string = qry)

    def index_sql_collect(self, sql, u_cd = "'I.001'"):

        qry = self.u_cd_changer(self.sql_date_change(sql), u_cd)

        dt = pd.read_sql(qry, con = self.conn)
        dt['TRD_DT'] = pd.to_datetime(dt['TRD_DT'])

        return dt.set_index('TRD_DT')

    def sql_collect(self, sql):

        qry = self.sql_date_change(sql)

        dt = pd.read_sql(qry, con=self.conn)
        dt['TRD_DT'] = pd.to_datetime(dt['TRD_DT'])

        return dt.set_index('TRD_DT')

    ## 후에 종목분석추가

class tech_analysis_price:

    def __init__(self, data):

        self.data = data
        self.strt_prc = data['STRT_PRC']
        self.high_prc = data['HIGH_PRC']
        self.low_prc = data['LOW_PRC']
        self.cls_prc = data['CLS_PRC']

    def candle_color(self):

        ## True: Up / False: Down

        if self.cls_prc >= self.strt_prc:
            return True

        else:
            return False

    def moving_avg(self, win, tpe = 'ema'):

        if tpe == 'sma':

            return self.cls_prc.rolling(window=win)

        elif tpe == 'ema':

            return self.cls_prc.ewm(span = win, min_periods = win).mean()

    def mvg_cross_over(self, win1, win2, tpe = 'golden', mvg_type = 'ema'):

        try:
            win1 <= win2
        except ValueError():
            print("win1 should be shorter than win2")
        else:
            mvg_1 = self.moving_avg(win1, tpe=mvg_type)
            mvg_2 = self.moving_avg(win2, tpe=mvg_type)
            prv_mvg_1 = mvg_1.shift(1)
            prv_mvg_2 = mvg_2.shift(1)

            if tpe == 'golden':

                return (mvg_1 >= mvg_2) & (prv_mvg_1 <= prv_mvg_2)

            elif tpe == 'dead':

                return (mvg_1 <= mvg_2) & (prv_mvg_1 >= prv_mvg_2)

    def macd(self, win1 = 12, win2 = 26, sig = 9):

        one_ema = self.moving_avg(win1, tpe='ema')
        two_ema = self.moving_avg(win2, tpe='ema')
        macd_ema = one_ema - two_ema
        macd_sig = macd_ema.ewm(span = sig, min_periods = sig).mean()

        hist = macd_ema - macd_sig

        dt = pd.concat([macd_ema, macd_sig, hist], axis = 1)
        dt.columns = ['macd', 'signal', 'macd_hist']

        return dt.dropna()

    def macd_cross_over(self, win1, win2, sig, tpe = 'golden'):

        MACD_stat = self.macd(win1=win1, win2=win2, sig=sig)
        macd = MACD_stat['macd']
        macd_sig = MACD_stat['signal']

        prev_macd = macd.shift(1)
        prev_macd_signal = macd_sig.shift(1)

        if tpe == 'golden':

            return (macd >= macd_sig) & (prev_macd <= prev_macd_signal)

        elif tpe == 'dead':

            return (macd <= macd_sig) & (prev_macd >= prev_macd_signal)







sql_kspi = """
select trd_dt, strt_prc, high_prc, low_prc, cls_prc 
from FNS_UD
where trd_dt between '20150101' and '20170731' and 
u_cd in ('I.001')
"""

a = data_provide('20180101', '20180530')
test_dt = a.index_sql_collect(sql_kspi, "'I.201'")
tech_analysis_price(test_dt).macd_cross_over(12, 26, 9, tpe = 'dead')




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


