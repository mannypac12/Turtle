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
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt

date = '20180821'

## mkt_gb / cap_size 지정 (예정)

mkt_gb = "1"
cap_size = "2"


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

    def sql_stk_change(self, sql, mkt_gb = "1", cap_size = "2"):

        mkt = "MKT_GB IN " + "(" + mkt_gb + ") "
        cap = "MKT_CAP_SIZE IN " + "(" + cap_size + ")"

        sql_chg = re.sub(pattern = "MKT_GB IN \(1\)", repl = mkt, string=sql)
        sql_chg = re.sub(pattern="MKT_CAP_SIZE IN \(1\)", repl=cap, string=sql_chg)

        return sql_chg

    def sql_stk_universe(self, mkt_gb = "1", cap_size = "2"):

        sql_1 = """
           SELECT B.GICODE, B.ITEMABBRNM, B.FGSC_CD, A.U_NM
           FROM 
           (
               SELECT U_CD, U_NM
               FROM FNS_U_MAST
               WHERE U_CD LIKE '%FGSC%'
                     AND DEPTH = 4
           ) A, 
           (
               SELECT GICODE, ITEMABBRNM, FGSC_CD
               FROM FNS_J_MAST
               WHERE 1=1 
                     AND SUBSTR(GICODE, 7, 1) = '0'
                     AND MKT_GB IN (1) 
                     AND MKT_CAP_SIZE IN (2)
            ) B
            WHERE B.FGSC_CD = A.U_CD(+) 
        """

        mkt = " AND MKT_GB IN " + "(" + mkt_gb + ") "
        cap = " AND MKT_CAP_SIZE IN " + "(" + cap_size + ")"

        sql = self.sql_stk_change(sql_1, mkt_gb=mkt_gb, cap_size=cap_size)
        dt = pd.read_sql(con=self.conn, sql=sql)

        return {'data': dt, 'gicode': dt['GICODE']}

    def sql_stk(self, mkt_gb = "1", cap_size = "2"):

        sql ="""
                SELECT A.GICODE
                       , A.TRD_DT
                       , A.STRT_PRC 
                       , A.HIGH_PRC
                       , A.LOW_PRC
                       , A.CLS_PRC       
                FROM 
                (
                    SELECT GICODE, TRD_DT
                          , ADJ_STRT_PRC STRT_PRC
                          , ADJ_HIGH_PRC HIGH_PRC
                          , ADJ_LOW_PRC LOW_PRC
                          , ADJ_PRC CLS_PRC      
                    FROM FNS_JD_ADJ
                    WHERE 1=1 
                          AND ADJ_GB = 2
                          AND SUBSTR(GICODE,1,1) = 'A'
                          AND SUBSTR(GICODE,7,1) = '0'
                          AND TRD_DT between '20150101' and '20170731'
                          AND GICODE IN 
                          (
                            SELECT GICODE
                            FROM FNS_J_MAST
                            WHERE 1=1
                                  AND SUBSTR(GICODE, 7, 1) = '0'
                                  AND MKT_GB IN (1)
                                  AND MKT_CAP_SIZE IN (2)          
                          )
                ) A, 
                (
                    SELECT B.GICODE, B.ITEMABBRNM, B.FGSC_CD, A.U_NM
                    FROM 
                    (
                        SELECT U_CD, U_NM
                        FROM FNS_U_MAST
                        WHERE U_CD LIKE '%FGSC%'
                              AND DEPTH = 4
                    ) A, 
                    (
                        SELECT GICODE, ITEMABBRNM, FGSC_CD
                        FROM FNS_J_MAST
                        WHERE 1=1
                              AND SUBSTR(GICODE, 7, 1) = '0'
                              AND MKT_GB IN (1)
                              AND MKT_CAP_SIZE IN (2)
                    ) B
                    WHERE B.FGSC_CD = A.U_CD(+) 
                ) B
                WHERE A.GICODE = B.GICODE
            """

        sql = self.sql_stk_change(sql, mkt_gb=mkt_gb, cap_size=cap_size)
        qry = self.sql_date_change(sql)

        dt = pd.read_sql(con = self.conn, sql = qry)
        dt['TRD_DT'] = pd.to_datetime(dt['TRD_DT'])

        return dt.sort_values(['GICODE', 'TRD_DT']).set_index('TRD_DT')

    ## 후에 종목분석추가

class Return_Analysis:

    """
    This objects analyse the return on each calender day
    or
    day of the date
    For your information, please use close prices
    """

    def __init__(self, data):

        # Data: cls_data
        self.rt = data.pct_change()

    def data_prep(self):

        data = self.rt
        data.rt['a'] = data.rt.index.year
        data.rt['m'] = data.rt.index.month
        data.rt['date'] = data.rt.index.date
        data.rt['wd'] = data.rt.index.weekday

        return data.reset_index().drop('TRD_DT', axis = 1)

    def day_of_return(self):

        pass


class tech_analysis_price:

    def __init__(self, data):

        self.data = data
        self.strt_prc = data['STRT_PRC']
        self.high_prc = data['HIGH_PRC']
        self.low_prc = data['LOW_PRC']
        self.cls_prc = data['CLS_PRC']

    def candle_color(self):

        ## True: Up / False: Down

        return self.cls_prc >= self.strt_prc

    def moving_avg(self, win, tpe = 'ema'):

        if tpe == 'sma':

            return self.cls_prc.rolling(window=win)

        elif tpe == 'ema':

            return self.cls_prc.ewm(span = win, min_periods = win).mean()

    def price_mov(self, win = 5, tpe = 'ema'):

        cond_1 = self.cls_prc > self.moving_avg(win=win, tpe=tpe)
        cond_2 = self.candle_color()

        return cond_1 & cond_2

    """
    5일 이평에 붙는 패턴 가즈아!
    """


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

    ## Trend retracement Tools

    def rete_move_1(self, win1 = 5, win2 = 20, tpe = 'golden', mvg_type = 'ema'):

        """
        If the 5 days meets 20 meets with Red candle

        If the close price between 5 and 20, showing upward
        :return:
        """

        cond1 = self.mvg_cross_over(win1=win1, win2=win2, tpe = tpe, mvg_type = mvg_type).shift(1)
        cond2 = self.candle_color()

        return cond1 & cond2

    def value_zone(self, win1 = 5, win2 = 20, strong = True):

        """
        :param win1: 20 day moving avg
        :param win2: 5 day moving avg
        :param win3: Candle should be +(red)
        :return:
        """

        cond1 = (self.strt_prc >= self.moving_avg(win = win2)) | (self.strt_prc < self.moving_avg(win = win2))
        cond2 = self.cls_prc <= self.moving_avg(win = win1)

        if strong:
            cond3 = self.candle_color()
            return cond1 & cond2 & cond3
        else:
            return cond1 & cond2

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

    ## Variables for money management

    def atr(self, win = 14, sig = 9):

        TR = self.high_prc.sub(self.low_prc.shift(1))
        Prp_ATR = TR.ewm(span = win, min_periods = win).mean()
        ## [Prior ATR * 13 + Current TR] / 14

        ATR = (Prp_ATR.shift(1) * (win-1)).sub(-TR).div(win)
        sig = ATR.ewm(span = sig, min_periods= sig).mean()

        ans = pd.concat([ATR, sig], axis=1)
        ans.columns = ['ATR', 'signal']

        return ans.dropna()


class money_management:

    def __init__(self, capital):

        self.cap = capital

    def trade_risk(self, entry, stop):

        return entry - stop

    def fxed_dol_risk(self, entry, stop, unit):

        trd_rsk = self.trade_risk(entry, stop)
        fxd_rsk = self.cap / unit

        return round(fxd_rsk / trd_rsk)

    def fxed_cap_risk(self, max_dd, blw_t_risk):

        ## Lag_dd = Dollar amount
        ## blw_t_risk = acceptable risk you endure(perct)

        fxd_cap = max_dd / blw_t_risk

        return round(self.cap / fxd_cap)

    def william_fxed(self, lag_loss, risk = 0.02):

        dol_risk = self.cap * risk

        return round(dol_risk / lag_loss)

"""
Back Test Tools 

If the price is moving up after the signal, 

then Get in.
"""

sql_test_1 = """
SELECT A.GICODE, B.ITEMABBRNM, A.TRD_DT, B.U_NM
       , A.STRT_PRC 
       , A.HIGH_PRC
       , A.LOW_PRC
       , A.CLS_PRC       
FROM 
(
    SELECT GICODE, TRD_DT
          , ADJ_STRT_PRC STRT_PRC
          , ADJ_HIGH_PRC HIGH_PRC
          , ADJ_LOW_PRC LOW_PRC
          , ADJ_PRC CLS_PRC      
    FROM FNS_JD_ADJ
    WHERE 1=1 
          AND ADJ_GB = 2
          AND SUBSTR(GICODE,1,1) = 'A'
          AND SUBSTR(GICODE,7,1) = '0'
          AND TRD_DT between '20170101' and '20170131'
          AND GICODE IN 
          (
            SELECT GICODE
            FROM FNS_J_MAST
            WHERE 1=1
                  AND SUBSTR(GICODE, 7, 1) = '0'
                  AND MKT_GB IN (1)
                  AND MKT_CAP_SIZE = 2          
          )
) A, 
(
    SELECT B.GICODE, B.ITEMABBRNM, B.FGSC_CD, A.U_NM
    FROM 
    (
        SELECT U_CD, U_NM
        FROM FNS_U_MAST
        WHERE U_CD LIKE '%FGSC%'
              AND DEPTH = 4
    ) A, 
    (
        SELECT GICODE, ITEMABBRNM, FGSC_CD
        FROM FNS_J_MAST
        WHERE 1=1
              AND SUBSTR(GICODE, 7, 1) = '0'
              AND MKT_GB IN (1)
              AND MKT_CAP_SIZE = 2
    ) B
    WHERE B.FGSC_CD = A.U_CD(+) 
) B
WHERE A.GICODE = B.GICODE
"""




a = data_provide('20180101', '20180110')
dt = a.sql_stk()

dt.sort_index()
a.sql_stk_universe(mkt_gb = "1", cap_size = "2")
pd.read_sql(sql = sql_test, con=a.conn)

def stk_date_creator(st_date, ed_date):




A = pd.date_range('20180101', '20180530', freq = 'MS')
B = pd.date_range('20180101', '20180531', freq = 'M')

for st, ed in zip(A, B):
    print("between TRD_DT " + st.strftime('%Y%m%d'), "AND " + ed.strftime('%Y%m%d'))




