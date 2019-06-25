import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import tushare as ts

import sys
sys.path.append(r'../../')
from down_k.get_trading_dates import get_trading_dates
from down_k.get_stk import get_stk_hfq
from down_k.get_stk import get_stk_bfq
from down_k.get_inx import get_inx
from down_k.get_daily import get_daily
from down_k.get_fut import get_fut
from hu_signal.hu_talib import MA
from hu_signal.macd import init_signal_macd, signal_macd_sell, signal_macd_buy

pro = ts.pro_api('e7d81e40fb30b0e7f7f8d420a81700f401ddd382d82b84c473afd854')


class Tactic(object):
    def __init__(self,dss,name):
        self.dss = dss
        self.tacticName = name
        self.hold_Array = []
        self.hold_Dict = {}

    def add_hold(self,code,cost,num):
        self.hold_Array.append([code,cost,num])
        df_stk = get_stk_bfq(self.dss,code)
        self.hold_Dict[code] = df_stk

    # 计算stk在回测区间的每日市值
    def get_tacticCap(self, date):
        r = 0
        for item in self.hold_Array:
            df1 = self.hold_Dict[item[0]]
            df1 = df1[df1.date<=date]
            df1 = df1.sort_values('date',ascending=False)
            assert(df1.empty == False)
            price = df1.iat[0,3]
            r += price*item[2]
        return r

    def daily_result(self, today):
        r = []
        for item in self.hold_Array:
            code = item[0]

            df = ts.get_realtime_quotes(code)
            name = df.at[0,'name']
            price = df.at[0,'price']

            #当日停牌
            if price == 0:
                df1 = self.hold_Dict[code]
                df1 = df1[df1.date<=today]
                df1 = df1.sort_values('date',ascending=False)
                assert(df1.empty == False)
                price = df1.iat[0,3]

            cap = int(float(price)*item[2])
            r.append( [today,self.tacticName,code,name,price,cap] )
        return r

    def has_factor(self, today):
        r = []
        for item in self.hold_Array:
            code = item[0]
            if code[0] == '6':
                code += '.SH'
            else:
                code += '.SZ'

            df = pro.adj_factor(ts_code=code, trade_date='')
            #print(df.head(2))
            if df.at[0,'adj_factor'] != df.at[1,'adj_factor']:
                r.append(code)

        return r

class Book(object):
    def __init__(self,dss,pfFile,beginDate,endDate):
        self.dss = dss
        self.cash = self._loadCash(pfFile)
        self.hold_TacticList = self._loadHold(pfFile)
        self.beginDate = beginDate
        self.endDate = endDate
        self.df_bookCap = None

        self._calc_bookCap()

    def _loadCash(self,pfFile):
        df1 = pd.read_csv(pfFile);#print(df1)
        df1 = df1[df1.agent=='pingan']
        df2 = df1[df1.portfolio=='cash']
        cash = df2.iat[0,2]
        return cash

    def _loadHold(self,pfFile):
        df1 = pd.read_csv(pfFile);#print(df1)
        df1 = df1[df1.agent=='pingan']
        df1 = df1[~df1.code.isin(['cash01','profit'])]
        #print(df1)
        pfSet = set(df1.portfolio)
        r = []
        for pf in pfSet:
            tactic = Tactic(self.dss, pf)
            df2 = df1[df1.portfolio==pf]
            for i, row in df2.iterrows():
                #print(row[0],row[1],row[2],row[3])
                tactic.add_hold(row[1],row[2],row[3])

            r.append(tactic)

        return  r

    # 计算组合每日的市值
    def _calc_bookCap(self):
        dates = get_trading_dates(self.dss, self.beginDate, self.endDate)
        r = []
        for date in dates:
            cap = 0
            for tactic in self.hold_TacticList:
                cap += tactic.get_tacticCap(date)
            r.append([date,cap])

        df1 = pd.DataFrame(r, columns=['date','cap'])
        df1 = df1.sort_values('date')
        df1 = df1.reset_index()
        baseCap = df1.at[0,'cap']
        df1['ret'] = round((df1['cap'] - baseCap)/baseCap, 2)

        self.df_bookCap = df1[['date','cap','ret']]


def has_factor(dss):
    r = []
    pfFile = dss + 'csv/hold.csv'
    p1 = Book(dss,pfFile, '2019-04-03','2019-06-01')

    dates = get_trading_dates(dss)
    today = dates[-1]

    for tactic in p1.hold_TacticList:
        r += tactic.has_factor(today)

    #print(r)
    return r


def stk_report(dss):
    dates = get_trading_dates(dss)
    preday = dates[-2]
    today = dates[-1]
    pfFile = dss + 'csv/hold.csv'
    p1 = Book(dss, pfFile,preday, today)

    codes = []
    for tactic in p1.hold_TacticList:
        codes += tactic.hold_Dict.keys()

    r = []
    for code in codes:
        df = ts.get_realtime_quotes(code)
        name = df.at[0,'name']

        df = get_stk_bfq(dss,code)
        df = df.loc[:30,]
        #print(df)
        one_change = round((df.at[0,'close']/df.at[1,'close'] - 1)*100, 2)
        three_change = round((df.at[0,'close']/df.at[3,'close'] - 1)*100, 2)
        five_change = round((df.at[0,'close']/df.at[5,'close'] - 1)*100, 2)
        ten_change = round((df.at[0,'close']/df.at[10,'close'] - 1)*100, 2)
        thirty_change = round((df.at[0,'close']/df.at[30,'close'] - 1)*100, 2)
        r.append( str([name,one_change,three_change,five_change,ten_change,thirty_change]) )

    return r

if __name__ == '__main__':
    dss = '../../../data/'
    stk_report(dss)
    #daily_report(dss)
    #tactic_signal(dss)
    #has_factor(dss)
