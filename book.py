import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import tushare as ts

from nature import (get_trading_dates, get_stk_bfq, is_price_time)

pro = ts.pro_api('e7d81e40fb30b0e7f7f8d420a81700f401ddd382d82b84c473afd854')


class Tactic(object):
    def __init__(self,dss,name,df):
        self.dss = dss
        self.tacticName = name
        self.hold_Array = self._load_hold(df)

    def _load_hold(self, df):
        r = []
        if df is not None:
            for i, row in df.iterrows():
                #print(row[0],row[1],row[2],row[3])
                r.append([row[1],row[2],row[3]])
        return r

    # 计算Tactic包含的code
    def get_codes(self):
        r = []
        for item in self.hold_Array:
            code = item[0]
            r.append(code)
        return r

    # 计算Tactic当前的市值
    def get_cost_cap(self):
        r_cost, r_cap = 0, 0

        for item in self.hold_Array:
            code = item[0]
            cost = int(item[1])
            num  = int(item[2])

            df = ts.get_realtime_quotes(code)
            price = float(df.at[0,'price'])
            cap = int(price*num)

            r_cost += cost
            r_cap  += cap

        return r_cost, r_cap

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

class Book(object):
    def __init__(self,dss):
        self.dss = dss
        self.holdFilename = dss + 'csv/hold.csv'
        self.cash = self._loadCash()
        self.tactic_List = self._loadHold()

    def _loadCash(self):
        df1 = pd.read_csv(self.holdFilename);#print(df1)
        df1 = df1[df1.agent=='pingan']
        df2 = df1[df1.portfolio=='cash']
        cash = df2.iat[0,2]
        return cash

    def _loadHold(self):
        df1 = pd.read_csv(self.holdFilename);#print(df1)
        df1 = df1[df1.agent=='pingan']
        df1 = df1[~df1.code.isin(['cash01','profit'])]
        #print(df1)
        pfSet = set(df1.portfolio)
        r = []
        for pfName in pfSet:
            df2 = df1[df1.portfolio==pfName]
            tactic = Tactic(self.dss, pfName, df2)
            r.append(tactic)

        return  r

    # 计算Book包含的code
    def get_codes(self):
        codes = []
        for tactic in self.tactic_List:
            codes += tactic.get_codes()
        return set(codes)

    # 计算Book当前的市值
    def get_cost_cap(self):
        cost, cap = 0, 0
        for tactic in self.tactic_List:
            cost1, cap1 = tactic.get_cost_cap()
            cost += cost1
            cap  += cap1
        return cost, cap

def has_factor(dss):
    r = []
    b1 = Book(dss)
    codes = b1.get_codes()

    for code in codes:
        if code[0] == '6':
            code += '.SH'
        else:
            code += '.SZ'

        df = pro.adj_factor(ts_code=code, trade_date='')
        #print(df.head(2))
        if df.at[0,'adj_factor'] != df.at[1,'adj_factor']:
            r.append(code)
    return r

def stk_report(dss):
    b1 = Book(dss)
    codes = b1.get_codes()

    r = []
    for code in codes:
        df = ts.get_realtime_quotes(code)
        name = df.at[0,'name']

        df = get_stk_bfq(dss,code)
        df = df.loc[:30,]
        #print(df)
        one_change = round((df.at[0,'close']/df.at[1,'close'] - 1)*100, 2)
        five_change = round((df.at[0,'close']/df.at[5,'close'] - 1)*100, 2)
        ten_change = round((df.at[0,'close']/df.at[10,'close'] - 1)*100, 2)
        r.append( str([name,one_change,five_change,ten_change]) )

    return r

if __name__ == '__main__':
    dss = '../data/'
    #stk_warn(dss)
    #print(stk_report(dss))
    print(has_factor(dss))

    # book = Book(dss)
    # print(book.get_cost_cap())


    pass
    #daily_report(dss)
    #tactic_signal(dss)
