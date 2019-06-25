import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import tushare as ts

import sys
sys.path.append(r'../')
from down_k.get_trading_dates import get_trading_dates
from down_k.get_stk import get_stk_hfq
from down_k.get_stk import get_stk_bfq
from down_k.get_inx import get_inx
from down_k.get_daily import get_daily
from down_k.get_fut import get_fut
from hu_signal.hu_talib import MA
from hu_signal.macd import init_signal_macd, signal_macd_sell, signal_macd_buy

pro = ts.pro_api('e7d81e40fb30b0e7f7f8d420a81700f401ddd382d82b84c473afd854')

class Book(object):
    def __init__(self,dss,name):
        self.dss = dss
        self.bookName = name
        self.hold_Array = []
        self.hold_Dict = {}
        self.bottle_Array = []

    def add_hold(self,code,cost,num):
        self.hold_Array.append([code,cost,num])
        df_stk = get_stk_bfq(self.dss,code)
        self.hold_Dict[code] = df_stk

    def add_bottle(self,code,name):
        self.bottle_Array.append([code,name])

    # 计算stk在回测区间的每日市值
    def get_bookCap(self, date):
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
            r.append( [today,self.bookName,code,name,price,cap] )
        return r

    def calc_signal(self, today):
        r = []
        codes = []
        for item in self.bottle_Array:
            codes.append(item[0])

        #print(codes)

        # 准备信号
        df_signal_macd = init_signal_macd(self.dss,codes)
        #print(len(df_signal_macd))

        #macd买入信号
        to_buy_codes = signal_macd_buy(codes, today, df_signal_macd)
        for code in to_buy_codes:
            df = ts.get_realtime_quotes(code)
            name = df.at[0,'name']
            r.append([today,code,'name',self.bookName,'buy'])

        #macd卖出信号
        to_sell_codes = signal_macd_sell(codes, today, df_signal_macd)
        for code in to_sell_codes:
            df = ts.get_realtime_quotes(code)
            name = df.at[0,'name']
            r.append([today,code,'name',self.bookName,'sell'])

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

class Portfolio(object):
    def __init__(self,dss,bottleFile,pfFile,benchCode,hedgeCode,beginDate,endDate):
        self.dss = dss
        self.bottle_BookDict = self._loadBottle(bottleFile)
        self.cash = self._loadCash(pfFile)
        self.hold_BookList = self._loadHold(pfFile)
        self.benchCode = benchCode
        self.hedgeCode = hedgeCode
        self.beginDate = beginDate
        self.endDate = endDate
        self.df_portfolioCap = None
        self.df_benchCap = None
        self.df_hedgeCap = None
        self.df_hedge_portfolioCap = None
        self.singlePosition = 3E4

        self._calc_portfolioCap()
        self._calc_benchCap()
        self._calc_hedgeCap()
        self._calc_hedge_portfolio()

    def _loadBottle(self,bottleFile):
        df1 = pd.read_csv(bottleFile, dtype='str')
        #print(df1)
        bookSet = set(df1.book)
        r = {}
        for bk in bookSet:
            book = Book(self.dss,bk)
            df2 = df1[df1.book==bk]
            #print(df2)
            for i, row in df2.iterrows():
                #print(row[1],row[3])
                book.add_bottle(row[1],row[3])
            r[bk] = book

        return r

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
            book = Book(self.dss, pf)
            df2 = df1[df1.portfolio==pf]
            for i, row in df2.iterrows():
                #print(row[0],row[1],row[2],row[3])
                book.add_hold(row[1],row[2],row[3])

            r.append(book)

        return  r

    # 计算组合每日的市值
    def _calc_portfolioCap(self):
        dates = get_trading_dates(self.dss, self.beginDate, self.endDate)
        r = []
        for date in dates:
            cap = 0
            for book in self.hold_BookList:
                cap += book.get_bookCap(date)
            r.append([date,cap])

        df1 = pd.DataFrame(r, columns=['date','cap'])
        df1 = df1.sort_values('date')
        df1 = df1.reset_index()
        baseCap = df1.at[0,'cap']
        df1['ret'] = round((df1['cap'] - baseCap)/baseCap, 2)

        self.df_portfolioCap = df1[['date','cap','ret']]

    # 获取基准的市值，返回df
    def _calc_benchCap(self):
        if self.df_benchCap is None:
            df2 = get_inx(self.dss, self.benchCode, self.beginDate, self.endDate)
            df2 = df2.sort_values('date')
            df2 = df2.reset_index()
            baseCap = df2.at[0,'close']
            df2['ret'] = round((df2['close'] - baseCap)/baseCap, 2)
            self.df_benchCap = df2[['date','ret']]
        return self.df_benchCap

    # 获取对冲的市值，返回df
    def _calc_hedgeCap(self):
        if self.df_hedgeCap is None:
            df = get_fut(self.dss, self.hedgeCode, self.beginDate, self.endDate)
            #print(df)
            self.df_hedgeCap = df[['trade_date', 'close']]
        return self.df_hedgeCap

    # 将组合进行对冲
    def _calc_hedge_portfolio(self):
        df1 = self.df_portfolioCap
        df2 = self.df_hedgeCap

        df1 = df1.sort_values('date')
        df1 = df1.reset_index()
        df2 = df2.sort_values('trade_date')
        df2 = df2.reset_index()

        df2['cap'] = (df2.at[0,'close'] - df2['close'])*200
        #print(df1)
        #print(df2)
        df1['cap'] = df1['cap'] + df2['cap']
        baseCap = df1.at[0,'cap']
        df1['ret'] = round((df1['cap'] - baseCap)/baseCap, 2)
        df1 = df1.loc[:,['date','cap','ret']]

        self.df_hedge_portfolioCap = df1

    # 画折线图，市值相对起始日期的收益率
    def bt_ret(self, df1=None, df2=None, df3=None):
        dates = []
        for date in list(df1['date']):
            dates.append( datetime.strptime(date,'%Y-%m-%d') )
        plt.plot(dates, df1['ret'])

        if df2 is None:
            print('here2')
        else:
            plt.plot(dates, df2['ret'])

        if df3 is None:
            print('here3')
        else:
            plt.plot(dates, df3['ret'])

        plt.yticks([-0.4,-0.3,-0.2,-0.1, 0, 0.1, 0.2,0.3,0.4])
        plt.show()

def book_signal(dss):
    dates = get_trading_dates(dss)
    preday = dates[-2]
    today = dates[-1]

    bottleFile = dss + 'csv/bottle.csv'
    pfFile = dss + 'csv/hold.csv'
    p1 = Portfolio(dss,bottleFile, pfFile,'399905','IC1909.CFX', preday, today)
    r = []

    for key in p1.bottle_BookDict.keys():
        book = p1.bottle_BookDict[key]
        r += book.calc_signal(today)
    #print(r)

    df = pd.DataFrame(r, columns=['date','code','name','book','signal'])
    filename = p1.dss + 'csv/signal_macd.csv'
    df.to_csv(filename, index=False, mode='a', header=None)

def has_factor(dss):
    r = []
    bottleFile = dss + 'csv/bottle.csv'
    pfFile = dss + 'csv/hold.csv'
    p1 = Portfolio(dss,bottleFile, pfFile,'399905','IC1909.CFX', '2019-04-03','2019-06-01')

    dates = get_trading_dates(dss)
    today = dates[-1]

    for book in p1.hold_BookList:
        r += book.has_factor(today)

    #print(r)
    return r


def backtest(dss):
    bottleFile = dss + 'csv/bottle.csv'
    pfFile = dss + 'csv/hold.csv'

    p1 = Portfolio(dss,bottleFile, pfFile,'399905','IC1909.CFX', '2019-04-03','2019-06-01')
    p1.bt_ret(p1.df_portfolioCap, p1.df_benchCap, p1.df_hedge_portfolioCap)


def daily_report(dss):
    dates = get_trading_dates(dss)
    preday = dates[-2]
    today = dates[-1]

    bottleFile = dss + 'csv/bottle.csv'
    pfFile = dss + 'csv/hold.csv'
    p1 = Portfolio(dss,bottleFile, pfFile,'399905','IC1909.CFX', preday, today)

    r = []
    for book in p1.hold_BookList:
        r += book.daily_result(today)

    df = pd.DataFrame(r, columns=['date','book','code','name','price','cap'])
    df = df[['date','book','price','cap']]

    cap = int(df.cap.sum())
    s1 = pd.Series( [today,'股票市值',p1.cash,cap], index=['date','book','price','cap'] )
    df.drop(df.index,inplace=True)
    df = df.append(s1, ignore_index=True)


    cap += p1.cash
    price = int(cap - 806000)
    s1 = pd.Series( [today,'账户市值',price,cap], index=['date','book','price','cap'] )
    df = df.append(s1, ignore_index=True)

    cap = int(p1.df_hedgeCap.iat[0,1]*200)
    price = 4515*200 - cap
    s1 = pd.Series( [today,'hedge',price,cap], index=['date','book','price','cap'] )
    df = df.append(s1, ignore_index=True)

    df.to_csv(dss+'csv/daily_result.csv', mode='a', index=False, header=None)

def stk_report(dss):
    dates = get_trading_dates(dss)
    preday = dates[-2]
    today = dates[-1]
    bottleFile = dss + 'csv/bottle.csv'
    pfFile = dss + 'csv/hold.csv'
    p1 = Portfolio(dss,bottleFile, pfFile,'399905','IC1909.CFX', preday, today)

    codes = []
    for book in p1.hold_BookList:
        codes += book.hold_Dict.keys()

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
    dss = '../../data/'
    stk_report(dss)
    #daily_report(dss)
    #book_signal(dss)
    #has_factor(dss)
