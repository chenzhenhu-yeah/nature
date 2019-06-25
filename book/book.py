import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

import sys
sys.path.append(r'../')
from down_k.get_trading_dates import get_trading_dates
from down_k.get_stk import get_stk_hfq
from down_k.get_inx import get_inx
from down_k.get_daily import get_daily
from down_k.get_fut import get_fut
from hu_signal.hu_talib import MA

dss = r'../../data/'

dates_fut = ['20180119','20180222','20180316','20180420','20180518','20180615',
             '20180720','20180817','20180921','20181019','20181116','20181221',
             '20190118','20190215','20190315','20190419','20190517']


class Book_fut(object):
    def __init__(self,code,benchCode,beginDate,endDate):
        self.futCode = code
        self.benchCode = benchCode
        self.df_bookCap = None
        self.df_benchCap = None
        self.beginDate = beginDate
        self.endDate = endDate
        self.singlePosition = 3E4

    def get_bookCap(self):
        # dates = get_trading_dates(self.beginDate, self.endDate)
        df = get_fut(dss, self.futCode, self.beginDate, self.endDate)
        df = df[['trade_date', 'settle']]
        return df

    def get_bookRet(self):
        # dates = get_trading_dates(self.beginDate, self.endDate)
        df = get_fut(dss, self.futCode, self.beginDate, self.endDate)
        df = df[['trade_date', 'close']]
        return df


class Book_stk(object):
    def __init__(self,codes,benchCode,hedgeCode,beginDate,endDate):
        self.stkCodes = codes
        self.benchCode = benchCode
        self.hedgeCode = hedgeCode
        self.df_bookCap = None
        self.df_benchCap = None
        self.df_hedgeCap = None
        self.df_hedge_bookCap = None
        self.beginDate = beginDate
        self.endDate = endDate
        self.singlePosition = 3E4

        self._open()
        self._calc_stkCap()
        self._calc_benchCap()
        self._calc_bookCap()
        self._calc_hedge()
        self._hedge_book()

    # 开仓，计算每支股票的开仓数量
    def _open(self):
        r = []
        for code in self.stkCodes:
            df_stk = get_stk_hfq(dss,code,self.beginDate, self.beginDate)
            assert(df_stk.empty == False)
            price = df_stk.iat[-1,3]
            num = int(self.singlePosition/price/10)*10
            r.append([code,num])
        df = pd.DataFrame(r, columns=['code','num'])
        df.to_csv('openBook.csv', index=False)

    # 计算stk在回测区间的每日市值
    def _calc_stkCap(self):
        r = []
        dates = get_trading_dates(dss, self.beginDate, self.endDate)
        df = pd.read_csv('openBook.csv', dtype={'code':'str'})
        for code,num in zip(df['code'], df['num']):
            df_stk = get_stk_hfq(dss,code,self.beginDate, self.endDate)
            for date in dates:
                df1 = df_stk[df_stk.date==date]
                if df1.empty:
                    price = 0
                else:
                    price = round(df1.iat[0,3],4)
                cap = round(price*num,2)
                r.append([date,code,num,price,cap])
        df = pd.DataFrame(r, columns=['date','code','num','price','cap'])

        # 处理停牌的情况（cap==0）
        codes = set(df['code'])
        for code in codes:
            df1 = df[df.code==code]
            df1 = df1.sort_values('date')
            df1 = df1.set_index('date')
            preCap, nowCap = 0, 0
            for date in df1.index:
                nowCap = df1.at[date,'cap']
                if nowCap == 0:
                    nowCap = preCap
                    inx = df[(df.date==date)&(df.code==code)].index.tolist()
                    df.at[inx[0],'cap'] = nowCap
                preCap = nowCap

        df.to_csv('stkCap.csv', index=False)

    # 计算组合每日的市值
    def _calc_bookCap(self):
        dates = get_trading_dates(dss, self.beginDate, self.endDate)
        df = pd.read_csv('stkCap.csv', dtype={'code':'str'})
        r = []
        for date in dates:
            df1 = df[df.date==date]
            r.append( [date,df1['cap'].sum()] )

        df1 = pd.DataFrame(r, columns=['date','cap'])
        df1 = df1.sort_values('date')
        df1 = df1.reset_index()
        baseCap = df1.at[0,'cap']
        df1['ret'] = round((df1['cap'] - baseCap)/baseCap, 2)

        self.df_bookCap = df1
        return self.df_bookCap

    # 获取基准的市值，返回df
    def _calc_benchCap(self):
        if self.df_benchCap is None:
            df2 = get_inx(dss, self.benchCode, self.beginDate, self.endDate)
            df2 = df2.sort_values('date')
            df2 = df2.reset_index()
            baseCap = df2.at[0,'close']
            df2['ret'] = round((df2['close'] - baseCap)/baseCap, 2)
            self.df_benchCap = df2[['date','ret']]
        return self.df_benchCap

    # 获取对冲的市值，返回df
    def _calc_hedge(self):
        if self.df_hedgeCap is None:
            df = get_fut(dss, self.hedgeCode, self.beginDate, self.endDate)
            #print(df)
            self.df_hedgeCap = df[['trade_date', 'close']]
        return self.df_hedgeCap

    # 将组合进行对冲
    def _hedge_book(self):
        print('here')
        df1 = self.df_bookCap
        df2 = self.df_hedgeCap

        df1 = df1.sort_values('date')
        df1 = df1.reset_index()
        df2 = df2.sort_values('trade_date')
        df2 = df2.reset_index()

        df2['cap'] = (df2.at[0,'close'] - df2['close'])*200
        df1['cap'] = df1['cap'] + df2['cap']
        baseCap = df1.at[0,'cap']
        df1['ret'] = round((df1['cap'] - baseCap)/baseCap, 2)

        self.df_hedge_bookCap = df1

    # 画折线图，市值相对起始日期的收益率
    def bt_ret(self, df1=None, df2=None, df3=None):
        dates = []
        for date in list(df1['date']):
            dates.append( datetime.strptime(date,'%Y-%m-%d') )
        plt.plot(dates, df1['ret'])

        if df2 is None:
            pass
        else:
            plt.plot(dates, df2['ret'])

        if df3 is None:
            pass
        else:
            plt.plot(dates, df3['ret'])

        plt.yticks([-0.4,-0.3,-0.2,-0.1, 0, 0.1, 0.2,0.3,0.4])
        plt.show()

df = pd.read_csv('b1.csv', dtype='str')
#print(df)
codes = set(df['code'])
print(codes)

s1 = Book_stk(codes,'399905','IC.CFX', '2018-05-16','2019-05-01')
#s1 = Book_stk(codes,'399905','IC.CFX', '2019-01-03','2019-05-01')
s1.bt_ret(s1.df_bookCap, s1.df_benchCap, s1.df_hedge_bookCap)

s1.df_bookCap.to_excel('e2.xls')
s1.df_benchCap.to_excel('e3.xls')
s1.df_hedgeCap.to_excel('e4.xls')

# f1 = Book_fut('IC.CFX','399905','2019-01-01','2019-03-01')
# print(f1.get_bookCap())
