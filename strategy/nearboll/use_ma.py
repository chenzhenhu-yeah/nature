import os
import re
import datetime
import time
import io
import zlib
import pandas as pd
import tushare as ts

import sys
#sys.path.append(r'../../')
sys.path.append('..\\..')
#sys.path.append(r'C:\Users\czh\Documents\critical\python_scripts\git_repo\hello-world')
from down_k.get_daily import get_codes
from down_k.get_stk import get_stk_hfq
from down_k.get_trading_dates import get_trading_dates

from strategy.nearboll.book import Book
#print(sys.path)
#print(__name__)
#print(sys.modules.items())




def hit_ma(dss,date):
    r = []

    codes = get_codes(dss)
    for code in codes:
        df = get_stk_hfq(dss,code,'2018-01-08')
        if df is None:
            continue

        df1 = df[df.date==date]
        if df1.empty:
            continue

        df = df[df.date<=date]
        df = df.sort_values(by=['date'])

        if len(df) < 60:  # 新股，数据长度不够
            continue
        df['10d'] = df['close'].rolling(10).mean()
        #df['20d'] = df['close'].rolling(20).mean()
        df['30d'] = df['close'].rolling(30).mean()
        df['60d'] = df['close'].rolling(60).mean()

        df = df.loc[:,['date','close','10d','30d','60d']]
        df = df.sort_values(by=['date'], ascending=False)
        #print(df.head())

        # 10日线高于30日线，30日线高于60日线
        # 长均线已转向上趋势
        if df.iat[0,1] > df.iat[0,2] and df.iat[0,2] > df.iat[0,3] and df.iat[0,3] > df.iat[0,4]:
            if df.iat[0,4] > df.iat[3,4]:
                #if df.iat[5,4] > df.iat[5,3] :
                    r.append(code)

    return r


def use_ma(dss):
    dates = get_trading_dates(dss)
    preday = dates[-2]
    today = dates[-1]
    print(today)
    pfFile = dss + 'csv/hold.csv'
    p1 = Book(dss, pfFile,preday, today)

    codes = []
    for tactic in p1.hold_TacticList:
        if tactic.tacticName == 'boll':
            for hold in tactic.hold_Array:
                code = hold[0]
                codes.append(code)

    codes += hit_ma(dss,today)

    r = []
    symbols = set(codes)
    print(symbols)
    for vtSymbol in symbols:
        df = ts.get_realtime_quotes(vtSymbol)
        name = df.at[0,'name']
        r.append([vtSymbol,1,0.01,'0.00003',0,0.01,name])
    df = pd.DataFrame(r, columns=['vtSymbol','size','priceTick','variableCommission','fixedCommission','slippage','name'])
    filename = dss + 'csv/setting_' + today + '.csv'
    #df.to_csv(filename, index=False, encoding='gbk')
    df.to_csv(filename, index=False)

    return r

if __name__ == '__main__':
    #use_ma('../../../data/')
    pass
