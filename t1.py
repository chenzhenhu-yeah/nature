import numpy as np
import pandas as pd

import smtplib
from email.mime.text import MIMEText

import os
import re
import datetime
import time

import json
import tushare as ts


from nature import get_dss, get_trading_dates, get_daily, get_stk_hfq
from nature import VtBarData, ArrayManager

r = []
startDt = '2019-11-21 21:00:00'
minx = 'min1'
vtSymbol = 'm1901'
initBars = 90

# 直接读取signal对应minx相关的文件。
fname = get_dss() + 'fut/bar/' + minx + '_' + vtSymbol + '.csv'
#print(fname)
df = pd.read_csv(fname)
#df['datetime'] = df['date'] + ' ' + df['time']
print(df.head(3))
print( len(df) )

gap = 45
price_deal = 2760
r =[]

for i, row in df.iterrows():
    d = dict(row)
    #print(d)
    # print(type(d))
    # bar = VtBarData()
    # bar.__dict__ = d
    # #print(bar.__dict__)
    if d['close'] >= price_deal+gap :
        price_deal = d['close']
        r.append(1)
    elif d['close'] <= price_deal-gap :
        price_deal = d['close']
        r.append(-1)

D=pd.Series(r)
d = D.cumsum()
#print(d)
c = {'r':r, 'D':list(D), 'd':list(d)}
df = pd.DataFrame(c)
df.to_csv('a1.csv', index=False)

#
# am = ArrayManager(initBars)        # K线容器
# for bar in r:
#     am.updateBar(bar)
#
# rsiValue = am.rsi(5, array=True)
# #rsiArray50 = am.rsi(10, array=True)
# rsiMa  = rsiValue[-30:].mean()
#
# #print(am.close)
# print(rsiValue)
# print(rsiMa)
