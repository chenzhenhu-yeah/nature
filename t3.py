import numpy as np
import pandas as pd
import talib

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
startDt = '2020-01-23 21:00:00'
minx = 'min15'
vtSymbol = 'rb2005'
initBars = 100

# 直接读取signal对应minx相关的文件。
fname = get_dss() + 'fut/bar/' + minx + '_' + vtSymbol + '.csv'
#print(fname)
df = pd.read_csv(fname)
df['datetime'] = df['date'] + ' ' + df['time']
df = df[df.datetime < startDt]
assert len(df) >= initBars

df = df.sort_values(by=['date','time'])
df = df.iloc[-initBars:]
# print(df)

for i, row in df.iterrows():
    d = dict(row)
    #print(d)
    # print(type(d))
    bar = VtBarData()
    bar.__dict__ = d
    #print(bar.__dict__)
    r.append(bar)


am = ArrayManager(initBars,200)        # K线容器
for bar in r:
    am.updateBar(bar)

# boll_up, boll_down = am.boll(20, 3.1, array=True)
# print(boll_up)
# print(boll_down)
#
# ma = am.sma(10, array=True)
# kama = am.kama(10, array=True)
# # print(ma)
# print(kama)
#
# s = kama[-10:].std(ddof=0)
# print(s)

#
#
# print(help(talib.SMA))
#
# print(help(talib.LINEARREG))

import statsmodels.api as sm
#print(dir(sm.OLS))
#print(help(sm.OLS.__init__))
print(help(sm.OLS))
