import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


import smtplib
from email.mime.text import MIMEText

import os
import re
import datetime
import time
import sys
import json
import tushare as ts

from nature import to_log, is_trade_day, send_email, get_dss, get_contract, is_market_date
from nature import rc_file


dirname = get_dss() + 'fut/statement'
fn = os.path.join(dirname, 'tmp_结算单_20201016.txt')

df = pd.read_csv(fn, encoding='gbk', sep='|', skiprows=2, header=None)
df = df.drop(columns=[0,14])

dt = '2020-09-28'
fn_greeks = get_dss() + 'opt/' + dt[:7] + '_greeks.csv'
df_greeks = pd.read_csv(fn_greeks)

pz_list = []
opt_list = []
delta_list = []
gamma_list = []
vega_list = []
for i, row in df.iterrows():
    symbol = row[2].strip()
    num = row[3] - row[5]
    pz = get_contract(symbol).pz
    pz_list.append(pz)
    if get_contract(symbol).be_opt:
        df2 = df_greeks[df_greeks.Instrument == symbol]
        # df = df[df.Localtime.str.slice(0,10) == dt]
        df2 = df2.drop_duplicates(subset=['Instrument'],keep='last')
        if df2.empty:
            delta_list.append(0)
            gamma_list.append(0)
            vega_list.append(0)
        else:
            rec = df2.iloc[0,:]
            # print(rec.Instrument, num, rec.delta, rec.gamma, rec.vega)
            delta_list.append(int(100 * num * rec.delta))
            gamma_list.append(round(100 * num * rec.gamma,2))
            vega_list.append(round(num * rec.vega,2))

        opt_list.append('期权')
    else:
        opt_list.append('期货')
        delta_list.append(100*num)
        gamma_list.append(0)
        vega_list.append(0)


df['pz'] = pz_list
df['opt'] = opt_list
df['delta'] = delta_list
df['gamma'] = gamma_list
df['vega'] = vega_list
df['magin'] = df[10].apply(int)

# print(df)

df2 = df.groupby(by=['pz','opt']).agg({'magin':np.sum, 'delta':np.sum, 'gamma':np.sum, 'vega':np.sum})
# print(type(g))
fn = os.path.join(dirname, '风控_结算单_20201016.txt.csv')
df2.to_csv(fn)


# send_email(get_dss(), '结算单', '', [fn])

# print(df)
# print(opt_list)




# duo  = [1749.0, 1799.0, 1849.0, 1899.0, 1900, 1900, 1949.0, 1999.0, 2049.0, 2099.0, 2149.0, 2199.0, 2249.0, 2299.0, 2349.0, 2399.0, 2399.0, 2449.0, 2479.0, 2499.0, 2549.0, 2599.0, 2649.0]
# kong = [1749.0, 1789.0, 1829.0, 1869.0, 1909.0, 1949.0, 1989.0, 2029.0, 2069.0, 2109.0, 2149.0, 2189.0, 2229.0, 2269.0, 2309.0, 2349.0, 2389.0, 2429.0, 2469.0, 2509.0, 2549.0, 2589.0, 2741.0]
#
# # [1749.0, 1799.0, 1849.0, 1899.0, 1900, 1900, 1949.0, 1999.0, 2049.0, 2099.0, 2149.0, 2199.0, 2249.0, 2299.0, 2349.0, 2399.0, 2399.0, 2449.0, 2479.0, 2499.0, 2549.0, 2599.0, 2649.0]
# # [1600, 1600, 1680.0, 1703.0, 1727.0]
# #
# # [1749.0, 1789.0, 1829.0, 1869.0, 1909.0, 1949.0, 1989.0, 2029.0, 2069.0, 2109.0, 2149.0, 2189.0, 2229.0, 2269.0, 2309.0, 2349.0, 2389.0, 2429.0, 2469.0, 2509.0, 2549.0, 2589.0, 2741.0]
# # [1727.0, 1694.0, 1661.0, 1628.0, 1600.0]
#
# print(sum(duo))
# print(sum(kong))
#
