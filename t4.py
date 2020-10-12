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


vtSymbol = 'RM101'

#----------------------------------------------------------------------

pz = str(get_contract(vtSymbol).pz)
filename = get_dss() +  'fut/engine/dali/signal_dali_multi_var_' + pz + '.csv'
if os.path.exists(filename):
    df = pd.read_csv(filename)
    df = df[df.vtSymbol == vtSymbol]
    df = df.sort_values(by='datetime')
    df = df.reset_index()
    if len(df) > 0:
        rec = df.iloc[-1,:]            # 取最近日期的记录
        price_duo_list = eval( rec.price_duo_list )
        price_kong_list = eval( rec.price_kong_list )
        size_duo_list = eval( rec.size_duo_list )
        size_kong_list = eval( rec.size_kong_list )
        # print(price_duo_list)
        # print(price_kong_list)
        # print(size_duo_list)
        # print(size_kong_list)

#----------------------------------------------------------------------
price_duo_list  = sorted( price_duo_list )
price_kong_list = sorted( price_kong_list, reverse=True )

pnl_trade = 0
commission = 0
slippage = 0
pz = str(get_contract(vtSymbol).pz)
filename = get_dss() + 'fut/engine/dali/signal_dali_multi_deal_' + pz + '.csv'
if os.path.exists(filename):
    df = pd.read_csv(filename)
    pnl_trade = df.pnl.sum()
    commission = df.commission.sum()
    slippage = df.slippage.sum()

settle = 2530
pnl_hold = 0
ct = get_contract(vtSymbol)
size = ct.size

d_list = [2,2,2,2,2] + size_duo_list
k_list = [2,2,2,2,2] + size_kong_list
print(d_list)
print(k_list)

for d, item in zip( d_list, sorted(price_duo_list,reverse=True) ):
    pnl_hold += (settle - item) * d * size

for k, item in zip( k_list, sorted(price_kong_list) ):
    pnl_hold += (item - settle) * k * size
    print(k, pnl_hold)


print(pnl_hold)

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
