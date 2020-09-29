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
#
# fn = get_dss() + 'opt/straddle_differ1.csv'
# df = pd.read_csv(fn)
# print(df)
#
# for i, row in df.iterrows():
#     # row.stat = 'n'
#     df.at[i, 'stat'] = 'n'


fn = get_dss() + 'opt/skew.csv'
df = pd.read_csv(fn)
del df['skew_mean_c']
del df['skew_mean_p']
df['skew_mean'] = ''
print(df.head())
df.to_csv(fn, index=False)

# print(df.describe())
# # df['diff'].plot(kind='hist')
# plt.plot(df['differ'])
# plt.show()

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
