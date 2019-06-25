import pandas as pd
import numpy as np
from datetime import datetime, timedelta

import sys
sys.path.append(r'../')
from down_k.get_trading_dates import get_trading_dates
from down_k.get_stk import get_stk_hfq
from down_k.get_inx import get_inx
from hu_signal.hu_talib import MA

df = get_inx(r'../../data/','399905')
print(df.head())
df['ret'] = np.log(df.close).diff()
df = df.dropna()
m = np.mean(df['ret'])
std = np.std(df['ret'])
df['ret_norm'] = (df['ret'] - m) / std
#
# fig = plt.figure(figsiz=(18,9))
# v = df['ret'].values
# x = np.linspace(avgRet - 3*stdRet, avgRet+3*stdRet, 100)
# y =
#
#
# plt.subp

import matplotlib.pyplot as plt

# plt.hist(df['ret'], bins=100)
plt.hist(df['ret_norm'], bins=100)



#plt.xlabel('Latitude Value')
plt.show()
