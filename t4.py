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
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import ctypes
import os
import platform
import sys

from nature import to_log, is_trade_day, send_email, get_dss, get_contract, is_market_date
from nature import rc_file, get_symbols_quote, get_tick, send_order


fn = '饲料.csv'
df = pd.read_csv(fn, encoding='gbk', skiprows=2, dtype='str')
df = df.iloc[:4,:]
df = df.set_index('指标')
df = df.T

cols = ['value_cur', 'value_cum', 'ratio_cur', 'ratio_cum']
df.columns = cols
for col in cols:
    df[col] = df[col].str.strip()

for dt in df.index:
    rec = df.loc[dt,:]
    print([dt, dt[:4], dt[5:-1].zfill(2)+'M', rec.value_cur, rec.value_cum, rec.ratio_cur, rec.ratio_cum])



# fn = 'ExportSalesDataByCommodity.xls'
#
# df = pd.read_excel(fn, dtype='str')
# print(df.iat[6,0])
# print(df.iat[9,0].strip()[:10])
# df = df.iloc[9:-1,:]
# df = df.dropna(how='all')                     # 该行全部元素为空时，删除该行
# # df = df.dropna(axis=1, how='all')             # 该列全部元素为空时，删除该列
# df.columns = ['Date','MarketYear','unnamed','Commodity','Country','WeeklyExports','AccumulatedExports','OutstandingSales','GrossNewSales','NetSales','TotalCommitment','NMY_OutstandingSales','NMY_NetSales','UnitDesc']
# df['Date'] = df['Date'].str.slice(0,10)
# del df['unnamed']
#
# print(df.head())
# print(df.tail())
#
# # send_email(get_dss(), '统计局数据', '', ['t1.py'])

# df = pd.DataFrame(np.random.randn(3, 3), columns=list('ABC'))
#
# # fig, ax = plt.subplots()
# # hide axes
# # fig.patch.set_visible(False)
# # ax.axis('off')
# # ax.axis('tight')
# # ax.table(cellText=df.values, colLabels=df.columns, loc='center')
# # fig.tight_layout()
#
#
# plt.subplot(2,1,1)
# plt.axis('off')
# plt.table(cellText=df.values, colLabels=df.columns, loc='center')
#
# plt.subplot(2,1,2)
# plt.axis('off')
# plt.table(cellText=df.values, colLabels=df.columns, loc='center')
#
# row_colors = ['red','gold','green']
#
# plt.figure(figsize=(15,3))
# plt.title('test')
# plt.axis('off')
# my_table = plt.table(cellText=df.values, colLabels=df.columns, colWidths=[0.3]*3,
#                      colColours=row_colors, cellColours=[row_colors,row_colors,row_colors],
#                      cellLoc='left', loc='center',
#                      bbox=[0, 0, 1, 1])
#
# my_table.set_fontsize(18)
# my_table.scale(1, 5)
# plt.show()
