
import warnings
warnings.filterwarnings("ignore")

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
import zipfile

from nature import to_log, is_trade_day, send_email, get_dss, get_contract, is_market_date
from nature import rc_file, get_symbols_quote, get_tick, send_order


def fit_df(df, type):
    df.columns = ['date', 'symbol', 'name', 'value', 'change']
    for col in df.columns:
        df[col] = df[col].str.strip()
    # df = df.replace(',', '')
    df = df.replace('', np.nan)
    df['name'] = df['name'].replace('-', np.nan)
    df = df.dropna()                                           # 该行有元素为空时，删除该行
    # print(df)
    if len(df) > 0:
        date = df.iat[0,0]
        if date.find('-') == -1:
            date = date[:4] + '-' + date[4:6] + '-' + date[6:]
        df['date'] = date
        df['symbol'] = df['symbol'].str.strip()
        df['name'] = df['name'].str.strip()
        df['value'] = df['value'].str.replace(',', '')
        df['value'] = df['value'].astype('int')
        df['change'] = df['change'].str.replace(',', '')
        df['change'] = df['change'].astype('int')
        rec = [date, df.iat[0,1], '总计', df.value.sum() , df.change.sum()]
        # print(rec)

        df1 = pd.DataFrame([rec], columns=['date', 'symbol', 'name', 'value', 'change'])
        df = pd.concat([df,df1])
        df = df.sort_values('value', ascending=False)
        df.insert(0, 'seq', range(int(len(df))) )
        df.insert(0, 'type', type)
        # print(df.tail())
        df = df.loc[:,['date', 'symbol', 'type', 'seq', 'name', 'value', 'change']]
        # print(df)

    return df


# fn = '20201203_DCE_DPL.zip'
# z = zipfile.ZipFile(fn, 'r') # 这里的第二个参数用r表示是读取zip文件，w或a是创建一个zip文件
#
# for f_name in z.namelist():            #z.namelist() 会返回压缩包内所有文件名的列表。
#     # print(f_name)
#     data = z.read(f_name)
#     with open('duoduo.txt','wb') as f:
#         f.write(data)
#         # for d in data:
#         #     f.write(d)
#     break
#
# z.close()

df = pd.read_csv('duoduo.txt', sep='\t', dtype='str')
df = df.dropna(how='all')                     # 该行全部元素为空时，删除该行
df = df.dropna(axis=1, how='all')             # 该列全部元素为空时，删除该列

# print(df)
# print(df.head())

checker = df.iat[0,0].strip()
symbol = checker[5:].strip()
date = df.iat[0,2].strip()[5:].strip()
checker = checker[:4]
# print(checker)
# print(symbol)
# print(date)

assert checker == '合约代码'

df_r = None

begin = 0
end = 0
t = 0
type_list = ['deal', 'long', 'short']

for i, row in df.iterrows():
    if str(row[0]).strip() == '名次':
        begin = i

    if str(row[0]).strip() == '总计':
        if begin > 0:
            end = i

    if end > begin:

        df0 = df.loc[begin+1:end-1, :]
        df0.insert(0,'symbol',symbol)
        df0.insert(0,'date',date)

        df1 = df0.iloc[:,[0,1,4,5,7]]
        df1 = fit_df( df1, type_list[t])
        t += 1
        # print(df1)

        if df_r is None:
            df_r = df1
        else:
            df_r = pd.concat([df_r,df1], sort=False)

        begin = end


print(df_r)
# print(df_r.head())
# print(df_r.tail())
#

# symbol_set = set(df['合约系列'])
# # print(symbol_set)
# for symbol in symbol_set:
#     df0 = df[df['合约系列'] == symbol]
#     df1 = fit_df( df0.iloc[:,[0,1,3,4,5]], 'deal' )
#     df2 = fit_df( df0.iloc[:,[0,1,6,7,8]], 'long' )
#     df3 = fit_df( df0.iloc[:,[0,1,9,10,11]], 'short' )

# print(df.head())
# print(df3.tail())
# print(df3.columns)

# dt = df.iat[0,0][3:].strip()
# indicator = df.iat[1,0].strip()
#
# if indicator == '工业主要产品产量及增长速度':
#     df = df.iloc[4:-1,:]
#     cols = ['product', 'value_cur', 'value_cum', 'ratio_cur', 'ratio_cum']
#     df.columns = cols
# elif indicator == '能源产品产量':
#     df = df.iloc[3:-1,:]
#     cols = ['product', 'value_cur', 'value_cum', 'ratio_cur', 'ratio_cum']
#     df.columns = cols
# elif indicator == '全社会客货运输量':
#     df = df.iloc[3:-1,:]
#     cols = ['product', 'value_cur', 'ratio_cur', 'value_cum', 'ratio_cum']
#     df.columns = cols
#     df = df.loc[:, ['product', 'value_cur', 'value_cum', 'ratio_cur', 'ratio_cum']]
# else:
#     raise ValueError



#
# fn = '饲料.csv'
# df = pd.read_csv(fn, encoding='gbk', skiprows=2, dtype='str')
# df = df.iloc[:4,:]
# df = df.set_index('指标')
# df = df.T
#
# cols = ['value_cur', 'value_cum', 'ratio_cur', 'ratio_cum']
# df.columns = cols
# for col in cols:
#     df[col] = df[col].str.strip()
#
# for dt in df.index:
#     rec = df.loc[dt,:]
#     print([dt, dt[:4], dt[5:-1].zfill(2)+'M', rec.value_cur, rec.value_cum, rec.ratio_cur, rec.ratio_cum])



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
