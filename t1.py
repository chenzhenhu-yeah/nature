import numpy as np
import pandas as pd

import smtplib
from email.mime.text import MIMEText

import os
import re
import datetime
import time

from nature import get_dss, get_trading_dates, get_daily, get_stk_hfq

import json
import tushare as ts






dict = {'Name': 'Runoob', 'Age': 7}
dict2 = {'Name': 'female' }

dict.update(dict2)
print ("更新字典 dict : ", dict)


#
# filename = get_dss() + 'fut/cfg/portfolio_var_' + 'atrrsi' + '.csv'
# df = pd.read_csv(filename, sep='$')
# df = df.sort_values(by='datetime',ascending=False)
# print(df)
#
# if len(df) > 0:
#     rec = df.iloc[-1,:]
#     print(rec)
#     p = rec.portfolioValue
#     d = eval(rec.posDict)
#     print(d)
#     print(type(d))

# dt = '20191011'
# r = [ [dt, 100, {'c2001':1, 'ag1912':2}] ]
# df = pd.DataFrame(r, columns=['datetime','portfolioValue','posDict'])
# filename = get_dss() + 'fut/cfg/portfolio_var_' + 'atrrsi' + '.csv'
# df.to_csv(filename,index=False,sep='$',mode='a',header=False)


# import traceback
# try:
#     i = 3/0
# except Exception as e:
#     print(e)
#     print('-'*30)
#     #traceback.print_exc()
#     s = traceback.format_exc()
#     print(s)












#
# # 加载配置
# config = open(get_dss()+'csv/config.json')
# setting = json.load(config)
# pro_id = setting['pro_id']              # 设置服务器
# pro = ts.pro_api(pro_id)

#
# # 获取分钟数据
# df = ts.pro_bar( api= pro, ts_code='300408.SZ', freq='15min', adj='bfq', start_date='20190901', end_date='20190913')
# #print(df.head(3))
# print(df)

#
# #df = ts.get_k_data(code='300408', start='2019-06-01', end='2019-09-11',  ktype='15', autype='hfq')
# df = ts.get_k_data(code='300408', ktype='5', autype=None)
# #print(df.head(3))
# print(df)
#
# from pyecharts.charts import Bar
# from pyecharts import options as opts
# from pyecharts.globals import ThemeType
#
# from pyecharts import options as opts
# from pyecharts.globals import ThemeType
#
# bar = Bar(init_opts=opts.InitOpts(theme=ThemeType.LIGHT))
# bar.add_xaxis(["衬衫", "羊毛衫", "雪纺衫", "裤子", "高跟鞋", "袜子"])
# bar.add_yaxis("商家A", [5, 20, 36, 10, 75, 90], itemstyle_opts=opts.ItemStyleOpts(color='blue'),)
# bar.render()

# .add_yaxis(
#             "商家A",
#             Faker.values(),
#             itemstyle_opts=opts.ItemStyleOpts(color=JsCode(color_function)),
#         )
