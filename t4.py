import numpy as np
import pandas as pd

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


import pdfkit
# url页面转化为pdf
# url = 'http://114.116.190.167:5000/fut'
# fn = 'out.pdf'
# pdfkit.from_url(url, fn)


import smtplib
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# 第三方 SMTP 服务
mail_host = 'smtp.yeah.net'              # 设置服务器
mail_user = 'chenzhenhu@yeah.net'    # 用户名
mail_pass = "852299"                     # 授权密码

sender = 'chenzhenhu@yeah.net'
receivers = 'chenzhenhu@yeah.net'

content = MIMEText('test')
message = MIMEMultipart()
message.attach(content)

message['From'] = sender
message['To'] =  receivers
message['Subject'] = 's1'

# 构造附件
att = MIMEText(open('out.pdf', "rb").read(), "base64", "utf-8")
att["Content-Type"] = "application/octet-stream"
att["Content-Disposition"] = 'attachment; filename="o1.pdf")'
message.attach(att)

# xlsx = MIMEApplication(open('test.xlsx', 'rb').read())
# xlsx["Content-Type"] = 'application/octet-stream'
# xlsx.add_header('Content-Disposition', 'attachment', filename='test.xlsx')
# message.attach(xlsx)
# try:

smtpObj = smtplib.SMTP()
smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
smtpObj.login(mail_user, mail_pass)
smtpObj.sendmail(sender, receivers, message.as_string())
print("邮件发送成功")

# except  Exception as e:
    # print(e)




# # 加载配置
# config = open(get_dss()+'csv/config.json')
# setting = json.load(config)
# pro_id = setting['pro_id']
# pro = ts.pro_api(pro_id)
#
#
# df = pro.index_daily(ts_code='000300.SH', start_date='20200401', end_date='20200430')
# print(df)


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
# pz ='m'
# fn = get_dss() +  'fut/engine/dali/signal_dali_multi_var_' + pz + '.csv'
# df = pd.read_csv(fn)
# df['price'] = 0
#
# cols = ['datetime','vtSymbol', 'price', 'unit', 'pnl_net','pnl_trade','pnl_hold', 'commission','slippage','price_duo_list','price_kong_list']
#
# df = df.loc[:,cols]
# df.to_csv(fn, index=False)
