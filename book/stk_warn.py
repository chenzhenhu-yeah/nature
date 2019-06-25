import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import smtplib
from email.mime.text import MIMEText

import tushare as ts
from portfolio import Portfolio

import sys
sys.path.append(r'../')
from down_k.get_trading_dates import get_trading_dates
from down_k.get_stk import get_stk_hfq
from down_k.get_inx import get_inx
from hu_signal.hu_talib import MA

def send_email(subject, content):
    # 第三方 SMTP 服务
    mail_host = 'smtp.yeah.net'              # 设置服务器
    mail_username = 'chenzhenhu@yeah.net'   # 用户名
    mail_auth_password = "852299"       # 授权密码

    sender = 'chenzhenhu@yeah.net'
    receivers = 'chenzhenhu@yeah.net'         # 一个收件人
    #receivers = '270114497@qq.com, zhenghaishu@126.com' # 多个收件人
    try:
        message = MIMEText(content, 'plain', 'utf-8')
        message['From'] = sender
        message['To'] =  receivers
        message['Subject'] = str(subject)

        #smtpObj = smtplib.SMTP(mail_host, 25)                               # 生成smtpObj对象，使用非SSL协议端口号25
        smtpObj = smtplib.SMTP_SSL(mail_host, 465)                         # 生成smtpObj对象，使用SSL协议端口号465
        smtpObj.login(mail_username, mail_auth_password)                    # 登录邮箱
        # smtpObj.sendmail(sender, receivers, message.as_string())          # 发送给一人
        smtpObj.sendmail(sender, receivers.split(','), message.as_string()) # 发送给多人
        print ("邮件发送成功")
    except smtplib.SMTPException:
        print ("Error: 无法发送邮件")

def is_trade_time():
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    weekday = int(now.strftime('%w'))
    #print(weekday)
    if 1 <= weekday <= 5:
        t = time.localtime()
        if (t.tm_hour>9 and t.tm_hour<15) or (t.tm_hour==9 and t.tm_min>20) :
            return True
    else:
        return False

def stk_warn(dss):
    print('beging stk_warn...')
    dates = get_trading_dates(dss)
    preday = dates[-2]
    today = dates[-1]

    loaded = False
    codes = []
    mailed = []


    while True:
        if is_trade_time():
            # 读入codes
            if not loaded:
                bottleFile = dss + 'csv/bottle.csv'
                pfFile = dss + 'csv/hold.csv'
                p1 = Portfolio(dss,bottleFile, pfFile,'399905','IC1909.CFX', preday, today)

                for book in p1.hold_BookList:
                    codes += book.hold_Dict.keys()
                loaded =True

            for code in codes:
                try:
                    time.sleep(1)
                    df = ts.get_realtime_quotes(code)
                    name = df.at[0,'name']
                    price = float(df.at[0,'price'])
                    pre_close = float(df.at[0,'pre_close'])
                    if code not in mailed:
                        if price/pre_close - 1 > 0.05:
                            send_email('up_warn', str(code+' '+name))
                            mailed.append(code)
                        if price/pre_close - 1 < -0.05:
                            send_email('down_warn', str(code+' '+name))
                            mailed.append(code)
                except Exception as e:
                    print('error')
                    print(e)
        else:
            loaded = False
            codes = []
            mailed = []
            time.sleep(300)

        time.sleep(30)

if __name__ == '__main__':
    dss = '../../data/'
    stk_warn(dss)
