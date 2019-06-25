#  -*- coding: utf-8 -*-
import pandas as pd
import smtplib
from email.mime.text import MIMEText
import schedule
import time
from datetime import datetime
from multiprocessing.connection import Client

from down_k.get_trading_dates import get_trading_dates
from down_k.down_inx import down_inx_all
from down_k.down_daily import down_daily_run
from down_k.down_fut import down_fut_all
from down_k.down_stk_hfq import down_stk_hfq_all
from down_k.down_stk_bfq import down_stk_bfq_all
from hu_signal.macd_signal import signal_run
from strategy.strategy_run import strategy_run
from strategy.nearboll.use_ma import use_ma
from book.portfolio import daily_report
from book.portfolio import book_signal
from book.portfolio import has_factor
from book.portfolio import stk_report

dss = r'../data/'

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
    except smtplib.SMTPException as e:
        print ("Error: 无法发送邮件")
        print(e)

def mail_log():
    try:
        now = datetime.now()
        today = now.strftime('%Y-%m-%d')
        weekday = int(now.strftime('%w'))
        if 0 <= weekday <= 6:
            logfile= r'auto_trade\log\autotrade.log'
            df = pd.read_csv(logfile,sep=' ',header=None,encoding='ansi')
            df = df[df[0]==today]
            #print('here')
            r = []
            for i, row in df.iterrows():
                r.append(str(list(row)))
            send_email('show log', '\n'.join(r))

            r = []
            txtfile = 'auto_trade/ini/ins.txt'
            with open(txtfile, 'r', encoding='utf-8') as f:
                line = f.readline()
                while line:
                    if line[0] == '{':
                        #line_dict = eval(line)
                        r.append(line)
                    line = f.readline()
            send_email('show ins in mail ', '\n'.join(r))

    except Exception as e:
        print('error')
        print(e)

def down_inx():
    try:
        now = datetime.now()
        weekday = int(now.strftime('%w'))
        if 0 <= weekday <= 6:
            print('\n' + str(now) + " down_inx begin...")
            down_inx_all(dss)
    except Exception as e:
        print('error')
        print(e)

def down_daily():
    try:
        now = datetime.now()
        weekday = int(now.strftime('%w'))
        if 0 <= weekday <= 6:
            print('\n' + str(now) + " down_daily begin...")
            down_daily_run(dss)
    except Exception as e:
        print('error')
        print(e)

def down_fut():
    try:
        now = datetime.now()
        weekday = int(now.strftime('%w'))
        if 0 <= weekday <= 6:
            print('\n' + str(now) + " down_fut begin...")
            down_fut_all(dss)
    except Exception as e:
        print('error')
        print(e)

def mail_daily_result():
    try:
        now = datetime.now()
        weekday = int(now.strftime('%w'))
        if 1 <= weekday <= 5:
            print('\n' + str(now) + " mail_daily_result begin...")
            daily_report(dss)
            print('\n' + str(now) + " process daily result success.")

            # 发邮件
            dates = get_trading_dates(dss)
            today = dates[-1]

            df = pd.read_csv(dss+'csv/daily_result.csv',dtype='str')
            df = df[df.date==today]
            r = []
            for i, row in df.iterrows():
                r.append(str(list(row)))
            send_email('daily_result', '\n'.join(r))

    except Exception as e:
        print('error')
        print(e)

def mail_factor():
    try:
        now = datetime.now()
        weekday = int(now.strftime('%w'))
        if 1 <= weekday <= 5:
            print('\n' + str(now) + " mail_factor begin...")
            r = has_factor(dss)

            if r != []:               # 发邮件
                send_email('has_factor', '\n'.join(r))
    except Exception as e:
        print('error')
        print(e)

def mail_ma():
    try:
        now = datetime.now()
        weekday = int(now.strftime('%w'))
        if 0 <= weekday <= 6:
            print('\n' + str(now) + " mail_ma begin...")
            r = use_ma(dss)

            if r != []:               # 发邮件
                send_email('setting.csv', '\n'.join(r))
    except Exception as e:
        print('error')
        print(e)

def mail_stk_report():
    try:
        now = datetime.now()
        weekday = int(now.strftime('%w'))
        if 1 <= weekday <= 5:
            print('\n' + str(now) + " mail_stk_report begin...")
            r = stk_report(dss)
            send_email('stk_report', '\n'.join(r))
    except Exception as e:
        print('error')
        print(e)


def down_stk_hfq():
    try:
        now = datetime.now()
        weekday = int(now.strftime('%w'))
        if 0 <= weekday <= 6:
            print('\n' + str(now) + " down_stk_hfq begin...")
            down_stk_hfq_all(dss)
    except Exception as e:
        print('error')
        print(e)

def down_stk_bfq():
    try:
        now = datetime.now()
        weekday = int(now.strftime('%w'))
        if 0 <= weekday <= 6:
            print('\n' + str(now) + " down_stk_bfq begin...")
            down_stk_bfq_all(dss)
    except Exception as e:
        print('error')
        print(e)

def strategy():
    try:
        now = datetime.now()
        weekday = int(now.strftime('%w'))
        if 0 <= weekday <= 6:
            strategy_run(dss)
            signal_run(dss)
    except Exception as e:
        print('error')
        print(e)

def calc_signal():
    try:
        now = datetime.now()
        weekday = int(now.strftime('%w'))
        if 0 <= weekday <= 6:
            print('here begin')
            book_signal(dss)
            print('here end')
    except Exception as e:
        print('error')
        print(e)


def mail_signal():
    try:
        now = datetime.now()
        weekday = int(now.strftime('%w'))
        if 0 <= weekday <= 6:
            dates = get_trading_dates(dss)
            today = dates[-1]

            df = pd.read_csv(dss+'csv/signal_macd.csv',dtype='str')
            df = df[df.date==today]
            r = []
            for i, row in df.iterrows():
                r.append(str(list(row)))
            send_email('show signal_macd', '\n'.join(r))

    except Exception as e:
        print('error')
        print(e)

if __name__ == '__main__':
    '''
    schedule.every(3).seconds.do(mail_signal)
    '''
    # 盘中
    #schedule.every().day.at("09:35").do(mail_log)
    schedule.every().day.at("15:15").do(mail_log)

    #盘后
    schedule.every().day.at("18:00").do(down_inx)
    schedule.every().day.at("18:05").do(down_fut)
    #schedule.every().day.at("18:10").do(mail_daily_result)
    schedule.every().day.at("18:15").do(mail_factor)
    schedule.every().day.at("18:20").do(mail_ma)
    schedule.every().day.at("18:30").do(down_stk_bfq)
    schedule.every().day.at("19:00").do(mail_stk_report)

    schedule.every().day.at("01:05").do(down_daily)
    schedule.every().day.at("01:15").do(down_stk_hfq)
    #schedule.every().day.at("02:00").do(strategy)
    schedule.every().day.at("02:20").do(calc_signal)
    schedule.every().day.at("02:30").do(mail_signal)

    print('schedule begin...')
    while True:
        schedule.run_pending()
        time.sleep(10)
