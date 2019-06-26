#  -*- coding: utf-8 -*-
import pandas as pd
import smtplib
from email.mime.text import MIMEText
import schedule
import time
from datetime import datetime
from multiprocessing.connection import Client


from nature import get_trading_dates, send_email
from nature.down_k.down_data import down_data
from nature.engine.nearboll.use_ma import use_ma
from nature.book import has_factor
from nature.book import stk_report

dss = r'../data/'

def mail_1515():
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

def mail_1815():
    try:
        now = datetime.now()
        weekday = int(now.strftime('%w'))
        if 1 <= weekday <= 5:
            print('\n' + str(now) + " mail_factor begin...")
            r = has_factor(dss)
            if r != []:
                send_email('has_factor', '\n'.join(r))

            print('\n' + str(now) + " mail_stk_report begin...")
            r = stk_report(dss)
            send_email('stk_report', '\n'.join(r))
    except Exception as e:
        print('error')
        print(e)

def mail_0200():
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

def down_data_0100():
    try:
        now = datetime.now()
        weekday = int(now.strftime('%w'))
        if 0 <= weekday <= 6:
            print('\n' + str(now) + " down_inx begin...")
            down_data(dss)
    except Exception as e:
        print('error')
        print(e)

if __name__ == '__main__':
    '''
    schedule.every(3).seconds.do(mail_signal)
    '''
    # 盘中
    schedule.every().day.at("15:15").do(mail_1515)

    #盘后
    schedule.every().day.at("18:15").do(mail_1815)
    schedule.every().day.at("01:00").do(down_data_0100)
    schedule.every().day.at("02:00").do(mail_0200)

    print('schedule begin...')
    while True:
        schedule.run_pending()
        time.sleep(10)
