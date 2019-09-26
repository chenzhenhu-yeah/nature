#  -*- coding: utf-8 -*-
import pandas as pd
import smtplib
from email.mime.text import MIMEText
import schedule
import time
from datetime import datetime
from multiprocessing.connection import Client
import traceback

from nature import to_log
from nature import get_trading_dates, send_email
from nature.down_k.down_data import down_data
from nature.engine.stk.nearboll.use_ma import use_ma
from nature import has_factor, stk_report
from nature.hu_signal.price_signal import price_signal
from nature.engine.fut.ctp_ht.tick2bar import tick2bar

dss = r'../data/'

def mail_1515():
    try:
        now = datetime.now()
        today = now.strftime('%Y-%m-%d')
        weekday = int(now.strftime('%w'))
        if 1 <= weekday <= 5:
            r = []
            logfile= dss + 'log/autotrade.log'
            df = pd.read_csv(logfile,sep=' ',header=None,encoding='ansi')
            df = df[df[0]==today]
            for i, row in df.iterrows():
                r.append(str(list(row)[:7]))
            send_email(dss, 'show log', '\n'.join(r))

            r = []
            txtfile = dss + 'csv/ins.txt'
            with open(txtfile, 'r', encoding='utf-8') as f:
                line = f.readline()
                while line:
                    if line[0] == '{':
                        #line_dict = eval(line)
                        r.append(line)
                    line = f.readline()
            send_email(dss, 'show ins ', '\n'.join(r))

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
            if r == []:
                send_email(dss, 'no factor', '')
            else:
                send_email(dss, 'has factor', '\n'.join(r))

            print('\n' + str(now) + " mail_stk_report begin...")
            r = stk_report(dss)
            send_email(dss, 'show stk_report', '\n'.join(r))
    except Exception as e:
        print('error')
        print(e)

def down_data_0100():

    now = datetime.now()
    weekday = int(now.strftime('%w'))
    if 2 <= weekday <= 6:
        print('\n' + str(now) + " down_data begin...")
        down_data(dss)

def mail_0200():
    try:
        now = datetime.now()
        weekday = int(now.strftime('%w'))
        if 2 <= weekday <= 6:
            print('\n' + str(now) + " mail_ma begin...")
            r = use_ma(dss)
            #print(r)
            #print(type(r))
            send_email(dss, str(len(r))+' setting items ', '')
    except Exception as e:
        print('error')
        print(e)

def run_price_signal():
    try:
        now = datetime.now()
        weekday = int(now.strftime('%w'))
        if 2 <= weekday <= 6:
            dates = get_trading_dates(dss)
            today = dates[-1]
            arr = price_signal(dss,today)
            r = []
            for a in arr:
                r.append(str(a))
            #print(str(r))
            send_email(dss, 'price_signal', '\n'.join(r))

    except Exception as e:
        print('error')
        print(e)

def run_tick2bar():
    try:
        now = datetime.now()
        today = now.strftime('%Y%m%d')
        weekday = int(now.strftime('%w'))
        print(today,weekday)
        if 1 <= weekday <= 5:
            tick2bar(today)

    except Exception as e:
        print('error')
        print(e)

if __name__ == '__main__':
    try:
        '''
        schedule.every(3).seconds.do(down_data_0100)
        '''

        # 盘中
        schedule.every().day.at("15:30").do(run_tick2bar)
        schedule.every().day.at("15:45").do(mail_1515)

        #盘后
        #schedule.every().day.at("00:15").do(mail_1815)
        schedule.every().day.at("01:00").do(down_data_0100)
        #schedule.every().day.at("02:00").do(mail_0200)
        schedule.every().day.at("02:30").do(run_price_signal)

        print('schedule begin...')
        while True:
            schedule.run_pending()
            time.sleep(10)
    except Exception as e:
        now = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        print(now)
        print('-'*60)
        traceback.print_exc()

        s = traceback.format_exc()
        to_log(s)
