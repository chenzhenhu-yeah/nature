#  -*- coding: utf-8 -*-
import pandas as pd
import smtplib
from email.mime.text import MIMEText
import schedule
import time
import datetime
from multiprocessing.connection import Client
import traceback

from nature import to_log, pandian_run, book_opt_run
from nature import get_trading_dates, send_email

from nature.engine.stk.nearboll.use_ma import use_ma
from nature import has_factor, stk_report
from nature.hu_signal.price_signal import price_signal

from nature.down_k.down_data import down_data
from nature.engine.fut.ctp_ht.tick2bar import tick2bar
from nature.engine.fut.risk.examine import examine
from nature.down_k.down_opt import down_opt

dss = r'../data/'

def mail_log():
    try:
        now = datetime.datetime.now()
        preday = now - datetime.timedelta(days=1)
        preday = preday.strftime('%Y-%m-%d %H:%M:%S')
        weekday = int(now.strftime('%w'))

        r = []
        logfile= dss + 'log/autotrade.log'
        #df = pd.read_csv(logfile,sep='$',header=None,encoding='ansi')
        df = pd.read_csv(logfile,sep='$',header=None,encoding='utf-8')
        df['datetime'] = df[0] + ' ' + df[1]
        df = df[df['datetime']>=preday]
        if len(df) > 0:
            df = df.sort_values('datetime', ascending=False)
            del df['datetime']
            for i, row in df.iterrows():
                s = str( list(row)[:7] )
                r.append(s)
            send_email(dss, 'show log', '\n'.join(r))

    except Exception as e:
        print(now, '-'*30)
        traceback.print_exc()

def mail_1815():
    try:
        now = datetime.datetime.now()
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
        s = traceback.format_exc()
        to_log(s)

def mail_0200():
    try:
        now = datetime.datetime.now()
        weekday = int(now.strftime('%w'))
        if 2 <= weekday <= 6:
            print('\n' + str(now) + " mail_ma begin...")
            r = use_ma(dss)
            #print(r)
            #print(type(r))
            send_email(dss, str(len(r))+' setting items ', '')
    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

def run_price_signal():
    try:
        now = datetime.datetime.now()
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
        s = traceback.format_exc()
        to_log(s)

def run_tick2bar():
    try:
        now = datetime.datetime.now()
        today = now.strftime('%Y%m%d')
        weekday = int(now.strftime('%w'))
        print(today,weekday)
        if 1 <= weekday <= 5:
            tick2bar(today)
    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

def run_pandian():
    try:
        now = datetime.datetime.now()
        weekday = int(now.strftime('%w'))
        if 1 <= weekday <= 5:
            pandian_run()
    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

def run_book_opt():
    try:
        now = datetime.datetime.now()
        weekday = int(now.strftime('%w'))
        if 1 <= weekday <= 5:
            book_opt_run()
    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

def run_down_data():
    now = datetime.datetime.now()
    weekday = int(now.strftime('%w'))
    if 1 <= weekday <= 5:
        print('\n' + str(now) + " down_data begin...")
        down_data(dss)


def run_down_opt():
    now = datetime.datetime.now()
    weekday = int(now.strftime('%w'))
    if 1 <= weekday <= 5:
        print('\n' + str(now) + " down_opt begin...")
        down_opt()

def run_examine():
    pass

if __name__ == '__main__':
    try:
        '''
        schedule.every(3).seconds.do(down_data_0100)
        '''

        # 盘中
        schedule.every().day.at("12:05").do(run_down_opt)
        schedule.every().day.at("15:03").do(run_down_opt)
        schedule.every().day.at("15:05").do(run_tick2bar)
        schedule.every().day.at("15:10").do(run_book_opt)
        schedule.every().day.at("15:15").do(run_pandian)
        schedule.every().day.at("15:18").do(mail_log)

        #盘后
        schedule.every().day.at("19:00").do(run_down_data)
        # schedule.every().day.at("03:30").do(run_examine)

        print('schedule begin...')
        while True:
            schedule.run_pending()
            time.sleep(10)
    except Exception as e:
        # now = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        # print(now)
        # print('-'*60)
        # traceback.print_exc()
        s = traceback.format_exc()
        to_log(s)
