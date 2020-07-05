#  -*- coding: utf-8 -*-

import os
import pandas as pd
import smtplib
from email.mime.text import MIMEText
import schedule
import time
import datetime
from multiprocessing.connection import Client
import traceback
import pdfkit

from nature import to_log, pandian_run, book_opt_run, get_dss, get_repo
from nature import get_trading_dates, send_email

from nature.engine.stk.nearboll.use_ma import use_ma
from nature import has_factor, stk_report
from nature.hu_signal.price_signal import price_signal

from nature.down_k.down_data import down_data
from nature.down_k.down_opt import down_opt
from nature.engine.fut.ctp_ht.tick2bar import tick2bar
from nature.engine.fut.risk.examine import examine
from nature.engine.fut.risk.greeks import calc_greeks
from nature.engine.fut.risk.sigma import calc_sigma
from nature.engine.fut.risk.arbitrage import calc_pcp, calc_die


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
    try:
        now = datetime.datetime.now()
        weekday = int(now.strftime('%w'))
        if 1 <= weekday <= 5:
            print('\n' + str(now) + " down_opt begin...")
            down_opt()
            time.sleep(180)
            print('\n' + str(now) + " calc_greeks begin...")
            calc_greeks()
            print('\n' + str(now) + " calc_sigma begin...")
            calc_sigma()

    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

def run_arbitrage():
    try:
        now = datetime.datetime.now()
        weekday = int(now.strftime('%w'))
        if 1 <= weekday <= 5:
            print('\n' + str(now) + " arbitrage begin...")
            if calc_pcp():
                fn = get_dss() + 'opt/pcp.csv'
                send_email(dss, 'pcp', '', [fn])
            if calc_die():
                fn = get_dss() + 'opt/die.csv'
                send_email(dss, 'die', '', [fn])

    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

def mail_pdf(s):
    try:
        # 将url页面转化为pdf
        url = 'http://114.116.190.167:5000/show_' + s
        fn = 'web/static/out3.pdf'
        pdfkit.from_url(url, fn)
        send_email(dss, url[33:], '', [fn])

    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

def run_mail_pdf():
    now = datetime.datetime.now()
    weekday = int(now.strftime('%w'))
    if 1 <= weekday <= 5:
        # 先清空static目录下的文件
        repo = get_repo()
        dirname = repo + 'nature/web/static/'
        listfile = os.listdir(dirname)
        for fn in listfile:
            os.remove(dirname+fn)
            # print(dirname+fn)

        # 分主题发送邮件
        s_list = ['dali', 'yue', 'vol', 'smile', 'star']
        for s in s_list:
            try:
                time.sleep(3)
                mail_pdf(s)
            except:
                continue

def run_examine():
    pass

if __name__ == '__main__':
    # run_mail_pdf()

    try:
        '''
        schedule.every(3).seconds.do(down_data_0100)
        '''

        # 盘中
        schedule.every().day.at("11:26").do(run_down_opt)
        schedule.every().day.at("11:35").do(run_arbitrage)
        schedule.every().day.at("14:56").do(run_down_opt)


        盘后
        schedule.every().day.at("15:15").do(run_tick2bar)
        schedule.every().day.at("15:20").do(run_book_opt)
        schedule.every().day.at("15:25").do(run_pandian)
        schedule.every().day.at("15:27").do(run_down_data)
        schedule.every().day.at("15:28").do(run_mail_pdf)
        schedule.every().day.at("15:30").do(mail_log)

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
