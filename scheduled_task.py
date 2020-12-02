#  -*- coding: utf-8 -*-

import os
import zipfile
import pandas as pd
import smtplib
from email.mime.text import MIMEText
import schedule
import time
import datetime
from multiprocessing.connection import Client
import traceback
import pdfkit

import ctypes
import platform
import sys

from nature import to_log, pandian_run, book_opt_run, get_dss, get_repo
from nature import get_trading_dates, send_email, is_market_date

from nature.engine.stk.nearboll.use_ma import use_ma
from nature import has_factor, stk_report
from nature.hu_signal.price_signal import price_signal

from nature.down_k.down_data import down_data
from nature.down_k.down_opt import down_opt
from nature.engine.fut.ctp_ht.tick2bar import tick2bar
from nature.engine.fut.risk.examine import examine
from nature.engine.fut.risk.greeks import calc_greeks
from nature.engine.fut.risk.sigma import calc_sigma
from nature.engine.fut.risk.skew import calc_skew
from nature.engine.fut.risk.arbitrage import calc_pcp, calc_die
from nature.engine.fut.risk.iv_atm import calc_iv_atm
from nature.engine.fut.risk.sdiffer import calc_sdiffer
from nature.engine.fut.risk.compass import compass

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

def mail_bak():
    try:
        now = datetime.datetime.now()
        weekday = int(now.strftime('%w'))
        if 1 <= weekday <= 5:
            dirname = os.path.join(get_dss(), 'fut')
            # 压缩文件路径
            zip_file = os.path.join(dirname, 'cfg_engine.zip')
            # 在指定zip压缩文件目录下创建zip文件
            create_zip_file = zipfile.ZipFile(zip_file, mode='w', compression=zipfile.ZIP_DEFLATED)
            for d in ['cfg', 'engine/']:
                path = os.path.join(get_dss(), 'fut/'+d)
                for folderName,subfolders,filenames in os.walk(path):
                    for file_name in filenames:
                        fn = os.path.join(folderName, file_name)
                        create_zip_file.write(fn, d+folderName[len(path):]+'/'+file_name)
            create_zip_file.close()

            # send_email(get_dss(), 'cfg_engine.zip', '', [zip_file])
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
        if 1 <= weekday <= 5 and is_market_date():
            print('\n' + str(now) + " tick2bar begin...")
            tick2bar(today)
            now = datetime.datetime.now()
            print('\n' + str(now) + " tick2bar end ")

    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

def run_iv():
    try:
        now = datetime.datetime.now()
        today = now.strftime('%Y%m%d')
        weekday = int(now.strftime('%w'))
        if 1 <= weekday <= 5 and is_market_date():
            print('\n' + str(now) + " calc_iv_atm begin...")
            calc_iv_atm()
            now = datetime.datetime.now()
            print('\n' + str(now) + " calc_iv_atm end ")

    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

def run_pandian():
    try:
        now = datetime.datetime.now()
        weekday = int(now.strftime('%w'))
        if 1 <= weekday <= 5 and is_market_date():
            print('\n' + str(now) + " pandian begin...")
            pandian_run()
            now = datetime.datetime.now()
            print('\n' + str(now) + " pandian end ")
    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

def run_sdiffer():
    try:
        now = datetime.datetime.now()
        today = now.strftime('%Y-%m-%d')
        weekday = int(now.strftime('%w'))
        if 1 <= weekday <= 5 and is_market_date():
            print('\n' + str(now) + " calc sdiffer begin...")
            calc_sdiffer(today)
            now = datetime.datetime.now()
            print('\n' + str(now) + " calc sdiffer end ")
    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

def run_book_opt():
    try:
        now = datetime.datetime.now()
        weekday = int(now.strftime('%w'))
        if 1 <= weekday <= 5 and is_market_date():
            print('\n' + str(now) + " book_opt begin...")
            book_opt_run()
            now = datetime.datetime.now()
            print('\n' + str(now) + " book_opt end ")
    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

def run_down_data():
    now = datetime.datetime.now()
    weekday = int(now.strftime('%w'))
    if 1 <= weekday <= 5 and is_market_date():
        print('\n' + str(now) + " down_data begin...")
        down_data(dss)
        now = datetime.datetime.now()
        print('\n' + str(now) + " down_data end ")


def run_down_opt():
    try:
        now = datetime.datetime.now()
        weekday = int(now.strftime('%w'))
        if 1 <= weekday <= 5 and is_market_date():
            print('\n' + str(now) + " down_opt begin...")
            down_opt()
            print(" down_opt end ")
            time.sleep(180)

            now = datetime.datetime.now()
            print('\n' + str(now) + " calc_greeks begin...")
            calc_greeks()
            print(" calc_greeks end ")

            now = datetime.datetime.now()
            print('\n' + str(now) + " calc_sigma begin...")
            calc_sigma()
            print(" calc_sigma end ")

            now = datetime.datetime.now()
            print('\n' + str(now) + " calc_skew begin...")
            calc_skew()
            print(" calc_skew end ")

    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

def run_arbitrage():
    try:
        now = datetime.datetime.now()
        weekday = int(now.strftime('%w'))
        if 1 <= weekday <= 5 and is_market_date():
            print('\n' + str(now) + " arbitrage begin...")
            if calc_pcp():
                fn = get_dss() + 'opt/pcp.csv'
                send_email(dss, 'pcp', '', [fn])
            if calc_die():
                fn = get_dss() + 'opt/die.csv'
                send_email(dss, 'die', '', [fn])
            print(" arbitrage end ")

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
    if 1 <= weekday <= 5 and is_market_date():
        # 先清空static目录下的文件
        repo = get_repo()
        dirname = repo + 'nature/web/static/'
        listfile = os.listdir(dirname)
        for fn in listfile:
            os.remove(dirname+fn)
            # print(dirname+fn)

        # 分主题发送邮件
        s_list = ['star', 'yue']
        for s in s_list:
            try:
                time.sleep(3)
                mail_pdf(s)
            except:
                continue

        # 发送品种指南
        date = now.strftime('%Y-%m-%d')
        compass(date)


def get_free_space_mb(folder):
  """
  Return folder/drive free space (in GB)
  """
  if platform.system() == 'Windows':
    free_bytes = ctypes.c_ulonglong(0)
    ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(folder), None, None, ctypes.pointer(free_bytes))
    return free_bytes.value/1024/1024/1024
  else:
    st = os.statvfs(folder)
    return st.f_bavail * st.f_frsize/1024/1024

def run_examine():
    kj = round(get_free_space_mb('C:\\'), 2)
    # print(kj,'GB')
    if kj < 6:
        send_email(dss, '预警：磁盘空间不足, 仅剩余', str(kj)+'GB')

    now = datetime.datetime.now()
    weekday = int(now.strftime('%w'))
    if 1 <= weekday <= 5 and is_market_date():
        print('\n' + str(now) + " examine begin...")
        examine()
        now = datetime.datetime.now()
        print('\n' + str(now) + " examine end ")

if __name__ == '__main__':
    # run_mail_pdf()

    try:
        '''
        schedule.every(3).seconds.do(down_data_0100)
        '''

        # 盘中
        # schedule.every().day.at("11:26").do(run_down_opt)
        # schedule.every().day.at("11:35").do(run_arbitrage)
        schedule.every().day.at("15:01").do(run_down_opt)

        # 盘后
        schedule.every().day.at("15:05").do(run_tick2bar)
        schedule.every().day.at("15:15").do(run_iv)
        schedule.every().day.at("15:20").do(run_book_opt)
        schedule.every().day.at("15:25").do(run_pandian)
        schedule.every().day.at("15:26").do(run_sdiffer)
        schedule.every().day.at("15:27").do(run_down_data)
        schedule.every().day.at("15:28").do(run_mail_pdf)
        schedule.every().day.at("15:29").do(run_examine)
        schedule.every().day.at("15:30").do(mail_log)
        schedule.every().day.at("15:31").do(mail_bak)

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
