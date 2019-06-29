import time
import os
import tushare as ts
import smtplib
from email.mime.text import MIMEText
from multiprocessing.connection import Client
import multiprocessing
from datetime import datetime

from nature import to_log, is_trade_time, is_price_time, send_email
from nature import send_instruction, rc_file, a_file
from nature import Book

dss = '../../data/'

def deal_single_ins(item):
    r = False
    if item['ins'] == 'down_sell':
        df = ts.get_realtime_quotes(item['code'])
        b3_price = float(df.at[0,'b3_p'])
        if b3_price <= item['price'] and b3_price > 0:
            to_log('reach price by down_sell ins ')
            item['ins'] = 'sell_order'
            send_instruction(item)
            r = True
    if item['ins'] == 'up_warn':
        df = ts.get_realtime_quotes(item['code'])
        b3_price = float(df.at[0,'b3_p'])
        if b3_price >= item['price']:
            to_log('reach price by up_warn ins ')
            send_email(dss, 'up_warn', str(item))
            r = True
    if item['ins'] == 'down_warn':
        df = ts.get_realtime_quotes(item['code'])
        b3_price = float(df.at[0,'b3_p'])
        if b3_price <= item['price'] and b3_price > 0 :
            to_log('reach price by down_warn ins ')
            send_email(dss, 'down_warn', str(item))
            r = True
    if item['ins'] in ['sell_order','buy_order']:
        to_log('send sell_order or buy_order ins ')
        send_instruction(item)
        r = True

def stare_ins():
    print('stare_ins begin... ')
    item_list = []
    del_list = []
    filename = 'csv/ins.txt'
    while True:
        try:
            time.sleep(3)
            if is_trade_time():
                # 读入ins文件后清空
                new_ins = rc_file(filename)

                # 分离 'del' 指令与其它指令，将其它指令加入到列表中
                c = []
                for i,item in enumerate(new_ins):
                    if item['ins'] == 'del':
                        del_list.append(item)
                    else:
                        c.append(item)
                item_list += c

                # 处理 'del' 指令
                if del_list != []:
                    d = del_list.pop()
                    for i,item in enumerate(item_list):
                        if item['code'] == d['code'] and item['num'] == d['num'] and item['price'] == d['price']:
                            item_list.pop(i)
                            break
                # 处理其他指令
                for i,item in enumerate(item_list):
                    if deal_single_ins(item):
                        item_list.pop(i)
                        break
            else:
                time.sleep(300)
                if item_list != []:
                    # 将未触发的指令保存到文件中
                    for i,item in enumerate(item_list):
                        a_file(filename, str(item))
                    item_list = []
        except Exception as e:
            print("error: ")
            print(e)

def stare_hold():
    print('stare_hold begin... ')
    loaded = False
    codes = ()
    mailed = []

    while True:
        time.sleep(30)
        if is_price_time():
            # 读入codes
            if not loaded:
                b1 = Book(dss)
                codes = b1.get_codes()
                loaded =True

            for code in codes:
                try:
                    time.sleep(1)
                    df = ts.get_realtime_quotes(code)
                    name = df.at[0,'name']
                    price = float(df.at[0,'price'])
                    pre_close = float(df.at[0,'pre_close'])
                    #print(code, price, pre_close)
                    if code not in mailed:
                        if price/pre_close - 1 > 0.05:
                            send_email(dss, 'up_warn', str(code+' '+name))
                            #print('up_warn', str(code+' '+name))
                            mailed.append(code)
                        if price/pre_close - 1 < -0.05:
                            send_email(dss, 'down_warn', str(code+' '+name))
                            #print('down_warn', str(code+' '+name))
                            mailed.append(code)
                except Exception as e:
                    print('error')
                    print(e)
        else:
            loaded = False
            codes = ()
            mailed = []
            time.sleep(300)

if __name__ == "__main__":
    print('stare begin... \n')

    p1 = multiprocessing.Process(target=stare_ins, args=())
    p1.start()
    time.sleep(1)

    p2 = multiprocessing.Process(target=stare_hold, args=())
    p2.start()

    p1.join()
    p2.join()
