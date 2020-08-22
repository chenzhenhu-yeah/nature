import numpy as np
import pandas as pd

import smtplib
from email.mime.text import MIMEText
import traceback

import os
import re
from datetime import datetime
import time
from csv import DictReader
from nature import get_dss, get_trading_dates, get_daily, get_stk_hfq, to_log, get_contract

import json
import tushare as ts

def pandian_dali(today):
    # 读取value_dali文件
    fn_value = get_dss() +  'fut/engine/star/value_dali.csv'
    if os.path.exists(fn_value):
        df_value = pd.read_csv(fn_value)
    else:
        return

    fn_param = get_dss() +  'fut/engine/dali/signal_dali_param.csv'
    df_param = pd.read_csv(fn_param)
    # df_param = df_param.set_index('pz')
    pz_list = list(df_param.pz)
    print(pz_list)

    r = []
    for pz in pz_list:
        # 更新该品种的最新数据
        fn_signal = get_dss() +  'fut/engine/dali/signal_dali_' +'multi'+ '_var_' + pz + '.csv'
        if os.path.exists(fn_signal):
            df = pd.read_csv(fn_signal)
            rec = df.iloc[-1,:]
            # print(rec)

            cur_value = int(rec.pnl_net)
            net_pos = rec.unit
            multi_duo_list = eval(rec.price_duo_list)
            multi_kong_list = eval(rec.price_kong_list)
            r.append( [today,pz,cur_value,net_pos,multi_duo_list,multi_kong_list] )

    df = pd.DataFrame(r)
    df.to_csv(fn_value, index=False, mode='a', header=None)

def fresh_p(today, fn_p):
    df_p = pd.read_csv(fn_p)
    if len(df_p) > 0:
        rec = df_p.iloc[-1,:]
        pos_dict = eval(rec.posDict)
        close_dict = eval(rec.closeDict)
        value = rec.portfolioValue
        net_pnl = 0
        item_list = list( pos_dict.keys() )
        book_list = [x for x in item_list if x.startswith('book')]
        fut_list =  [x for x in item_list if x not in book_list]

        # 处理期权项
        for book in book_list:
            fn = get_dss() + 'fut/engine/opt/' + book + '.csv'
            df = pd.read_csv(fn)
            row = df.iloc[-1,:]
            net_pnl += row.netPnl

        # 处理期货项
        for fut in fut_list:
            fn = get_dss() + 'fut/bar/day_' + fut + '.csv'
            if os.path.exists(fn):
                # 读取bar文件
                df = pd.read_csv(fn)
                row = df.iloc[-1,:]
                price_new = row.close
                price_last = close_dict[fut]
                size = int(get_contract(fut).size)
                value += (price_new - price_last) * pos_dict[fut] * size
                close_dict[fut] = price_new

        rec.datetime = today + ' 15:00:00'
        rec.portfolioValue = value
        rec.netPnl = net_pnl
        rec.posDict = str(pos_dict)
        rec.closeDict = str(close_dict)
        df_p = pd.DataFrame([rec])
        df_p.to_csv(fn_p, index=False, header=None, mode='a')

def fresh_mutual():
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')

    dirname = get_dss() + 'fut/engine/mutual/'
    listfile = os.listdir(dirname)
    for fn in listfile:
        fresh_p(today, dirname+fn)

def fresh_star():
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    pz_list = ['CF','m','IO']
    for pz in pz_list:
        fn = get_dss() + 'fut/engine/star/portfolio_star_' + pz + '_var.csv'
        if os.path.exists(fn) == False:
            continue
        df = pd.read_csv(fn)
        if len(df) == 0:
            continue

        rec = df.iloc[-1,:]
        pos_dict = eval(rec.posDict)
        value = 0
        net_pnl = 0
        item_list = list( pos_dict.keys() )

        for item in item_list:
            if item == 'mutual':
                fn = get_dss() + 'fut/engine/mutual/portfolio_mutual_' + pz + '_var.csv'
                df = pd.read_csv(fn)
                row = df.iloc[-1,:]
                value += row.portfolioValue
                net_pnl += row.netPnl

            if item == 'dali':
                fn = get_dss() +  'fut/engine/star/value_dali.csv'
                df = pd.read_csv(fn)
                df = df[df.pz == pz]
                row = df.iloc[-1,:]
                value += row.cur_value

        rec.datetime = today + ' 15:00:00'
        rec.portfolioValue = value
        rec.netPnl = net_pnl
        df = pd.DataFrame([rec])
        fn = get_dss() + 'fut/engine/star/portfolio_star_' + pz + '_var.csv'
        df.to_csv(fn, index=False, header=None, mode='a')

def pandian_run():
    try:
        now = datetime.now()
        today = now.strftime('%Y-%m-%d')

        pandian_dali(today)
        fresh_mutual()
        fresh_star()

    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

if __name__ == '__main__':
    # pandian_run()
    pass
