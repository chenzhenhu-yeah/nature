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
    dss = get_dss()

    # 加载品种
    config = open(dss + 'fut/cfg/config.json')
    setting = json.load(config)
    symbols = setting['symbols_dali']
    dali_symbol_list = symbols.split(',')

    # 读取value_dali文件
    fn_value = dss +  'fut/engine/value_dali.csv'
    if os.path.exists(fn_value):
        #df_value = pd.read_csv(fn_value, sep='$')
        df_value = pd.read_csv(fn_value)
    else:
        return

    r = []
    for symbol in dali_symbol_list:
        pz = str(get_contract(symbol).pz)
        df = df_value[df_value.pz == pz]
        # 获得该品种最近日期的一条记录
        if len(df) > 0:
            rec = df.iloc[-1,:]
            capital = rec.capital
            date = rec.date
            if today[:4] != date[:4]:
                newyear_value = rec.newyear_value   # 换年
            else:
                newyear_value = rec.cur_value

        else:
            continue

        # 更新该品种的最新数据
        fn_signal = dss +  'fut/engine/dali/signal_dali_' +'multi'+ '_var_' + pz + '.csv'
        if os.path.exists(fn_signal):
            df = pd.read_csv(fn_signal, sep='$')
            df = df[df.vtSymbol == symbol]
            rec = df.iloc[-1,:]
            # print(rec)

            cur_value = int( capital + rec.pnl_net )
            year_ratio= round(100*(cur_value/newyear_value-1), 2)
            margin = 0
            risk = 0
            net_pos = rec.unit
            multi_duo_list = rec.price_duo_list
            multi_kong_list = rec.price_kong_list
            r.append( [today,pz,capital,cur_value,newyear_value,year_ratio,margin,risk,net_pos,multi_duo_list,multi_kong_list] )
            #print(r)
            #return

    df = pd.DataFrame(r)
    df.to_csv(fn_value, index=False, mode='a', header=None)

def pandian_p(today):
    # 读取value_p文件
    fn_value = get_dss() +  'fut/engine/value_p.csv'
    if os.path.exists(fn_value):
        #df_value = pd.read_csv(fn_value, sep='$')
        df_value = pd.read_csv(fn_value)
        #print(df_value)
    else:
        return

    r = []
    p_list = ['rsiboll', 'cciboll', 'dali']
    for p in p_list:
        df = df_value[df_value.p == p]
        # 获得该组合最近日期的一条记录
        if len(df) > 0:
            rec = df.iloc[-1,:]
            capital = rec.capital
            date = rec.date
            if today[:4] != date[:4]:
                newyear_value = rec.newyear_value   # 换年
            else:
                newyear_value = rec.cur_value

        else:
            continue

        # 更新该品种的最新数据
        fn = get_dss() +  'fut/engine/' + p + '/portfolio_' +p+ '_var.csv'
        #fn = get_dss() +  'fut/engine/rsiboll/portfolio_rsiboll_var.csv'
        if os.path.exists(fn):
            df = pd.read_csv(fn, sep='$')
            #print(df)
            rec = df.iloc[-1,:]
            #print(rec)
            cur_value = rec.portfolioValue

            year_ratio= round(100*(cur_value/newyear_value-1), 2)
            pos_dict = rec.posDict
            r.append( [today,p,capital,cur_value,newyear_value,year_ratio,pos_dict] )
            #print(r)

    df = pd.DataFrame(r)
    df.to_csv(fn_value, index=False, mode='a', header=None)

def render_dali(today):
    pass

def render_p(today):
    pass

def pandian_run():
    try:
        now = datetime.now()
        #today = now.strftime('%Y-%m-%d') + ' 15:00:00'
        today = now.strftime('%Y-%m-%d')

        pandian_dali(today)
        pandian_p(today)
    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

if __name__ == '__main__':
    pandian_run()
    #pass
