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
        df_value = pd.read_csv(fn_value)
    else:
        return

    fn_param = dss +  'fut/engine/dali/signal_dali_param.csv'
    df_param = pd.read_csv(fn_param)
    df_param = df_param.set_index('symbol')
    # print(df_param)

    r = []
    for symbol in dali_symbol_list:
        c = get_contract(symbol)
        pz = str(c.pz)
        df = df_value[df_value.pz == pz]
        # 获得该品种最近日期的一条记录，新品种必须事先维护一条记录！！！
        if len(df) > 0:
            rec = df.iloc[-1,:]
            capital = rec.capital
            date = rec.date
            # 跨年了
            if today[:4] == date[:4]:
                newyear_value = rec.newyear_value
            else:
                newyear_value = rec.cur_value                 # 换年

        else:
            continue

        # 更新该品种的最新数据
        fn_signal = dss +  'fut/engine/dali/signal_dali_' +'multi'+ '_var_' + pz + '.csv'
        if os.path.exists(fn_signal):
            df = pd.read_csv(fn_signal)
            df = df[df.vtSymbol == symbol]
            rec = df.iloc[-1,:]
            # print(rec)

            cur_value = int( capital + rec.pnl_net )
            year_ratio= round(100*(cur_value/newyear_value-1), 2)
            net_pos = rec.unit
            multi_duo_list = eval(rec.price_duo_list)
            multi_kong_list = eval(rec.price_kong_list)

            if len(multi_duo_list) > len(multi_kong_list):
                active_price = float( multi_duo_list[0] )
            else:
                active_price = float( multi_kong_list[0] )

            margin = df_param.at[symbol,'fixed_size']*active_price*float(c.size)*float(c.margin) * ( len(multi_duo_list) + len(multi_kong_list) )
            risk = round(100*(margin/cur_value), 2)

            r.append( [today,pz,capital,cur_value,newyear_value,year_ratio,margin,risk,net_pos,multi_duo_list,multi_kong_list] )
            #print(r)
            #return

    df = pd.DataFrame(r)
    df.to_csv(fn_value, index=False, mode='a', header=None)

def pandian_p(today):
    # 读取value_p文件
    fn_value = get_dss() +  'fut/engine/value_p.csv'
    if os.path.exists(fn_value):
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
            if today[:4] == date[:4]:
                newyear_value = rec.newyear_value
            else:
                newyear_value = rec.cur_value           # 换年

        else:
            continue

        # 更新该品种的最新数据
        fn = get_dss() +  'fut/engine/' + p + '/portfolio_' + p + '_var.csv'
        #fn = get_dss() +  'fut/engine/rsiboll/portfolio_rsiboll_var.csv'
        if os.path.exists(fn):
            df = pd.read_csv(fn)
            rec = df.iloc[-1,:]
            cur_value = rec.portfolioValue
            year_ratio= round(100*(cur_value/newyear_value-1), 2)
            pos_dict = eval(rec.posDict)
            close_dict = eval(rec.closeDict)

            margin = 0
            for symbol in pos_dict:
                # print(symbol, pos_dict[symbol], close_dict[symbol])
                c = get_contract(symbol)
                margin += abs(pos_dict[symbol])*close_dict[symbol]*float(c.size)*float(c.margin)

            risk = round(100*(margin/cur_value), 2)
            r.append( [today,p,capital,cur_value,newyear_value,year_ratio,margin,risk,str(pos_dict)] )

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
