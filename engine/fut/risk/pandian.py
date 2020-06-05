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
    df_param = df_param.set_index('pz')
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


            active_price = (int(multi_duo_list[0]) + int(multi_kong_list[0])) * 0.5

            margin = df_param.at[pz,'fixed_size']*active_price*float(c.size)*float(c.margin) * ( len(multi_duo_list) + len(multi_kong_list) )
            risk = round(100*(margin/capital), 2)

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
    # p_list = [ 'dali', 'ic', 'opt', 'star', 'cui' ]
    p_list = [ 'dali', 'opt', 'star']
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
        # print(fn)
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

            risk = round(100*(margin/capital), 2)
            r.append( [today,p,capital,cur_value,newyear_value,year_ratio,margin,risk,str(pos_dict)] )

    # print(r)
    df = pd.DataFrame(r)
    df.to_csv(fn_value, index=False, mode='a', header=None)

#-----------------------------------------------------------------------------------------------------------
def pandian_dali_m(today):
    #date,name,capital,cur_value,newyear_value,year_ratio,margin,risk,
    r = []
    capital = 0
    cur_value = 0
    newyear_value = 0
    dss = get_dss()

    # 读取value_dali文件
    fn = dss +  'fut/engine/value_dali.csv'
    if os.path.exists(fn):
        df = pd.read_csv(fn)
    else:
        return

    df = df[df.pz == 'm']
    if len(df) > 0:
        rec = df.iloc[-1,:]
        capital += rec.capital
        cur_value += rec.cur_value
        newyear_value += rec.newyear_value
        r.append([today, 'dali', rec.capital, rec.cur_value, rec.newyear_value,0,0,0])
    else:
        return

    fn = dss +  'fut/engine/daliopt/portfolio_daliopt_m_var.csv'
    if os.path.exists(fn):
        df = pd.read_csv(fn)
    else:
        return

    if len(df) > 0:
        rec = df.iloc[-1,:]
        capital += 20000
        cur_value += rec.portfolioValue
        newyear_value += 20000
        r.append([today, 'daliopt', 20000, rec.portfolioValue, 20000,0,0,0])
    else:
        return

    capital += 30000
    cur_value += 30000
    newyear_value += 30000
    r.append([today, 'dalicta', 30000, 30000, 30000, 0,0,0])
    r.append([today, 'daliall', capital, cur_value, newyear_value, 0,0,0])
    # print( r )

    df = pd.DataFrame(r)
    df.columns = ['date','name','capital','cur_value','newyear_value','year_ratio','margin','risk']
    df['year_ratio'] = round( (df.cur_value / df.newyear_value - 1)*100, 2)
    fn = dss + 'fut/engine/value_dali_m.csv'
    df.to_csv(fn, index=False, mode='a', header=None)

#-----------------------------------------------------------------------------------------------------------
def pandian_dali_RM(today):
    #date,name,capital,cur_value,newyear_value,year_ratio,margin,risk,
    r = []
    capital = 0
    cur_value = 0
    newyear_value = 0
    dss = get_dss()

    # 读取value_dali文件
    fn = dss + 'fut/engine/value_dali.csv'
    if os.path.exists(fn):
        df = pd.read_csv(fn)
    else:
        return

    df = df[df.pz == 'RM']
    if len(df) > 0:
        rec = df.iloc[-1,:]
        capital += rec.capital
        cur_value += rec.cur_value
        newyear_value += rec.newyear_value
        r.append([today, 'dali', rec.capital, rec.cur_value, rec.newyear_value,0,0,0])
    else:
        return

    fn = dss +  'fut/engine/daliopt/portfolio_daliopt_RM_var.csv'
    if os.path.exists(fn):
        df = pd.read_csv(fn)
    else:
        return

    if len(df) > 0:
        rec = df.iloc[-1,:]
        capital += 20000
        cur_value += rec.portfolioValue
        newyear_value += 20000
        r.append([today, 'daliopt', 20000, rec.portfolioValue, 20000,0,0,0])
    else:
        return

    capital += 30000
    cur_value += 30000
    newyear_value += 30000
    r.append([today, 'dalicta', 30000, 30000, 30000, 0,0,0])
    r.append([today, 'daliall', capital, cur_value, newyear_value, 0,0,0])
    # print( r )

    df = pd.DataFrame(r)
    df.columns = ['date','name','capital','cur_value','newyear_value','year_ratio','margin','risk']
    df['year_ratio'] = round( (df.cur_value / df.newyear_value - 1)*100, 2)
    fn = dss + 'fut/engine/value_dali_RM.csv'
    df.to_csv(fn, index=False, mode='a', header=None)

#-----------------------------------------------------------------------------------------------------------
def pandian_dali_MA(today):
    r = []
    capital = 0
    cur_value = 0
    newyear_value = 0
    dss = get_dss()

    # 读取value_dali文件
    fn = dss + 'fut/engine/value_dali.csv'
    if os.path.exists(fn):
        df = pd.read_csv(fn)
    else:
        return

    df = df[ df.pz == 'MA' ]
    if len(df) > 0:
        rec = df.iloc[-1,:]
        capital += rec.capital
        cur_value += rec.cur_value
        newyear_value += rec.newyear_value
        r.append([today, 'dali', rec.capital, rec.cur_value, rec.newyear_value,0,0,0])
    else:
        return

    fn = dss +  'fut/engine/daliopt/portfolio_daliopt_MA_var.csv'
    if os.path.exists(fn):
        df = pd.read_csv(fn)
    else:
        return

    if len(df) > 0:
        rec = df.iloc[-1,:]
        capital += 20000
        cur_value += rec.portfolioValue
        newyear_value += 20000
        r.append([today, 'daliopt', 20000, rec.portfolioValue, 20000,0,0,0])
    else:
        return

    capital += 30000
    cur_value += 30000
    newyear_value += 30000
    r.append([today, 'dalicta', 30000, 30000, 30000, 0,0,0])
    r.append([today, 'daliall', capital, cur_value, newyear_value, 0,0,0])
    # print( r )

    df = pd.DataFrame(r)
    df.columns = ['date','name','capital','cur_value','newyear_value','year_ratio','margin','risk']
    df['year_ratio'] = round( (df.cur_value / df.newyear_value - 1)*100, 2)
    fn = dss + 'fut/engine/value_dali_MA.csv'
    df.to_csv(fn, index=False, mode='a', header=None)

def fresh_p(today, fn_p):
    print(today, fn_p)

    df_p = pd.read_csv(fn_p)
    rec = df_p.iloc[-1,:]
    pos_dict = eval(rec.posDict)
    close_dict = eval(rec.closeDict)
    value = rec.portfolioValue
    net_pnl = 0
    item_list = list( pos_dict.keys() )
    book_list = [x for x in item_list if x.startswith('booking')]
    fut_list =  [x for x in item_list if x not in book_list]

    # 处理期权项
    for book in book_list:
        fn = get_dss() + 'fut/engine/opt/' + book + '.csv'
        if os.path.exists(fn):
            # 读取booking文件
            df = pd.read_csv(fn)
            row = df.iloc[-1,:]
            net_pnl += row.netPnl
        else:
            # 读取booked文件
            fn = fn.replace('booking', 'booked')
            if os.path.exists(fn):
                df = pd.read_csv(fn)
                row = df.iloc[-1,:]
                value += row.netPnl
            pos_dict.pop(book)

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

def fresh_star():
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    pz_list = ['CF','IO']
    for pz in pz_list:
        fn_p = get_dss() + 'fut/engine/star/portfolio_star_' + pz + '_var.csv'
        fresh_p(today, fn_p)

def fresh_daliopt():
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    pz_list = ['m','RM','MA']
    for pz in pz_list:
        fn_p = get_dss() + 'fut/engine/daliopt/portfolio_daliopt_' + pz + '_var.csv'
        fresh_p(today, fn_p)

def fresh_mutual():
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    pz_list = ['m','RM','MA','CF','IO']
    for pz in pz_list:
        fn_p = get_dss() + 'fut/engine/mutual/portfolio_mutual_' + pz + '_var.csv'
        fresh_p(today, fn_p)

def pandian_run():
    try:
        now = datetime.now()
        #today = now.strftime('%Y-%m-%d') + ' 15:00:00'
        today = now.strftime('%Y-%m-%d')

        pandian_dali(today)
        pandian_p(today)
        pandian_dali_m(today)
        pandian_dali_RM(today)
        pandian_dali_MA(today)

        fresh_daliopt()
        fresh_mutual()
        fresh_star()

    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

if __name__ == '__main__':
    pandian_run()
    #pass
