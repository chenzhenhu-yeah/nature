import numpy as np
import pandas as pd

import smtplib
from email.mime.text import MIMEText

import os
import re
from datetime import datetime
import time
from csv import DictReader
from nature import get_dss, get_trading_dates, get_daily, get_stk_hfq, to_log, get_contract

import json
import tushare as ts


def pandian_run():
    r = []
    dss = get_dss()
    now = datetime.now()
    #today = now.strftime('%Y-%m-%d') + ' 15:00:00'
    today = now.strftime('%Y-%m-%d')

    # 加载品种
    config = open(dss + 'fut/cfg/config.json')
    setting = json.load(config)
    symbols = setting['symbols_dali']
    dali_symbol_list = symbols.split(',')

    fn_value = dss +  'fut/engine/value_dali.csv'
    if os.path.exists(fn_value):
        #df_value = pd.read_csv(fn_value, sep='$')
        df_value = pd.read_csv(fn_value)
    else:
        return

    for symbol in dali_symbol_list:
        pz = str(get_contract(symbol).pz)
        df = df_value[df_value.pz == pz]
        if len(df) > 0:
            rec = df.iloc[-1,:]
            capital = rec.capital
            newyear_value = rec.newyear_value
        else:
            continue

        fn_signal = dss +  'fut/engine/dali/signal_dali_' +'multi'+ '_var_' + symbol + '.csv'
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
            one_duo_list,one_kong_list = "[]", "[]"
            r.append( [today,pz,capital,cur_value,newyear_value,year_ratio,margin,risk,net_pos,multi_duo_list,multi_kong_list,one_duo_list,one_kong_list] )
            #print(r)
            #return

    df = pd.DataFrame(r)
    df.to_csv(fn_value, index=False, mode='a', header=None)

if __name__ == '__main__':
    # pandian_run()
    pass
