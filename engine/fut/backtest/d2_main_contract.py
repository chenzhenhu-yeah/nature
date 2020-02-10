
import numpy as np
import pandas as pd
import os
import re
import datetime
import time

from nature import get_dss

def connect(fn_pz, fn_symbol, end_month):
    df_symbol = pd.read_csv(fn_symbol)

    if os.path.exists(fn_pz):
        df_pz = pd.read_csv(fn_pz)
        #print(df_pz.head(3))
        s = df_pz.iloc[-1,]
        close_pz = float(s.close)
        symbol_pz = s.symbol
        begin_dt = s.date + ' ' + s.time
        end_dt = end_month + '-31 99:99:99'

        df_symbol['datetime'] = df_symbol['date'] + ' ' + df_symbol['time']
        df_symbol = df_symbol[ (df_symbol.datetime>=begin_dt) & (df_symbol.datetime<=end_dt) & (df_symbol.symbol>=symbol_pz)]
        if len(df_symbol) > 0:
            del df_symbol['datetime']
            s = df_symbol.iloc[0,]
            close_symbol = float(s.close)
            gap = close_pz - close_symbol
            print(close_pz)
            print(close_symbol)
            print(gap)
            print(df_symbol.head(3))
            print(df_symbol.tail(3))

            df_symbol = df_symbol[1:]
            df_symbol.open += gap
            df_symbol.high += gap
            df_symbol.low  += gap
            df_symbol.close += gap

            df_symbol.to_csv(fn_pz, mode='a', index=False, header=False)
            print('追加bar数量：', len(df_symbol))

    else:
        df_symbol[:1].to_csv(fn_pz, index=False)

def bu():
    pz = 'bu'
    fn_pz = get_dss() +'backtest/fut/' + pz + '/min1_' + pz + '_0612_0410.csv'

    param_list = []

    symbol = 'bu1806'
    fn_symbol = get_dss() +'backtest/fut/' + pz + '/' + symbol + '.csv'
    end_month = '2018-04'
    param_list.append( [fn_symbol, end_month] )

    symbol = 'bu1806'
    fn_symbol = get_dss() +'backtest/fut/' + pz + '/' + symbol + '.csv'
    end_month = '2018-04'
    param_list.append( [fn_symbol, end_month] )

    symbol = 'bu1812'
    fn_symbol = get_dss() +'backtest/fut/' + pz + '/' + symbol + '.csv'
    end_month = '2018-10'
    param_list.append( [fn_symbol, end_month] )

    symbol = 'bu1906'
    fn_symbol = get_dss() +'backtest/fut/' + pz + '/' + symbol + '.csv'
    end_month = '2019-04'
    param_list.append( [fn_symbol, end_month] )

    symbol = 'bu1912'
    fn_symbol = get_dss() +'backtest/fut/' + pz + '/' + symbol + '.csv'
    end_month = '2019-10'
    param_list.append( [fn_symbol, end_month] )

    symbol = 'bu2006'
    fn_symbol = get_dss() +'backtest/fut/' + pz + '/' + symbol + '.csv'
    end_month = '2019-12'
    param_list.append( [fn_symbol, end_month] )

    for p in param_list:
        connect(fn_pz, p[0], p[1])

if __name__ == '__main__':
    # 按以下顺序：
    # 05(2018-03)、创立
    # 05(2018-03)、 01(2018-10)、
    # 05(2019-03)、 01(2019-10)、
    # 05(2019-12)
    pass

    #bu()
