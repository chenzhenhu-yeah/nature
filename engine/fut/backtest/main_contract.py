
import numpy as np
import pandas as pd
import os
import re
import datetime
import time

from nature import get_dss

def bt_single(strategyClass, code):
    pass


if __name__ == '__main__':
    # fn_pz = 'bar/' + input('file of pz: ')
    # fn_symbol = 'bar/'+ input('file of symbol: ')
    # end_month = input('end month(YYYY-MM): ')

    fn_pz = get_dss() +'backtest/fut/m/' + 'm_01_05.csv'
    fn_symbol = get_dss() +'backtest/fut/m/' + 'min1_m1905.csv'
    end_month = '2018-12'

    df_pz = pd.read_csv(fn_pz)
    #print(df_pz.head(3))
    s = df_pz.iloc[-1,]
    close_pz = float(s.close)
    print(close_pz)
    begin_dt = s.datetime
    end_dt = end_month + '-31 99:99:99'

    df_symbol = pd.read_csv(fn_symbol)
    df_symbol = df_symbol[(df_symbol.datetime>=begin_dt) & (df_symbol.datetime<=end_dt)]
    s = df_symbol.iloc[0,]
    close_symbol = float(s.close)
    gap = close_pz - close_symbol
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
