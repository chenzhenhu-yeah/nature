
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

from nature import get_trading_dates
from nature import init_signal_macd, signal_macd_sell, signal_macd_buy
from nature import signal_k_pattern

def calc_signal_buy(dss, _date):
    """

    """
    r = []

    # date,code, stratege, expire
    filename = dss + 'csv/bottle.csv'
    df = pd.read_csv(filename,dtype={'code':str})
    df = df[df.expire>=_date]
    codes = set(df['code'])

    # 准备信号
    df_signal_macd = init_signal_macd(dss,codes)
    #print(len(df_signal_macd))

    #macd买入出信号
    to_buy_codes = signal_macd_buy(codes, _date, df_signal_macd)
    for code in to_buy_codes:
        df1 = df[df.code==code]
        stratege = df1.iat[0,2]
        r.append([_date,code,stratege])

    return r

def calc_signal_sell(dss, _date):
    """

    """
    r = []

    # code,name
    filename = dss + 'csv/holds.csv'
    df = pd.read_csv(filename,dtype={'code':str})
    codes = set(df['code'])

    # 准备信号
    df_signal_macd = init_signal_macd(dss,codes)
    #print(len(df_signal_macd))

    #macd买入出信号
    to_sell_codes = signal_macd_sell(codes, _date, df_signal_macd)
    for code in to_sell_codes:
        r.append([_date,code,'sell'])

    return r

def signal_run(dss):
    all_dates = get_trading_dates(dss)
    date = all_dates[-1]

    #print(codes)
    r1 = calc_signal_buy(dss, date)
    r2 = calc_signal_sell(dss, date)
    df = pd.DataFrame(r1+r2, columns=['date','code','stratege'])
    filename = dss + 'csv/signal_macd.csv'
    df.to_csv(filename, index=False, mode='a', header=None)
    return r1+r2

if __name__ == "__main__":
    signal_run(r'../../data/')
