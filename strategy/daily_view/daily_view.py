
from pyecharts import Bar

import pandas as pd
from datetime import datetime, timedelta

import sys
sys.path.append(r'../../')
from down_k.get_trading_dates import get_trading_dates
from down_k.get_daily import get_daily
from hu_signal.macd import init_signal_macd, signal_macd_sell,signal_macd_buy

df_macd = None

def calc_signal_macd(dss, date):
    df = pd.read_csv('399006.csv')
    codes = list(df['symbol'])
    codes = [x[:6] for x in codes]

    global df_macd
    if df_macd is None:
        df_macd = init_signal_macd(dss, codes)
    r_buy  = signal_macd_buy(codes, date, df_macd)
    r_sell = signal_macd_sell(codes, date, df_macd)
    return [len(r_buy), len(r_sell)]


def calc_signal_zdt(dss, date):
    r = [date]
    df = get_daily(dss,date)
    if df is None:
        r = [date,0,0,0,0,0]
    else:
        df1 = df[(df.p_change>9.9)&(df.p_change<11)]
        r.append(len(df1))
        df1 = df[(df.p_change<-9.9)&(df.p_change>-11)]
        r.append(len(df1))
        df1 = df[df.p_change>0]
        r.append(len(df1))
        df1 = df[df.p_change==0]
        r.append(len(df1))
        df1 = df[df.p_change<0]
        r.append(len(df1))

    return r

def daily_view_run(dss):
    all_dates = get_trading_dates(dss)
    dates = all_dates[-1:]
    #dates = all_dates[-30:-1]

    for date in dates:
        r1 = calc_signal_zdt(dss, date)
        r2 = calc_signal_macd(dss, date)
        df = pd.DataFrame([r1+r2],columns=['date', '涨停数','跌停数','上涨数','平盘数','下跌数','macd_up','macd_down'])
        filename = dss + 'csv/daily_view.csv'
        df.to_csv(filename,index=False,mode='a',header=None)

def graph():
    filename = '../../../data/csv/daily_view.csv'
    df = pd.read_csv(filename)

    df = df[-90:]
    #print(df.head())

    attr = df['date']
    v1 = df['涨停数']
    v2 = df['跌停数']
    bar = Bar("涨跌停数")

    bar.add("涨停数", attr, v1, )
    bar.add("跌停数", attr, v2, )

    bar.render()

if __name__ == '__main__':
    daily_view_run(r'../../../data/')
    # r = calc_signal_macd('../../../data/', '2019-04-19')
    # print(r)
    # #graph()
