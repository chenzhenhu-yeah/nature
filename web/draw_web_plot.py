import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar, Grid, Scatter

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import os
import time
from datetime import datetime, timedelta
import talib

from nature import get_dss


def ic(symbol1, symbol2):
    fn = get_dss() +'fut/bar/day_' + symbol1 + '.csv'
    df1 = pd.read_csv(fn)
    fn = get_dss() +'fut/bar/day_' + symbol2 + '.csv'
    df2 = pd.read_csv(fn)
    start_dt = df1.at[0,'date'] if df1.at[0,'date'] > df2.at[0,'date'] else df2.at[0,'date']

    df1 = df1[df1.date >= start_dt]
    df1 = df1.reset_index()
    # print(df1.head(3))

    df2 = df2[df2.date >= start_dt]
    df2 = df2.reset_index()
    # print(df2.head(3))

    df1['close'] = df1.close - df2.close
    df1 = df1.set_index('date')

    n = 10          # 均线周期
    df1['ma'] = df1['close'].rolling(n).mean()

    plt.figure(figsize=(13,7))
    plt.xticks(rotation=45)
    plt.plot(df1.close)
    plt.plot(df1.ma)

    title = symbol1 + ' - ' + symbol2
    plt.title(title)
    plt.grid(True, axis='y')
    ax = plt.gca()

    for label in ax.get_xticklabels():
        label.set_visible(False)
    for label in ax.get_xticklabels()[1::5]:
        label.set_visible(True)
    for label in ax.get_xticklabels()[-1:]:
        label.set_visible(True)

    fn = 'static/ic_' + symbol1 + '_' + symbol2 + '.jpg'
    plt.savefig(fn)

def ic_show(symbol1, symbol2):
    r = ''
    ic(symbol1, symbol2)
    fn = 'ic_' + symbol1 + '_'+ symbol2+ '.jpg'
    now = str(int(time.time()))
    r = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return r

def ip_show(seq):
    r = ''
    seq = 'ip' + str(seq)
    fn = 'mates.csv'
    df = pd.read_csv(fn)
    df = df[df.seq == seq]
    if len(df) > 0:
        rec = df.iloc[0,:]
        symbol1 = rec.mate1
        symbol2 = rec.mate2
        ic(symbol1, symbol2)
        fn = 'ic_' + symbol1 + '_'+ symbol2+ '.jpg'
        now = str(int(time.time()))
        r = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return r


def yue():

    fn = get_dss() +  'fut/engine/yue/portfolio_yue_param.csv'
    df = pd.read_csv(fn)
    for i, row in df.iterrows():
        # print( row.symbol_dual )
        fn = get_dss() +  'fut/engine/yue/bar_yue_mix_' + row.symbol_dual +  '.csv'
        if os.path.exists(fn) == False:
            continue
        df2 = pd.read_csv(fn)
        df2.columns = ['date', 'time', 'sell_price', 'buy_price', 'f1', 'f2']
        df2 = df2[ (df2['date'] > '2020-05-01') & (df2['time'] == '14:59:00') ]
        df2 = df2.set_index('date')
        del df2['time']
        del df2['f1']
        del df2['f2']
        # print(df2.head())

        plt.figure(figsize=(12,7))
        plt.plot(df2.sell_price)
        plt.plot(df2.buy_price)

        plt.title(row.symbol_dual)
        plt.xticks(rotation=45)
        plt.grid(True, axis='y')
        ax = plt.gca()

        for label in ax.get_xticklabels():
            label.set_visible(False)
        for label in ax.get_xticklabels()[1::5]:
            label.set_visible(True)
        for label in ax.get_xticklabels()[-1:]:
            label.set_visible(True)

        plt.legend()
        # plt.show()
        fn = 'static/yue_' + row.symbol_dual + '.jpg'
        plt.savefig(fn)

def dali():
    pz_list = ['m', 'RM', 'MA']
    # pz_list = ['m']
    for pz in pz_list:
        # 读取品种每日盈亏情况，清洗数据为每日一个记录
        fn = get_dss() +  'fut/engine/dali/signal_dali_multi_var_' + pz + '.csv'
        df1 = pd.read_csv(fn)
        df1['date'] = df1.datetime.str.slice(0,10)
        df1['time'] = df1.datetime.str.slice(11,19)
        df1 = df1[df1.time.isin(['14:59:00', '15:00:00'])]
        df1 = df1.drop_duplicates(subset=['date'],keep='last')
        df1['dali'] = df1['net_pnl']
        df1 = df1.loc[:, ['date', 'dali']]
        df1 = df1.set_index('date')
        # print(df1.head(3))

        fn = get_dss() + 'fut/engine/daliopt/portfolio_daliopt_' + pz + '_var.csv'
        df2 = pd.read_csv(fn)
        df2['date'] = df2.datetime.str.slice(0,10)
        df2['time'] = df2.datetime.str.slice(11,19)
        df2 = df2[df2.time.isin(['14:59:00', '15:00:00'])]
        df2 = df2.drop_duplicates(subset=['date'],keep='last')
        df2['daliopt'] = df2['portfolioValue'] + df1['netPnl']
        df2 = df2.loc[:, ['date', 'daliopt']]
        df2 = df2.set_index('date')
        # print(df2.head(3))

        fn = get_dss() + 'fut/engine/dalicta/portfolio_mutual_' + pz + '_var.csv'
        df3 = pd.read_csv(fn)
        df3['date'] = df3.datetime.str.slice(0,10)
        df3['time'] = df3.datetime.str.slice(11,19)
        df3 = df3[df3.time.isin(['14:59:00', '15:00:00'])]
        df3 = df3.drop_duplicates(subset=['date'],keep='last')
        df3['mutual'] = df3['portfolioValue'] + df3['netPnl']
        df3 = df3.loc[:, ['date', 'mutual']]
        df3 = df3.set_index('date')
        # print(df3.head(3))

        df = df1.join(df2)
        df = df.join(df3)
        df['total'] = df['dali'] + df['daliopt'] + df['mutual']
        # print(df)

        plt.figure(figsize=(12,7))
        plt.title(pz)
        plt.plot(df.dali)
        plt.plot(df.daliopt)
        plt.plot(df.mutual)
        plt.plot(df.total)

        plt.xticks(rotation=45)
        plt.grid(True, axis='y')
        ax = plt.gca()

        for label in ax.get_xticklabels():
            label.set_visible(False)
        for label in ax.get_xticklabels()[1::25]:
            label.set_visible(True)
        for label in ax.get_xticklabels()[-1:]:
            label.set_visible(True)

        plt.legend()
        fn = 'static/dali_' + pz + '.jpg'
        plt.savefig(fn)
        # plt.show()

        # break


def star():
    pz_list = ['CF', 'SR', 'IO']
    for pz in pz_list:
        # 读取品种每日盈亏情况，清洗数据为每日一个记录
        fn = get_dss() + 'fut/engine/star/portfolio_star_' + pz + '_var.csv'
        df2 = pd.read_csv(fn)
        df2['date'] = df2.datetime.str.slice(0,10)
        df2['time'] = df2.datetime.str.slice(11,19)
        df2 = df2[df2.time.isin(['14:59:00', '15:00:00'])]
        df2 = df2.drop_duplicates(subset=['date'],keep='last')
        df2['star'] = df2['portfolioValue'] + df2['netPnl']
        df2 = df2.loc[:, ['date', 'star']]
        df2 = df2.set_index('date')
        # print(df2.head(3))

        fn = get_dss() + 'fut/engine/mutual/portfolio_mutual_' + pz + '_var.csv'
        df3 = pd.read_csv(fn)
        df3['date'] = df3.datetime.str.slice(0,10)
        df3['time'] = df3.datetime.str.slice(11,19)
        df3 = df3[df3.time.isin(['14:59:00', '15:00:00'])]
        df3 = df3.drop_duplicates(subset=['date'],keep='last')
        df3['mutual'] = df3['portfolioValue'] + df3['netPnl']
        df3 = df3.loc[:, ['date', 'mutual']]
        df3 = df3.set_index('date')
        # print(df3.head(3))

        df = df2.join(df3)
        df['total'] = df['star'] + df['mutual']
        # print(df)

        plt.figure(figsize=(12,7))
        plt.title(pz)
        plt.plot(df.star)
        plt.plot(df.mutual)
        plt.plot(df.total)

        plt.xticks(rotation=45)
        plt.grid(True, axis='y')
        ax = plt.gca()

        for label in ax.get_xticklabels():
            label.set_visible(False)
        for label in ax.get_xticklabels()[1::25]:
            label.set_visible(True)
        for label in ax.get_xticklabels()[-1:]:
            label.set_visible(True)

        plt.legend()
        fn = 'static/star_' + pz + '.jpg'
        plt.savefig(fn)
        # plt.show()

        # break

def opt():
    dirname = get_dss() + 'fut/engine/opt/'
    listfile = os.listdir(dirname)

    for filename in listfile:
        if filename[:7] == 'booking':
            print(filename)
            fn = dirname + filename
            df = pd.read_csv(fn)
            df = df.set_index('date')
            # print(df.head())

            plt.figure(figsize=(12,7))
            plt.title(filename)
            plt.plot(df.netPnl)
            plt.xticks(rotation=45)
            plt.grid(True, axis='y')
            ax = plt.gca()

            for label in ax.get_xticklabels():
                label.set_visible(False)
            for label in ax.get_xticklabels()[1::25]:
                label.set_visible(True)
            for label in ax.get_xticklabels()[-1:]:
                label.set_visible(True)

            # plt.legend()
            fn = 'static/opt_' + filename + '.jpg'
            plt.savefig(fn)
            # plt.show()

            # break

def mates():

    fn = 'mates.csv'
    df = pd.read_csv(fn)
    df = df.set_index('seq')
    # print(df)
    for i in range(10):
        rec = df.loc['ic'+str(i),:]
        symbol1 = rec.mate1
        symbol2 = rec.mate2
        ic(symbol1, symbol2)

        rec = df.loc['ip'+str(i),:]
        symbol1 = rec.mate1
        symbol2 = rec.mate2
        ic(symbol1, symbol2)

def smile():
    """期权微笑曲线"""
    now = datetime.now()
    # today = now.strftime('%Y-%m-%d %H:%M:%S')
    today = now.strftime('%Y-%m-%d')
    # today = '2020-05-29'

    fn = get_dss() + 'opt/' +  today[:7] + '_sigma.csv'
    df = pd.read_csv(fn)
    df = df[df.date == today]
    df = df.drop_duplicates(subset=['term'], keep='last')
    # print(df.head())

    for i, row in df.iterrows():
        c_curve_dict = eval(row.c_curve)
        p_curve_dict = eval(row.p_curve)

        df1 = pd.DataFrame([c_curve_dict, p_curve_dict])
        df1 = df1.T
        df1.columns = ['call', 'put']

        df1.plot()
        plt.title(today + '_' + row.term)

        fn = 'static/smile_' + row.term + '.jpg'
        plt.savefig(fn)

def iv_ts():
    """隐波时序图"""
    now = datetime.now()

    # 本月第一天
    first_day = datetime(now.year, now.month, 1)
    # print(first_day)
    #前一个月最后一天
    pre_month = first_day - timedelta(days = 1)
    # print(pre_month)
    today = now.strftime('%Y-%m-%d')
    pre = pre_month.strftime('%Y-%m-%d')
    # today = '2020-05-29'

    fn = get_dss() + 'opt/' +  pre[:7] + '_greeks.csv'
    df_pre = pd.read_csv(fn)
    fn = get_dss() + 'opt/' +  today[:7] + '_greeks.csv'
    df_today = pd.read_csv(fn)

    df = pd.concat([df_pre, df_today])
    df['dt'] = df.Localtime.str.slice(0,13)
    df = df.set_index('dt')
    print(df.head())
    print(df.tail())

    term_dict = {'IO2006-':['-3800', '-3900'],
                 'm2009-': ['-2800', '-2750'],
                 'RM009':['2300', '2350'],
                 'MA009':['1800', '1850'],
                 'CF009':['11800', '11200'],
                }

    for term in term_dict.keys():
        for strike in term_dict[term]:
            symbol = term + 'C' + strike
            # print(symbol)
            df1 = df[df.Instrument == symbol]
            df1['iv_call'] = df1.iv
            # df1 = df1.reset_index()
            # print(df1)

            symbol = term + 'P' + strike
            # print(symbol)
            df2 = df[df.Instrument == symbol]
            df2['iv_put'] = df2.iv
            # df2 = df2.reset_index()
            # print(df2)

            if len(df1) > 0 and len(df2) > 0:
                plt.figure(figsize=(12,7))
                plt.plot(df1.iv_call)
                plt.plot(df2.iv_put)
                plt.title(today + '_iv_ts_' + term + strike)

                plt.legend()
                # plt.show()
                fn = 'static/iv_ts_' + term + strike + '.jpg'
                plt.savefig(fn)

if __name__ == '__main__':
    pass
    # yue()
    # dali()
    # opt()
    # mates()
    # smile()
    # iv_ts()
    star()
