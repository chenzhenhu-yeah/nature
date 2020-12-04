import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar, Grid, Scatter

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import os
import time
from datetime import datetime, timedelta
import talib

from nature import get_dss, get_inx, get_contract
from nature import bsm_call_imp_vol, bsm_put_imp_vol

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

    plt.figure(figsize=(15,7))
    plt.xticks(rotation=90)
    plt.plot(df1.close)
    plt.plot(df1.ma)

    title = symbol1 + ' - ' + symbol2
    plt.title(title)
    plt.grid(True, axis='y')
    ax = plt.gca()

    for label in ax.get_xticklabels():
        label.set_visible(False)
    for label in ax.get_xticklabels()[1::30]:
        label.set_visible(True)
    for label in ax.get_xticklabels()[-1:]:
        label.set_visible(True)

    fn = 'static/ic_' + symbol1 + '_' + symbol2 + '.jpg'
    plt.savefig(fn)
    plt.close()

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
        plt.cla()

def dali_show(pz):
    # pz_list = ['m', 'RM', 'MA']

    # 读取品种每日盈亏情况，清洗数据为每日一个记录
    fn = get_dss() +  'fut/engine/dali/signal_dali_multi_var_' + pz + '.csv'
    df1 = pd.read_csv(fn)
    df1['date'] = df1.datetime.str.slice(0,10)
    df1['time'] = df1.datetime.str.slice(11,19)
    df1 = df1[df1.time.isin(['14:59:00', '15:00:00'])]
    df1 = df1.drop_duplicates(subset=['date'],keep='last')
    df1['dali'] = df1['pnl_net']
    df1 = df1.loc[:, ['date', 'dali']]
    df1 = df1.set_index('date')
    # print(df1.head(3))

    plt.figure(figsize=(12,7))
    plt.title(pz)
    plt.plot(df1.dali)

    plt.xticks(rotation=90)
    plt.grid(True, axis='y')
    ax = plt.gca()

    for label in ax.get_xticklabels():
        label.set_visible(False)
    for label in ax.get_xticklabels()[1::25]:
        label.set_visible(True)
    for label in ax.get_xticklabels()[-1:]:
        label.set_visible(True)

    # plt.legend()
    fn = 'static/dali_show.jpg'
    plt.savefig(fn)
    plt.cla()

    r = ''
    fn = 'dali_show.jpg'
    now = str(int(time.time()))
    r = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return r

def mutual():
    pz_list = ['CF', 'SR', 'IO', 'MA', 'RM', 'm', 'c', 'al']
    for pz in pz_list:
        # 读取品种每日盈亏情况，清洗数据为每日一个记录

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

        df = df3

        plt.figure(figsize=(12,7))
        plt.title(pz)
        plt.plot(df.mutual)

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
        fn = 'static/mutual_' + pz + '.jpg'
        plt.savefig(fn)
        plt.cla()


def star():
    pz_list = ['ru', 'c', 'al', 'CF', 'IO', 'MA', 'RM', 'm', 'p', 'FG', 'all']
    for pz in pz_list:
        # 读取品种每日盈亏情况，清洗数据为每日一个记录

        if pz == 'all':
            fn = get_dss() + 'fut/engine/star/value_all.csv'
            df3 = pd.read_csv(fn)
            df3['star'] = df3['cur_value']
        else:
            fn = get_dss() + 'fut/engine/star/portfolio_star_' + pz + '_var.csv'
            df3 = pd.read_csv(fn)
            df3['date'] = df3.datetime.str.slice(0,10)
            df3['time'] = df3.datetime.str.slice(11,19)
            df3 = df3[df3.time.isin(['14:59:00', '15:00:00'])]
            df3 = df3.drop_duplicates(subset=['date'],keep='last')
            df3['star'] = df3['portfolioValue'] + df3['netPnl']


        df3 = df3.loc[:, ['date', 'star']]
        df3 = df3.set_index('date')
        df = df3

        plt.figure(figsize=(12,7))
        plt.title(pz)
        plt.plot(df.star)

        plt.xticks(rotation=90)
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
        plt.cla()

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
            plt.cla()

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


def smile_symbol(symbol, date, atm, gap):

    now = datetime.strptime(date, '%Y-%m-%d')
    # now = datetime.now()

    # 本月第一天
    first_day = datetime(now.year, now.month, 1)
    #前一个月最后一天
    pre_month = first_day - timedelta(days = 1)
    today = now.strftime('%Y-%m-%d')
    pre = pre_month.strftime('%Y-%m-%d')

    fn = get_dss() + 'opt/' +  pre[:7] + '_sigma.csv'
    df_pre = pd.read_csv(fn)
    fn = get_dss() + 'opt/' +  today[:7] + '_sigma.csv'
    df_today = pd.read_csv(fn)
    df = pd.concat([df_pre, df_today])
    df = df[df.term == symbol]
    df = df[df.date <= today]
    df = df.drop_duplicates(subset=['date'], keep='last')
    # print(df.tail())

    plt.figure(figsize=(12,7))
    plt.title(today + '_' + symbol)

    row = df.iloc[-2,:]
    c_curve_dict = eval(row.c_curve)
    p_curve_dict = eval(row.p_curve)
    df2 = pd.DataFrame([c_curve_dict, p_curve_dict])
    df2 = df2.T
    df2.columns = ['call', 'put']
    df2 = df2.loc[atm-5*gap :atm+5*gap, :]
    plt.plot(df2.call, '--', label='pre call')
    plt.plot(df2.put, '--', label='pre put')

    row = df.iloc[-1,:]
    c_curve_dict = eval(row.c_curve)
    p_curve_dict = eval(row.p_curve)
    df1 = pd.DataFrame([c_curve_dict, p_curve_dict])
    df1 = df1.T
    df1.columns = ['call', 'put']
    df1 = df1.loc[atm-5*gap :atm+5*gap, :]
    plt.plot(df1.call, label='next call')
    plt.plot(df1.put,  label='next put')

    plt.legend()
    fn = 'static/smile_symbol.jpg'
    plt.savefig(fn)


def smile_show_symbol(symbol, date):
    pz = str(get_contract(symbol).pz)
    gap = 100
    if pz == 'CF':
        gap = 200
    if pz in ['RM', 'MA', 'm']:
        gap = 50

    if pz == 'IO':
        cur_if = 'IF' + symbol[2:]
    else:
        cur_if = pz + symbol[len(pz):]

    fn = get_dss() + 'fut/bar/day_' + cur_if + '.csv'
    df = pd.read_csv(fn)
    row = df.iloc[-1,:]
    strike = row.open*0.5 + row.close*0.5
    strike = int(round( round(strike*(100/gap)/1E4,2)*1E4/(100/gap), 0))
    atm = strike                                    # 获得平值

    smile_symbol(symbol, date, atm, gap)

    fn = 'smile_symbol.jpg'
    now = str(int(time.time()))
    r = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return r

def smile_pz(pz, symbol_list, atm, call, date, gap):
    now = datetime.strptime(date, '%Y-%m-%d')
    # now = datetime.now()

    # 本月第一天
    first_day = datetime(now.year, now.month, 1)
    # print(first_day)
    #前一个月最后一天
    pre_month = first_day - timedelta(days = 1)
    # print(pre_month)
    today = now.strftime('%Y-%m-%d')
    pre = pre_month.strftime('%Y-%m-%d')
    # today = '2020-06-16'

    fn = get_dss() + 'opt/' +  pre[:7] + '_sigma.csv'
    df_pre = pd.read_csv(fn)
    fn = get_dss() + 'opt/' +  today[:7] + '_sigma.csv'
    df_today = pd.read_csv(fn)
    df = pd.concat([df_pre, df_today])

    plt.figure(figsize=(12,7))
    plt.title('smile_' + today + '_' + pz+ '_' + call)

    for i, symbol in enumerate(symbol_list):
        df1 = df[df.term == symbol]
        df1 = df1.drop_duplicates(subset=['date'], keep='last')
        df1 = df1[df1.date <= today]
        if len(df1) < 2:
            continue

        # print(symbol)
        # 上一日的曲线
        if i in [0,1]:
            row = df1.iloc[-2, :]
            c_curve_dict = eval(row.c_curve)
            p_curve_dict = eval(row.p_curve)
            df2 = pd.DataFrame([c_curve_dict, p_curve_dict])
            df2 = df2.T
            df2.columns = ['call', 'put']
            df2['avg'] = df2.call*0.5 + df2.put*0.5
            df2.index = df2.index.astype('int')
            df2 = df2.sort_index()

            if call == 'call':
                df2 = df2[(df2.index <= atm+5*gap) & (df2.index >= atm-2*gap)]
                plt.plot(df2.call, '--', label=row.term)
            elif call == 'put':
                df2 = df2[(df2.index <= atm+2*gap) & (df2.index >= atm-5*gap)]
                plt.plot(df2.put, '--', label=row.term)

        # 当日的曲线
        row = df1.iloc[-1, :]
        c_curve_dict = eval(row.c_curve)
        p_curve_dict = eval(row.p_curve)
        df2 = pd.DataFrame([c_curve_dict, p_curve_dict])
        df2 = df2.T
        df2.columns = ['call', 'put']
        df2['avg'] = df2.call*0.5 + df2.put*0.5
        df2.index = df2.index.astype('int')
        df2 = df2.sort_index()

        if call == 'call':
            df2 = df2[(df2.index <= atm+5*gap) & (df2.index >= atm-2*gap)]
            plt.plot(df2.call, label=row.term)
        elif call == 'put':
            df2 = df2[(df2.index <= atm+2*gap) & (df2.index >= atm-5*gap)]
            plt.plot(df2.put, label=row.term)

    plt.grid(True, axis='x')
    plt.legend()
    fn = 'static/smile_show.jpg'
    plt.savefig(fn)
    plt.cla()


def smile_show_pz(pz, type, date):
    fn = get_dss() + 'fut/cfg/opt_mature.csv'
    df_opt = pd.read_csv(fn)

    df = df_opt[(df_opt.pz == pz) & (df_opt.flag == df_opt.flag)]                 # 筛选出不为空的记录
    df = df.sort_values('symbol')
    symbol_list = list(df.symbol)

    gap = 100
    if pz == 'CF':
        gap = 200
    if pz in ['RM', 'MA', 'm']:
        gap = 50

    if pz == 'IO':
        cur_if = 'IF' + symbol_list[0][2:]
    else:
        cur_if = pz + symbol_list[0][len(pz):]

    fn = get_dss() + 'fut/bar/day_' + cur_if + '.csv'
    df = pd.read_csv(fn)
    row = df.iloc[-1,:]
    strike = row.open*0.5 + row.close*0.5
    strike = int(round( round(strike*(100/gap)/1E4,2)*1E4/(100/gap) ,0))
    atm = strike                                    # 获得平值
    # print(atm)

    if type == 'call':
        smile_pz(pz, symbol_list, atm, 'call', date, gap)
    if type == 'put':
        smile_pz(pz, symbol_list, atm, 'put', date, gap)

    r = ''
    fn = 'smile_show.jpg'
    now = str(int(time.time()))
    r = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return r

def smile_show(pz, type, date, kind, symbol):
    """期权微笑曲线"""
    if kind == 'pz':
        return smile_show_pz(pz, type, date)
    elif kind == 'symbol':
        return smile_show_symbol(symbol, date)
    else:
        return ''

def open_interest_show_distribution(basic, type, date):
    now = datetime.now()
    # 本月第一天
    first_day = datetime(now.year, now.month, 1)
    #前一个月最后一天
    pre_month = first_day - timedelta(days = 1)
    today = now.strftime('%Y-%m-%d')
    pre = pre_month.strftime('%Y-%m-%d')

    fn = get_dss() + 'opt/' +  pre[:7] + '_greeks.csv'
    df_pre = pd.read_csv(fn)
    fn = get_dss() + 'opt/' +  today[:7] + '_greeks.csv'
    df_today = pd.read_csv(fn)
    df = pd.concat([df_pre, df_today])

    df = df[df.Instrument.str.slice(0,len(basic)) == basic]
    df['date'] = df.Localtime.str.slice(0,10)
    date_list = sorted(list(set(list(df.date))), reverse=True)
    print(date_list)
    if len(date_list) < 10:
        return ''

    plt.figure(figsize=(12,7))
    plt.title(' ')

    for i in [0, 1, 9]:
    # for i in [0]:
        df1 = df[df.date == date_list[i]]
        df1 = df1.sort_values('Instrument')
        n = len(df1)
        assert n % 2 == 0
        n = int(n / 2)
        df_c = df1.iloc[:n , :]
        df_p = df1.iloc[n: , :]
        # print(df_c)
        # print(df_p)
        if type == 'call':
            df2 = df_c
        else:
            df2 = df_p

        strike_list = []
        for j, row in df2.iterrows():
            strike_list.append( get_contract(row.Instrument).strike )
        df2['strike'] = strike_list
        df2 = df2.set_index('strike')
        if i == 0:
            plt.plot(df2.OpenInterest, label=date_list[i])
        else:
            plt.plot(df2.OpenInterest, '--', label=date_list[i])
        # print(df2.OpenInterest)

    plt.xticks(rotation=90)
    plt.legend()
    fn = 'static/open_interest.jpg'
    plt.savefig(fn)
    plt.cla()

    fn = 'open_interest.jpg'
    now = str(int(time.time()))
    r = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return r



def open_interest_show_total(basic, date):
    now = datetime.now()
    # 本月第一天
    first_day = datetime(now.year, now.month, 1)
    #前一个月最后一天
    pre_month = first_day - timedelta(days = 1)
    today = now.strftime('%Y-%m-%d')
    pre = pre_month.strftime('%Y-%m-%d')

    fn = get_dss() + 'opt/' +  pre[:7] + '_greeks.csv'
    df_pre = pd.read_csv(fn)
    fn = get_dss() + 'opt/' +  today[:7] + '_greeks.csv'
    df_today = pd.read_csv(fn)
    df = pd.concat([df_pre, df_today])

    df = df[df.Instrument.str.slice(0,len(basic)) == basic]
    df['date'] = df.Localtime.str.slice(0,10)
    df = df[df.date >= date]
    df = df.sort_values('Instrument')
    n = len(df)
    assert n % 2 == 0
    n = int(n / 2)
    df_c = df.iloc[:n , :]
    df_p = df.iloc[n: , :]

    df_c = df_c.sort_values('date')
    df_c = df_c.set_index('date')
    s_c = df_c.groupby('date')['OpenInterest'].sum()

    df_p = df_p.sort_values('date')
    df_p = df_p.set_index('date')
    s_p = df_p.groupby('date')['OpenInterest'].sum()

    plt.figure(figsize=(12,7))
    plt.title(' ')
    plt.plot(s_c, label='call')
    plt.plot(s_p, label='put')
    plt.xticks(rotation=90)

    plt.legend()
    fn = 'static/open_interest.jpg'
    plt.savefig(fn)
    plt.cla()

    fn = 'open_interest.jpg'
    now = str(int(time.time()))
    r = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return r

def open_interest_show(basic, type, date, kind):
    if kind == 'distribution':
        return open_interest_show_distribution(basic, type, date)
    elif kind == 'total':
        return open_interest_show_total(basic, date)
    else:
        return ''

def iv_ts():
    """隐波时序图"""
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    # today = '2020-05-29'

    fn = get_dss() + 'opt/iv_atm_' + today[:4] + '.csv'
    df = pd.read_csv(fn)

    fn = get_dss() + 'fut/cfg/opt_mature.csv'
    df1 = pd.read_csv(fn)
    df1 = df1[df1.flag == df1.flag]                 # 筛选出不为空的记录
    df1 = df1[df1.mature >= today]                  # 过期的合约就不要了

    for pz in ['IO']:
        df2 = df[df.pz == pz]
        df3 = df1[df1.pz == pz]

        # 画日线图
        plt.figure(figsize=(12,7))
        plt.title(today + 'iv_ts_day_' + pz)
        plt.ylim([0.15,0.5])
        for symbol in sorted(list(df3['symbol'])):
            # print(symbol)
            df21 = df2[df2.symbol == symbol]
            df21 = df21[df21.time == '14:59:00']
            df21 = df21.set_index('date')
            # print(df21)
            plt.plot(df21.iv_a, label=symbol)

        plt.legend()
        fn = 'static/iv_ts_day_' + pz + '.jpg'
        plt.savefig(fn)
        plt.cla()

        # 画分钟图
        plt.figure(figsize=(12,7))
        plt.title(today + 'iv_ts_min5_' + pz)
        plt.ylim([0.15,0.5])
        for symbol in sorted(list(df3['symbol'])):
            # print(symbol)
            df21 = df2[df2.symbol == symbol]
            df21 = df21.iloc[-240:,:]                    # 5日分时线，每天48个bar
            df21 = df21.reset_index()
            # print(df21)
            plt.plot(df21.iv_a, label=symbol)

        plt.legend()
        fn = 'static/iv_ts_min5_' + pz + '.jpg'
        plt.savefig(fn)
        plt.cla()

def hv_show(code):
    # hv()
    now = datetime.now()
    start_day = now - timedelta(days = 480)
    start_day = start_day.strftime('%Y-%m-%d')

    if code == '000300':
        df = get_inx('000300', start_day)
    else:
        fn = get_dss() + 'fut/bar/day_' + code + '.csv'
        df = pd.read_csv(fn)
        df = df[df.date >= start_day]

    df = df.set_index('date')
    df = df.sort_index()

    df['ln'] = np.log(df.close)
    df['rt'] = df['ln'].diff(1)
    df['hv'] = df['rt'].rolling(20).std()
    df['hv'] *= np.sqrt(242)

    df = df.iloc[-242:,:]
    cur = df.iloc[-1,:]
    hv = round(cur['hv'], 2)
    hv_rank = round( (hv - df['hv'].min()) / (df['hv'].max() - df['hv'].min()), 2 )
    hv_percentile = round( len(df[df.hv < hv]) / 242, 2 )

    plt.figure(figsize=(13,7))
    plt.title( code + '       hv: ' + str(hv) + '      hv Rank: ' + str(hv_rank) + '      hv Percentile: ' + str(hv_percentile) )
    plt.xticks(rotation=90)
    plt.plot(df['hv'], '-*')
    plt.grid(True, axis='y')

    ax = plt.gca()
    for label in ax.get_xticklabels():
        label.set_visible(False)
    for label in ax.get_xticklabels()[1::30]:
        label.set_visible(True)
    for label in ax.get_xticklabels()[-1:]:
        label.set_visible(True)

    fn = 'static/hv_show.jpg'
    plt.savefig(fn)
    plt.cla()

    r = ''
    fn = 'hv_show.jpg'
    now = str(int(time.time()))
    r = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return r

def book_min5_show(startdate, dual_list):
    plt.figure(figsize=(12,8))

    for dual in dual_list:
        symbol_a = dual[0]
        num_a    = int(dual[1])
        symbol_b = dual[2]
        num_b    = int(dual[3])

        fn = get_dss() + 'fut/bar/min5_' + symbol_a + '.csv'
        df_a = pd.read_csv(fn)
        fn = get_dss() + 'fut/bar/min5_' + symbol_b + '.csv'
        df_b = pd.read_csv(fn)
        df_a = df_a[df_a.date >= startdate]
        df_b = df_b[df_b.date >= startdate]
        df_a = df_a.reset_index()
        df_b = df_b.reset_index()
        assert len(df_a) == len(df_b)
        # print(df_a.head())
        # print(df_b.head())
        df_a['dt'] = df_a['date'] + ' ' + df_a['time']
        df_a['value'] = abs(num_a*df_a['close'] + num_b*df_b['close'])
        df_a = df_a.set_index('dt')
        # print(df_a.tail())
        # print(df_b.tail())
        plt.plot(df_a.value, label=symbol_a+' '+str(num_a)+ '   '+symbol_b+' '+ str(num_b))

    plt.xticks(rotation=90)
    plt.grid(True, axis='y')
    ax = plt.gca()

    for label in ax.get_xticklabels():
        label.set_visible(False)
    for label in ax.get_xticklabels()[1::25]:
        label.set_visible(True)
    for label in ax.get_xticklabels()[-1:]:
        label.set_visible(True)

    plt.legend()
    fn = 'static/book_min5_show.jpg'
    plt.savefig(fn)
    plt.cla()

    r = ''
    fn = 'book_min5_show.jpg'
    now = str(int(time.time()))
    r = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return r

def book_min5_now_show(startdate, dual_list):
    now = datetime.now()
    startdate = now.strftime('%Y-%m-%d')

    plt.figure(figsize=(12,8))
    for dual in dual_list:
        symbol_a = dual[0]
        num_a    = int(dual[1])
        symbol_b = dual[2]
        num_b    = int(dual[3])

        fn = get_dss() + 'fut/put/rec/min5_' + symbol_a + '.csv'
        if os.path.exists(fn) == False:
            continue
        df_a = pd.read_csv(fn)
        fn = get_dss() + 'fut/put/rec/min5_' + symbol_b + '.csv'
        if os.path.exists(fn) == False:
            continue
        df_b = pd.read_csv(fn)
        df_a = df_a[df_a.date >= startdate]
        df_b = df_b[df_b.date >= startdate]
        df_a = df_a.reset_index()
        df_b = df_b.reset_index()
        assert len(df_a) == len(df_b)
        # print(df_a.head())
        # print(df_b.head())
        df_a['dt'] = df_a['date'] + ' ' + df_a['time']
        df_a['value'] = abs(num_a*df_a['close'] + num_b*df_b['close'])
        df_a = df_a.set_index('dt')
        # print(df_a.tail())
        # print(df_b.tail())
        plt.plot(df_a.value, label=symbol_a+' '+str(num_a)+ '   '+symbol_b+' '+ str(num_b))

    plt.xticks(rotation=90)
    plt.grid(True, axis='y')
    ax = plt.gca()

    for label in ax.get_xticklabels():
        label.set_visible(False)
    for label in ax.get_xticklabels()[1::25]:
        label.set_visible(True)
    for label in ax.get_xticklabels()[-1:]:
        label.set_visible(True)

    plt.legend()
    fn = 'static/book_min5_now_show.jpg'
    plt.savefig(fn)
    plt.cla()

    r = ''
    fn = 'book_min5_now_show.jpg'
    now = str(int(time.time()))
    r = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return r

def iv_straddle_show(symbol, strike_list, startdate, kind):
    plt.figure(figsize=(12,8))
    for strike in strike_list:
        exchangeID = str(get_contract(symbol).exchangeID)
        if exchangeID in ['CFFEX', 'DCE']:
            s_a = symbol + '-C-' + strike
            s_b = symbol + '-P-' + strike
        else:
            s_a = symbol + 'C' + strike
            s_b = symbol + 'P' + strike

        if kind == 'daily':
            fn = get_dss() + 'fut/bar/min5_' + s_a + '.csv'
        else:
            fn = get_dss() + 'fut/put/rec/min5_' + s_a + '.csv'
        df_a = pd.read_csv(fn)

        if kind == 'daily':
            fn = get_dss() + 'fut/bar/min5_' + s_b + '.csv'
        else:
            fn = get_dss() + 'fut/put/rec/min5_' + s_b + '.csv'
        df_b = pd.read_csv(fn)

        df_a = df_a[df_a.date >= startdate]
        df_b = df_b[df_b.date >= startdate]
        assert len(df_a) == len(df_b)
        df_a['dt'] = df_a['date'] + ' ' + df_a['time']
        df_a['value'] = df_a['close'] + df_b['close']
        df_a = df_a.set_index('dt')

        # df_a['next'] = df_a['value'].shift(1)
        # df_a['value'] = np.log(df_a['value']) - np.log(df_a['next'])
        # df_a['value'] = df_a['value'].cumsum()
        # print(df_a.head())
        plt.plot(df_a.value, label=strike)

    plt.xticks(rotation=90)
    plt.grid(True, axis='y')

    ax = plt.gca()
    for label in ax.get_xticklabels():
        label.set_visible(False)
    for label in ax.get_xticklabels()[1::25]:
        label.set_visible(True)
    for label in ax.get_xticklabels()[-1:]:
        label.set_visible(True)

    plt.legend()
    fn = 'static/iv_straddle_show.jpg'
    plt.savefig(fn)
    plt.cla()

    r = ''
    fn = 'iv_straddle_show.jpg'
    now = str(int(time.time()))
    r = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return r

def hs300_spread_show(start_day):
    df_300 = get_inx('000300', start_day)
    df_300 = df_300.set_index('date')
    df_300 = df_300.sort_index()
    # print(df_300.tail())

    fn = get_dss() + 'fut/cfg/opt_mature.csv'
    df_opt = pd.read_csv(fn)
    df = df_opt[(df_opt.pz == 'IO') & (df_opt.flag == df_opt.flag)]               # 筛选出不为空的记录
    df = df.sort_values('symbol')
    symbol_list = list(df.symbol)
    # print(symbol_list)

    plt.figure(figsize=(12,8))
    plt.title('hs300_spread')
    for symbol in symbol_list:
        code = 'IF' + symbol[2:]
        fn = get_dss() + 'fut/bar/day_' + code + '.csv'
        if os.path.exists(fn) == False:
            continue
        df = pd.read_csv(fn)
        df = df[df.date >= start_day]
        df = df.set_index('date')
        df = df.sort_index()
        df['value'] = df.close - df_300.close
        # print(df.tail())

        plt.plot(df.value, label=code)

    plt.xticks(rotation=45)
    plt.grid(True, axis='y')
    ax = plt.gca()

    # for label in ax.get_xticklabels():
    #     label.set_visible(False)
    # for label in ax.get_xticklabels()[1::25]:
    #     label.set_visible(True)
    # for label in ax.get_xticklabels()[-1:]:
    #     label.set_visible(True)

    plt.legend()
    fn = 'static/hs300_spread_show.jpg'
    plt.savefig(fn)
    plt.cla()

    r = ''
    fn = 'hs300_spread_show.jpg'
    now = str(int(time.time()))
    r = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return r

def straddle_diff_day_show(basic_m0, basic_m1, date_begin, date_end):
    plt.figure(figsize=(12,8))

    fn = get_dss() + 'opt/straddle_differ.csv'
    df = pd.read_csv(fn)
    df = df[(df.basic_m0 == basic_m0) & (df.basic_m1 == basic_m1)]
    df = df[df.date <= date_end]
    per_10 = ''
    per_50 = ''
    per_90 = ''

    if len(df) >= 480:
        df = df.iloc[-480:, :]
        per_10 = int(df.differ.quantile(0.01))
        per_50 = int(df.differ.quantile(0.5))
        per_90 = int(df.differ.quantile(0.99))

    df = df[df.date >= date_begin]
    df['dt'] = df.date + ' ' + df.time
    df = df.set_index('dt')
    # print(df)

    plt.title('date: ' + date_begin + '   5:' + str(per_10) + '   50:' + str(per_50) + '   95:' + str(per_90))
    plt.plot(df['differ'])
    plt.xticks(rotation=90)
    plt.grid(True, axis='y')

    ax = plt.gca()
    for label in ax.get_xticklabels():
        label.set_visible(False)
    for label in ax.get_xticklabels()[1::25]:
        label.set_visible(True)
    for label in ax.get_xticklabels()[-1:]:
        label.set_visible(True)

    plt.legend()
    fn = 'static/straddle_diff_show.jpg'
    plt.savefig(fn)
    plt.cla()

    r = ''
    fn = 'straddle_diff_show.jpg'
    now = str(int(time.time()))
    r = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return r

def straddle_diff_now_show(basic_m0, basic_m1, date):
    fn = get_dss() + 'opt/straddle_differ.csv'
    df = pd.read_csv(fn)
    df = df[(df.basic_m0 == basic_m0) & (df.basic_m1 == basic_m1)]
    df = df[df.date <= date]
    per_10 = ''
    per_50 = ''
    per_90 = ''

    if len(df) >= 480:
        df = df.iloc[-480:, :]
        per_10 = int(df.differ.quantile(0.01))
        per_50 = int(df.differ.quantile(0.5))
        per_90 = int(df.differ.quantile(0.99))

    # 获得上一个交易日，用于确定差值的基准
    symbol_obj = 'IF' + basic_m0[2:]
    fn = get_dss() + 'fut/bar/day_' + symbol_obj + '.csv'
    df = pd.read_csv(fn)
    df = df[df.date <= date]
    if date == df.iat[-1,0]:
        date_pre = df.iat[-2,0]
    else:
        date_pre = df.iat[-1,0]

    # 取开盘数据的atm
    fn = get_dss() + 'fut/put/rec/min5_' + symbol_obj + '.csv'
    df = pd.read_csv(fn)
    df = df[df.date == date]
    df = df[df.time == '09:34:00']
    if len(df) == 0:
        return '当前日期无数据'

    # print(df)
    rec = df.iloc[0,:]
    obj = rec.close
    gap = 50
    # gap = 100
    atm = int(round(round(obj*(100/gap)/1E4,2) * 1E4/(100/gap), 0))     # 获得平值

    fn = get_dss() + 'fut/put/rec/min5_' + basic_m0 + '-C-' + str(atm) + '.csv'
    df_m0_c = pd.read_csv(fn)
    df_m0_c_pre = df_m0_c[df_m0_c.date == date_pre]
    df_m0_c = df_m0_c[df_m0_c.date == date]

    fn = get_dss() + 'fut/put/rec/min5_' + basic_m0 + '-P-' + str(atm) + '.csv'
    df_m0_p = pd.read_csv(fn)
    df_m0_p_pre = df_m0_p[df_m0_p.date == date_pre]
    df_m0_p = df_m0_p[df_m0_p.date == date]

    fn = get_dss() + 'fut/put/rec/min5_' + basic_m1 + '-C-' + str(atm) + '.csv'
    df_m1_c = pd.read_csv(fn)
    df_m1_c_pre = df_m1_c[df_m1_c.date == date_pre]
    df_m1_c = df_m1_c[df_m1_c.date == date]

    fn = get_dss() + 'fut/put/rec/min5_' + basic_m1 + '-P-' + str(atm) + '.csv'
    df_m1_p = pd.read_csv(fn)
    df_m1_p_pre = df_m1_p[df_m1_p.date == date_pre]
    df_m1_p = df_m1_p[df_m1_p.date == date]

    d_base_m1 = df_m1_c_pre.iat[-1,5] + df_m1_p_pre.iat[-1,5]
    d_base_m0 = df_m0_c_pre.iat[-1,5] + df_m0_p_pre.iat[-1,5]

    df_m1_c = df_m1_c.reset_index()
    df_m1_p = df_m1_p.reset_index()
    df_m0_c = df_m0_c.reset_index()
    df_m0_p = df_m0_p.reset_index()

    df_m1_c['diff_m1'] = df_m1_c.close + df_m1_p.close - d_base_m1
    df_m0_c['diff_m0'] = df_m0_c.close + df_m0_p.close - d_base_m0
    df_m1_c['differ'] = df_m1_c.diff_m1 - df_m0_c.diff_m0

    plt.figure(figsize=(12,8))
    plt.title('date: ' + date + '   atm: ' + str(atm) + '   5:' + str(per_10) + '   50:' + str(per_50) + '   95:' + str(per_90))
    plt.plot(df_m1_c['differ'])
    plt.xticks(rotation=90)
    plt.grid(True, axis='y')

    ax = plt.gca()
    for label in ax.get_xticklabels():
        label.set_visible(False)
    for label in ax.get_xticklabels()[1::25]:
        label.set_visible(True)
    for label in ax.get_xticklabels()[-1:]:
        label.set_visible(True)

    plt.legend()
    fn = 'static/straddle_diff_show.jpg'
    plt.savefig(fn)
    plt.cla()

    r = ''
    fn = 'straddle_diff_show.jpg'
    now = str(int(time.time()))
    r = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return r

def straddle_diff_show(basic_m0, basic_m1, startdate, enddate, kind):
    if kind == 'now':
        return straddle_diff_now_show(basic_m0, basic_m1, startdate)
    else:
        return straddle_diff_day_show(basic_m0, basic_m1, startdate, enddate)


def skew_day_show(basic):
    fn = get_dss() + 'opt/skew.csv'
    df = pd.read_csv(fn)
    df = df[df.basic == basic]

    df = df.set_index('date')
    df = df.sort_index()
    rec = df.iloc[-1,:]

    plt.figure(figsize=(13,7))
    plt.title( basic + '   ' + str(rec.atm) )
    plt.xticks(rotation=90)
    plt.plot(df.skew_c, label='c')
    plt.plot(df.skew_p, label='p')
    # plt.plot(df.skew_mean_c, label='mean_c')
    # plt.plot(df.skew_mean_p, label='mean_p')
    plt.plot(df.skew_mean, label='mean')
    plt.legend()

    # ax = plt.gca()
    # for label in ax.get_xticklabels():
    #     label.set_visible(False)
    # for label in ax.get_xticklabels()[1::30]:
    #     label.set_visible(True)
    # for label in ax.get_xticklabels()[-1:]:
    #     label.set_visible(True)

    fn = 'static/skew_show.jpg'
    plt.savefig(fn)
    plt.cla()

    r = ''
    fn = 'skew_show.jpg'
    now = str(int(time.time()))
    r = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return r

def iv_min5_show(symbol_list, date):
    plt.figure(figsize=(13,7))
    plt.title(date)

    for symbol in symbol_list:
        basic = get_contract(symbol).basic
        strike = get_contract(symbol).strike
        opt_flag = get_contract(symbol).opt_flag

        if basic[:2] == 'IO':
            symbol_obj = 'IF' + basic[2:]
        else:
            symbol_obj = basic

        fn = get_dss() + 'fut/put/rec/min5_' + symbol_obj + '.csv'
        df_obj = pd.read_csv(fn)
        df_obj = df_obj.set_index(['date', 'time'])
        df_obj['close_obj'] = df_obj['close']
        df_obj = df_obj[['close_obj']]
        # print(df_obj.tail())

        fn = get_dss() + 'fut/put/rec/min5_' + symbol + '.csv'
        df = pd.read_csv(fn)
        df = df[df.date == date]
        df = df.set_index(['date', 'time'])
        # print(df.tail())

        df = df.join(df_obj)
        df = df.dropna()
        df = df.reset_index()
        df = df.set_index('time')
        # print(df.tail())

        r = 0.03
        fn = get_dss() + 'fut/cfg/opt_mature.csv'
        df2 = pd.read_csv(fn)
        df2 = df2[df2.flag == df2.flag]                 # 筛选出不为空的记录
        df2 = df2.set_index('symbol')
        mature_dict = dict(df2.mature)
        date_mature = mature_dict[ basic ]
        date_mature = datetime.strptime(date_mature, '%Y-%m-%d')
        td = datetime.strptime(date, '%Y-%m-%d')
        T = float((date_mature - td).days) / 365                       # 剩余期限

        iv_list = []
        for i, row in df.iterrows():
            try:
                if opt_flag == 'C':
                    iv = bsm_call_imp_vol(row.close_obj, strike, T, r, row.close)
                else:
                    iv = bsm_put_imp_vol(row.close_obj, strike, T, r, row.close)
                iv_list.append( round(100*iv, 2) )
            except:
                break
        df['iv'] = iv_list
        plt.plot(df.iv, label=symbol)

    plt.xticks(rotation=90)
    plt.legend()

    fn = 'static/iv_min5_show.jpg'
    plt.savefig(fn)
    plt.cla()

    rr = ''
    fn = 'iv_min5_show.jpg'
    now = str(int(time.time()))
    rr = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return rr


def skew_now_show(basic, date):
    # 获得上一个交易日，用于确定差值的基准
    symbol_obj = 'IF' + basic[2:]
    fn = get_dss() + 'fut/bar/day_' + symbol_obj + '.csv'
    df = pd.read_csv(fn)
    df = df[df.date <= date]
    if date == df.iat[-1,0]:
        date_pre = df.iat[-2,0]
    else:
        date_pre = df.iat[-1,0]

    # 取开盘数据的atm
    fn = get_dss() + 'fut/put/rec/min5_' + symbol_obj + '.csv'
    df = pd.read_csv(fn)
    df = df[df.date == date]
    df1 = df[df.time == '09:34:00']
    if len(df1) == 0:
        return '当前日期无数据'

    rec = df1.iloc[0,:]
    obj = rec.close
    gap = 50
    atm = int(round(round(obj*(100/gap)/1E4,2) * 1E4/(100/gap), 0))     # 获得平值

    fn = get_dss() + 'fut/put/rec/min5_' + basic + '-C-' + str(atm) + '.csv'
    df_atm_c = pd.read_csv(fn)
    df_atm_c = df_atm_c[df_atm_c.date == date]

    fn = get_dss() + 'fut/put/rec/min5_' + basic + '-P-' + str(atm) + '.csv'
    df_atm_p = pd.read_csv(fn)
    df_atm_p = df_atm_p[df_atm_p.date == date]

    fn = get_dss() + 'fut/put/rec/min5_' + basic + '-C-' + str(atm+2*gap) + '.csv'
    df_right_c = pd.read_csv(fn)
    df_right_c = df_right_c[df_right_c.date == date]

    fn = get_dss() + 'fut/put/rec/min5_' + basic + '-P-' + str(atm+2*gap) + '.csv'
    df_right_p = pd.read_csv(fn)
    df_right_p = df_right_p[df_right_p.date == date]

    fn = get_dss() + 'fut/put/rec/min5_' + basic + '-C-' + str(atm-2*gap) + '.csv'
    df_left_c = pd.read_csv(fn)
    df_left_c = df_left_c[df_left_c.date == date]

    fn = get_dss() + 'fut/put/rec/min5_' + basic + '-P-' + str(atm-2*gap) + '.csv'
    df_left_p = pd.read_csv(fn)
    df_left_p = df_left_p[df_left_p.date == date]

    df = df.reset_index()
    df_atm_c = df_atm_c.reset_index()
    df_atm_p = df_atm_p.reset_index()
    df_right_c = df_right_c.reset_index()
    df_right_p = df_right_p.reset_index()
    df_left_c = df_left_c.reset_index()
    df_left_p = df_left_p.reset_index()

    r = 0.03
    fn = get_dss() + 'fut/cfg/opt_mature.csv'
    df2 = pd.read_csv(fn)
    df2 = df2[df2.pz == 'IO']                 # 筛选出不为空的记录
    df2 = df2.set_index('symbol')
    mature_dict = dict(df2.mature)
    date_mature = mature_dict[ basic ]
    date_mature = datetime.strptime(date_mature, '%Y-%m-%d')
    td = datetime.strptime(date, '%Y-%m-%d')
    T = float((date_mature - td).days) / 365                       # 剩余期限

    skew_c_list = []
    skew_p_list = []
    # skew_mean_c_list = []
    # skew_mean_p_list = []
    skew_mean_list = []

    for i, row in df.iterrows():
        try:
            S0 = row.close
            iv_atm_c = bsm_call_imp_vol(S0, atm, T, r, df_atm_c.at[i,'close'])
            iv_atm_p = bsm_put_imp_vol(S0, atm, T, r, df_atm_p.at[i,'close'])
            iv_right_c = bsm_call_imp_vol(S0, atm+2*gap, T, r, df_right_c.at[i,'close'])
            iv_right_p = bsm_put_imp_vol(S0, atm+2*gap, T, r, df_right_p.at[i,'close'])
            iv_left_c = bsm_call_imp_vol(S0, atm-2*gap, T, r, df_left_c.at[i,'close'])
            iv_left_p = bsm_put_imp_vol(S0, atm-2*gap, T, r, df_left_p.at[i,'close'])

            skew_c_list.append( round( 100*(iv_right_c - iv_atm_c) / iv_atm_c, 2) )
            skew_p_list.append( round( 100*(iv_left_p - iv_atm_p)  / iv_atm_p, 2) )
            # skew_mean_c_list.append( round( 100*( iv_right_c + iv_right_p - iv_atm_c - iv_atm_p ) / (iv_atm_c + iv_atm_p), 2) )
            # skew_mean_p_list.append( round( 100*( iv_left_c  + iv_left_p  - iv_atm_c - iv_atm_p ) / (iv_atm_c + iv_atm_p), 2) )
            skew_mean_list.append( round( 100*( iv_right_c + iv_right_p - iv_left_c - iv_left_p ) / (iv_left_c + iv_left_p), 2) )
        except:
            break

    plt.figure(figsize=(13,7))
    plt.title( basic + '   ' + str(atm) )
    plt.xticks(rotation=90)
    plt.plot(skew_c_list, label='c')
    plt.plot(skew_p_list, label='p')
    # plt.plot(skew_mean_c_list, label='mean_c')
    # plt.plot(skew_mean_p_list, label='mean_p')
    plt.plot(skew_mean_list, label='mean')
    plt.legend()

    fn = 'static/skew_show.jpg'
    plt.savefig(fn)
    plt.cla()

    rr = ''
    fn = 'skew_show.jpg'
    now = str(int(time.time()))
    rr = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return rr


def skew_show(basic, date, kind):
    if kind == 'now':
        return skew_now_show(basic, date)
    else:
        return skew_day_show(basic)

def iv_show(basic_list, both, date):

    now = datetime.strptime(date, '%Y-%m-%d')
    # now = datetime.now()

    # 本月第一天
    first_day = datetime(now.year, now.month, 1)
    #前一个月最后一天
    pre_month = first_day - timedelta(days = 1)
    today = now.strftime('%Y-%m-%d')
    pre = pre_month.strftime('%Y-%m-%d')

    fn = get_dss() + 'opt/' +  pre[:7] + '_sigma.csv'
    df_pre = pd.read_csv(fn)
    fn = get_dss() + 'opt/' +  today[:7] + '_sigma.csv'
    df_today = pd.read_csv(fn)
    df = pd.concat([df_pre, df_today])
    df = df[df.date <= today]
    # df = df.sort_values('date')

    plt.figure(figsize=(12,7))
    plt.title('iv_' + today)

    for basic in basic_list:
        df1 = df[df.term == basic]
        df1 = df1.drop_duplicates(subset=['date'], keep='last')
        df1 = df1.set_index('date')
        # print(df1.tail())

        plt.plot(df1.c_iv,  label=basic+'_call')
        if both == 'yes':
            plt.plot(df1.p_iv,  label=basic+'_put')

    plt.xticks(rotation=90)
    plt.legend()
    fn = 'static/iv_show.jpg'
    plt.savefig(fn)

    r = ''
    fn = 'iv_show.jpg'
    now = str(int(time.time()))
    r = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return r


if __name__ == '__main__':
    pass
    # yue()
    # dali()
    # opt()
    # mates()
    # smile_show_symbol('IO2008', '2020-08-12')
    # iv_ts()
    # star()
    # hv_show()
    # skew_show('IO2009')
    # open_interest_show('IO2009', 'call', '2020-08-01', 'total')
    # open_interest_show('IO2009', 'call', '2020-08-01', 'distribution')
    # open_interest_show('IO2010', 'put', '2020-08-01', 'distribution')

    # book_min5_show('2020-08-01', [['IO2008-C-4200', '1', 'IO2008-C-4300', '-2'], ['IO2008-C-4600', '1', 'IO2008-C-4700', '-2']])
    # iv_straddle_show('IO2008', ['4900'], '2020-08-01')
    # hs300_spread_show('2020-08-01')
    # straddle_diff_show('IO2009', 'IO2010','2020-09-01', '2020-09-10')
    # straddle_diff_now_show('IO2010', 'IO2011','2020-09-17')

    # skew_show('IO2010', 'no', '2020-09-18', 'now')
    # iv_show(['IO2012','IO2010'], 'no', '2020-09-28')
    # iv_show(['CF101','CF011'], 'no', '2020-09-28')
    iv_min5_show(['IO2011-C-4700','IO2011-C-4800'], '2020-09-28')
