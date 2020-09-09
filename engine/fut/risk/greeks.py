import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import os
from datetime import datetime

import math
from math import sqrt, log
from scipy import stats
import scipy.stats as si

from nature import get_dss, get_inx
from nature import bsm_call_value, bsm_put_value, bsm_call_imp_vol, bsm_put_imp_vol

# import warnings
# warnings.filterwarnings('error')

def bsm_vega(S0, K, T, r, sigma):
    """
    Vega 计算
    """
    try:
        S0 = float(S0)
        d1 = (np.log(S0/K)) + (r+0.5*sigma**2)*T /(sigma*sqrt(T))
        vega = S0 * stats.norm.cdf(d1, 0, 1) * np.sqrt(T)
    except:
        vega = float('nan')

    return vega

# ------------------------------------------------------------------------------------

def d(s,k,r,T,sigma):
    d1 = (np.log(s / k) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return (d1,d2)

def delta(s,k,r,T,sigma,n):
    try:
        d1 = d(s,k,r,T,sigma)[0]
        delta0 = n * si.norm.cdf(n * d1)
    except:
        delta0 = float('nan')

    return delta0


def gamma(s,k,r,T,sigma):
    try:
        d1 = d(s,k,r,T,sigma)[0]
        gamma0 = si.norm.pdf(d1) / (s * sigma * np.sqrt(T))
    except:
        gamma0 = float('nan')

    return gamma0

def vega(s,k,r,T,sigma):
    try:
        d1 = d(s,k,r,T,sigma)[0]
        vega0 = (s * si.norm.pdf(d1) * np.sqrt(T)) / 100
    except:
        vega0 = float('nan')
    return vega0

def theta(s,k,r,T,sigma,n):
    try:
        d1 = d(s,k,r,T,sigma)[0]
        d2 = d(s,k,r,T,sigma)[1]
        # theta0 = (-1 * (s * si.norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) - n * r * k * np.exp(-r * T) * si.norm.cdf(n * d2)) / 365
        theta0 = (-1 * (s * si.norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) - n * r * k * np.exp(-r * T) * si.norm.cdf(n * d2)) / 100
    except:
        theta0 = float('nan')

    return theta0


def calc_greeks_common(symbol, row, S0, is_call, r, today, mature_dict, term, K):
    """
    计算新下载数据的希腊字母值，保存到yyyy-dd_greeks.csv文件
    Parameters:
    S0: 标的价格
    K:  行权价格
    T:  剩余期限，已做年化处理
    r:  固定无风险短期利率
    C0：期权价格
    """

    C0 = float( row['LastPrice'] )                                 # 期权价格
    date_mature = mature_dict[ term ]
    date_mature = datetime.strptime(date_mature, '%Y-%m-%d')
    td = datetime.strptime(today, '%Y-%m-%d')
    T = float((date_mature - td).days) / 365                       # 剩余期限
    # print(S0, K, C0, T)

    if is_call == True:
        n = 1
        iv = bsm_call_imp_vol(S0, K, T, r, C0)
    else:
        n = -1
        iv = bsm_put_imp_vol(S0, K, T, r, C0)

    row['obj'] = S0
    row['delta'] = delta(S0,K,r,T,iv,n)
    row['gamma'] = gamma(S0,K,r,T,iv)
    row['theta'] = theta(S0,K,r,T,iv,n)
    row['vega'] = vega(S0,K,r,T,iv)
    row['iv'] = iv

    df2 = pd.DataFrame([row])
    df2.index.name = 'Instrument'
    fn2 = get_dss() + 'opt/' + today[:7] + '_greeks.csv'
    if os.path.exists(fn2):
        df2.to_csv(fn2, mode='a', header=None)
    else:
        df2.to_csv(fn2)

def calc_greeks_IO(df, today, r):
    df1 = df[df.index.str.startswith('IO')]
    # print(df1.head())

    # mature_dict = {'IO2002':'2020-02-21','IO2003':'2020-03-20','IO2004':'2020-04-17','IO2005':'2020-05-15','IO2006':'2020-06-19',
    #                'IO2007':'2020-07-17','IO2009':'2020-09-18','IO2012':'2020-12-18','IO2103':'2021-03-19'}

    fn = get_dss() + 'fut/cfg/opt_mature.csv'
    df2 = pd.read_csv(fn)
    df2 = df2[df2.pz == 'IO']                 # 筛选出不为空的记录
    df2 = df2.set_index('symbol')
    mature_dict = dict(df2.mature)
    # print(mature_dict)

    for symbol, row in df1.iterrows():
        if symbol[:6] not in mature_dict.keys():
            continue
        symbol_obj = 'IF' + symbol[2:6]
        df_obj = df[df.index == symbol_obj]
        term = symbol[:6]
        if len(df_obj) == 1:
            K = float( symbol[9:] )                                       # 行权价格
            S0 = df_obj.at[symbol_obj,'LastPrice']                      # 标的价格

            is_call = True if symbol[7] == 'C' else False
            calc_greeks_common(symbol, row, S0, is_call, r, today, mature_dict, term, K)

        # break

def calc_greeks_m(df, today, r):
    df1 = df[df.index.str.startswith('m')]
    # print(df1.head())

    # mature_dict = {'m2007':'2020-06-05','m2009':'2020-08-07','m2011':'2020-10-15',
    #                'm2101':'2020-12-07','m2103':'2021-02-05','m2105':'2021-04-07',}

    fn = get_dss() + 'fut/cfg/opt_mature.csv'
    df2 = pd.read_csv(fn)
    df2 = df2[df2.pz == 'm']                 # 筛选出不为空的记录
    df2 = df2.set_index('symbol')
    mature_dict = dict(df2.mature)
    # print(mature_dict)

    for symbol, row in df1.iterrows():
        term = symbol[:5]
        if term not in mature_dict.keys():
            continue
        if len(symbol) <= 5:
            continue
        symbol_obj = term
        df_obj = df[df.index == symbol_obj]

        if len(df_obj) == 1:
            K = float( symbol[8:] )                                       # 行权价格
            # S0 = df_obj.at[symbol_obj,'LastPrice']                         # 标的价格
            ask_price = float(df_obj.at[symbol_obj,'AskPrice'])
            ask_price = 0 if ask_price < 0 else ask_price
            ask_price = 100E4 if ask_price > 100E4 else ask_price
            bid_price = float(df_obj.at[symbol_obj,'BidPrice'])
            bid_price = 0 if bid_price < 0 else bid_price
            bid_price = 100E4 if bid_price > 100E4 else bid_price
            S0 = (ask_price + bid_price) * 0.5                            # 标的价格

            is_call = True if symbol[6] == 'C' else False
            calc_greeks_common(symbol, row, S0, is_call, r, today, mature_dict, term, K)

        # break

def calc_greeks_RM(df, today, r):
    df1 = df[df.index.str.startswith('RM')]
    # print(df1.head())

    # mature_dict = {'RM007':'2020-06-03','RM009':'2020-08-05','RM011':'2020-10-13',
    #                'RM101':'2020-12-03','RM103':'2021-02-03',}
    fn = get_dss() + 'fut/cfg/opt_mature.csv'
    df2 = pd.read_csv(fn)
    df2 = df2[df2.pz == 'RM']                 # 筛选出不为空的记录
    df2 = df2.set_index('symbol')
    mature_dict = dict(df2.mature)
    # print(mature_dict)

    for symbol, row in df1.iterrows():
        term = symbol[:5]
        if term not in mature_dict.keys():
            continue
        if len(symbol) <= 5:
            continue
        symbol_obj = term
        df_obj = df[df.index == symbol_obj]

        if len(df_obj) == 1:
            K = float( symbol[6:] )                                       # 行权价格
            # S0 = df_obj.at[symbol_obj,'LastPrice']                         # 标的价格
            ask_price = float(df_obj.at[symbol_obj,'AskPrice'])
            ask_price = 0 if ask_price < 0 else ask_price
            ask_price = 100E4 if ask_price > 100E4 else ask_price
            bid_price = float(df_obj.at[symbol_obj,'BidPrice'])
            bid_price = 0 if bid_price < 0 else bid_price
            bid_price = 100E4 if bid_price > 100E4 else bid_price
            S0 = (ask_price + bid_price) * 0.5                            # 标的价格

            is_call = True if symbol[5] == 'C' else False
            calc_greeks_common(symbol, row, S0, is_call, r, today, mature_dict, term, K)

        # break

def calc_greeks_MA(df, today, r):
    df1 = df[df.index.str.startswith('MA')]
    # print(df1.head())

    # mature_dict = {'MA007':'2020-06-03','MA009':'2020-08-05','MA011':'2020-10-13',
    #                'MA101':'2020-12-03','MA103':'2021-02-03',}
    fn = get_dss() + 'fut/cfg/opt_mature.csv'
    df2 = pd.read_csv(fn)
    df2 = df2[df2.pz == 'MA']                 # 筛选出不为空的记录
    df2 = df2.set_index('symbol')
    mature_dict = dict(df2.mature)
    # print(mature_dict)

    for symbol, row in df1.iterrows():
        term = symbol[:5]
        if term not in mature_dict.keys():
            continue
        if len(symbol) <= 5:
            continue
        symbol_obj = term
        df_obj = df[df.index == symbol_obj]

        if len(df_obj) == 1:
            K = float( symbol[6:] )                                       # 行权价格
            # S0 = df_obj.at[symbol_obj,'LastPrice']                         # 标的价格
            ask_price = float(df_obj.at[symbol_obj,'AskPrice'])
            ask_price = 0 if ask_price < 0 else ask_price
            ask_price = 100E4 if ask_price > 100E4 else ask_price
            bid_price = float(df_obj.at[symbol_obj,'BidPrice'])
            bid_price = 0 if bid_price < 0 else bid_price
            bid_price = 100E4 if bid_price > 100E4 else bid_price
            S0 = (ask_price + bid_price) * 0.5                            # 标的价格

            is_call = True if symbol[5] == 'C' else False
            calc_greeks_common(symbol, row, S0, is_call, r, today, mature_dict, term, K)

        # break

def calc_greeks_CF(df, today, r):
    df1 = df[df.index.str.startswith('CF')]
    # print(df1.head())

    # mature_dict = {'CF007':'2020-06-03','CF009':'2020-08-05','CF011':'2020-10-13',
    #                'CF101':'2020-12-03','CF103':'2021-02-03',}
    fn = get_dss() + 'fut/cfg/opt_mature.csv'
    df2 = pd.read_csv(fn)
    df2 = df2[df2.pz == 'CF']                 # 筛选出不为空的记录
    df2 = df2.set_index('symbol')
    mature_dict = dict(df2.mature)
    # print(mature_dict)

    for symbol, row in df1.iterrows():
        term = symbol[:5]
        if term not in mature_dict.keys():
            continue
        if len(symbol) <= 5:
            continue
        symbol_obj = term
        df_obj = df[df.index == symbol_obj]

        if len(df_obj) == 1:
            K = float( symbol[6:] )                                       # 行权价格
            # S0 = df_obj.at[symbol_obj,'LastPrice']                         # 标的价格
            ask_price = float(df_obj.at[symbol_obj,'AskPrice'])
            ask_price = 0 if ask_price < 0 else ask_price
            ask_price = 100E4 if ask_price > 100E4 else ask_price
            bid_price = float(df_obj.at[symbol_obj,'BidPrice'])
            bid_price = 0 if bid_price < 0 else bid_price
            bid_price = 100E4 if bid_price > 100E4 else bid_price
            S0 = (ask_price + bid_price) * 0.5                            # 标的价格

            is_call = True if symbol[5] == 'C' else False
            calc_greeks_common(symbol, row, S0, is_call, r, today, mature_dict, term, K)

        # break

def calc_greeks():
    r = 0.03
    now = datetime.now()
    # today = now.strftime('%Y-%m-%d %H:%M:%S')
    today = now.strftime('%Y-%m-%d')
    # today = '2020-05-29'

    fn = get_dss() + 'opt/' + today[:7] + '.csv'
    df = pd.read_csv(fn)
    df = df.drop_duplicates(subset=['Instrument'], keep='last')
    df = df.set_index('Instrument')
    # print(df.head())

    if len(df) > 0:
        calc_greeks_IO(df, today, r)
        calc_greeks_m(df, today, r)
        calc_greeks_RM(df, today, r)
        calc_greeks_MA(df, today, r)
        calc_greeks_CF(df, today, r)

if __name__ == '__main__':
    # calc_greeks()
    pass
