
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


import os
from datetime import datetime

import sys
import math
from math import sqrt, log
from scipy import stats
import scipy.stats as si

from nature import get_dss, get_inx

def die_forward(df, term, type, gap):
    # 正向蝶式，若权利金支出<0，则存在无风险套利
    result = []
    df = df.set_index('strike')
    x_list = sorted(list(set(df.index)))
    # print(x_list)
    n = len(x_list)

    for i in range(n):
        if i+1+1 > n-1:
            break
        s1 = x_list[i]
        s2 = x_list[i+1]
        s3 = x_list[i+1+1]

        c1 = float( df.at[s1,'AskPrice'] )
        c2 = float( df.at[s2,'BidPrice'] )
        c3 = float( df.at[s3,'AskPrice'] )
        if c1 == 0 or c2 == 0 or c3 == 0:
            break
        cost = round(c1 - 2*c2 + c3, 2)
        result.append( [term, type, s1, c1, s2, c2, s3, c3, cost] )

    for i in range(n):
        if i+2+2 > n-1:
            break
        s1 = x_list[i]
        s2 = x_list[i+2]
        s3 = x_list[i+2+2]

        c1 = float( df.at[s1,'AskPrice'] )
        c2 = float( df.at[s2,'BidPrice'] )
        c3 = float( df.at[s3,'AskPrice'] )
        if c1 == 0 or c2 == 0 or c3 == 0:
            break
        cost = round(c1 - 2*c2 + c3, 2)
        result.append( [term, type, s1, c1, s2, c2, s3, c3, cost] )

    for i in range(n):
        if i+3+3 > n-1:
            break
        s1 = x_list[i]
        s2 = x_list[i+3]
        s3 = x_list[i+3+3]

        c1 = float( df.at[s1,'AskPrice'] )
        c2 = float( df.at[s2,'BidPrice'] )
        c3 = float( df.at[s3,'AskPrice'] )
        if c1 == 0 or c2 == 0 or c3 == 0:
            break
        cost = round(c1 - 2*c2 + c3, 2)
        result.append( [term, type, s1, c1, s2, c2, s3, c3, cost] )

    return result

def die_m(df):
    r = []
    gap = 50
    term_list = ['m2007','m2009','m2101']
    for term in term_list:
        # df = df[df.Instrument.str.slice(0,5).isin(term_list)]
        df1 = df[df.Instrument.str.startswith(term)]
        df1 = df1.set_index('Instrument')
        obj_price = df1.at[term, 'LastPrice']
        # print(obj_price)
        df1 = df1.drop(index=[term])
        df1['strike'] = df1.index.str.slice(8,12)
        df1['type'] = df1.index.str.slice(6,7)
        # print(df1.head())
        df1_c = df1[df1.type == 'C']
        df1_p = df1[df1.type == 'P']
        # print(df1_c.head())
        # print(df1_p.head())

        r += die_forward(df1_c, term, 'call', gap)
        r += die_forward(df1_p, term, 'put', gap)

        break
    df2 = pd.DataFrame(r, columns=['term', 'c/p', 's1', 'c1', 's2', 'c2', 's3', 'c3', 'cost'])
    df2 = df2[(df2.cost < 3) & (df2.cost != float('-inf'))]
    fn = get_dss() + 'opt/die_m.csv'
    df2.to_csv(fn, index=False)

def die_RM(df):
    r = []
    gap = 25
    term_list = ['RM007','RM009','RM101']
    for term in term_list:
        # df = df[df.Instrument.str.slice(0,5).isin(term_list)]
        df1 = df[df.Instrument.str.startswith(term)]
        df1 = df1.set_index('Instrument')
        obj_price = df1.at[term, 'LastPrice']
        # print(obj_price)
        df1 = df1.drop(index=[term])
        df1['strike'] = df1.index.str.slice(6,10)
        df1['type'] = df1.index.str.slice(5,6)
        # print(df1.head())
        df1_c = df1[df1.type == 'C']
        df1_p = df1[df1.type == 'P']
        # print(df1_c.head())
        # print(df1_p.head())

        r += die_forward(df1_c, term, 'call', gap)
        r += die_forward(df1_p, term, 'put', gap)

        # break
    df2 = pd.DataFrame(r, columns=['term', 'c/p', 's1', 'c1', 's2', 'c2', 's3', 'c3', 'cost'])
    df2 = df2[(df2.cost < 3) & (df2.cost != float('-inf'))]
    fn = get_dss() + 'opt/die_RM.csv'
    df2.to_csv(fn, index=False)

# 正向套利
def pcp(df_all, term_list, mature_dict, today):
    result = []
    for term in term_list:
        df = df_all[df_all.index.str.startswith(term)]
        # print(df1)

        r = 0.03
        date_mature = mature_dict[ term ]
        date_mature = datetime.strptime(date_mature, '%Y-%m-%d')
        td = datetime.strptime(today, '%Y-%m-%d')
        T = float((date_mature - td).days) / 365                       # 剩余期限
        if T == 0 :
            break

        df = df.set_index('strike')
        x_list = sorted(list(set(df.index)))
        # print(x_list)
        # n = len(x_list)

        for x in x_list:
            df_c = df[df.type == 'C']
            df_p = df[df.type == 'P']

            row_c = df_c.loc[x,:]
            row_p = df_p.loc[x,:]

            S = float(row_c.obj)
            p = float(row_p.AskPrice)
            c = float(row_c.BidPrice)
            # print(p, c, float("inf"))
            if p > 1E8 or p == 0:
                continue
            if c > 1E8 or c == 0:
                continue
            pSc = int( p + S - c )
            # pSc_final = int( pSc * (1 + r*T) )
            pSc_final = pSc
            diff = float(x)-pSc_final
            rt = diff/(S*2*0.1)/T
            if rt > 0.01:
                result.append( [today, term, x, S, c, p, pSc, pSc_final, diff, rt] )

        # break

    fn = get_dss() + 'opt/pcp.csv'
    df2 = pd.DataFrame(result, columns=['date', 'term', 'X', 'S', 'call', 'put', 'pSc', 'pSc_final', 'diff', 'rt'])
    if os.path.exists(fn):
        df2.to_csv(fn, index=False, mode='a', header=False)
    else:
        df2.to_csv(fn, index=False)


# 反向套利
# def pcp(df_all, term_list, mature_dict, today):
#     result = []
#     for term in term_list:
#         df = df_all[df_all.index.str.startswith(term)]
#         # print(df1)
#
#         r = 0.03
#         date_mature = mature_dict[ term ]
#         date_mature = datetime.strptime(date_mature, '%Y-%m-%d')
#         td = datetime.strptime(today, '%Y-%m-%d')
#         T = float((date_mature - td).days) / 365                       # 剩余期限
#         if T == 0 :
#             break
#
#         df = df.set_index('strike')
#         x_list = sorted(list(set(df.index)))
#         # print(x_list)
#         # n = len(x_list)
#
#         for x in x_list:
#             df_c = df[df.type == 'C']
#             df_p = df[df.type == 'P']
#
#             row_c = df_c.loc[x,:]
#             row_p = df_p.loc[x,:]
#
#             S = float(row_c.obj)
#             p = float(row_p.BidPrice)
#             c = float(row_c.AskPrice)
#             # print(p, c, float("inf"))
#             if p > 1E8 or p == 0:
#                 continue
#             if c > 1E8 or c == 0:
#                 continue
#             pSc = int( p + S - c )
#             pSc_final = int( pSc * (1 + r*T) )
#             diff = float(x)-pSc_final
#             rt = diff/(S*2*0.1)/T
#             if rt < -0.05:
#                 result.append( [today, term, x, S, c, p, pSc, pSc_final, diff, rt] )
#
#         # break
#
#     fn = get_dss() + 'opt/pcp.csv'
#     df2 = pd.DataFrame(result, columns=['date', 'term', 'X', 'S', 'call', 'put', 'pSc', 'pSc_final', 'diff', 'rt'])
#     if os.path.exists(fn):
#         df2.to_csv(fn, index=False, mode='a', header=False)
#     else:
#         df2.to_csv(fn, index=False)


def pcp_m(df_all, today):
    mature_dict = {'m2007':'2020-06-05','m2009':'2020-08-07','m2011':'2020-10-15',
                   'm2101':'2020-12-07','m2103':'2021-02-05','m2105':'2021-04-07',}

    df = df_all[df_all.index.str.startswith('m')]
    term_list = sorted( list(set([ x[:5] for x in df.index ])) )
    df['strike'] = df.index.str.slice(8)
    df['type'] = df.index.str.get(6)
    # print(df.head())
    # print(term_list)
    pcp(df, term_list, mature_dict, today)


def pcp_RM(df_all, today):
    mature_dict = {'RM007':'2020-06-03','RM009':'2020-08-05','RM011':'2020-10-13',
                   'RM101':'2020-12-03','RM103':'2021-02-03',}

    df = df_all[df_all.index.str.startswith('RM')]
    term_list = sorted( list(set([ x[:5] for x in df.index ])) )
    df['strike'] = df.index.str.slice(6)
    df['type'] = df.index.str.get(5)
    # print(df.head())
    # print(term_list)
    pcp(df, term_list, mature_dict, today)


def pcp_MA(df_all, today):
    mature_dict = {'MA007':'2020-06-03','MA009':'2020-08-05','MA011':'2020-10-13',
                   'MA101':'2020-12-03','MA103':'2021-02-03',}

    df = df_all[df_all.index.str.startswith('MA')]
    term_list = sorted( list(set([ x[:5] for x in df.index ])) )
    df['strike'] = df.index.str.slice(6)
    df['type'] = df.index.str.get(5)
    # print(df.head())
    # print(term_list)
    pcp(df, term_list, mature_dict, today)


def pcp_CF(df_all, today):
    mature_dict = {'CF007':'2020-06-03','CF009':'2020-08-05','CF011':'2020-10-13',
                   'CF101':'2020-12-03','CF103':'2021-02-03',}

    df = df_all[df_all.index.str.startswith('CF')]
    term_list = sorted( list(set([ x[:5] for x in df.index ])) )
    df['strike'] = df.index.str.slice(6)
    df['type'] = df.index.str.get(5)
    # print(df.head())
    # print(term_list)
    pcp(df, term_list, mature_dict, today)


def calc_pcp():

    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    # today = '2020-05-22'

    fn = get_dss() + 'opt/' + today[:7] + '_greeks.csv'
    df = pd.read_csv(fn)
    df = df[df.Localtime > today+' 15:00:00']
    df = df.set_index('Instrument')

    pcp_m(df, today)
    pcp_RM(df, today)
    pcp_MA(df, today)
    pcp_CF(df, today)


if __name__ == '__main__':

    calc_pcp()

    # die_m(df)
    # die_RM(df)
