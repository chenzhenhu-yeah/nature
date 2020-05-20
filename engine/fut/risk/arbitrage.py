import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

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


def pcp(df, term):
    r = 0.03
    T = 15/365
    S = 3912

    result = []
    df = df.set_index('strike')
    x_list = sorted(list(set(df.index)))
    # print(x_list)
    n = len(x_list)

    for x in x_list:
        df_c = df[df.type == 'C']
        df_p = df[df.type == 'P']

        row_c = df_c.loc[x,:]
        row_p = df_p.loc[x,:]

        pSc = int( row_p.LastPrice + S -row_c.LastPrice )
        pSc_final = int( pSc * (1 + r*T) )
        result.append( [S, row_c.LastPrice, row_p.LastPrice, x, pSc, pSc_final, float(x)-pSc_final] )

    return result

def pcp_m(df):
    r = []
    gap = 50
    term_list = ['m2007']
    for term in term_list:
        df1 = df[df.Instrument.str.startswith(term)]
        df1 = df1.set_index('Instrument')
        obj_price = df1.at[term, 'LastPrice']
        print(obj_price)
        df1 = df1.drop(index=[term])
        df1['strike'] = df1.index.str.slice(8,12)
        df1['type'] = df1.index.str.slice(6,7)

        r += pcp(df1, term)

        # break

    df2 = pd.DataFrame(r, columns=['S', 'call', 'put', 'X', 'pSc', 'pSc_final', 'diff'])
    fn = get_dss() + 'opt/pcp_m.csv'
    df2.to_csv(fn, index=False)

if __name__ == '__main__':
    year = '2020'
    dt = '2020-05-19'

    fn = get_dss() + 'opt/' + dt[:7] + '.csv'
    df = pd.read_csv(fn)
    df = df[df.Localtime.str.slice(0,10) == dt]
    # die_m(df)
    # die_RM(df)

    pcp_m(df)
