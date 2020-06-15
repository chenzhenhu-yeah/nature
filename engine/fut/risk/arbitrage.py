
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

def pcp(df_all, term_list, mature_dict, today, obj_dict):
    result = []
    for term in term_list:
        try:
            if term not in mature_dict:
                continue
            if term not in obj_dict:
                continue

            S = float( obj_dict[term] )
            df = df_all[df_all.index.str.startswith(term)]

            date_mature = mature_dict[ term ]
            date_mature = datetime.strptime(date_mature, '%Y-%m-%d')
            td = datetime.strptime(today, '%Y-%m-%d')
            T = float((date_mature - td).days) / 365                       # 剩余期限
            if T == 0 or T >= 0.2:
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

                # 正向套利
                cb = float(row_c.BidPrice)
                pa = float(row_p.AskPrice)
                if cb > 1E8 or cb == 0 or pa > 1E8 or pa == 0:
                    pass
                else:
                    pSc_forward = int( pa + S - cb )
                    diff_forward = float(x) - pSc_forward
                    rt_forward = round( diff_forward/(S*2*0.1)/T, 2 )
                    if rt_forward > 0.01:
                        result.append( [today, 'forward', term, x, S, cb, pa, pSc_forward, diff_forward, rt_forward] )

                # 反向套利
                ca = float(row_c.AskPrice)
                pb = float(row_p.BidPrice)
                if ca > 1E8 or ca == 0 or pb > 1E8 or pb == 0:
                    pass
                else:
                    pSc_back = int( pb + S - ca )
                    diff_back = float(x) - pSc_back
                    rt_back = round( diff_back/(S*2*0.1)/T, 2 )
                    if rt_back < -0.01:
                        result.append( [today, 'back', term, x, S, ca, pb, pSc_back, diff_back, rt_back] )

        except:
            continue

    return result


# 这段代码已编写完毕，待测试！！！
def pcp_IO(df_all, today):
    mature_dict = {'IO2006':'2020-06-19','IO2007':'2020-07-17','IO2009':'2020-09-18',
                   'IO2012':'2020-12-18','IO2103':'2021-03-19'}

    df = df_all[df_all.index.str.startswith('IO') ]
    term_list = sorted( list(set([ x[:6] for x in df.index ])) )
    # print(term_list)
    obj_dict = {}
    for term in term_list:
        obj = 'IF' + term[2:]
        if obj in df_all.index:
            # print( term, df_all.at[obj, 'LastPrice'] )
            obj_dict[term] = df_all.at[obj, 'LastPrice']
    # print(obj_dict)

    # 删除期货合约
    df = df[df.index.str.len() > 7]
    df['strike'] = df.index.str.slice(9)
    df['type'] = df.index.str.get(7)
    # print(df.head())
    # print(len(df))

    return pcp(df, term_list, mature_dict, today, obj_dict)

def pcp_m(df_all, today):
    mature_dict = {'m2007':'2020-06-05','m2009':'2020-08-07','m2011':'2020-10-15',
                   'm2101':'2020-12-07','m2103':'2021-02-05','m2105':'2021-04-07',}

    df = df_all[df_all.index.str.startswith('m') ]
    term_list = sorted( list(set([ x[:5] for x in df.index ])) )
    # print(term_list)
    obj_dict = {}
    for term in term_list:
        # print( term, df.at[term, 'LastPrice'] )
        if term in df.index:
            obj_dict[term] = df.at[term, 'LastPrice']
    # print(obj_dict)

    # 删除期货合约
    df = df[df.index.str.len() > 6]
    df['strike'] = df.index.str.slice(8)
    df['type'] = df.index.str.get(6)
    # print(df.head())
    # print(len(df))

    return pcp(df, term_list, mature_dict, today, obj_dict)

def pcp_RM(df_all, today):
    mature_dict = {'RM007':'2020-06-03','RM009':'2020-08-05','RM011':'2020-10-13',
                   'RM101':'2020-12-03','RM103':'2021-02-03',}

    df = df_all[df_all.index.str.startswith('RM')]
    term_list = sorted( list(set([ x[:5] for x in df.index ])) )
    # print(term_list)

    obj_dict = {}
    for term in term_list:
        # print( term, df.at[term, 'LastPrice'] )
        if term in df.index:
            obj_dict[term] = df.at[term, 'LastPrice']
    # print(obj_dict)

    # 删除期货合约
    df = df[df.index.str.len() > 6]

    df['strike'] = df.index.str.slice(6)
    df['type'] = df.index.str.get(5)
    # print(df.head())

    return pcp(df, term_list, mature_dict, today, obj_dict)


def pcp_MA(df_all, today):
    mature_dict = {'MA007':'2020-06-03','MA009':'2020-08-05','MA011':'2020-10-13',
                   'MA101':'2020-12-03','MA103':'2021-02-03',}

    df = df_all[df_all.index.str.startswith('MA')]
    term_list = sorted( list(set([ x[:5] for x in df.index ])) )
    # print(term_list)

    obj_dict = {}
    for term in term_list:
        # print( term, df.at[term, 'LastPrice'] )
        if term in df.index:
            obj_dict[term] = df.at[term, 'LastPrice']
    # print(obj_dict)

    # 删除期货合约
    df = df[df.index.str.len() > 6]

    df['strike'] = df.index.str.slice(6)
    df['type'] = df.index.str.get(5)
    # print(df.head())

    return pcp(df, term_list, mature_dict, today, obj_dict)


def pcp_CF(df_all, today):
    mature_dict = {'CF007':'2020-06-03','CF009':'2020-08-05','CF011':'2020-10-13',
                   'CF101':'2020-12-03','CF103':'2021-02-03',}

    df = df_all[df_all.index.str.startswith('CF')]
    term_list = sorted( list(set([ x[:5] for x in df.index ])) )
    # print(term_list)

    obj_dict = {}
    for term in term_list:
        # print( term, df.at[term, 'LastPrice'] )
        if term in df.index:
            obj_dict[term] = df.at[term, 'LastPrice']
    # print(obj_dict)

    # 删除期货合约
    df = df[df.index.str.len() > 6]

    df['strike'] = df.index.str.slice(6)
    df['type'] = df.index.str.get(5)
    # print(df.head())

    return pcp(df, term_list, mature_dict, today, obj_dict)

def calc_pcp():
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    # today = '2020-06-12'

    # fn = get_dss() + 'opt/' + today[:7] + '_greeks.csv'
    fn = get_dss() + 'opt/' + today[:7] + '.csv'
    df = pd.read_csv(fn)
    df = df[df.Localtime > today+' 11:00:00']
    df = df[df.Localtime < today+' 12:00:00']
    df = df.set_index('Instrument')

    r = []
    if len(df) > 0:
        r += pcp_IO(df, today)
        r += pcp_m(df, today)
        r += pcp_RM(df, today)
        r += pcp_MA(df, today)
        r += pcp_CF(df, today)

        fn = get_dss() + 'opt/pcp.csv'
        df2 = pd.DataFrame(r, columns=['date', 'type', 'term', 'X', 'S', 'call', 'put', 'pSc', 'diff', 'rt'])
        df2 = df2.sort_values('type')
        df2.to_csv(fn, index=False)
        return True
    else:
        return False


def die_forward(df, term, type, today):
    # 正向蝶式，若权利金支出<0，则存在无风险套利
    result = []
    df['strike'] = df['strike'].astype('int')
    df = df.set_index('strike')
    x_list = sorted( list(set(df.index)) )
    # print(x_list)
    n = len(x_list)

    for i in range(n):
        if i+1+1 > n-1:
            break
        s1 = x_list[i]
        s2 = x_list[i+1]
        s3 = x_list[i+1+1]
        if s2 -s1 != s3 -s2:
            continue

        c1 = float( df.at[s1,'AskPrice'] )
        c2 = float( df.at[s2,'BidPrice'] )
        c3 = float( df.at[s3,'AskPrice'] )
        if c1 == 0 or c2 == 0 or c3 == 0:
            break
        cost = round(c1 - 2*c2 + c3, 2)
        result.append( [today, term, type, s1, c1, s2, c2, s3, c3, cost] )

    for i in range(n):
        if i+2+2 > n-1:
            break
        s1 = x_list[i]
        s2 = x_list[i+2]
        s3 = x_list[i+2+2]
        if s2 -s1 != s3 -s2:
            continue

        c1 = float( df.at[s1,'AskPrice'] )
        c2 = float( df.at[s2,'BidPrice'] )
        c3 = float( df.at[s3,'AskPrice'] )
        if c1 == 0 or c2 == 0 or c3 == 0:
            break
        cost = round(c1 - 2*c2 + c3, 2)
        result.append( [today, term, type, s1, c1, s2, c2, s3, c3, cost] )

    for i in range(n):
        if i+3+3 > n-1:
            break
        s1 = x_list[i]
        s2 = x_list[i+3]
        s3 = x_list[i+3+3]
        if s2 -s1 != s3 -s2:
            continue

        c1 = float( df.at[s1,'AskPrice'] )
        c2 = float( df.at[s2,'BidPrice'] )
        c3 = float( df.at[s3,'AskPrice'] )
        if c1 == 0 or c2 == 0 or c3 == 0:
            break
        cost = round(c1 - 2*c2 + c3, 2)
        result.append( [today, term, type, s1, c1, s2, c2, s3, c3, cost] )

    return result

def die_IO(df_all, today):
    r = []

    df = df_all[df_all.index.str.startswith('IO')]
    term_list = sorted( list(set([ x[:6] for x in df.index ])) )
    print(term_list)

    # 删除期货合约
    df = df[df.index.str.len() > 7]
    df['strike'] = df.index.str.slice(9)
    df['type'] = df.index.str.get(7)
    # print(df.head())

    for term in term_list:
        df1 = df[df.index.str.startswith(term)]
        df1_c = df1[df1.type == 'C']
        df1_p = df1[df1.type == 'P']

        r += die_forward(df1_c, term, 'call', today)
        r += die_forward(df1_p, term, 'put', today)

        # break

    return r

def die_m(df_all, today):
    r = []

    df = df_all[df_all.index.str.startswith('m')]
    term_list = sorted( list(set([ x[:5] for x in df.index ])) )
    print(term_list)

    obj_dict = {}
    for term in term_list:
        if term in df.index:
            obj_dict[term] = df.at[term, 'LastPrice']
    # print(obj_dict)

    # 删除期货合约
    df = df[df.index.str.len() > 6]
    df['strike'] = df.index.str.slice(8)
    df['type'] = df.index.str.get(6)
    # print(df.head())

    for term in term_list:
        df1 = df[df.index.str.startswith(term)]
        if term not in obj_dict:
            continue

        obj_price = float( obj_dict[term] )
        df1_c = df1[df1.type == 'C']
        df1_p = df1[df1.type == 'P']

        r += die_forward(df1_c, term, 'call', today)
        r += die_forward(df1_p, term, 'put', today)

        # break

    return r

def die_RM(df_all, today):
    r = []

    df = df_all[df_all.index.str.startswith('RM')]
    term_list = sorted( list(set([ x[:5] for x in df.index ])) )
    print(term_list)

    obj_dict = {}
    for term in term_list:
        if term in df.index:
            obj_dict[term] = df.at[term, 'LastPrice']
    # print(obj_dict)

    # 删除期货合约
    df = df[df.index.str.len() > 6]
    df['strike'] = df.index.str.slice(6)
    df['type'] = df.index.str.get(5)
    # print(df.head())

    for term in term_list:
        df1 = df[df.index.str.startswith(term)]
        if term not in obj_dict:
            continue

        obj_price = float( obj_dict[term] )
        df1_c = df1[df1.type == 'C']
        df1_p = df1[df1.type == 'P']

        r += die_forward(df1_c, term, 'call', today)
        r += die_forward(df1_p, term, 'put', today)

    return r

def die_MA(df_all, today):
    r = []

    df = df_all[df_all.index.str.startswith('MA')]
    term_list = sorted( list(set([ x[:5] for x in df.index ])) )
    print(term_list)

    obj_dict = {}
    for term in term_list:
        if term in df.index:
            obj_dict[term] = df.at[term, 'LastPrice']
    # print(obj_dict)

    # 删除期货合约
    df = df[df.index.str.len() > 6]
    df['strike'] = df.index.str.slice(6)
    df['type'] = df.index.str.get(5)
    # print(df.head())

    for term in term_list:
        df1 = df[df.index.str.startswith(term)]
        if term not in obj_dict:
            continue

        obj_price = float( obj_dict[term] )
        df1_c = df1[df1.type == 'C']
        df1_p = df1[df1.type == 'P']

        r += die_forward(df1_c, term, 'call', today)
        r += die_forward(df1_p, term, 'put', today)

    return r

def die_CF(df_all, today):
    r = []

    df = df_all[df_all.index.str.startswith('CF')]
    term_list = sorted( list(set([ x[:5] for x in df.index ])) )
    print(term_list)

    obj_dict = {}
    for term in term_list:
        if term in df.index:
            obj_dict[term] = df.at[term, 'LastPrice']
    # print(obj_dict)

    # 删除期货合约
    df = df[df.index.str.len() > 6]
    df['strike'] = df.index.str.slice(6)
    df['type'] = df.index.str.get(5)
    # print(df.head())

    for term in term_list:
        df1 = df[df.index.str.startswith(term)]
        if term not in obj_dict:
            continue

        obj_price = float( obj_dict[term] )
        df1_c = df1[df1.type == 'C']
        df1_p = df1[df1.type == 'P']

        r += die_forward(df1_c, term, 'call', today)
        r += die_forward(df1_p, term, 'put', today)

    return r

def calc_die():
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    today = '2020-06-12'

    fn = get_dss() + 'opt/' + today[:7] + '.csv'
    df = pd.read_csv(fn)
    df = df[df.Localtime > today+' 11:00:00']
    df = df[df.Localtime < today+' 12:00:00']
    df = df.set_index('Instrument')

    r = []
    if len(df) > 0:
        r += die_IO(df, today)
        r += die_m(df, today)
        r += die_RM(df, today)
        r += die_MA(df, today)
        r += die_CF(df, today)
        df2 = pd.DataFrame(r, columns=['date', 'term', 'c/p', 's1', 'c1', 's2', 'c2', 's3', 'c3', 'cost'])
        df2 = df2[(df2.cost < 0) & (df2.cost != float('-inf'))]
        fn = get_dss() + 'opt/die.csv'
        df2.to_csv(fn, index=False)
        return True
    else:
        return False


if __name__ == '__main__':
    # calc_pcp()
    calc_die()
