import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import os
from datetime import datetime

from nature import get_dss, get_inx, get_contract
from nature import bsm_call_value, bsm_put_value, bsm_call_imp_vol, bsm_put_imp_vol



def proc_iv(df, T):
    r  = 0.03                             # 固定无风险短期利率
    iv_c_list = []
    iv_p_list = []

    for i, row in df.iterrows():
        S0 = float(row.obj)                   # 标的价格
        K  = int(row.atm)                     # 行权价格

        C0 = float(row.close_C)               # 期权价格
        iv_c = bsm_call_imp_vol(S0, K, T, r, C0)
        iv_c_list.append(iv_c)

        C0 = float(row.close_P)               # 期权价格
        iv_p = bsm_put_imp_vol(S0, K, T, r, C0)
        iv_p_list.append(iv_p)

        # print(T, S0, K, iv_c, iv_p)
    df['iv_c'] = iv_c_list
    df['iv_p'] = iv_p_list
    df['iv_a'] = (df['iv_c'] + df['iv_p']) / 2

    df = df[ ['date','time','pz','symbol','atm','obj','close_C','close_P','iv_c','iv_p','iv_a'] ]
    return df

def calc_iv_atm():
    ''' 计算IO合约min5的iv值，保存在文件中'''

    now = datetime.now()
    # today = now.strftime('%Y-%m-%d %H:%M:%S')
    today = now.strftime('%Y-%m-%d')
    # today = '2020-07-09'

    fn = get_dss() + 'fut/cfg/opt_mature.csv'
    df2 = pd.read_csv(fn)
    df2 = df2[df2.pz == 'IO']
    df2 = df2[df2.flag == df2.flag]                 # 筛选出不为空的记录
    df2 = df2[df2.mature >= today]                  # 过期的合约就不要了
    df2 = df2.set_index('symbol')
    mature_dict = dict(df2.mature)
    print(mature_dict)
    # for symbol in mature_dict.keys():
    #     if mature_dict[symbol] < today:
    #         mature_dict.pop(symbol)
    #         break
    # print(mature_dict)

    cur_io = min(mature_dict.keys())
    cur_if = 'IF' + cur_io[2:]
    # print(cur_if)
    fn = get_dss() + 'fut/bar/day_' + cur_if + '.csv'
    df = pd.read_csv(fn)
    df = df[df.date == today]
    # print(df)
    if len(df) > 0:
        row = df.iloc[0,:]
        strike = row.open*0.5 + row.close*0.5
        strike = int(round(strike/1E4,2)*1E4)
    else:
        return

    atm = str(strike)                                 # 获得平值
    print(atm)

    # for symbol in ['IO2007']:
    for symbol in mature_dict.keys():
        # print(symbol)
        # 取IF min5数据
        fn = get_dss() + 'fut/bar/min5_IF' + symbol[2:] + '.csv'
        df = pd.read_csv(fn)
        df = df[df.date == today]
        if len(df) == 0:
            continue

        df['obj'] = df['close']
        df = df.set_index(['date', 'time'])
        df_IF = df[['obj']]
        # print(df_IF.head())

        for flag in ['C', 'P']:
            fn = get_dss() + 'fut/bar/min5_' + symbol + '-' + flag  + '-' + atm + '.csv'
            df = pd.read_csv(fn)
            df = df[df.date == today]
            df = df.set_index(['date', 'time'])
            df['close'+'_'+flag] = df['close']
            df_IO = df[['close'+'_'+flag]]
            # print(df_IO.head())

            # 与IF 数据进行join
            df_IF = df_IF.join(df_IO)
            # print(df_IF.head())

        df_IF = df_IF.reset_index()
        # print(df_IF.head())
        df_IF['pz'] = str(get_contract(symbol).pz)
        df_IF['symbol'] = symbol
        df_IF['atm'] = atm
        date_mature = datetime.strptime(mature_dict[symbol], '%Y-%m-%d')
        td = datetime.strptime(today, '%Y-%m-%d')
        T = float((date_mature - td).days) / 365        # 剩余期限，已做年化处理
        T = 0.0015 if T == 0  else T                    # 最后一天特殊处理
        df = proc_iv(df_IF, T)
        # print(df.head())
        fn = get_dss() + 'opt/iv_atm_' + today[:4] + '.csv'
        if os.path.exists(fn):
            df.to_csv(fn, index=False, header=None, mode='a')
        else:
            df.to_csv(fn, index=False)


def test_iv():
    r = 0.03
    S0 = 4489

    K = 4600
    T = 10/365

    C0 = 64.8
    sa = bsm_call_imp_vol(S0, K, T, r, C0)

    # C0 = 71.4
    # sa =  bsm_put_imp_vol(S0, K, T, r, C0)

    print(sa)

if __name__ == '__main__':
    calc_iv_atm()

    # test_iv()
