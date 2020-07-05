import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import os
from datetime import datetime

import zipfile
import os

from nature import get_dss, get_inx

def calc_hv(date):
    df = get_inx('000300', '2020-01-01', date)
    df = df.set_index('date')
    df = df.sort_index()

    df['ln'] = np.log(df.close)
    df['rt'] = df['ln'].diff(1)
    df['hv'] = df['rt'].rolling(20).std()
    df['hv'] *= np.sqrt(242)

    cur = df.iloc[-1,:]
    return float(cur.hv), float(cur.close)

def term_structure(date):
    year = date[:4]
    fn = get_dss() + 'backtest/IO/' + 'IO' + year + '_sigma.csv'
    df = pd.read_csv(fn)
    df = df[df.date == date]
    print(df.head())
    print( len(df) )
    df = df.set_index('term')

    plt.figure(figsize=(15,7))
    # plt.xticks(rotation=45)
    plt.plot(df.iv)

    # plt.title(title)
    plt.grid(True, axis='y')

    # ax = plt.gca()
    # for label in ax.get_xticklabels():
    #     label.set_visible(False)
    # for label in ax.get_xticklabels()[1::7]:
    #     label.set_visible(True)

    # plt.show()
    plt.savefig('fig1.jpg')

def iv_ts(date):
    year = date[:4]
    fn = get_dss() + 'backtest/IO/' + 'IO' + year + '_sigma.csv'
    df = pd.read_csv(fn)
    df = df[df.term == 'IO2005']
    df = df.set_index('date')
    print(df)
    plt.figure(figsize=(15,7))
    plt.xticks(rotation=45)
    plt.plot(df.c_iv)
    plt.plot(df.p_iv)
    # plt.show()
    plt.savefig('fig3.jpg')


def calc_sigma_common(df, today, term_list, strike_pos, gap, dash):
    """
    参数
    df：来源于_greeks，当前只包含一个期权品种某天的数据
    today:
    term_list:合约月份
    strike_pos:价格从第几位开始取值
    gap:行权价的间隔
    dash:'-'和''两种

    逻辑：每个term生成一条记录，包含该term的微笑曲线
    """
    r = []
    for term in term_list:
        try:
            df1 = df[df.index.str.startswith(term)]
            # strike_list = sorted( list(set([ x[strike_pos:] for x in df1.index ])) )
            strike_list = list(set([ x[strike_pos:] for x in df1.index ]))
            strike_list = sorted( [int(x) for x in strike_list] )
            # print(strike_list)

            i = 0
            c_iv = 0
            p_iv = 0
            c_curve_dict = {}
            p_curve_dict = {}
            for strike in strike_list:
                symbol_c = term + dash + 'C' + dash + str(strike)
                symbol_p = term + dash + 'P' + dash + str(strike)
                c_curve_dict[strike] = df1.at[symbol_c, 'iv']
                p_curve_dict[strike] = df1.at[symbol_p, 'iv']

                obj_price = float( df1.at[symbol_c, 'obj'] )
                # print(symbol_c, obj_price)

                # 只计算标的价格附近的行权价对应的iv
                if float(strike) >= obj_price-gap*2 and float(strike) <= obj_price+gap*2:
                    i += 1
                    c_iv += df1.at[symbol_c, 'iv']
                    p_iv += df1.at[symbol_p, 'iv']
            c_iv = c_iv / i
            p_iv = p_iv / i
            iv = c_iv*0.5 + p_iv*0.5
            r.append( [today, term, iv, c_iv, p_iv, obj_price, str(c_curve_dict), str(p_curve_dict)] )

        except:
            continue

    fn = get_dss() + 'opt/' + today[:7] + '_sigma.csv'
    df = pd.DataFrame(r, columns=['date', 'term', 'iv', 'c_iv', 'p_iv', 'obj_price', 'c_curve', 'p_curve'])
    if os.path.exists(fn):
        df.to_csv(fn, index=False, mode='a', header=False)
    else:
        df.to_csv(fn, index=False)
    # print(r)


def calc_sigma_IO(df_all, today):
    df = df_all[df_all.index.str.startswith('IO')]
    term_list = sorted( list(set([ x[:6] for x in df.index ])) )
    # print(term_list)
    strike_pos = 9
    gap = 50
    dash = '-'
    calc_sigma_common(df, today, term_list, strike_pos, gap, dash)


def calc_iv_atm():
    # 计算IO合约min5的iv值，保存在文件中
    now = datetime.now()
    # today = now.strftime('%Y-%m-%d %H:%M:%S')
    today = now.strftime('%Y-%m-%d')
    today = '2020-07-03'

    fn = get_dss() + 'fut/cfg/opt_mature.csv'
    df2 = pd.read_csv(fn)
    df2 = df2[df2.pz == 'IO']
    df2 = df2[df2.flag == df2.flag]                 # 筛选出不为空的记录
    df2 = df2[df2.mature > today]
    df2 = df2.set_index('symbol')
    mature_dict = dict(df2.mature)
    print(mature_dict)
    # print(df2)
    cur_io = min(mature_dict.keys())
    cur_if = 'IF' + cur_io[2:]
    # print(cur_if)
    fn = get_dss() + 'fut/bar/day_' + cur_if + '.csv'
    df = pd.read_csv(fn)
    df = df[df.date == today]
    print(df)
    if len(df) > 0:
        row = df.iloc[0,:]
        strike = row.open*0.5 + row.close*0.5
        strike = int(round(strike/1E4,2)*1E4)
    else:
        return

    atm = str(strike)                                 # 获得平值
    print(atm)

    atm = '4000'
    for symbol in ['IO2007']:

    # for symbol in mature_dict.keys():
        # print(symbol)
        # 取IF min5数据



        for flag in ['-C-', '-P-']:
            fn = get_dss() + 'fut/bar/min5_' + symbol + flag + atm + '.csv'
            df = pd.read_csv(fn)
            df = df[df.date == today]
            # print(df.head())

            # 与IF 数据进行join


            for i, row in df.iterrows():
                print(row)
                break


        # df = df.drop_duplicates(subset=['Instrument'], keep='last')
    #     df = df.set_index('Instrument')
    #     # print(df.head())
    #
    #     if len(df) > 0:
    #         calc_sigma_IO(df, today)

if __name__ == '__main__':
    calc_iv_atm()