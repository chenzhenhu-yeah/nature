import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import zipfile
import os

from nature import get_dss, get_inx

def calc_hv(date):
    df = get_inx('000300', '2020-01-01', date)
    df = df.set_index('date')
    df = df.sort_index()

    df['ln'] = np.log(df.close)
    df['rt'] = df['ln'].diff(1)
    df['hv'] = df['rt'].rolling(60).std()
    df['hv'] *= np.sqrt(242)

    cur = df.iloc[-1,:]
    return float(cur.hv), float(cur.close)

def IO(date):
    year = date[:4]
    hv, obj_price = calc_hv(date)
    # print(hv, obj_price)

    fn = get_dss() + 'backtest/IO/' + 'IO' + year + '_greeks.csv'
    df = pd.read_csv(fn)
    df = df[(df.date == date) & (df.symbol.str.startswith('IO'))]
    # print(df.head())
    # print( len(df) )
    term_list = sorted( list(set([ x[:6] for x in df.symbol ])) )
    # print(term_list)

    r = []
    for term in term_list:
        df1 = df[df.symbol.str.startswith(term)]
        strike_list = sorted( list(set([ x[-4:] for x in df1.symbol ])) )
        # print(term, strike_list)
        df1 = df1.set_index('symbol')
        i = 0
        c_iv = 0
        p_iv = 0
        c_curve_dict = {}
        p_curve_dict = {}
        for strike in strike_list:
            symbol_c = term + '-C-' + strike
            symbol_p = term + '-P-' + strike
            c_curve_dict[strike] = df1.at[symbol_c, 'iv']
            p_curve_dict[strike] = df1.at[symbol_p, 'iv']

            if float(strike) >= obj_price-100 and float(strike) <= obj_price+100:
                i += 1
                c_iv += df1.at[symbol_c, 'iv']
                p_iv += df1.at[symbol_p, 'iv']
        c_iv = c_iv / i
        p_iv = p_iv / i
        iv = c_iv*0.5 + p_iv*0.5
        r.append( [date, term, iv, c_iv, p_iv, hv, obj_price, str(c_curve_dict), str(p_curve_dict)] )

    fn = get_dss() + 'backtest/IO/' + 'IO' + year + '_sigma.csv'
    df = pd.DataFrame(r, columns=['date', 'term', 'iv', 'c_iv', 'p_iv', 'hv', 'obj_price', 'c_curve', 'p_curve'])
    if os.path.exists(fn):
        df.to_csv(fn, index=False, mode='a', header=False)
    else:
        df.to_csv(fn, index=False)




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

def smile(date):
    """期权微笑曲线"""
    year = date[:4]
    fn = get_dss() + 'backtest/IO/' + 'IO' + year + '_sigma.csv'
    df = pd.read_csv(fn)
    df = df[df.date == date]
    # df = df.reset_index()
    row = df.iloc[0,:]
    print(row)
    c_curve_dict = eval(row.c_curve)
    p_curve_dict = eval(row.p_curve)

    df1 = pd.DataFrame([c_curve_dict, p_curve_dict])
    df1 = df1.T
    # print(df1)
    df1.plot()
    # plt.show()
    plt.savefig('fig2.jpg')

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


if __name__ == '__main__':
    # dates = ['2020-04-29', '2020-04-30', '2020-05-06', '2020-05-07', '2020-05-08', '2020-05-11', '2020-05-12', '2020-05-13']
    # for date in dates:
    #     IO(date)

    date = '2020-05-12'
    term_structure(date)
    smile(date)
    iv_ts(date)
