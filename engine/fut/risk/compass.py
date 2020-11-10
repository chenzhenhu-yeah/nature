import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import os
import time
from datetime import datetime, timedelta
import talib
import fitz

from nature import get_dss, get_inx, get_contract, get_repo
from nature import bsm_call_value, bsm_put_value, bsm_call_imp_vol, bsm_put_imp_vol


dirname = os.path.join(get_repo(), r'nature/engine/fut/risk/img/')

def show_11(df_list, title):
    """
    """
    plt.figure(figsize=(15,9))

    for df in df_list:
        plt.plot(df['value'], label=df.index.name)
        title += '_' + df.index.name

    plt.title(title)
    plt.xticks(rotation=90)
    plt.grid(True, axis='y')
    plt.legend()
    ax = plt.gca()
    for label in ax.get_xticklabels():
        label.set_visible(False)
    for label in ax.get_xticklabels()[1::30]:
        label.set_visible(True)
    for label in ax.get_xticklabels()[-1:]:
        label.set_visible(True)

    fn = dirname + title + '.jpg'
    plt.savefig(fn)
    plt.cla()

def show_21(df_list, df2_list, code):
    """
    """
    plt.figure(figsize=(15,12))

    plt.subplot(2,1,1)
    title1 = code
    for df in df_list:
        plt.plot(df['value'], label=df.index.name)
        title1 += '_' + df.index.name

    plt.title(title1)
    plt.xticks([])
    plt.grid(True, axis='y')
    plt.legend()
    ax = plt.gca()

    plt.subplot(2,1,2)
    title2 = code
    for df in df2_list:
        plt.plot(df['value'], label=df.index.name)
        title2 += '_' + df.index.name

    plt.title(title2)
    plt.xticks(rotation=90)
    plt.grid(True, axis='y')
    plt.legend()
    ax = plt.gca()
    for label in ax.get_xticklabels():
        label.set_visible(False)
    for label in ax.get_xticklabels()[1::30]:
        label.set_visible(True)
    for label in ax.get_xticklabels()[-1:]:
        label.set_visible(True)

    fn = dirname + title1 +'_' + title2 + '.jpg'
    plt.savefig(fn)
    plt.cla()

def show_31(df_list, df2_list, df3_list, code):
    """
    """
    plt.figure(figsize=(15,15))

    plt.subplot(3,1,1)
    title1 = code
    for df in df_list:
        plt.plot(df['value'], label=df.index.name)
        title1 += '_' + df.index.name

    plt.title(title1)
    plt.xticks([])
    plt.grid(True, axis='y')
    plt.legend()
    ax = plt.gca()

    plt.subplot(3,1,2)
    title2 = code
    for df in df2_list:
        plt.plot(df['value'], label=df.index.name)
        title2 += '_' + df.index.name

    plt.title(title2)
    plt.xticks([])
    plt.grid(True, axis='y')
    plt.legend()
    ax = plt.gca()

    plt.subplot(3,1,3)
    title3 = code
    for df in df3_list:
        plt.plot(df['value'], label=df.index.name)
        title3 += '_' + df.index.name

    plt.title(title3)
    plt.xticks(rotation=90)
    plt.grid(True, axis='y')
    plt.legend()
    ax = plt.gca()
    for label in ax.get_xticklabels():
        label.set_visible(False)
    for label in ax.get_xticklabels()[1::30]:
        label.set_visible(True)
    for label in ax.get_xticklabels()[-1:]:
        label.set_visible(True)

    fn = dirname + title1 + '_' + title2 + '_' + title3 + '.jpg'
    plt.savefig(fn)
    plt.cla()

def show_ic(symbol1, symbol2, code):
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

    plt.figure(figsize=(15,9))
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

    fn = dirname + code + '_ic_' + symbol1 + '_' + symbol2 + '.jpg'
    plt.savefig(fn)
    plt.cla()

def df_hv(code):
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
    df['hv'] *= np.sqrt(242)*100

    df = df.iloc[-242:, ]
    df['value'] = df['hv']
    df = df[['value']]
    df.index.name = 'hv_' + code
    return df


def df_iv(basic, df):
    """
    输入：df，包含字段 dt, symbol, close, close_obj
    """

    r = 0.03
    fn = get_dss() + 'fut/cfg/opt_mature.csv'
    df2 = pd.read_csv(fn)
    df2 = df2[pd.notnull(df2.flag)]
    df2 = df2.set_index('symbol')
    mature_dict = dict(df2.mature)

    iv_list = []
    for i, row in df.iterrows():
        strike = get_contract(row.symbol).strike
        opt_flag = get_contract(row.symbol).opt_flag
        date_mature = mature_dict[basic]
        date_mature = datetime.strptime(date_mature, '%Y-%m-%d')
        td = datetime.strptime(row['dt'][:10], '%Y-%m-%d')
        T = float((date_mature - td).days) / 365                       # 剩余期限
        T = 0.0015 if T == 0  else T                    # 最后一天特殊处理

        try:
            if opt_flag == 'C':
                iv = bsm_call_imp_vol(row.close_obj, strike, T, r, row.close)
            else:
                iv = bsm_put_imp_vol(row.close_obj, strike, T, r, row.close)
        except:
            iv = 0
        iv_list.append( round(100*iv, 2) )

    df['iv'] = iv_list
    df['value'] = df['iv']
    df2 = df.set_index('dt')
    df2 = df2.sort_index()
    df2 = df2[['value']]
    df2.index.name = df.index.name
    return df2

def df_atm_plus_obj_day(basic, gap, shift=0):
    """
    选取每天的平值合约，按要求组装包含close_obj的df
    返回的df包括以下字段：dt, symbol, close, close_obj
    """

    if basic[:2] == 'IO':
        symbol_obj = 'IF' + basic[2:]
    else:
        symbol_obj = basic

    # fn = get_dss() + 'fut/put/rec/min5_' + symbol_obj + '.csv'
    fn = get_dss() + 'fut/bar/day_' + symbol_obj + '.csv'
    df_obj = pd.read_csv(fn)
    # print(df_obj.head())

    r_c = []
    r_p = []
    for i, row in df_obj.iterrows():
        close_obj = row.open
        atm = int(round(round(close_obj*(100/gap)/1E4,2) * 1E4/(100/gap), 0))     # 获得平值

        symbol_c = basic + get_contract(basic).opt_flag_C + str(atm+shift*gap)
        fn = get_dss() + 'fut/bar/min5_' + symbol_c + '.csv'
        df_atm_c = pd.read_csv(fn)
        df_atm_c = df_atm_c[(df_atm_c.date == row.date) & (df_atm_c.time == '14:54:00')]

        symbol_p = basic + get_contract(basic).opt_flag_P + str(atm+shift*gap)
        fn = get_dss() + 'fut/bar/min5_' + symbol_p + '.csv'
        df_atm_p = pd.read_csv(fn)
        df_atm_p = df_atm_p[(df_atm_p.date == row.date) & (df_atm_c.time == '14:54:00')]

        if df_atm_c.empty or df_atm_p.empty:
            pass
        else:
            rec = df_atm_c.iloc[0,:]
            r_c.append([row.date, symbol_c, rec.close, close_obj])
            rec = df_atm_p.iloc[0,:]
            r_p.append([row.date, symbol_p, rec.close, close_obj])


    cols = ['dt', 'symbol', 'close', 'close_obj']
    df_c = pd.DataFrame(r_c, columns=cols)
    df_p = pd.DataFrame(r_p, columns=cols)
    df_c.index.name = 'iv_c'
    df_p.index.name = 'iv_p'

    return df_c, df_p

def df_atm_plus_obj_min5(basic, date, gap, shift=0):
    """
    选取每天的平值合约，按要求组装包含close_obj的df
    返回的df包括以下字段：dt, symbol, close, close_obj
    """
    r_c = []
    r_p = []

    if basic[:2] == 'IO':
        symbol_obj = 'IF' + basic[2:]
    else:
        symbol_obj = basic

    fn = get_dss() + 'fut/bar/min5_' + symbol_obj + '.csv'
    df_obj = pd.read_csv(fn)
    df_obj = df_obj[(df_obj.date == date) & (df_obj.time <= '14:54:00')]
    df_obj = df_obj.reset_index()
    # print(df_obj.head())

    if df_obj.empty:
        pass
    else:
        close_obj = df_obj.iloc[0,:].open
        atm = int(round(round(close_obj*(100/gap)/1E4,2) * 1E4/(100/gap), 0))     # 获得平值

        symbol_c = basic + get_contract(basic).opt_flag_C + str(atm+shift*gap)
        fn = get_dss() + 'fut/bar/min5_' + symbol_c + '.csv'
        df_atm_c = pd.read_csv(fn)
        df_atm_c = df_atm_c[(df_atm_c.date == date) & (df_atm_c.time <= '14:54:00')]

        symbol_p = basic + get_contract(basic).opt_flag_P + str(atm+shift*gap)
        fn = get_dss() + 'fut/bar/min5_' + symbol_p + '.csv'
        df_atm_p = pd.read_csv(fn)
        df_atm_p = df_atm_p[(df_atm_p.date == date) & (df_atm_c.time <= '14:54:00')]

        if df_atm_c.empty or df_atm_p.empty:
            pass
        else:
            for i, row in df_obj.iterrows():
                rec = df_atm_c.iloc[i,:]
                r_c.append([row.date + ' ' + row.time, symbol_c, rec.close, row.close])
                rec = df_atm_p.iloc[i,:]
                r_p.append([row.date + ' ' + row.time, symbol_p, rec.close, row.close])

    cols = ['dt', 'symbol', 'close', 'close_obj']
    df_c = pd.DataFrame(r_c, columns=cols)
    df_p = pd.DataFrame(r_p, columns=cols)
    df_c.index.name = 'iv_c'
    df_p.index.name = 'iv_p'
    return df_c, df_p

def df_open_interest(basic):
    now = datetime.now()
    # 本月第一天
    first_day = datetime(now.year, now.month, 1)
    #前一个月最后一天
    pre_month = first_day - timedelta(days = 21)
    today = now.strftime('%Y-%m-%d')
    pre = pre_month.strftime('%Y-%m-%d')
    # print(pre)

    fn = get_dss() + 'opt/' +  pre[:7] + '_greeks.csv'
    df_pre = pd.read_csv(fn)
    fn = get_dss() + 'opt/' +  today[:7] + '_greeks.csv'
    df_today = pd.read_csv(fn)
    df = pd.concat([df_pre, df_today])

    df = df[df.Instrument.str.slice(0,len(basic)) == basic]
    df['date'] = df.Localtime.str.slice(0,10)
    df = df[df.date >= pre]
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

    df_c = pd.DataFrame(s_c)
    df_p = pd.DataFrame(s_p)
    df_c['value'] = df_c['OpenInterest']
    df_p['value'] = df_p['OpenInterest']
    df_c.index.name = 'openinterest_c'
    df_p.index.name = 'openinterest_p'
    # print(df_p)

    return df_c, df_p

def img2pdf(pdfname, img_list):
    doc = fitz.open()
    for img in img_list:
        img = os.path.join(dirname, img)
        # print(img)
        imgdoc = fitz.open(img)
        pdfbytes = imgdoc.convertToPDF()
        imgpdf = fitz.open("pdf", pdfbytes)
        doc.insertPDF(imgpdf)

    doc.save(os.path.join(dirname, pdfname))
    doc.close()

def common(date, code, gap):
    # 历波及当月合约隐波图
    df1 = df_hv(code)
    df2, dud = df_atm_plus_obj_day(code, gap)
    df2 = df_iv(code, df2)
    show_11([df1, df2], code)

    # 当月合约当天隐波分时图
    df31, df32  = df_atm_plus_obj_min5(code, date, gap)
    df31 = df_iv(code, df31)
    df32 = df_iv(code, df32)
    fn = get_dss() + 'fut/bar/min5_' + code + '.csv'
    df_obj = pd.read_csv(fn)
    df_obj = df_obj[(df_obj.date == date) & (df_obj.time <= '14:54:00')]
    df_obj = df_obj.set_index('time')
    df_obj['value'] = df_obj['close']
    df_obj.index.name = 'obj'
    show_31([df31], [df32], [df_obj], code)

    df_c, df_p = df_open_interest(code)
    show_11([df_c, df_p], code)

    # skew_day
    df_right_c, df_right_p = df_atm_plus_obj_day(code, gap, shift=2)
    df_left_c, df_left_p = df_atm_plus_obj_day(code, gap, shift=-2)
    df_right_c = df_iv(code, df_right_c)
    df_right_p = df_iv(code, df_right_p)
    df_left_c = df_iv(code, df_left_c)
    df_left_p = df_iv(code, df_left_p)
    # print(df_left_c)
    # print(df_left_p)
    # print(df_right_c)
    # print(df_right_p)
    df_right_c['value'] = df_right_c['value']/df_left_c['value'] - 1
    df_left_p['value'] = df_left_p['value']/df_right_p['value'] - 1
    df_right_c.index.name = 'skew_day_c'
    df_left_p.index.name = 'skew_day_p'
    # print(df_right_c)
    # print(df_left_p)
    show_21([df_left_p], [df_right_c], code)

    # skew__min5
    df_right_c, df_right_p = df_atm_plus_obj_min5(code, date, gap, shift=2)
    df_left_c, df_left_p = df_atm_plus_obj_min5(code, date, gap, shift=-2)
    df_right_c = df_iv(code, df_right_c)
    df_right_p = df_iv(code, df_right_p)
    df_left_c = df_iv(code, df_left_c)
    df_left_p = df_iv(code, df_left_p)
    df_right_c['value'] = df_right_c['value']/df_left_c['value'] - 1
    df_left_p['value'] = df_left_p['value']/df_right_p['value'] - 1
    df_right_c.index.name = 'skew_min5_c'
    df_left_p.index.name = 'skew_min5_p'
    # print(df_right_c)
    # print(df_left_p)
    show_21([df_left_p], [df_right_c], code)

def CF(date, df):
    m0 = df[(df.pz == 'CF') & (df.flag == 'm0')].iloc[0,:].symbol
    m1 = df[(df.pz == 'CF') & (df.flag == 'm1')].iloc[0,:].symbol

    code = m0
    gap = 200

    common(date, code, gap)

    # ic
    show_ic(m0, m1, code)

    img2pdf('CF_'+date+'.pdf',
            [m0+'_hv_'+m0+'_iv_c.jpg' ,
             m0+'_iv_c_'+m0+'_iv_p_'+m0+'_obj.jpg',
             m0+'_skew_day_p_'+m0+'_skew_day_c.jpg',
             m0+'_skew_min5_p_'+m0+'_skew_min5_c.jpg',
             m0+'_openinterest_c_openinterest_p.jpg',
             m0+'_ic_'+m0+'_'+m1+'.jpg',
            ])

def m(date, df):
    m0 = df[(df.pz == 'm') & (df.flag == 'm0')].iloc[0,:].symbol
    m1 = df[(df.pz == 'm') & (df.flag == 'm1')].iloc[0,:].symbol
    # m0 = 'm2101'
    # m1 = 'm2105'
    y0 = 'y' + m0[1:]
    code = m0
    gap = 50

    common(date, code, gap)

    # ic
    show_ic(m0, m1, code)
    show_ic(y0, m0, code)

    img2pdf('m_'+date+'.pdf',
            [m0+'_hv_'+m0+'_iv_c.jpg' ,
             m0+'_iv_c_'+m0+'_iv_p_'+m0+'_obj.jpg',
             m0+'_skew_day_p_'+m0+'_skew_day_c.jpg',
             m0+'_skew_min5_p_'+m0+'_skew_min5_c.jpg',
             m0+'_openinterest_c_openinterest_p.jpg',
             m0+'_ic_'+m0+'_'+m1+'.jpg',
             m0+'_ic_'+y0+'_'+m0+'.jpg', ]
            )


def RM(date, df):
    m0 = df[(df.pz == 'RM') & (df.flag == 'm0')].iloc[0,:].symbol
    m1 = df[(df.pz == 'RM') & (df.flag == 'm1')].iloc[0,:].symbol

    code = m0
    gap = 25

    common(date, code, gap)

    # ic
    show_ic(m0, m1, code)

    img2pdf('RM_'+date+'.pdf',
            [m0+'_hv_'+m0+'_iv_c.jpg' ,
             m0+'_iv_c_'+m0+'_iv_p_'+m0+'_obj.jpg',
             m0+'_skew_day_p_'+m0+'_skew_day_c.jpg',
             m0+'_skew_min5_p_'+m0+'_skew_min5_c.jpg',
             m0+'_openinterest_c_openinterest_p.jpg',
             m0+'_ic_'+m0+'_'+m1+'.jpg',
            ])

def IO(date, df):
    m0 = df[(df.pz == 'IO') & (df.flag == 'm0')].iloc[0,:].symbol
    m1 = df[(df.pz == 'IO') & (df.flag == 'm1')].iloc[0,:].symbol

    code = m0
    gap = 50

    common(date, code, gap)

    # ic
    show_ic(m0, m1, code)

    img2pdf('RM_'+date+'.pdf',
            [m0+'_hv_'+m0+'_iv_c.jpg' ,
             m0+'_iv_c_'+m0+'_iv_p_'+m0+'_obj.jpg',
             m0+'_skew_day_p_'+m0+'_skew_day_c.jpg',
             m0+'_skew_min5_p_'+m0+'_skew_min5_c.jpg',
             m0+'_openinterest_c_openinterest_p.jpg',
             m0+'_ic_'+m0+'_'+m1+'.jpg',
            ])

def compass(date):
    fn = get_dss() + 'fut/cfg/opt_mature.csv'
    df2 = pd.read_csv(fn)
    df2 = df2[pd.notnull(df2.flag)]

    # CF(date, df2)
    m(date, df2)
    RM(date, df2)
    IO(date, df2)


if __name__ == '__main__':
    now = datetime.now()
    date = now.strftime('%Y-%m-%d')
    date = '2020-11-10'
    # date = '2020-09-28'

    compass(date)
