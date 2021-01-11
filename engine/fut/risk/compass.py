
import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import os
import time
from datetime import datetime, timedelta
import talib
import fitz
import traceback

from nature import get_dss, get_inx, get_contract, get_repo, to_log, send_email, get_trade_preday
from nature import bsm_call_value, bsm_put_value, bsm_call_imp_vol, bsm_put_imp_vol

from nature.web.hold import hold_product

dirname = os.path.join(get_repo(), r'nature/engine/fut/risk/img/')

def show_11(df_list, title, xticks_filter=True, filename=None):
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
    if xticks_filter == True:
        for label in ax.get_xticklabels():
            label.set_visible(False)
        for label in ax.get_xticklabels()[1::30]:
            label.set_visible(True)
        for label in ax.get_xticklabels()[-1:]:
            label.set_visible(True)

    if filename is None:
        fn = dirname + title + '.jpg'
    else:
        fn = dirname + filename + '.jpg'

    plt.savefig(fn)
    plt.cla()

def show_21(df_list, df2_list, code, xticks_filter=True, filename=None):
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
    if xticks_filter == True:
        for label in ax.get_xticklabels():
            label.set_visible(False)
        for label in ax.get_xticklabels()[1::30]:
            label.set_visible(True)
        for label in ax.get_xticklabels()[-1:]:
            label.set_visible(True)

    if filename is None:
        fn = dirname + title1 +'_' + title2 + '.jpg'
    else:
        fn = dirname + filename + '.jpg'

    plt.savefig(fn)
    plt.cla()

def show_31(df_list, df2_list, df3_list, code, xticks_filter=True, filename=None):
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
    if xticks_filter == True:
        for label in ax.get_xticklabels():
            label.set_visible(False)
        for label in ax.get_xticklabels()[1::30]:
            label.set_visible(True)
        for label in ax.get_xticklabels()[-1:]:
            label.set_visible(True)


    if filename is None:
        fn = dirname + title1 + '_' + title2 + '_' + title3 + '.jpg'
    else:
        fn = dirname + filename + '.jpg'

    plt.savefig(fn)
    plt.cla()

def show_ic(symbol1, symbol2, code, date):
    fn = get_dss() +'fut/bar/day_' + symbol1 + '.csv'
    df1 = pd.read_csv(fn)
    fn = get_dss() +'fut/bar/day_' + symbol2 + '.csv'
    df2 = pd.read_csv(fn)

    df1 = df1.sort_values('date')
    df2 = df2.sort_values('date')
    row1 = df1.iloc[0,:]
    row2 = df2.iloc[0,:]
    start_dt = row1['date'] if row1['date'] > row2['date'] else row2['date']
    row1 = df1.iloc[-1,:]
    row2 = df2.iloc[-1,:]
    end_dt = row1['date'] if row1['date'] < row2['date'] else row2['date']

    df1 = df1[(df1.date >= start_dt) & (df1.date <= end_dt)]
    df1 = df1.set_index('date')

    df2 = df2[(df2.date >= start_dt) & (df2.date <= end_dt)]
    df2 = df2.set_index('date')

    df1['close'] = df1.close - df2.close
    df1 = df1.dropna()

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

def df_hv(code, date):
    try:
        now = datetime.now()
        start_day = now - timedelta(days = 480)
        start_day = start_day.strftime('%Y-%m-%d')

        if code[:2] == 'IO':
            df = get_inx('000300', start_day)
        else:
            fn = get_dss() + 'fut/bar/day_' + code + '.csv'
            df = pd.read_csv(fn)
        df = df[(df.date >= start_day) & (df.date <= date)]
        df = df.set_index('date')
        df = df.sort_index()

        df['ln'] = np.log(df.close)
        df['rt'] = df['ln'].diff(1)
        df['hv'] = df['rt'].rolling(20).std()
        df['hv'] *= np.sqrt(242)*100

        df = df.iloc[-242:, ]
        df['value'] = df['hv']
        df = df[['value']]
        df.index.name = code + '_hv'
        return df
    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

        return None


def df_iv(basic, df):
    """
    输入：df，包含字段 dt, symbol, close, close_obj
    """
    try:
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
    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

        return None

def df_atm_plus_obj_day(basic, date, gap, shift=0):
    """
    选取每天的平值合约，按要求组装包含close_obj的df
    返回的df包括以下字段：dt, symbol, close, close_obj
    """
    try:

        if basic[:2] == 'IO':
            symbol_obj = 'IF' + basic[2:]
        else:
            symbol_obj = basic

        fn = get_dss() + 'fut/bar/min5_' + symbol_obj + '.csv'
        df_obj = pd.read_csv(fn)
        date_list = sorted(list(set(df_obj.date)))
        date_list = [d for d in date_list if d <= date]
        # print(date_list)

        r_c = []
        r_p = []
        for date in date_list:
            df2 = df_obj[(df_obj.date == date) & (df_obj.time <= '14:54:00')]
            if df2.empty:
                print('df empty: ', basic, date)
                continue
            close_obj = df2.iloc[-1,:].close
            atm = int(round(round(close_obj*(100/gap)/1E4,2) * 1E4/(100/gap), 0))     # 获得平值
            # if date == '2020-11-10':
            #     print(basic, '-------------------------', close_obj, atm )

            symbol_c = basic + get_contract(basic).opt_flag_C + str(atm+shift*gap)
            fn = get_dss() + 'fut/bar/min5_' + symbol_c + '.csv'
            df_atm_c = pd.read_csv(fn)
            df_atm_c = df_atm_c[(df_atm_c.date == date) & (df_atm_c.time == '14:54:00')]

            symbol_p = basic + get_contract(basic).opt_flag_P + str(atm+shift*gap)
            fn = get_dss() + 'fut/bar/min5_' + symbol_p + '.csv'
            df_atm_p = pd.read_csv(fn)
            df_atm_p = df_atm_p[(df_atm_p.date == date) & (df_atm_p.time == '14:54:00')]


            if df_atm_c.empty or df_atm_p.empty:
                pass
                # print('df empty: ', basic, date)
            else:
                rec = df_atm_c.iloc[0,:]
                r_c.append([date, symbol_c, rec.close, close_obj])
                rec = df_atm_p.iloc[0,:]
                r_p.append([date, symbol_p, rec.close, close_obj])


        cols = ['dt', 'symbol', 'close', 'close_obj']
        df_c = pd.DataFrame(r_c, columns=cols)
        df_p = pd.DataFrame(r_p, columns=cols)
        df_c.index.name = basic + '_iv_c'
        df_p.index.name = basic + '_iv_p'
        # print(df_c.tail())
        # print(df_p.tail())

        return df_c, df_p
    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

        return None, None

def df_atm_plus_obj_min5(basic, date, gap, shift=0):
    """
    选取每天的平值合约，按要求组装包含close_obj的df
    返回的df包括以下字段：dt, symbol, close, close_obj
    """
    try:
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
            close_obj = df_obj.iloc[-1,:].close
            atm = int(round(round(close_obj*(100/gap)/1E4,2) * 1E4/(100/gap), 0))     # 获得平值
            # if date == '2020-11-10':
            #     print(basic, '-------------------------', close_obj, atm )

            symbol_c = basic + get_contract(basic).opt_flag_C + str(atm+shift*gap)
            fn = get_dss() + 'fut/bar/min5_' + symbol_c + '.csv'
            df_atm_c = pd.read_csv(fn)
            df_atm_c = df_atm_c[(df_atm_c.date == date) & (df_atm_c.time <= '14:54:00')]

            symbol_p = basic + get_contract(basic).opt_flag_P + str(atm+shift*gap)
            fn = get_dss() + 'fut/bar/min5_' + symbol_p + '.csv'
            df_atm_p = pd.read_csv(fn)
            df_atm_p = df_atm_p[(df_atm_p.date == date) & (df_atm_p.time <= '14:54:00')]

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
        df_c.index.name = basic + '_iv_c'
        df_p.index.name = basic + '_iv_p'
        # print(df_c.tail())
        # print(df_p.tail())

        return df_c, df_p
    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

        return None, None

def df_open_interest(basic):
    try:
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
        df_c.index.name = basic + '_openinterest_c'
        df_p.index.name = basic + '_openinterest_p'
        # print(df_p)

        return df_c, df_p
    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

        return None, None


def df_smile(date, basic):
    try:
        fn = get_dss() + 'opt/' +  date[:7] + '_greeks.csv'
        df = pd.read_csv(fn)
        df['date'] = df.Localtime.str.slice(0,10)
        df = df[df.date == date]
        df = df[df.Instrument.str.slice(0,len(basic)) == basic]

        strike_list = []
        for i, row in df.iterrows():
            strike_list.append( get_contract(row.Instrument).strike )
        strike_list = sorted(list(set(strike_list)))
        # print(strike_list)

        if basic[:2] == 'IO':
            symbol_obj = 'IF' + basic[2:]
        else:
            symbol_obj = basic

        fn = get_dss() + 'fut/bar/min5_' + symbol_obj + '.csv'
        df = pd.read_csv(fn)
        df = df[(df.date == date) & (df.time == '14:54:00')]
        close_obj = df.iat[0,5]
        # print(obj_close)

        r_c = []
        r_p = []
        strike_c = []
        strike_p = []
        for strike in strike_list:
            # print(strike)
            symbol_c = basic + get_contract(basic).opt_flag_C + str(strike)
            fn = get_dss() + 'fut/bar/min5_' + symbol_c + '.csv'
            df = pd.read_csv(fn)
            df = df[(df.date == date) & (df.time == '14:54:00')]
            if df.empty:
                continue
            r_c.append([date, symbol_c, df.iat[0,5], close_obj])
            strike_c.append(str(strike))

            symbol_p = basic + get_contract(basic).opt_flag_P + str(strike)
            fn = get_dss() + 'fut/bar/min5_' + symbol_p + '.csv'
            df = pd.read_csv(fn)
            df = df[(df.date == date) & (df.time == '14:54:00')]
            # print(date, symbol_p, df.tail())
            if df.empty:
                continue
            r_p.append([date, symbol_p, df.iat[0,5], close_obj])
            strike_p.append(str(strike))

        df_c = pd.DataFrame(r_c, columns=['dt', 'symbol', 'close', 'close_obj'])
        df_p = pd.DataFrame(r_p, columns=['dt', 'symbol', 'close', 'close_obj'])
        # print(df_c)
        # print(df_p)

        df_c = df_iv(basic, df_c)
        df_p = df_iv(basic, df_p)

        # 去掉深度虚值的干扰项
        for i in [0,1,2]:
            df_c.iat[i,0] = np.nan
            df_p.iat[-i-1,0] = np.nan

        df_c.index = strike_c
        df_c.index.name = date + '_smile_c'
        df_p.index = strike_p
        df_p.index.name = date + '_smile_p'

        # print(df_c)
        # print(df_p)
        return df_c, df_p

    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

        return None, None

def implied_dist(date, basic, gap):
    try:
        fn = get_dss() + 'opt/' +  date[:7] + '_greeks.csv'
        df = pd.read_csv(fn)
        df['date'] = df.Localtime.str.slice(0,10)
        df = df[df.date == date]
        df = df[df.Instrument.str.slice(0,len(basic)) == basic]

        strike_list = []
        for i, row in df.iterrows():
            strike_list.append( get_contract(row.Instrument).strike )

        atm = int(round(round(min(strike_list)*(100/gap)/1E4,2) * 1E4/(100/gap), 0))     # 获得平值

        strike_list = range(atm, max(strike_list)+1, gap)
        # print( [*strike_list] )


        if basic[:2] == 'IO':
            symbol_obj = 'IF' + basic[2:]
        else:
            symbol_obj = basic

        fn = get_dss() + 'fut/bar/min5_' + symbol_obj + '.csv'
        df = pd.read_csv(fn)
        df = df[(df.date == date) & (df.time == '14:54:00')]
        obj_close = df.iat[0,5]
        # print(obj_close)

        c_list = []
        p_list = []
        for strike in strike_list:
            # print(strike)
            symbol_c = basic + get_contract(basic).opt_flag_C + str(strike)
            fn = get_dss() + 'fut/bar/min5_' + symbol_c + '.csv'
            df = pd.read_csv(fn)
            df = df[(df.date == date) & (df.time == '14:54:00')]
            c_list.append(df.iat[0,5])

            symbol_p = basic + get_contract(basic).opt_flag_P + str(strike)
            fn = get_dss() + 'fut/bar/min5_' + symbol_p + '.csv'
            df = pd.read_csv(fn)
            df = df[(df.date == date) & (df.time == '14:54:00')]
            # print(date, symbol_p, df.tail())
            p_list.append(df.iat[0,5])

        prop_list = []
        n = len(strike_list)
        for i in range(1, n-1):
            if strike_list[i] < obj_close:
                prop_list.append(int(100*(p_list[i-1]-2*p_list[i]+p_list[i+1])/gap))
            else:
                prop_list.append(int(100*(c_list[i-1]-2*c_list[i]+c_list[i+1])/gap))

        df = pd.DataFrame([])
        df['strike'] = strike_list[1:n-1]
        df['strike'] = df['strike'].astype('str')
        df['value'] = prop_list
        df = df.set_index('strike')
        df.index.name = basic + '_implied_dist'

        return df
    except Exception as e:
        s = traceback.format_exc()
        # to_log(s)
        to_log(date + ' ' + basic + ' 数据缺失 in implied_dist from file compass.py')

        return None


def smile(date, m0, m1=None):
    preday = get_trade_preday(date)

    df00_c, df00_p = df_smile(date, m0)
    df01_c, df01_p = df_smile(preday, m0)
    if df00_c is not None and df01_c is not None and df00_p is not None and  df01_p is not None:
        # show_21([df00_c], [df00_p], m0, False, m0+'_smile')
        show_21([df00_c, df01_c], [df00_p, df01_p], m0, False, m0+'_smile')

    if m1 is not None:
        df10_c, df10_p = df_smile(date, m1)
        df11_c, df11_p = df_smile(preday, m1)
        if df10_c is not None and df11_c is not None and df10_p is not None and  df11_p is not None:
            show_21([df10_c, df11_c], [df10_p, df11_p], m1, False, m1+'_smile')


def term_structure(m0, m1, m2, m3, date, gap):
    df01, df02 = df_atm_plus_obj_day(m0, date, gap)
    if df01 is None or df02 is None:
        return
    df01 = df_iv(m0, df01)
    df02 = df_iv(m0, df02)
    # print(df02.tail())

    df11, df12 = df_atm_plus_obj_day(m1, date, gap)
    if df11 is None or df12 is None:
        return
    df11 = df_iv(m1, df11)
    df12 = df_iv(m1, df12)
    # print(df12.tail())

    df21, df22 = df_atm_plus_obj_day(m2, date, gap)
    if df21 is None or df22 is None:
        return
    df21 = df_iv(m2, df21)
    df22 = df_iv(m2, df22)
    # print(df22.tail())

    df31, df32 = df_atm_plus_obj_day(m3, date, gap)
    if df31 is None or df32 is None:
        return
    df31 = df_iv(m3, df31)
    df32 = df_iv(m3, df32)
    # print(df32.tail())

    df_c = pd.DataFrame({'iv_c':[m0,m1,m2,m3],'value': [df01.iat[-1,0], df11.iat[-1,0], df21.iat[-1,0], df31.iat[-1,0]]})
    df_c = df_c.set_index('iv_c')
    df_c.index.name = df01.index[-1]+'_iv_c'
    # print(df_c)

    df_c2 = pd.DataFrame({'iv_c':[m0,m1,m2,m3],'value': [df01.iat[-2,0], df11.iat[-2,0], df21.iat[-2,0], df31.iat[-2,0]]})
    df_c2 = df_c2.set_index('iv_c')
    df_c2.index.name = df01.index[-2]+'_iv_c'
    # print(df_c2)

    df_p = pd.DataFrame({'iv_p':[m0,m1,m2,m3],'value': [df02.iat[-1,0], df12.iat[-1,0], df22.iat[-1,0], df32.iat[-1,0]]})
    df_p = df_p.set_index('iv_p')
    df_p.index.name = df01.index[-1]+'_iv_p'
    # print(df_p)

    df_p2 = pd.DataFrame({'iv_p':[m0,m1,m2,m3],'value': [df02.iat[-2,0], df12.iat[-2,0], df22.iat[-2,0], df32.iat[-2,0]]})
    df_p2 = df_p2.set_index('iv_p')
    df_p2.index.name = df01.index[-2]+'_iv_p'
    # print(df_p2)

    show_21([df_c2, df_c], [df_p2, df_p], m0, False, m0+'_term_structure')


def sdiffer(m0, m1, date, gap):
    df01, df02 = df_atm_plus_obj_day(m0, date, gap)
    if df01 is None or df02 is None:
        return
    df01 = df_iv(m0, df01)
    df02 = df_iv(m0, df02)
    c_base_m0 = df01.iat[-2,0]
    p_base_m0 = df02.iat[-2,0]
    # print(df01.tail())
    # print(df02.tail())
    # print(c_base_m0, p_base_m0)

    df11, df12 = df_atm_plus_obj_day(m1, date, gap)
    if df11 is None or df12 is None:
        return
    df11 = df_iv(m1, df11)
    df12 = df_iv(m1, df12)
    c_base_m1 = df11.iat[-2,0]
    p_base_m1 = df12.iat[-2,0]
    # print(df11.tail())
    # print(df12.tail())
    # print(c_base_m1, p_base_m1)

    # 当月合约当天隐波分时图
    df0_c, df0_p  = df_atm_plus_obj_min5(m0, date, gap)
    if df0_c is None or df0_p is None:
        return
    df0_c = df_iv(m0, df0_c)
    df0_p = df_iv(m0, df0_p)
    df0_c['value'] -= c_base_m0
    df0_p['value'] -= p_base_m0
    # print(df0_c.tail())
    # print(df0_p.tail())

    # 次月合约当天隐波分时图
    df1_c, df1_p  = df_atm_plus_obj_min5(m1, date, gap)
    if df1_c is None or df1_p is None:
        return
    df1_c = df_iv(m1, df1_c)
    df1_p = df_iv(m1, df1_p)
    df1_c['value'] -= c_base_m1
    df1_p['value'] -= p_base_m1
    # print(df1_c.tail())
    # print(df1_p.tail())

    df0_c['value'] -= df1_c['value']
    df0_p['value'] -= df1_p['value']
    df0_c.index.name = 'sdiffer_c'
    df0_p.index.name = 'sdiffer_p'

    show_21([df0_c], [df0_p], m0, True, m0+'_sdiffer')


def img2pdf(pdfname, img_list):
    doc = fitz.open()
    for img in img_list:
        img = os.path.join(dirname, img)
        if os.path.exists(img):
            imgdoc = fitz.open(img)
            pdfbytes = imgdoc.convertToPDF()
            imgpdf = fitz.open("pdf", pdfbytes)
            doc.insertPDF(imgpdf)

    doc.save(os.path.join(dirname, pdfname))
    doc.close()

def common(date, m0, gap, m1=None):
    code = m0

    # 历波及当月合约隐波图
    df1 = df_hv(code, date)
    df2, dud = df_atm_plus_obj_day(code, date, gap)
    if df1 is not None and df2 is not None:
        df2 = df_iv(code, df2)
        if m1 is None:
            show_21([df1], [df1.iloc[-60:,:], df2.iloc[-60:,:]], code)
        else:
            df3, dud = df_atm_plus_obj_day(m1, date, gap)
            # print(df3.tail())
            df3 = df_iv(m1, df3)
            # print(df3.tail())
            if df3 is not None:
                show_21([df1], [df1.iloc[-60:,:], df2.iloc[-60:,:], df3.iloc[-60:,:]], code)


    # 当月（次月）合约当天隐波分时图
    df31, df32  = df_atm_plus_obj_min5(code, date, gap)
    if df31 is not None and df32 is not None:
        df31 = df_iv(code, df31)
        df32 = df_iv(code, df32)

        if code[:2] == 'IO':
            symbol_obj = 'IF' + code[2:]
        else:
            symbol_obj = code
        fn = get_dss() + 'fut/bar/min5_' + symbol_obj + '.csv'
        df_obj = pd.read_csv(fn)
        df_obj = df_obj[(df_obj.date == date) & (df_obj.time <= '14:54:00')]
        df_obj = df_obj.set_index('time')
        df_obj['value'] = df_obj['close']
        df_obj.index.name = 'obj'
        if df_obj is not None:
            if m1 is None:
                show_31([df31], [df32], [df_obj], code)
            else:
                df41, df42  = df_atm_plus_obj_min5(m1, date, gap)
                df41 = df_iv(m1, df41)
                df42 = df_iv(m1, df42)
                if df41 is not None and df42 is not None:
                    show_31([df31, df41], [df32, df42], [df_obj], code)

    # 持仓量
    df_c, df_p = df_open_interest(code)
    if df_c is not None and df_p is not None:
        show_11([df_c, df_p], code)
    if m1 is not None:
        df_c, df_p = df_open_interest(m1)
        if df_c is not None and df_p is not None:
            show_11([df_c, df_p], m1)

    # 隐含分布
    df = implied_dist(date, code, gap)
    if df is not None:
        show_11([df], code, False)

    # # skew_day
    # for symbol in [m0, m1]:
    #     code = symbol
    #     if code is not None:
    #         df_right_c, df_right_p = df_atm_plus_obj_day(code, date, gap, shift=2)
    #         df_left_c, df_left_p = df_atm_plus_obj_day(code, date, gap, shift=-2)
    #         if df_right_c is not None and df_right_p is not None and df_left_c is not None and df_left_p is not None:
    #             df_right_c = df_iv(code, df_right_c)
    #             df_right_p = df_iv(code, df_right_p)
    #             df_left_c = df_iv(code, df_left_c)
    #             df_left_p = df_iv(code, df_left_p)
    #             # print(df_right_c.tail())
    #             # print(df_left_c.tail())
    #             df_right_c['value'] = 100*(df_right_c['value']/df_left_c['value'] - 1)
    #             df_left_p['value'] = 100*(df_left_p['value']/df_right_p['value'] - 1)
    #             df_right_c.index.name = 'skew_day_c'
    #             df_left_p.index.name = 'skew_day_p'
    #             if df_right_c is not None and df_left_p is not None:
    #                 show_21([df_left_p], [df_right_c], code)
    #                 # print(df_right_c.tail())
    #
    # # skew__min5
    # for symbol in [m0, m1]:
    #     code = symbol
    #     if code is not None:
    #         df_right_c, df_right_p = df_atm_plus_obj_min5(code, date, gap, shift=2)
    #         df_left_c, df_left_p = df_atm_plus_obj_min5(code, date, gap, shift=-2)
    #         if df_right_c is not None and df_right_p is not None and df_left_c is not None and df_left_p is not None:
    #             df_right_c = df_iv(code, df_right_c)
    #             df_right_p = df_iv(code, df_right_p)
    #             df_left_c = df_iv(code, df_left_c)
    #             df_left_p = df_iv(code, df_left_p)
    #             df_right_c['value'] = 100*(df_right_c['value']/df_left_c['value'] - 1)
    #             df_left_p['value'] = 100*(df_left_p['value']/df_right_p['value'] - 1)
    #             df_right_c.index.name = 'skew_min5_c'
    #             df_left_p.index.name = 'skew_min5_p'
    #             if df_right_c is not None and df_left_p is not None:
    #                 show_21([df_left_p], [df_right_c], code)
    #                 # print(df_right_c.tail())

def CF(date, df):
    m0 = df[(df.pz == 'CF') & (df.flag == 'm0')].iloc[0,:].symbol
    m1 = df[(df.pz == 'CF') & (df.flag == 'm1')].iloc[0,:].symbol

    cy0 = 'CY' + m0[2:]
    code = m0
    gap = 200

    common(date, code, gap)
    smile(date, m0)

    # ic
    show_ic(m0, m1, code, date)
    show_ic(m0, cy0, code, date)

    img2pdf('compass_CF_'+date+'.pdf',
            [m0+'_'+m0+'_hv_'+m0+'_'+m0+'_hv_'+m0+'_iv_c.jpg',
             m0+'_'+m0+'_iv_c_'+m0+'_'+m0+'_iv_p_'+m0+'_obj.jpg',
             m0+'_smile.jpg',
             m0+'_skew_day_p_'+m0+'_skew_day_c.jpg',
             m0+'_skew_min5_p_'+m0+'_skew_min5_c.jpg',
             m0+'_'+m0+'_implied_dist.jpg',
             m0+'_'+m0+'_openinterest_c_'+m0+'_openinterest_p.jpg',
             m0+'_ic_'+m0+'_'+m1+'.jpg',
             m0+'_ic_'+m0+'_'+cy0+'.jpg',
            ])
    fn1 = dirname+'compass_CF_'+date+'.pdf'
    return [fn1]

def m(date, df):
    m0 = df[(df.pz == 'm') & (df.flag == 'm0')].iloc[0,:].symbol
    m1 = df[(df.pz == 'm') & (df.flag == 'm1')].iloc[0,:].symbol
    # m2 = df[(df.pz == 'm') & (df.flag == 'm2')].iloc[0,:].symbol
    # m0 = 'm2101'
    # m1 = 'm2105'
    y0 = 'y' + m0[1:]
    y1 = 'y' + m1[1:]
    p0 = 'p' + m0[1:]
    p1 = 'p' + m1[1:]
    # b0 = 'b' + m0[1:]
    # b1 = 'b' + m1[1:]
    RM0 = 'RM' + m0[2:]
    RM1 = 'RM' + m1[2:]
    code = m0
    gap = 50

    common(date, code, gap)
    smile(date, m0)

    # ip
    show_ic(m0, m1, code, date)

    # ip
    show_ic(y0, m0, code, date)
    show_ic(y1, m1, code, date)
    # show_ic(b0, m0, code, date)
    # show_ic(b1, m1, code, date)
    show_ic(y0, p0, code, date)
    show_ic(y1, p1, code, date)
    show_ic(m0, RM0, code, date)
    show_ic(m1, RM1, code, date)

    img2pdf('compass_m_'+date+'.pdf',
            [
             m0+'_'+m0+'_iv_c_'+m0+'_'+m0+'_iv_p_'+m0+'_obj.jpg',
             m0+'_'+m0+'_hv_'+m0+'_'+m0+'_hv_'+m0+'_iv_c.jpg',
             m0+'_smile.jpg',
             m0+'_'+m0+'_implied_dist.jpg',
             m0+'_'+m0+'_openinterest_c_'+m0+'_openinterest_p.jpg',
             m0+'_ic_'+m0+'_'+m1+'.jpg',
             m0+'_ic_'+y0+'_'+m0+'.jpg',
             m0+'_ic_'+y1+'_'+m1+'.jpg',
             # m0+'_ic_'+b0+'_'+m0+'.jpg',
             # m0+'_ic_'+b1+'_'+m1+'.jpg',
             m0+'_ic_'+y0+'_'+p0+'.jpg',
             m0+'_ic_'+y1+'_'+p1+'.jpg',
             m0+'_ic_'+m0+'_'+RM0+'.jpg',
             m0+'_ic_'+m1+'_'+RM1+'.jpg',
            ])

    fn1 = dirname+'compass_m_'+date+'.pdf'

    d2 = get_dss() + 'info/hold/img/'
    listfile = os.listdir(d2)
    for fn in listfile:
        os.remove(d2+fn)
    hold_product('dce', m0)
    fn2 = d2 + 'dce.pdf'
    return [fn1, fn2]

def RM(date, df):
    m0 = df[(df.pz == 'RM') & (df.flag == 'm0')].iloc[0,:].symbol
    m1 = df[(df.pz == 'RM') & (df.flag == 'm1')].iloc[0,:].symbol

    code = m0
    gap = 50

    common(date, code, gap)
    smile(date, m0)

    # ic
    show_ic(m0, m1, code, date)

    img2pdf('compass_RM_'+date+'.pdf',
            [m0+'_'+m0+'_hv_'+m0+'_'+m0+'_hv_'+m0+'_iv_c.jpg',
             m0+'_'+m0+'_iv_c_'+m0+'_'+m0+'_iv_p_'+m0+'_obj.jpg',
             m0+'_smile.jpg',
             m0+'_skew_day_p_'+m0+'_skew_day_c.jpg',
             m0+'_skew_min5_p_'+m0+'_skew_min5_c.jpg',
             m0+'_'+m0+'_implied_dist.jpg',
             m0+'_'+m0+'_openinterest_c_'+m0+'_openinterest_p.jpg',
             m0+'_ic_'+m0+'_'+m1+'.jpg',
            ])
    fn1 = dirname+'compass_RM_'+date+'.pdf'
    return [fn1]

def IO(date, df):
    m0 = df[(df.pz == 'IO') & (df.flag == 'm0')].iloc[0,:].symbol
    m1 = df[(df.pz == 'IO') & (df.flag == 'm1')].iloc[0,:].symbol
    m2 = df[(df.pz == 'IO') & (df.flag == 'm2')].iloc[0,:].symbol
    m3 = df[(df.pz == 'IO') & (df.flag == 'm3')].iloc[0,:].symbol

    code = m0
    gap = 100

    common(date, code, gap, m1)
    term_structure(m0, m1, m2, m3, date, gap)
    sdiffer(m0, m1, date, gap)
    smile(date, m0, m1)

    img2pdf('compass_IO_'+date+'.pdf',
            [m0+'_'+m0+'_hv_'+m0+'_'+m0+'_hv_'+m0+'_iv_c_'+m1+'_iv_c.jpg',
             m0+'_'+m0+'_iv_c_'+m1+'_iv_c_'+m0+'_'+m0+'_iv_p_'+m1+'_iv_p_'+m0+'_obj.jpg',
             m0+'_sdiffer.jpg',
             m0+'_term_structure.jpg',
             m0+'_smile.jpg',
             m1+'_smile.jpg',
             m0+'_skew_day_p_'+m0+'_skew_day_c.jpg',
             m0+'_skew_min5_p_'+m0+'_skew_min5_c.jpg',
             m1+'_skew_day_p_'+m1+'_skew_day_c.jpg',
             m1+'_skew_min5_p_'+m1+'_skew_min5_c.jpg',
             m0+'_'+m0+'_implied_dist.jpg',
             m0+'_'+m0+'_openinterest_c_'+m0+'_openinterest_p.jpg',
             m1+'_'+m1+'_openinterest_c_'+m1+'_openinterest_p.jpg',
            ])

    fn1 = dirname+'compass_IO_'+date+'.pdf'
    return [fn1]

def al(date, df):
    m0 = df[(df.pz == 'al') & (df.flag == 'm0')].iloc[0,:].symbol
    m1 = df[(df.pz == 'al') & (df.flag == 'm1')].iloc[0,:].symbol

    code = m0
    gap = 100

    common(date, code, gap)

    img2pdf('compass_al_'+date+'.pdf',
            [m0+'_'+m0+'_hv_'+m0+'_'+m0+'_hv_'+m0+'_iv_c.jpg',
             m0+'_'+m0+'_iv_c_'+m0+'_'+m0+'_iv_p_'+m0+'_obj.jpg',
             m0+'_skew_day_p_'+m0+'_skew_day_c.jpg',
             m0+'_skew_min5_p_'+m0+'_skew_min5_c.jpg',
             m0+'_'+m0+'_implied_dist.jpg',
             m0+'_'+m0+'_openinterest_c_'+m0+'_openinterest_p.jpg',
            ])

    fn1 = dirname+'compass_al_'+date+'.pdf'
    return [fn1]

def compass(date):
    listfile = os.listdir(dirname)
    for fn in listfile:
        os.remove(dirname+fn)

    fn = get_dss() + 'fut/cfg/opt_mature.csv'
    df2 = pd.read_csv(fn)
    df2 = df2[pd.notnull(df2.flag)]

    fn_list = []
    # for func in [CF, m, RM, IO]:
    for func in [m]:
        try:
            fn_list += func(date, df2)
        except Exception as e:
            s = traceback.format_exc()
            to_log(s)

    # print(fn_list)
    send_email(get_dss(), 'compass', '', fn_list)

if __name__ == '__main__':
    now = datetime.now()
    date = now.strftime('%Y-%m-%d')
    date = '2020-11-20'
    # date = '2020-11-23'

    compass(date)
