
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta


import sys
sys.path.append(r'../../')
from down_k.get_trading_dates import get_trading_dates
from down_k.get_stk import get_stk_hfq
from down_k.get_inx import get_inx
from hu_signal.hu_talib import MA

def calc_inx(dss, code,base_date, end_date):
    df = get_inx(dss, '399001',base_date, end_date)
    l = len(df)

    #v = df.iat[-1,6]  # amount
    #df['rate'] = round((df['amount']/v -1),4)
    v = df.at[l-1,'volume']
    df['rate_v'] = round((df['volume']/v -1),4)

    c = df.at[l-1,'close']
    df['rate_c'] = round((df['close']/c -1),4)

    df = df.set_index('date')
    #print(df)
    return df


def calc_stk(dss,code,base_date, end_date,df_inx):
    df_v, df_c = None,None
    df = get_stk_hfq(dss,code,base_date, end_date)
    l = len(df)
    if l> 20:
        v = df.at[l-1,'volume']
        c = df.at[l-1,'close']

        df = df.set_index('date')
        df = df.sort_index()

        df['rate_v'] = round((df['volume']/v -1),4)
        df['rate_v'] = df['rate_v'] - df_inx['rate_v']
        df_v =  MA(df, 5, 'rate_v')
        df_v =  MA(df_v, 20, 'rate_v')
        #print(df_v)

        df['rate_c'] = round((df['close']/c -1),4)
        df['rate_c'] = df['rate_c'] - df_inx['rate_c']
        df_c =  MA(df, 5, 'rate_c')
        df_c =  MA(df_c, 20, 'rate_c')
        #print(df_c)
    return df_v, df_c

def to_bottle(code, df_v, df_c):
    r = []
    dates = list(df_v.index)
    date = dates.pop()
    pre_date = dates.pop()
    expire_date =  (datetime.now()+timedelta(days=30)).strftime('%Y-%m-%d')

    if df_v.at[date,'ma_5'] > df_v.at[date,'ma_20'] and \
       df_v.at[pre_date,'ma_5'] <= df_v.at[pre_date,'ma_20'] and \
       df_c.at[date,'ma_5'] > df_c.at[date,'ma_20'] :
        r.append([date,code,'simmer',expire_date])

    df = pd.DataFrame(r, columns=['date','code','stratege','expire'])
    return df

# 量升价涨有金主
def simmer_run(dss):
    all_dates = get_trading_dates(dss)

    base_date = all_dates[-30]
    end_date  = all_dates[-1]

    df_inx = calc_inx(dss, '399001',base_date, end_date)

    filename = dss + 'csv/stk_cyb.csv'
    df = pd.read_csv(filename,dtype={'code':str},encoding='gbk')
    codes = set(df['code'])
    #codes = ['002273','300433']

    df_bottle = pd.DataFrame([],columns=['date','code','stratege','expire'])
    for code in codes:
        df_v, df_c = calc_stk(dss,code,base_date, end_date, df_inx)
        if df_v is None or df_c is None:
            pass
        else:
            df_bottle = df_bottle.append(to_bottle(code,df_v, df_c))

    df_bottle = df_bottle.sort_values('date')
    filename = dss + 'csv/bottle.csv'
    df_bottle.to_csv(filename,index=False,mode='a',header=None)


if __name__ == "__main__":
    simmer_run(r'../../../data/')
