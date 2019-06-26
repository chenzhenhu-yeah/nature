
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta


from nature import get_trading_dates, get_stk_hfq

def MACD(df, n_fast=12, n_slow=26, ksgn='close'):
    '''
    【输入】
        df, pd.dataframe格式数据源
        n，时间长度
        ksgn，列名，一般是：close收盘价
    【输出】
        df, pd.dataframe格式数据源,
        增加了3栏：macd,sign,mdiff
    '''
    EMAfast = pd.Series(df[ksgn].ewm(span = n_fast).mean())
    EMAslow = pd.Series(df[ksgn].ewm(span = n_slow).mean())

    MACDdiff = pd.Series(EMAfast - EMAslow, name='DIFF')
    MACDdea = pd.Series(MACDdiff.ewm(span = 9).mean(), name='DEA')
    MACD = pd.Series((MACDdiff - MACDdea)*2, name = 'MACD')

    df = df.join(MACD)
    df = df.join(MACDdiff)
    df = df.join(MACDdea)
    return df

def get_signal(code,_date,df_signal):
    #return df: columns=[date,macd]
    df1 = df_signal[(df_signal.code==code) & (df_signal.date<=_date)]
    df1 = df1.sort_values(by='date')
    #print(df1.tail())
    df1 = df1.loc[:,['date','MACD']]
    #ipdb.set_trace()
    return df1

def signal_macd_sell(codes, _date,df_signal):
    r = []
    for code in codes:
        df1 = get_signal(code,_date,df_signal) #columns=[date,macd]
        if len(df1) >= 30:
            if df1.iat[-1,0] == _date:
                if df1.iat[-1,1] <= 0 and df1.iat[-2,1] > 0:
                    r.append(code)
    return r

def signal_macd_buy(codes, _date,df_signal):
    r = []
    for code in codes:
        df1 = get_signal(code,_date,df_signal) #columns=[date,macd]
        if len(df1) >= 30:
            if df1.iat[-1,0] == _date:
                if df1.iat[-1,1] > 0 and df1.iat[-2,1] <= 0:
                    df2 = df1.iloc[-21:-1,:]
                    if len(df2[df2.MACD>0]) in [3,4,5,6,7,8,9]:
                        r.append(code)
                        #print(df1)
    return r

def init_signal_macd(dss,codes):
    df_signal = pd.DataFrame(columns=['code','date','close','MACD','DIFF','DEA'])

    for code in codes:
        df1 = df_signal[df_signal.code==code]
        if len(df1) == 0:
            df1 = get_stk_hfq(dss,code)
            df1 = df1[['date','close']]
            df1 = df1.sort_values(by='date')
            #print(df1)
            if len(df1) >= 30:
                df1 = MACD(df1)
                #print(df1)
                df1['code'] = code
                df_signal = df_signal.append(df1,sort=False)
                #print(df_signal)
                #ipdb.set_trace()

    return df_signal

if __name__ == "__main__":
    pass
