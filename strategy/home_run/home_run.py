
import pandas as pd
from datetime import datetime, timedelta

import sys
sys.path.append(r'../../')
from down_k.get_trading_dates import get_trading_dates
from down_k.get_stk import get_stk_hfq

def keep(dss,code, date):
    r = True

    df = get_stk_hfq(dss,code,date)
    n = len(df)
    #print(code,date,n)
    if n > 0:
        price_g = df.loc[n-1,'open']
        df1 =df.loc[:n-1,:]
        min_low = df1['low'].min()
        if min_low < price_g:
            r = False

    return r

# 高位盘整搏突破，全垒打
def home_run(dss):
    r = []

    all_dates = get_trading_dates(dss)
    date = all_dates[-1]

    filename = dss + 'csv/home_run.csv'
    df = pd.read_csv(filename,dtype='str')
    for i,row in df.iterrows():
        if keep(dss, row.code, row.date):
            r.append([date, row.code,'home_run', date])

    df = pd.DataFrame(r,columns=['date', 'code','strategy','expire'])
    filename = dss + 'csv/bottle.csv'
    df.to_csv(filename,index=False,mode='a',header=None)

if __name__ == '__main__':
    home_run(r'../../../data/')
