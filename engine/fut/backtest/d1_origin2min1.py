
import os
import time
import datetime
import json
import pandas as pd

from nature import get_dss

if __name__ == "__main__":
    # 05合约：2018年（1805,1905），2019年（1905,2005），
    # 01合约：2018年（1901），2019年（2001），

    pz = 'ru'
    symbol = 'ru2001'
    fn = "C:\\Users\\czh\\Documents\\critical\\data\\FutAC_Min1_Std_2019\\" + symbol + '.csv'

    df = pd.read_csv(fn, skiprows=1, header=None, names=['market','symbol','datetime','open','high','low','close','volume','amout','hold'])
    #print(df.head(3))
    df['date'] = df.datetime.str[:10]
    df['time'] = df.datetime.str[11:]
    df = df.loc[:,['symbol','date','time','open','high','low','close','volume','amout','hold']]
    print(df.head(3))
    fn = get_dss() +'backtest/fut/'+  pz + '/' + symbol + '.csv'
    if os.path.exists(fn):
        df.to_csv(fn, index=False, mode='a', header=None)
    else:
        df.to_csv(fn, index=False)
