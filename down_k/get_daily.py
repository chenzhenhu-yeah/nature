import os
import pandas as pd
from datetime import datetime, timedelta

def get_daily(dss,day):
    df = None
    fss = dss + 'daily/' + day + '_stk_all.csv'
    if os.path.exists(fss):
        df = pd.read_csv(fss,dtype={'code':'str'},encoding='gbk')

    return df


def get_stk_codes(dss,day='2018-08-08',market=['CYB','ZXB','SZ']):
    df = get_daily(dss,day)
    df = df[~df.code.str.startswith('6')]
    # print(len(df))
    # print(df.sample(100))

    codes = list(df['code'])
    return codes

if __name__ == "__main__":
    # df = get_daily(r'../../data/','2018-05-30')
    # print(df.head())

    #get_stk_codes(r'../../data/')
    pass
