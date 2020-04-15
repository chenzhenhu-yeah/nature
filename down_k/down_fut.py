# -*- coding: utf-8 -*-

import os
import time
import numpy as np
import pandas as pd
import tushare as ts
import json

from nature import get_dss

# 加载配置
config = open(get_dss()+'csv/config.json')
setting = json.load(config)
pro_id = setting['pro_id']
pro = ts.pro_api(pro_id)

def get_hu_k_data(xcod,tim0):


    df3 = pro.fut_daily(ts_code=xcod, start_date=tim0)
    #df3.index=df3['trade_date']
    df3 = df3.set_index('trade_date')
    df3.index=pd.to_datetime(df3.index)

    # print(df3.head())
    return df3

def down_fut_single(code,dss,tim01):
    xcod = code
    tim0 = tim01
    xd0=[];xd=[];
    fss = dss +'fut/' + code + '.csv'

    #若文件已存在，获取最近日期
    xfg=os.path.exists(fss)
    if xfg:
        xd0= pd.read_csv(fss,index_col=0,parse_dates=[0])
        if len(xd0.index) > 0:
            xd0=xd0.sort_index(ascending=False);
            _xt=xd0.index[0];#xt=xd0.index[-1];###
            s2=str(_xt);
            tim0=s2.split(" ")[0]

    xd=get_hu_k_data(xcod,tim0)

    # 将下载的最新数据追加到文件中
    if not xd.empty:
        if (len(xd0)>0):
            xd2 =xd0.append(xd,sort=False)
            xd2["index"]=xd2.index
            xd2.drop_duplicates(subset='index', keep='last', inplace=True);
            del(xd2["index"]);
            xd=xd2;
        xd=xd.sort_index(ascending=False);
        xd=np.round(xd,3);
        xd.to_csv(fss)

def down_fut_all(dss,time0='2020-01-01'):
    # codes = ['IC1909.CFX','IC1906.CFX','IC.CFX',]
    codes = ['al2006.SHFE','al2007.SHFE','al2008.SHFE',]

    print('down_fut begin ...')

    for code in codes:
        #down_fut_single(code,dss,time0);
        try:
            down_fut_single(code,dss,time0);
            #print('{} got it'.format(code))
        except:
            print('{} error!'.format(code))

if __name__ == '__main__':
    down_fut_all(r'../../data/')
