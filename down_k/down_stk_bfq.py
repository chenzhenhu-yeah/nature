# -*- coding: utf-8 -*-

import os
import time
import numpy as np
import pandas as pd
import tushare as ts

def get_hu_k_data(xcod,tim0,strInterface='k'):
    if strInterface == 'hist':
        #df3=ts.get_hist_data(xcod,start=tim0,end=None,retry_count=5,pause=1);
        #df3.index=pd.to_datetime(df3.index)
        pass
    else:
        df3=ts.get_k_data(xcod,start=tim0,autype='bfq',);
        df3.index=df3['date']
        df3.index=pd.to_datetime(df3.index)

    #print(df3.head())
    #date,open,high,close,low,volume,amount
    col = ['open','high','close','low','volume']
    df2 = df3.loc[:,col]
    df2['amount'] = 0
    #print(df2.head())
    return df2

def down_stk_bfq_single(code,dss,tim01,strInterface='k'):
    xcod = code
    tim0 = tim01
    xd0=[];xd=[];
    fss = dss +'bfq/' + code + '.csv'

    #若文件已存在，获取最近日期
    xfg=os.path.exists(fss)
    if xfg:
        xd0= pd.read_csv(fss,index_col=0,parse_dates=[0])
        if len(xd0.index) > 0:
            xd0=xd0.sort_index(ascending=False);
            _xt=xd0.index[0];#xt=xd0.index[-1];###
            s2=str(_xt);
            tim0=s2.split(" ")[0]

    xd=get_hu_k_data(xcod,tim0,strInterface)

    # 将下载的最新数据追加到文件中
    if not xd.empty:
        if (len(xd0)>0):
            xd2 =xd0.append(xd)
            xd2["index"]=xd2.index
            xd2.drop_duplicates(subset='index', keep='last', inplace=True);
            del(xd2["index"]);
            xd=xd2;
        xd=xd.sort_index(ascending=False);
        xd=np.round(xd,3);
        xd.to_csv(fss)

def down_stk_bfq_all(dss,time0='2018-01-01',strInterface='k'):
    codes = []
    df = pd.read_csv(dss+'daily/2019-05-23_stk_all.csv', dtype='str', encoding='gbk')
    codes = list(df['code'])

    # df = pd.read_csv(dss+'csv/stk_cyb.csv', dtype='str', encoding='gbk')
    # codes += list(df['code'])
    # df = pd.read_csv(dss+'csv/stk_zxb.csv', dtype='str', encoding='gbk')
    # codes += list(df['code'])
    # df = pd.read_csv(dss+'csv/stk_sz.csv', dtype='str', encoding='gbk')
    # codes += list(df['code'])


    #codes = ['300408']
    print('down_stk_bfq begin ...')

    for code in codes:
        #down_stk_hfq_single(code,time0,strInterface);
        try:
            down_stk_bfq_single(code,dss,time0,strInterface);
            #print('{} got it'.format(code))
        except:
            print('{} error!'.format(code))

if __name__ == '__main__':
    down_stk_bfq_all(r'../../data/')
