# -*- coding: utf-8 -*-

import os
import time
import numpy as np
import pandas as pd
import tushare as ts
import json

from nature import get_dss, get_ts_code

# 加载配置
config = open(get_dss()+'csv/config.json')
setting = json.load(config)
pro_id = setting['pro_id']
pro = ts.pro_api(pro_id)

def get_hu_k_data(xcod,tim0,strInterface='pro'):
    if strInterface == 'hist':
        #df3=ts.get_hist_data(xcod,start=tim0,end=None,retry_count=5,pause=1);
        #df3.index=pd.to_datetime(df3.index)
        pass
    elif strInterface == 'pro':
        begin_dt = tim0[:4] + tim0[5:7] + tim0[8:10]
        df3 = ts.pro_bar(api=pro, ts_code=get_ts_code(xcod), adj='hfq', start_date=begin_dt)
        df3['volume']=df3['vol']
        df3['date']=df3['trade_date']
        df3.index=df3['date']
        df3.index=pd.to_datetime(df3.index, format='%Y%m%d')
    else:
        df3=ts.get_k_data(xcod,start=tim0,autype='hfq',);
        df3.index=df3['date']
        df3.index=pd.to_datetime(df3.index)

    #print(df3.head())
    #date,open,high,close,low,volume,amount
    col = ['open','high','close','low','volume']
    df2 = df3.loc[:,col]
    df2['amount'] = 0
    #print(df2.head())
    return df2

def down_stk_hfq_single(code,dss,tim01,strInterface='pro'):
    xcod = code
    tim0 = tim01
    xd0=[];xd=[];
    fss = dss +'hfq/' + code + '.csv'

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

def down_stk_hfq_all(dss,time0='2018-01-01',strInterface='pro'):
    codes = []
    listfile = os.listdir(dss + 'daily')
    listfile.sort(reverse=True)
    lastday = listfile[0][:10]
    fn = dss+'daily/'+lastday+'_stk_all.csv'
    print(fn)
    df = pd.read_csv(fn, dtype='str', encoding='gbk')
    codes = list(df['code'])

    #codes = ['300408']
    print('down_stk_hfq begin ...')

    for code in codes:
        #down_stk_hfq_single(code,time0,strInterface);
        try:
            down_stk_hfq_single(code,dss,time0,strInterface);
            # 流控，每分种200次, 稳妥起见，实际按每分钟约120次
            time.sleep(0.5)
            #print('{} got it'.format(code))
        except:
            print('{} error!'.format(code))

if __name__ == '__main__':
    down_stk_hfq_all(r'../../data/')
    #down_stk_hfq_single('002918', r'../../data/','2018-01-01','pro')
