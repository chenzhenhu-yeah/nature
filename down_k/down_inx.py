# -*- coding: utf-8 -*-

import os
import numpy as np
import pandas as pd

import io
import json
import tushare as ts

from nature import get_dss


# 加载配置
config = open(get_dss()+'csv/config.json')
setting = json.load(config)
pro_id = setting['pro_id']
pro = ts.pro_api(pro_id)
ts_code_dict = {'000001':'000001.SH','000300':'000300.SH','399001':'399001.SZ','399005':'399005.SZ','399006':'399006.SZ','399905':'399905.SZ'}

def down_inx(xcod, start):
    start = start.replace('-','')
    df = pro.index_daily( ts_code=ts_code_dict[xcod], start_date=start, end_date=None )
    df['date'] = df['trade_date'].str.slice(0,4) + '-' + df['trade_date'].str.slice(4,6) + '-' + df['trade_date'].str.slice(6,8)
    df['volume'] = df['vol']
    df = df.loc[:, ['date','open','high','close','low','volume','amount'] ]
    df = df.set_index('date')

    return df


def down_inx_single(code,dss,xtim0='2018-01-01'):
    ''' 下载大盘指数数据
    '''
    xcod=code;tim0=xtim0;xd=[];
    fss = dss + 'inx/' + code + '.csv'

    xfg=os.path.exists(fss);xd0=[];
    if xfg:
        # xd0= pd.read_csv(fss,index_col=0,parse_dates=[0])
        xd0= pd.read_csv(fss,index_col=0)
        xd0=xd0.sort_index(ascending=False);
        #tim0=xd0.index[0];
        _xt=xd0.index[0];#xt=xd0.index[-1];###
        s2=str(_xt);tim0=s2.split(" ")[0]

    print('\n',fss,"lastdate: ",tim0);

    try:
        # xd=ts.get_h_data(xcod,start=tim0,index=True,end=None,retry_count=5,pause=1)     #Day9
        xd = down_inx(xcod, tim0)

        #-------------
        if xd is not None:
            if (len(xd0)>0):
                xd2 =xd0.append(xd)
                #  flt.dup
                xd2["index"]=xd2.index
                xd2.drop_duplicates(subset='index', keep='last', inplace=True);
                del(xd2["index"]);
                #xd2.index=pd.to_datetime(xd2.index)
                xd=xd2;

            xd=xd.sort_index(ascending=False);
            xd=np.round(xd,3);
            xd.to_csv(fss)
    except IOError:
        print('error!')
        pass    #skip,error

    return xd


def down_inx_all(dss):
    codes = ['000001','000300','399001','399005','399006','399905']
    # codes = ['000001']
    for code in codes:
        down_inx_single(code,dss)

if __name__ == '__main__':
    down_inx_all(r'../../data/')
