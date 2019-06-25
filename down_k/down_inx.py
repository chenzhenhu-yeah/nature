# -*- coding: utf-8 -*-

import os
import numpy as np
import pandas as pd
import tushare as ts
import io

def down_inx_single(code,dss,xtim0='2018-01-01'):
    ''' 下载大盘指数数据
    '''
    xcod=code;tim0=xtim0;xd=[];
    fss = dss + 'inx/' + code + '.csv'

    xfg=os.path.exists(fss);xd0=[];
    if xfg:
        xd0= pd.read_csv(fss,index_col=0,parse_dates=[0])
        xd0=xd0.sort_index(ascending=False);
        #tim0=xd0.index[0];
        _xt=xd0.index[0];#xt=xd0.index[-1];###
        s2=str(_xt);tim0=s2.split(" ")[0]

    print('\n',fss,"lastdate: ",tim0);

    try:
        xd=ts.get_h_data(xcod,start=tim0,index=True,end=None,retry_count=5,pause=1)     #Day9
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
    #codes = ['000001']
    for code in codes:
        down_inx_single(code,dss)

if __name__ == '__main__':
    down_inx_all(r'../../data/')
