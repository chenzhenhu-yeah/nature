import numpy as np
import pandas as pd

import smtplib
from email.mime.text import MIMEText

import os
import re
import datetime
import time

from nature import get_dss, get_trading_dates, get_daily, get_stk_hfq

import json
import tushare as ts
# 加载配置
config = open(get_dss()+'csv/config.json')
setting = json.load(config)
pro_id = setting['pro_id']              # 设置服务器
pro = ts.pro_api(pro_id)

def get_ts_code(code):
    if code[0] == '6':
        code += '.SH'
    else:
        code += '.SZ'

    return code

def be_bottom(code, day):
    df = get_stk_hfq(get_dss(),code,end_date=day)
    if df is None:
        return False

    if len(df) <= 360:
        return False

    df30 = df.loc[:30]
    df360 = df.loc[:360]
    if df30.low.min() <= df360.low.min():
        return True
    else:
        return False

def price_signal(dss, day):
    df = get_daily(dss, day)
    # print(df.head(3))
    # print(len(df))

    df1 = df[df.p_change > 6.18]
    # print(len(df1))
    # print(df1.head(3))

    r = []
    for i,row in df1.iterrows():
        # print(row.code, row['name'], row.p_change)
        if be_bottom(row.code, day):
            df2 = pro.concept_detail(ts_code = get_ts_code(row.code))
            concept_name = df2.concept_name.tolist()
            r.append( str([day, row.code, row['name'], row.p_change, concept_name]) )

            # 拷贝数据文件到下载目录，以备下载。
            ins = 'copy ' + get_dss() + 'hfq/' + row.code + '.csv ' + 'C:/Users/Administrator/Downloads/' + row.code + '_' + row['name']+ '.csv '
            print(ins)
            os.system(ins)
        #break
    return r

if __name__ == '__main__':
    dates = get_trading_dates(get_dss())
    #preday = dates[-2]
    today = dates[-1]
    print(today)
    r = price_signal(get_dss(),today)
    print(r)
