import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import zipfile
import os

from nature import get_dss



date_pre =  '2020-09-11'
date_begin = '2020-09-14'
date_end = '2020-09-14'

fn = get_dss() + 'fut/bar/min5_IF2009.csv'
df = pd.read_csv(fn)
df = df[df.time == '09:34:00']
df = df[(df.date >= date_begin) & (df.date <= date_end)]

date_list = sorted(list(df.date))
# print(df)
gap = 50
# gap = 100

for date in date_list:
    df1 = df[df.date == date]
    rec = df1.iloc[0,:]
    obj = rec.close
    atm = int(round(round(obj*(100/gap)/1E4,2) * 1E4/(100/gap), 0))     # 获得平值
    print(date, obj, atm)

    fn = get_dss() + 'fut/bar/min5_IO2009-C-' + str(atm) + '.csv'
    df_m0_c = pd.read_csv(fn)
    df_m0_c_pre = df_m0_c[df_m0_c.date == date_pre]
    df_m0_c = df_m0_c[df_m0_c.date == date]

    fn = get_dss() + 'fut/bar/min5_IO2009-P-' + str(atm) + '.csv'
    df_m0_p = pd.read_csv(fn)
    df_m0_p_pre = df_m0_p[df_m0_p.date == date_pre]
    df_m0_p = df_m0_p[df_m0_p.date == date]

    fn = get_dss() + 'fut/bar/min5_IO2010-C-' + str(atm) + '.csv'
    df_m1_c = pd.read_csv(fn)
    df_m1_c_pre = df_m1_c[df_m1_c.date == date_pre]
    df_m1_c = df_m1_c[df_m1_c.date == date]

    fn = get_dss() + 'fut/bar/min5_IO2010-P-' + str(atm) + '.csv'
    df_m1_p = pd.read_csv(fn)
    df_m1_p_pre = df_m1_p[df_m1_p.date  == date_pre]
    df_m1_p = df_m1_p[df_m1_p.date  == date]

    open_m1 = df_m1_c_pre.iat[-1,5] + df_m1_p_pre.iat[-1,5]
    open_m0 = df_m0_c_pre.iat[-1,5] + df_m0_p_pre.iat[-1,5]
    print(open_m0, open_m1)
    date_pre = date

    df_m1_c = df_m1_c.reset_index()
    df_m1_p = df_m1_p.reset_index()
    df_m0_c = df_m0_c.reset_index()
    df_m0_p = df_m0_p.reset_index()

    # print(len(df_m0_c), len(df_m0_p),len(df_m1_c), len(df_m1_p))
    # print(df_m1_c.head())
    # print(df_m1_p .head())
    # print(df_m0_c.head())
    # print(df_m0_p .head())

    df_m1_c['diff_m1'] = df_m1_c.close + df_m1_p.close - open_m1
    df_m0_c['diff_m0'] = df_m0_c.close + df_m0_p.close - open_m0
    df_m1_c['differ'] = df_m1_c.diff_m1 - df_m0_c.diff_m0
    # print(df_m1_c.head())
    # print(df_m0_c.head())


    df_m1_c['pz'] = 'IO'
    df_m1_c['basic_m0'] = 'IO2009'
    df_m1_c['basic_m1'] = 'IO2010'
    df_m1_c['atm'] = atm
    df_m1_c['diff_m0'] = df_m0_c['diff_m0']
    df_m1_c['stat'] = 'y'
    df2 = df_m1_c[['date','time','pz','basic_m0','basic_m1','atm','diff_m0','diff_m1','differ','stat']]
    # print(df2.head())
    fn = get_dss() + 'opt/straddle_differ.csv'

    if os.path.exists(fn):
        df2.to_csv(fn, index=False, mode='a', header=False)
    else:
        df2.to_csv(fn, index=False)

    # break
