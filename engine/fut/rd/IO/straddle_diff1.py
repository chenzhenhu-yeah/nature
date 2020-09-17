import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import zipfile
import os

from nature import get_dss



date_begin = '2020-07-20'
date_end = '2020-08-10'

fn = get_dss() + 'fut/bar/min5_IF2008.csv'
df = pd.read_csv(fn)
df = df[df.time == '09:34:00']
df = df[(df.date >= date_begin) & (df.date <= date_end)]

date_list = sorted(list(df.date))
# print(df)
# gap = 50
gap = 100
for date in date_list:
    df1 = df[df.date == date]
    rec = df1.iloc[0,:]
    obj = rec.close
    atm = int(round(round(obj*(100/gap)/1E4,2) * 1E4/(100/gap), 0))     # 获得平值
    print(date, obj, atm)

    fn = get_dss() + 'fut/bar/min5_IO2008-C-' + str(atm) + '.csv'
    df_m0_c = pd.read_csv(fn)
    df_m0_c = df_m0_c[df_m0_c.date == date]

    fn = get_dss() + 'fut/bar/min5_IO2008-P-' + str(atm) + '.csv'
    df_m0_p = pd.read_csv(fn)
    df_m0_p = df_m0_p[df_m0_p.date == date]

    fn = get_dss() + 'fut/bar/min5_IO2009-C-' + str(atm) + '.csv'
    df_m1_c = pd.read_csv(fn)
    df_m1_c = df_m1_c[df_m1_c.date == date]

    fn = get_dss() + 'fut/bar/min5_IO2009-P-' + str(atm) + '.csv'
    df_m1_p = pd.read_csv(fn)
    df_m1_p = df_m1_p[df_m1_p.date  == date]

    # print(len(df_m0_c), len(df_m0_p),len(df_m1_c), len(df_m1_p))
    # print(df_m1_c.head())
    # print(df_m1_p .head())
    base_m1 = df_m1_c.iat[0,5] + df_m1_p.iat[0,5]
    base_m0 = df_m0_c.iat[0,5] + df_m0_p.iat[0,5]


    df_m1_c['diff_m1'] = df_m1_c.close + df_m1_p.close - base_m1
    df_m0_c['diff_m0'] = df_m0_c.close + df_m0_p.close - base_m0
    df_m1_c['diff'] = df_m1_c.diff_m1 - df_m0_c.diff_m0
    # # print(df_m1_c.head())
    #
    df2 = df_m1_c[['date', 'time', 'diff']]
    # print(df2.head())
    fn = get_dss() + 'opt/straddle_diff.csv'
    if os.path.exists(fn):
        df2.to_csv(fn, index=False, mode='a', header=False)
    else:
        df2.to_csv(fn, index=False)

    # break