import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import os
from datetime import datetime

import zipfile
import os

from nature import get_dss, get_inx, get_contract

# def calc_skew(date):
def calc_skew():
    now = datetime.now()
    date = now.strftime('%Y-%m-%d')
    # date = '2020-09-02'

    # 数据基于greeks文件，该文件中已计算好合约的iv
    fn = get_dss() + 'opt/' + date[:7] + '_greeks.csv'
    if os.path.exists(fn) == False:
        return

    r = []
    basic_list = []                                           # 提取当日的basic
    df = pd.read_csv(fn)
    df = df[df.Localtime.str.slice(0,10) == date]
    for i, row in df.iterrows():
        basic_list.append( get_contract(row.Instrument).basic )
    basic_list = list(set(basic_list))
    for basic in basic_list:
        try:
            df1 = df[df.Instrument.str.slice(0,len(basic)) == basic]
            df1 = df1.drop_duplicates(subset=['Instrument'],keep='last')
            # df1 = df1.sort_values('Instrument')
            # print(df1.tail())

            if len(df1) > 0:
                rec = df1.iloc[0, :]
                obj = int(rec.obj)
                pz = str(get_contract(rec.Instrument).pz)
                gap = 50
                if pz == 'CF':
                    gap = 200
                if pz == 'm':
                    gap = 50
                if pz in ['RM', 'MA']:
                    gap = 25

                atm = int(round(round(obj*(100/gap)/1E4,2) * 1E4/(100/gap), 0))     # 获得平值
                # print(gap)
                # print(obj)
                # print(atm)

                exchangeID = str(get_contract(rec.Instrument).exchangeID)
                if exchangeID in ['CFFEX', 'DCE']:
                    atm_c = basic + '-C-' + str(atm)
                    atm_p = basic + '-P-' + str(atm)
                    otm_c = basic + '-C-' + str(atm+2*gap)
                    otm_p = basic + '-P-' + str(atm-2*gap)
                else:
                    atm_c = basic + 'C' + str(atm)
                    atm_p = basic + 'P' + str(atm)
                    otm_c = basic + 'C' + str(atm+2*gap)
                    otm_p = basic + 'P' + str(atm-2*gap)

                df1 = df1.set_index('Instrument')
                skew_c = round( 100*(df1.at[otm_c,'iv']-df1.at[atm_c,'iv']) / df1.at[atm_c,'iv'], 2)
                skew_p = round( 100*(df1.at[otm_p,'iv']-df1.at[atm_p,'iv']) / df1.at[atm_p,'iv'], 2)
                skew_mean = round( 100*( df1.at[otm_c,'iv'] + df1.at[otm_p,'iv'] - df1.at[atm_c,'iv'] - df1.at[atm_p,'iv'] ) / (df1.at[atm_c,'iv'] + df1.at[atm_p,'iv']), 2)
                # print(skew_c)
                # print(skew_p)
                print(skew_mean)
                r.append([date, pz, basic, obj, atm, atm+2*gap, atm-2*gap, skew_c, skew_p, skew_mean])

        except:
            pass

    df = pd.DataFrame(r, columns=['date', 'pz', 'basic', 'obj', 'atm', 'otm_c', 'otm_p', 'skew_c', 'skew_p', 'skew_mean'])
    fn = get_dss() + 'opt/skew.csv'
    if os.path.exists(fn):
        df.to_csv(fn, index=False, header=None, mode='a')
    else:
        df.to_csv(fn, index=False)


if __name__ == '__main__':
    # calc_skew()

    pass

    # fn = get_dss() + 'opt/2020-09_greeks.csv'
    # df = pd.read_csv(fn)
    # date_list = list(df.Localtime)
    # date_list = sorted(list(set([x[:10] for x in date_list])))
    # print(date_list)
    # for date in date_list:
    #     calc_skew(date)
