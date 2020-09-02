import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import os
from datetime import datetime

import zipfile
import os

from nature import get_dss, get_inx, get_contract

def calc_skew():
    now = datetime.now()
    date = now.strftime('%Y-%m-%d')
    date = '2020-08-06'


    fn = get_dss() + 'opt/' + date[:7] + '_greeks.csv'
    if os.path.exists(fn) == False:
        return

    basic_list = []
    df = pd.read_csv(fn)
    df = df[df.Localtime.str.slice(0,10) == date]
    for i, row in df.iterrows():
        basic_list.append( get_contract(row.Instrument).basic )
    basic_list = list(set(basic_list))

    for basic in basic_list:
        try:
            df1 = df[df.Instrument.str.slice(0,len(basic)) == basic]
            df1 = df1.drop_duplicates(subset=['Instrument'],keep='last')
            df1 = df1.sort_values('Instrument')
            print(df1.tail())

            if len(df1) > 0:
                rec = df1.iloc[0, :]
                obj = rec.obj
                pz = str(get_contract(rec.Instrument).pz)
                gap = 50
                if pz == 'CF':
                    gap = 200
                if pz == 'm':
                    gap = 50
                if pz in ['RM', 'MA']:
                    gap = 25

                atm = int(round(round(obj*(100/gap)/1E4,2) * 1E4/(100/gap), 0))     # 获得平值
                print(gap)
                print(obj)
                print(atm)

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
                skew_c = (df1.at[otm_c,'iv']-df1.at[atm_c,'iv']) / df1.at[atm_c,'iv']
                skew_p = (df1.at[otm_p,'iv']-df1.at[atm_p,'iv']) / df1.at[atm_p,'iv']
                print(skew_c)
                print(skew_p)
        except:
            pass

        break


if __name__ == '__main__':
    calc_skew()

    pass
