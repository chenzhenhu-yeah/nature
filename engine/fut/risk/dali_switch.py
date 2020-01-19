import numpy as np
import pandas as pd

import smtplib
from email.mime.text import MIMEText

import os
import re
from datetime import datetime
import time
from csv import DictReader
from nature import get_dss, get_trading_dates, get_daily, get_stk_hfq, to_log, get_contract

import json
import tushare as ts

def dali_switch_run(symbol, old_price, new_price):
    pz = str(get_contract(new_symbol).pz)
    fn = get_dss() +  'fut/engine/dali/signal_dali_multi_var_' + pz + '.csv'
    if os.path.exists(fn):
        df = pd.read_csv(fn, sep='$')
        if len(df) > 0:
            rec = df.iloc[-1,:]            # 取最近日期的记录
            old_duo_list = eval( rec.price_duo_list )
            old_kong_list = eval( rec.price_kong_list )
            print(rec)
            print(old_duo_list)
            print(old_kong_list)

            gap = new_price - old_price
            new_duo_list = [x+gap for x in old_duo_list]
            new_kong_list = [x+gap for x in old_kong_list]
            print(gap)
            print(new_duo_list)
            print(new_kong_list)

            dt = rec.datetime
            rec.datetime = dt[:17] + '59'
            rec.vtSymbol = new_symbol
            rec.price_duo_list = new_duo_list
            rec.price_kong_list = new_kong_list
            print(rec)

            df2 = pd.DataFrame( [rec.tolist()] )
            df2.to_csv(fn, index=False, sep='$', mode='a', header=False)


if __name__ == '__main__':
    new_symbol = 'm2101'
    old_price = 2600
    new_price = 2900
    dali_dali_switch_run(new_symbol, old_price, new_price)
