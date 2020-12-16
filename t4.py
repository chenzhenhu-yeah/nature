
import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import smtplib
from email.mime.text import MIMEText

import os
import re
import datetime
import time
import sys
import json
import tushare as ts
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import ctypes
import os
import platform
import sys
import zipfile

from nature import to_log, is_trade_day, send_email, get_dss, get_contract, is_market_date
from nature import rc_file, get_symbols_quote, get_tick, send_order


fn_p = 'industry.csv'
df_p = pd.read_csv(fn_p)

r = []
product_list = ['柴油(万吨)', '燃料油(万吨)',]


for product in product_list:
    fn = 'native/single_product/' + product + '.csv'
    if os.path.exists(fn) == False:
        continue
    df = pd.read_csv(fn, encoding='gbk', skiprows=2, dtype='str')
    df = df.iloc[:4,:]
    df = df.set_index('指标')
    df = df.T

    cols = ['value_cur', 'value_cum', 'ratio_cur', 'ratio_cum']
    df.columns = cols
    for col in cols:
        df[col] = df[col].str.strip()

    for dt in df.index:
        df1 = df_p[(df_p['dt'] == dt) & (df_p['product'] == product)]
        if df1.empty:
            rec = df.loc[dt,:]
            r.append([dt, dt[:4], dt[5:-1].zfill(2)+'M', product, rec.value_cur, rec.value_cum, rec.ratio_cur, rec.ratio_cum])

print(r[0])
print(r[-1])
# df = pd.DataFrame(r, columns=['dt', 'year', 'month', 'product', 'value_cur', 'value_cum', 'ratio_cur', 'ratio_cum'])
# df.to_csv(fn_p, mode='a', header=None, index=False)
