import numpy as np
import pandas as pd
import talib

import smtplib
from email.mime.text import MIMEText

import os
import re
import datetime
import time

import json
import tushare as ts


from nature import get_dss, get_trading_dates, get_daily, get_stk_hfq
from nature import VtBarData, ArrayManager

dss = get_dss()

fn_list = [ dss+'fut/cfg/signal_pause_var.csv',
            dss+'fut/engine/ic/portfolio_ic_var.csv',
            dss+'fut/engine/aberration_enhance/portfolio_aberration_enhance_var.csv' ,
            dss+'fut/engine/owl/portfolio_owl_var.csv',
            dss+'fut/engine/owl/signal_owl_mix_var_CF009.csv',
            dss+'fut/engine/owl/signal_owl_mix_var_m2005.csv',
            dss+'fut/engine/owl/signal_owl_mix_var_m2009.csv',
            dss+'fut/engine/turtle/portfolio_turtle_var.csv',
            dss+'fut/engine/dali/portfolio_dali_var.csv',
            dss+'fut/engine/dali/signal_dali_multi_var_RM.csv',
            dss+'fut/engine/dali/signal_dali_multi_var_MA.csv',
            dss+'fut/engine/dali/signal_dali_multi_var_m.csv',
            dss+'fut/engine/dali/signal_dali_multi_var_c.csv',
            dss+'fut/engine/dali/signal_dali_multi_var_y.csv',
            dss+'fut/engine/cci_raw/portfolio_cci_raw_var.csv',
            dss+'fut/engine/cci_raw/signal_cci_raw_duo_var_ru.csv',
            dss+'fut/engine/cci_raw/signal_cci_raw_kong_var_ru.csv',
            dss+'fut/engine/ma/portfolio_ma_var.csv',
            dss+'fut/engine/ma/signal_ma_kong_var_CF.csv',
            dss+'fut/engine/ma/signal_ma_kong_var_ru.csv',
            dss+'fut/engine/ma/signal_ma_duo_var_ru.csv',
            dss+'fut/engine/ma/signal_ma_duo_var_CF.csv',
            dss+'fut/engine/dalicta/portfolio_dalicta_var.csv',
            dss+'fut/engine/dalicta/signal_dalicta_duo_var_m.csv',
            dss+'fut/engine/dalicta/signal_dalicta_kong_var_m.csv',
            dss+'fut/engine/cciboll/portfolio_cciboll_var.csv',
            dss+'fut/engine/cciboll/signal_cciboll_duo_var_rb.csv',
            dss+'fut/engine/cciboll/signal_cciboll_kong_var_rb.csv',
            dss+'fut/engine/cciboll/signal_cciboll_kong_var.csv',
            dss+'fut/engine/cciboll/signal_cciboll_kong_var.csv',
            dss+'fut/engine/rsiboll/portfolio_rsiboll_var.csv',
            dss+'fut/engine/rsiboll/signal_rsiboll_duo_var_CF.csv',
            dss+'fut/engine/rsiboll/signal_rsiboll_kong_var_CF.csv',
            dss+'fut/engine/rsiboll/signal_rsiboll_duo_var.csv',
            dss+'fut/engine/rsiboll/signal_rsiboll_kong_var.csv',
            dss+'fut/engine/atrrsi/portfolio_atrrsi_var.csv',
            dss+'fut/engine/atrrsi/signal_atrrsi_var.csv',
          ]

for fn in fn_list:
    if os.path.exists(fn):
        df = pd.read_csv(fn, sep='$',  dtype='str')
        df.to_csv(fn, index=False)
        print('转换成功： ', fn)
    else:
        print('文件不存在： ', fn)
