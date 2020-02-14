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
            dss+'fut/cfg/signal_pause_var.csv',
          ]

fn2 = 'var_NEW.csv'

for fn in fn_list:
    df = pd.read_csv(fn, sep='$',  dtype='str')
    print(fn)

    df.to_csv(fn2, index=False)
