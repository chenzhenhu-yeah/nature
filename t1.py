import numpy as np
import pandas as pd

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

fname = get_dss() + 'fut/engine/dali/signal_dali_m1901.csv'

df = pd.read_csv(fname)
p = df.pnl.sum()
print(p)

duo_list = [2765.0, 2781.0, 2801.0, 2810.0, 2832.0, 2848.0, 2882.0, 2908.0, 3077.0, 3079.0, 3080.0, 3080.0, 3082.0, 3085.0, 3098.0]
kong_list = [2735.0, 2739.0, 2752.0, 2753.0, 2815.0]
settle = 2815

pnl = 0
for item in duo_list:
    pnl += settle - item

for item in kong_list:
    pnl += item - settle
print(pnl)
