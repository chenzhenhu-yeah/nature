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

#
# from tqsdk import TqApi
#
# api = TqApi()
#
# klines = api.get_kline_serial("SHFE.cu2002", 60)
#
# #klines = api.get_kline_serial("KQ.m@CFFEX.IF", 14400)
# print( len(klines) )
#
# klines.to_csv('k.csv')
#
# while api.wait_update():
#     pass



# duo = [2245.0, 2285.0]
# kong = [2240.0, 2205.0, 2170.0, 2145.0, 2110.0, 2060.0]

duo = [2245.0]
kong = [2240.0, 2190.0, 2015.0]

[2245.0]$[2240.0, 2190.0, 2015.0]
print(sum(duo))
print(sum(kong))
