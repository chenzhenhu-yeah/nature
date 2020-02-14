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



duo  = [2736.0, 2711.0, 2687.0]
kong = [2675.0, 2651.0, 2627.0, 2503.0, 2479.0, 2455.0, 2516.0]

# duo  = [2736.0, 2811.0, 2887.0]
# kong = [2675.0, 2651.0, 2627.0, 2603.0, 2579.0, 2555.0, 2516.0]

# [2736.0, 2811.0, 2887.0]$[2675.0, 2651.0, 2627.0, 2603.0, 2579.0, 2555.0, 2516.0]
# [2736.0, 2711.0, 2687.0]$[2675.0, 2651.0, 2627.0, 2503.0, 2479.0, 2455.0, 2516.0]

print(sum(duo))
print(sum(kong))
