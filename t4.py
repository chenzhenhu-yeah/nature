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


from tqsdk import TqApi

api = TqApi()

klines = api.get_kline_serial("SHFE.cu2002", 60)

#klines = api.get_kline_serial("KQ.m@CFFEX.IF", 14400)
print( len(klines) )

klines.to_csv('k.csv')

while api.wait_update():
    pass
