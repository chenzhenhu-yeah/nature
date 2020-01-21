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

def duo_adjust(a1):
    duo_list = [212,33,55.6]
    n = len(duo_list)
    A = sum(duo_list)
    x = int( (A-n*a1)/(0.5*n*(n-1)) )
    print(x)
    r = []
    for i in range(n):
        ai = a1 + i*x
        if i == n-1:
            ai = A - sum(r)
        r.append(ai)
    print(r)


def kong_adjust(b1):
    kong_list = [212]
    n = len(kong_list)
    B = sum(kong_list)
    x = int( (n*b1-B)/(0.5*n*(n-1)) )
    print(x)
    r = []
    for i in range(n):
        bi = b1 - i*x
        if i == n-1:
            bi = B - sum(r)
        r.append(bi)
    print(r)

if __name__ == '__main__':
    # a1 = 30
    # duo_adjust(a1)
    #pass
    b1 = 200
    kong_adjust(b1)
