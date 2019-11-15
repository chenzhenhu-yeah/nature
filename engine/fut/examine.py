import numpy as np
import pandas as pd

import smtplib
from email.mime.text import MIMEText

import os
import re
import datetime
import time
from csv import DictReader
from nature import get_dss, get_trading_dates, get_daily, get_stk_hfq, to_log, get_contract

import json
import tushare as ts


def examine():
    dss = get_dss()

    # setting_pz中的品种已在trade_time中维护
    #读取交易时段文件
    fn = dss + 'fut/cfg/trade_time.csv'
    df = pd.read_csv(fn)
    tm_pz_set = set(df.symbol)
    #print(tm_pz_set)

    fn = dss + 'fut/cfg/setting_pz.csv'
    df = pd.read_csv(fn)
    setting_pz_list = list(df.pz)
    #print(setting_pz_list)

    for pz in setting_pz_list:
        if pz not in tm_pz_set:
            to_log('examine: ' + pz + ' not in trade_time.csv')

    # 加载配置，目前盯市哪些业务品种
    config = open(dss + 'fut/cfg/config.json')
    setting = json.load(config)
    symbols = setting['symbols_quote']
    symbol_list = symbols.split(',')
    #print(symbol_list)
    for symbol in symbol_list:
        c = get_contract(symbol)
        #print( c.pz )
        if c is None:
            to_log('examine: ' + symbol + ' not in setting_pz.csv')






    # symbols_quote中的品种已在setting_pz中维护


    # symbols_quote涵盖symbols_trade
    # symbols_trade涵盖其他symbols

    #



if __name__ == '__main__':
    examine()

#
# # 加载配置
# config = open(get_dss()+'csv/config.json')
# setting = json.load(config)
# pro_id = setting['pro_id']              # 设置服务器
# pro = ts.pro_api(pro_id)
