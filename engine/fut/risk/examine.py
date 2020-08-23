import numpy as np
import pandas as pd

import smtplib
from email.mime.text import MIMEText

import os
import re
from datetime import datetime, timedelta
import time
from csv import DictReader
from nature import get_dss, get_trading_dates, get_daily, get_stk_hfq, get_contract, to_log
from nature import get_symbols_quote, get_symbols_trade

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
            to_log('examine: ' + pz + ' of setting_pz.csv not in trade_time.csv')

    # 加载配置，目前盯市的品种已在setting_pz中维护
    symbols_quote_list = get_symbols_quote()
    for symbol in symbols_quote_list:
        c = get_contract(symbol)
        #print( c.pz )
        if c is None:
            to_log('examine: ' + symbol + ' of config.json not in setting_pz.csv')

    # symbols_quote涵盖symbols_trade
    symbols_trade_list = get_symbols_trade()
    for symbol in symbols_trade_list:
        if symbol not in symbols_quote_list:
            to_log('examine: ' + symbol + ' of symbols_trade not in symbols_quote')

    # 行情接收器是否存在超时情况
    config = open(dss + 'fut/cfg/config.json')
    setting = json.load(config)
    symbol = setting['symbols_quote_canary']
    # symbols_quote_list = symbols.split(',')
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    # today = '20200515'
    fn = get_dss() + 'fut/tick/tick_' + today  + '_' + symbol + '.csv'
    if os.path.exists(fn):
        df = pd.read_csv(fn)
        df = df[(df.UpdateTime >= '14:50:59') & (df.UpdateTime <= '14:59:59')]
        df = df.sample(3)
        for i, row in df.iterrows():
            u = datetime.strptime(row.UpdateDate + ' ' + row.UpdateTime, '%Y-%m-%d %H:%M:%S')
            u += timedelta(seconds=3)
            u = datetime.strftime(u, '%Y-%m-%d %H:%M:%S')
            # print(row.Localtime, u)
            if row.Localtime > u:
                to_log('examine: over time! ' + row.Localtime + ' > ' + u)

if __name__ == '__main__':
    examine()
