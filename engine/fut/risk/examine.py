import numpy as np
import pandas as pd

import smtplib
from email.mime.text import MIMEText

import os
import re
import datetime
import time
from csv import DictReader
from nature import get_dss, get_trading_dates, get_daily, get_stk_hfq, get_contract, to_log
from nature import get_symbols_quote

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
            to_log('examine: ' + pz + 'of setting_pz.csv not in trade_time.csv')

#--------------------------------------------------------------------------------
    config = open(dss + 'fut/cfg/config.json')
    setting = json.load(config)
    # 加载配置，目前盯市的品种已在setting_pz中维护
    # symbols = setting['symbols_quote']
    # symbols_quote_list = symbols.split(',')
    symbols_quote_list = get_symbols_quote()
    print(symbols_quote_list)
    for symbol in symbols_quote_list:
        c = get_contract(symbol)
        #print( c.pz )
        if c is None:
            to_log('examine: ' + symbol + 'of config.json not in setting_pz.csv')

    # symbols_quote涵盖symbols_trade
    symbols = setting['symbols_trade']
    symbols_trade_list = symbols.split(',')
    for symbol in symbols_trade_list:
        if symbol not in symbols_quote_list:
            to_log('examine: ' + symbol + 'of symbols_trade not in symbols_quote')
#--------------------------------------------------------------------------------

    # symbols_trade涵盖其他symbols，如symbols_rsiboll,symbols_atrrsi,symbols_cciboll
    symbols = setting['symbols_rsiboll']
    symbols_rsiboll_list = symbols.split(',')
    for symbol in symbols_rsiboll_list:
        if symbol not in symbols_trade_list:
            to_log('examine: ' + symbol + 'of symbols_rsiboll not in symbols_trade')

    symbols = setting['symbols_atrrsi']
    symbols_atrrsi_list = symbols.split(',')
    for symbol in symbols_atrrsi_list:
        if symbol not in symbols_trade_list:
            to_log('examine: ' + symbol + 'of symbols_atrrsi not in symbols_trade')

    # symbols = setting['symbols_cciboll']
    # symbols_cciboll_list = symbols.split(',')
    # for symbol in symbols_cciboll_list:
    #     if symbol not in symbols_trade_list:
    #         to_log('examine: ' + symbol + 'of symbols_cciboll not in symbols_trade')


    #



if __name__ == '__main__':
    examine()

#
# # 加载配置
# config = open(get_dss()+'csv/config.json')
# setting = json.load(config)
# pro_id = setting['pro_id']              # 设置服务器
# pro = ts.pro_api(pro_id)
