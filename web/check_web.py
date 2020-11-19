
import pandas as pd
from flask import Flask, render_template, request, redirect
from flask import url_for
from datetime import datetime
from multiprocessing.connection import Client
import time
import tushare as ts
import json
import os

from nature import get_dss, get_symbols_trade

def del_blank(c):
    s = str(c).strip()
    s = s.replace('\t','')
    s = s.replace(' ','')
    return s

def check_symbols_p(key, value):
    r = ''

    if key == 'gateway_pf':
        v  = eval(value)
        # if type(v) != type({}):
        if isinstance(v, dict) == False:
            r = '非字典'

    if key == 'symbols_aberration_enhance':
        if len(value) > 0:
            symbol_list = value.split(',')
        else:
            symbol_list = []

        for symbol in symbol_list:
            fn = get_dss() + 'fut/put/rec/day_' + symbol + '.csv'
            if os.path.exists(fn):
                df = pd.read_csv(fn)
                if len(df) <= 30:
                    r = 'day_' + symbol + '.csv 记录数不足30'
            else:
                r = 'day_' + symbol + '.csv 记录数不足30'

    if key in ['symbols_dalicta', 'symbols_dualband']:
        if len(value) > 0:
            symbol_list = value.split(',')
        else:
            symbol_list = []

        for symbol in symbol_list:
            fn = get_dss() + 'fut/put/rec/day_' + symbol + '.csv'
            if os.path.exists(fn):
                df = pd.read_csv(fn)
                if len(df) <= 60:
                    r = 'day_' + symbol + '.csv 记录数不足60'
            else:
                r = 'day_' + symbol + '.csv 记录数不足60'

    if key == 'symbols_cci_raw':
        if len(value) > 0:
            symbol_list = value.split(',')
        else:
            symbol_list = []

        for symbol in symbol_list:
            fn = get_dss() + 'fut/put/rec/day_' + symbol + '.csv'
            if os.path.exists(fn):
                df = pd.read_csv(fn)
                if len(df) <= 100:
                    r = 'day_' + symbol + '.csv 记录数不足100'
            else:
                r = 'day_' + symbol + '.csv 记录数不足100'

    if key in ['symbols_dali']:
        if len(value) > 0:
            symbol_list = value.split(',')
        else:
            symbol_list = []

        for symbol in symbol_list:
            fn = get_dss() + 'fut/put/rec/min5_' + symbol + '.csv'
            if os.path.exists(fn):
                df = pd.read_csv(fn)
                if len(df) <= 60:
                    r = 'min5_' + symbol + '.csv 记录数不足60'
            else:
                r = 'min5_' + symbol + '.csv 记录数不足60'

    if key in ['symbols_rsiboll', 'symbols_cciboll']:
        if len(value) > 0:
            symbol_list = value.split(',')
        else:
            symbol_list = []

        for symbol in symbol_list:
            fn = get_dss() + 'fut/put/rec/min15_' + symbol + '.csv'
            if os.path.exists(fn):
                df = pd.read_csv(fn)
                if len(df) <= 60:
                    r = 'min15_' + symbol + '.csv 记录数不足100'
            else:
                r = 'min15_' + symbol + '.csv 记录数不足100'

    if key in ['symbols_quote','symbols_quote_01','symbols_quote_05','symbols_quote_06','symbols_quote_09','symbols_quote_10','symbols_quote_12']:
        if len(value) > 0:
            symbol_list = value.split(',')
            for symbol in symbol_list:
                pz = symbol[:2]
                if pz.isalpha():
                    pass
                else:
                    pz = symbol[:1]

                fn = get_dss() + 'fut/cfg/setting_pz.csv'
                df = pd.read_csv(fn)
                pz_set = set(df.pz)
                if pz in pz_set:
                    pass
                else:
                    r = pz + '未在setting_pz中维护'

                fn = get_dss() + 'fut/cfg/trade_time.csv'
                df = pd.read_csv(fn)
                pz_set = set(df.symbol)
                if pz in pz_set:
                    pass
                else:
                    r = pz + '未在trade_time中维护'

    # 判读策略将交易的品种是否在symbols_trade中维护
    if key not in ['symbols_arbitrage','symbols_ratio','symbols_straddle','symbols_sdiffer','symbols_skew_strd','symbols_skew_bili','symbols_quote','symbols_quote_01','symbols_quote_05','symbols_quote_06','symbols_quote_09','symbols_quote_10','symbols_quote_12','symbols_trade','gateway_pz','gateway_pf']:
        if len(value) > 0:
            symbols_trade_list = get_symbols_trade()
            symbol_list = value.split(',')
            for symbol in symbol_list:
                if symbol not in  symbols_trade_list:
                    r = symbol + ' 未在symbols_trade中维护'

    symbols_all = ['symbols_quote','symbols_quote_01','symbols_quote_05','symbols_quote_06','symbols_quote_09','symbols_quote_10','symbols_quote_12',
                   'symbols_trade','gateway_pz','gateway_pf','symbols_owl','symbols_cci_raw','symbols_aberration_enhance',
                   'symbols_cciboll','symbols_dali','symbols_rsiboll','symbols_atrrsi','symbols_turtle','symbols_dalicta',
                   'symbols_dualband','symbols_ic','symbols_ma','symbols_yue','symbols_avenger','symbols_follow','symbols_quote_canary',
                   'symbols_ratio','symbols_straddle','symbols_sdiffer','symbols_skew_strd','symbols_skew_bili',
                   'symbols_arbitrage',
                  ]
    if key not in symbols_all:
        r = '新symbols，未在web端进行风控'

    return r



if __name__ == '__main__':
    pass
