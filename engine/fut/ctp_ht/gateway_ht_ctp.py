

import pandas as pd
import smtplib
from email.mime.text import MIMEText
import schedule
import time
from datetime import datetime
import threading
import traceback
import json
from csv import DictReader
from datetime import datetime
from collections import OrderedDict, defaultdict

from nature import CtpTrade
from nature import CtpQuote
from nature import DirectType, OffsetType
from nature.strategy import DIRECTION_LONG, DIRECTION_SHORT, OFFSET_OPEN, OFFSET_CLOSE
from nature import get_dss, send_email, to_log, get_contract

def get_exchangeID(symbol):
    c = get_contract(symbol)
    return c.exchangeID

class Gateway_Ht_CTP(object):
    def __init__(self):
            # 加载配置
        config = open(get_dss()+'fut/cfg/config.json')
        setting = json.load(config)

        self.front = setting['front_trade']
        self.broker = setting['broker']
        self.investor = setting['investor']
        self.pwd = setting['pwd']
        self.appid = setting['appid']
        self.authcode = setting['auth_code']
        self.proc = ''

        self.t = CtpTrade()
        self.t.OnConnected = self.on_connect
        self.t.OnUserLogin = lambda o, x: print('Trade logon:', x)
        self.t.OnDisConnected = lambda o, x: print(x)
        self.t.OnRtnNotice = lambda obj, time, msg: print(f'OnNotice: {time}:{msg}')
        self.t.OnErrRtnQuote = lambda obj, quote, info: None
        self.t.OnErrRtnQuoteInsert = lambda obj, o: None
        self.t.OnOrder = lambda obj, o: None
        self.t.OnErrOrder = lambda obj, f, info: None
        self.t.OnTrade = lambda obj, o: None
        self.t.OnInstrumentStatus = lambda obj, inst, stat: None

        pz= setting['gateway_pz']
        self.pz_list = pz.split(',')
        pf = setting['gateway_pf']
        self.pf_list = pf.split(',')

    def on_connect(self, obj):
        self.t.ReqUserLogin(self.investor, self.pwd, self.broker, self.proc, self.appid, self.authcode)

    def run(self):
        self.t.ReqConnect(self.front)

    def release(self):
        self.t.ReqUserLogout()

    #----------------------------------------------------------------------
    #停止单需要盯min1,肯定要单启一个线程，线程中循环遍历队列（内部变量），无需同步，用List的pop(0)和append来实现。
    #----------------------------------------------------------------------
    def _bc_sendOrder(self, code, direction, offset, price, volume, portfolio):
        try:
            # 流控
            time.sleep(1)
            pz = get_contract(code).pz
            if pz in self.pz_list and portfolio in self.pf_list:
                print(pz, portfolio, ' send order here!')
            else:
                print(pz, ' just test order here!')
                return

            if self.t.logined == False:
                print('ctp trade not login')
                return ''

            exchangeID = get_exchangeID(code)
            if exchangeID == '':
                return 'error'

            # 当前还处于测试阶段，只开1仓，降低价格不成交
            volume = 1
            # 对价格四舍五入
            priceTick = get_contract(code).price_tick
            price = int(round(price/priceTick, 0)) * priceTick

            if direction == DIRECTION_LONG and offset == '开仓':
                self.t.ReqOrderInsert(code, DirectType.Buy, OffsetType.Open, price, volume, exchangeID)
            if direction == DIRECTION_SHORT and offset == '开仓':
                self.t.ReqOrderInsert(code, DirectType.Sell, OffsetType.Open, price, volume, exchangeID)
            if direction == DIRECTION_LONG and offset == '平仓':
                self.t.ReqOrderInsert(code, DirectType.Buy, OffsetType.Close, price, volume, exchangeID)
            if direction == DIRECTION_SHORT and offset == '平仓':
                self.t.ReqOrderInsert(code, DirectType.Sell, OffsetType.Close, price, volume, exchangeID)

        except Exception as e:
            s = traceback.format_exc()
            to_log(s)

if __name__ == "__main__":
    pass
