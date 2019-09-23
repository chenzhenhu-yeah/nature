#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = 'test py ctp of se'
__author__ = 'HaiFeng'
__mtime__ = '20190506'

from py_ctp.trade import CtpTrade
from py_ctp.quote import CtpQuote
from py_ctp.enums import *
import time


class TestTrade(object):
    def __init__(self, addr: str, broker: str, investor: str, pwd: str, appid: str, auth_code: str, proc: str):
        self.front = addr
        self.broker = broker
        self.investor = investor
        self.pwd = pwd
        self.appid = appid
        self.authcode = auth_code
        self.proc = proc

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

    def on_connect(self, obj):
        self.t.ReqUserLogin(self.investor, self.pwd, self.broker, self.proc, self.appid, self.authcode)

    def run(self):
        self.t.ReqConnect(self.front)

    def release(self):
        self.t.ReqUserLogout()


if __name__ == "__main__":
    front_trade = 'tcp://180.168.212.79:31207'
    front_quote = 'tcp://180.168.146.97:31219'
    broker = '8000'
    investor = '41013941'
    pwd = 'zhenhu123'
    appid = 'client_nature_1.0.0'
    auth_code = 'TSTCTPGNYLYYEYWS'
    #proc = 'naturev1.0.0'
    proc = ''

    tt = TestTrade(front_trade, broker, investor, pwd, appid, auth_code, proc)
    tt.run()
    time.sleep(5)

    # simnow 发单测试成功(2019-09-19), 上期-SHFE, 中金-CFFEX, 大商-DCE, 能源所-INE、郑商所-CZCE
    # 修改了 trade.py 中的 ReqOrderInsert，添加 ExchangeID 字段
    # tt.t.ReqOrderInsert('c2001', DirectType.Buy, OffsetType.Open, 1831, 1, 'DCE')

    input()
    tt.release()
    input()
