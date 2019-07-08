#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = 'test py ctp of se'
__author__ = 'HaiFeng'
__mtime__ = '20190506'

import time
import pandas as pd
import schedule

from py_ctp.trade import CtpTrade
from py_ctp.quote import CtpQuote
from py_ctp.enums import *
from py_ctp.structs import Tick

class HuQuote(CtpQuote):

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        #to_log('in BollEngine.__init__')
        CtpQuote.__init__(self)
        self.tradeDay = ''


    #----------------------------------------------------------------------
    def _OnRtnDepthMarketData(self, pDepthMarketData):
        """"""
        print('in _OnRtnDepthMarketData: ', pDepthMarketData.getInstrumentID())
        tick = Tick()

        tick.AskPrice = pDepthMarketData.getAskPrice1()
        tick.AskVolume = pDepthMarketData.getAskVolume1()
        tick.AveragePrice = pDepthMarketData.getAveragePrice()
        tick.BidPrice = pDepthMarketData.getBidPrice1()
        tick.BidVolume = pDepthMarketData.getBidVolume1()
        tick.Instrument = pDepthMarketData.getInstrumentID()
        tick.LastPrice = pDepthMarketData.getLastPrice()
        tick.OpenInterest = pDepthMarketData.getOpenInterest()
        tick.Volume = pDepthMarketData.getVolume()

        self.tradeDay = pDepthMarketData.getTradingDay()

        tick.UpdateTime = pDepthMarketData.getUpdateTime()
        tick.UpdateMillisec = pDepthMarketData.getUpdateMillisec()
        tick.UpperLimitPrice = pDepthMarketData.getUpperLimitPrice()
        tick.LowerLimitPrice = pDepthMarketData.getLowerLimitPrice()
        tick.PreOpenInterest = pDepthMarketData.getPreOpenInterest()

        self.OnTick(self, tick)

    #----------------------------------------------------------------------
    def OnTick(self, obj, f: Tick):
        """"""
        #print(f'=== [QUOTE] OnTick ===\n{f.__dict__}')
        #print(type(f.__dict__))
        now = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        df = pd.DataFrame([f.__dict__])
        df['Localtime'] = now
        cols = ['Localtime','LastPrice','AveragePrice','Volume',
                'OpenInterest','PreOpenInterest','UpdateMillisec','UpdateTime']
        df = df[cols]

        fname = 'tick/' + f.Instrument + '_' + self.tradeDay + '.csv'
        df.to_csv(fname, index=False, mode='a', header=False)


class TestQuote(object):
    """TestQuote"""

    def __init__(self, addr: str, broker: str, investor: str, pwd: str):
        """"""
        self.front = addr
        self.broker = broker
        self.investor = investor
        self.pwd = pwd

        self.q = HuQuote()
        self.q.OnConnected = lambda x: self.q.ReqUserLogin(self.investor, self.pwd, self.broker)
        #self.q.OnUserLogin = lambda o, i: self.q.ReqSubscribeMarketData('rb1910')
        ids = ['IC1909','IF1909','IH1909','c1909','SR1909','CF1909','rb1910']
        self.q.OnUserLogin = lambda o, i: self.subscribe_ids(ids)

    def subscribe_ids(self, ids):
        for id in ids:
            self.q.ReqSubscribeMarketData(id)

    def run(self):
        self.q.ReqConnect(self.front)

    def release(self):
        self.q.ReqUserLogout()

    #----------------------------------------------------------------------
    def daily_worker(self):
        """运行"""
        schedule.every().day.at("08:08").do(self.run)
        schedule.every().day.at("18:00").do(self.release)
        schedule.every().day.at("08:30").do(self.run)
        schedule.every().day.at("01:00").do(self.release)

        print(u'行情接收器开始运行')
        while True:
            schedule.run_pending()
            time.sleep(10)

if __name__ == "__main__":
    front_trade = 'tcp://180.168.146.187:10101'
    front_quote = 'tcp://180.168.146.187:10111'
    broker = '9999'
    investor = ''
    pwd = ''
    appid = 'simnow_client_test'
    auth_code = '0000000000000000'

    time.sleep(3)
    qq = TestQuote(front_quote, broker, investor, pwd)
    qq.daily_worker()

    # qq.run()
    # input()

    # qq.release()
    # input()
