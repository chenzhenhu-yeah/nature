#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = 'test py ctp of se'
__author__ = 'HaiFeng'
__mtime__ = '20190506'

import os
import time
import pandas as pd
import schedule
import threading

from nature import CtpTrade
from nature import CtpQuote
from nature import Tick
from nature import VtBarData
from nature import SOCKET_BAR

class HuQuote(CtpQuote):

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        #to_log('in BollEngine.__init__')
        CtpQuote.__init__(self)
        self.id_list = ['IC1909','IF1909','IH1909','c1909','SR909','CF909','rb1910']
        self.dss = '../../../data/'
        self.tradeDay = ''
        self.bar_dict = {}

    #----------------------------------------------------------------------
    def _OnRtnDepthMarketData(self, pDepthMarketData):
        """"""
        #print('in _OnRtnDepthMarketData: ', pDepthMarketData.getInstrumentID())
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

        if (tick.UpdateTime>='08:59:59' and tick.UpdateTime <= '15:00:01') or (tick.UpdateTime>='20:59:59' and tick.UpdateTime <= '23:30:01'):
            threading.Thread( target=self.OnTick, args=(self, tick) ).start()

    #----------------------------------------------------------------------
    def OnTick(self, obj, f: Tick):
        """"""
        #print(f'=== [QUOTE] OnTick ===\n{f.__dict__}')
        #print(type(f.__dict__))

        # 处理Bar
        self._Generate_Bar(f)

        # 保存Tick到文件
        now = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        df = pd.DataFrame([f.__dict__])
        df['Localtime'] = now
        cols = ['Localtime','LastPrice','AveragePrice','Volume',
                'OpenInterest','PreOpenInterest','UpdateMillisec','UpdateTime']
        df = df[cols]

        fname = self.dss + 'fut/tick/tick_' + self.tradeDay + '_' + f.Instrument + '.csv'
        if os.path.exists(fname):
            df.to_csv(fname, index=False, mode='a', header=False)
        else:
            df.to_csv(fname, index=False, mode='a')



    #----------------------------------------------------------------------
    def send_bar(self, s):
        address = ('localhost', SOCKET_BAR)
        again = True
        while again:
            try :
                with Client(address, authkey=b'secret password') as conn2:
                    conn2.send(s)
                again = False
            except Exception as e:
                print('error')
                print(e)
    #----------------------------------------------------------------------
    def _Generate_Bar(self, tick):
        """生成、推送、保存Bar"""
        id = tick.Instrument
        if id in self.bar_dict:
            bar = self.bar_dict[id]
        else:
            bar = VtBarData()

        today = time.strftime('%Y-%m-%d',time.localtime())
        min = ''.join(tick.UpdateTime)
        #print('before ',min)
        min = min[:-2] + '00'
        #print('after  ',min)

        if bar.time != min:
            if len(bar.time) > 0:
                # send bar to port
                self.send_bar(str(bar.__dict__))

                # save bar
                df = pd.DataFrame([bar.__dict__])
                cols = ['date','time','open','high','low','close','volume']
                df = df[cols]

                # 出现了怪的问题，发布了多条重复的tick，但分种线的加工不应该出现此问题，多线程 ? ? ?
                fname = self.dss + 'fut/bar/min1_' + self.tradeDay + '_' + id + '.csv'
                if os.path.exists(fname):
                    df.to_csv(fname, index=False, mode='a', header=False)
                else:
                    df.to_csv(fname, index=False, mode='a')
            bar.date = today
            bar.time = min
            bar.vtSymbol = tick.Instrument
            bar.open = tick.LastPrice
            bar.high = tick.LastPrice
            bar.low =  tick.LastPrice
            bar.close = tick.LastPrice
            bar.volume = tick.Volume
        else:
            if bar.high < tick.LastPrice:
                bar.high = tick.LastPrice
            if bar.low > tick.LastPrice:
                bar.low =  tick.LastPrice
            bar.close = tick.LastPrice
            bar.volume = tick.Volume

        self.bar_dict[id] = bar


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
        self.q.OnUserLogin = lambda o, i: self.subscribe_ids(self.q.id_list)

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
        schedule.every().day.at("23:50").do(self.release)

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

    time.sleep(3)
    qq = TestQuote(front_quote, broker, investor, pwd)

    #qq.daily_worker()

    qq.run()
    input()
    qq.release()
    input()
