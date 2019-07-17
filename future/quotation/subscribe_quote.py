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
from multiprocessing.connection import Client

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
        self.id_list = ['c1909','c2001',
                        'SR909','SR001',
                        'CF909','CF001',
                        'rb1910','rb2001',
                        'au1912','au2002',
                        'ru1909','ru2001',
                        'IH1909','IH1912',
                        'IC1909','IC1912',
                        'IF1909','IF1912',
                        ]

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
            #threading.Thread( target=self.OnTick, args=(tick,) ).start()
            #多线程容易出错。
            self.OnTick(tick)

    #----------------------------------------------------------------------
    def OnTick(self, f: Tick):
        """"""
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
        try :
            with Client(address, authkey=b'secret password') as conn2:
                conn2.send(s)
                time.sleep(0.030)
        except Exception as e:
            #print('error，发送太密集')
            #print(e)
            pass

    #----------------------------------------------------------------------
    def put_bar(self, df, id):
        fname = self.dss + 'fut/put/min1_' + id + '.csv'
        df.to_csv(fname, index=False)

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
                df = pd.DataFrame([bar.__dict__])
                cols = ['date','time','open','high','low','close','volume']
                df = df[cols]

                # send bar to port
                self.send_bar(str(bar.__dict__))

                #self.put_bar(df, id)

                # save bar
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

        # 保存最后一根K线
        for id in self.bar_dict:
            bar = self.bar_dict[id]
            df = pd.DataFrame([bar.__dict__])
            cols = ['date','time','open','high','low','close','volume']
            df = df[cols]
            fname = self.dss + 'fut/bar/min1_' + self.tradeDay + '_' + id + '.csv'
            df.to_csv(fname, index=False, mode='a', header=False)

        #清空，以免第二日重复保存
        self.bar_dict = {}


    #----------------------------------------------------------------------
    def daily_worker(self):
        """运行"""
        schedule.every().day.at("20:08").do(self.run)
        schedule.every().day.at("15:50").do(self.release)

        print(u'行情接收器开始运行')
        while True:
            schedule.run_pending()
            time.sleep(10)

if __name__ == "__main__":
    try:
        front_trade = 'tcp://180.168.146.187:10101'
        front_quote = 'tcp://180.168.146.187:10111'
        broker = '9999'
        investor = ''
        pwd = ''

        time.sleep(3)
        qq = TestQuote(front_quote, broker, investor, pwd)

        qq.daily_worker()

        # qq.run()
        # input()
        # qq.release()
        # input()
    except Exception as e:
        print('error')
        print(e)
        while True:
            time.sleep(300)
