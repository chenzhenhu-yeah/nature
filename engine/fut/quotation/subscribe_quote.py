#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = 'test py ctp of se'
__author__ = 'HaiFeng'
__mtime__ = '20190506'

import os
import time
import json
import pandas as pd
import schedule
import threading
from multiprocessing.connection import Client

from nature import CtpTrade
from nature import CtpQuote
from nature import Tick
from nature import VtBarData
from nature import SOCKET_BAR, get_dss

class HuQuote(CtpQuote):

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        #to_log('in BollEngine.__init__')
        CtpQuote.__init__(self)

        # 加载配置
        config = open(get_dss()+'fut/cfg/config.json')
        setting = json.load(config)
        symbols = setting['symbols']
        self.id_list = symbols.split(',')

        self.dss = get_dss()
        self.tradeDay = ''
        self.bar_min1_dict = {}
        self.bar_min5_dict = {}
        self.bar_min15_dict = {}

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

        if (tick.UpdateTime>='08:59:59' and tick.UpdateTime <= '15:00:01') or (tick.UpdateTime>='20:59:59' and tick.UpdateTime <= '04:30:01'):
            #threading.Thread( target=self.OnTick, args=(tick,) ).start()
            #多线程容易出错。
            self.OnTick(tick)

    #----------------------------------------------------------------------
    def OnTick(self, f: Tick):
        """"""
        # 处理Bar
        #self._Generate_Bar_Min1(f)
        self._Generate_Bar_MinOne(f)

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
    def put_bar(self, df, id, minx):
        fname = self.dss + 'fut/put/' + minx + '_' + id + '.csv'
        df.to_csv(fname, index=False)

    #----------------------------------------------------------------------
    def _Generate_Bar_Min1(self, tick):
        """生成、推送、保存Bar"""
        id = tick.Instrument
        if id in self.bar_min1_dict:
            bar = self.bar_min1_dict[id]
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
                #self.send_bar(str(bar.__dict__))

                self.put_bar(df, id, 'min1')
                self._Generate_Bar_Min5(bar)

                # save bar
                # 出现了怪的问题，发布了多条重复的tick，但分种线的加工不应该出现此问题，多线程 ? ? ?
                fname = self.dss + 'fut/put/rec/min1_' + self.tradeDay + '_' + id + '.csv'
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

        self.bar_min1_dict[id] = bar

    #----------------------------------------------------------------------
    def _Generate_Bar_MinOne(self, tick):
        """生成、推送、保存Bar"""

        today = time.strftime('%Y-%m-%d',time.localtime())

        new_bar = VtBarData()
        new_bar.date = today
        new_bar.time = tick.UpdateTime
        new_bar.vtSymbol = tick.Instrument
        new_bar.open = tick.LastPrice
        new_bar.high = tick.LastPrice
        new_bar.low =  tick.LastPrice
        new_bar.close = tick.LastPrice

        id = new_bar.vtSymbol
        if id in self.bar_min1_dict:
            bar = self.bar_min1_dict[id]
        else:
            bar = new_bar
            self.bar_min1_dict[id] = bar
            return

        # 更新数据
        if bar.high < new_bar.high:
            bar.high = new_bar.high
        if bar.low > new_bar.low:
            bar.low =  new_bar.low
        bar.close = new_bar.close

        if bar.time[3:5] != new_bar.time[3:5] :
            # 将 bar的分钟改为整点，推送并保存bar
            bar.time = new_bar.time
            #bar.time[6:8] = '00'

            self.put_bar(bar, 'min1')
            self._Generate_Bar_Min5(bar)
            self._Generate_Bar_Min15(bar)

            fname = self.dss + 'fut/put/rec/min1_' + self.tradeDay + '_' + id + '.csv'
            if os.path.exists(fname):
                df.to_csv(fname, index=False, mode='a', header=False)
            else:
                df.to_csv(fname, index=False, mode='a')

        self.bar_min1_dict[id] = bar

    #----------------------------------------------------------------------
    def _Generate_Bar_Min5(self, new_bar):
        """生成、推送、保存Bar"""

        id = new_bar.vtSymbol
        if id in self.bar_min5_dict:
            bar = self.bar_min5_dict[id]
        else:
            bar = new_bar
            self.bar_min5_dict[id] = bar
            return

        # 更新数据
        if bar.high < new_bar.high:
            bar.high = new_bar.high
        if bar.low > new_bar.low:
            bar.low =  new_bar.low
        bar.close = new_bar.close

        if new_bar.time[3:5] in ['05','10','15','20','25','30','35','40','45','50','55','00']:
            # 将 bar的分钟改为整点，推送并保存bar
            bar.time = new_bar.time
            self.put_bar(bar, 'min5')

            fname = self.dss + 'fut/put/rec/min5_' + self.tradeDay + '_' + id + '.csv'
            if os.path.exists(fname):
                df.to_csv(fname, index=False, mode='a', header=False)
            else:
                df.to_csv(fname, index=False, mode='a')

        self.bar_min5_dict[id] = bar

    #----------------------------------------------------------------------
    def _Generate_Bar_Min15(self, new_bar):
        """生成、推送、保存Bar"""

        id = new_bar.vtSymbol
        if id in self.bar_min15_dict:
            bar = self.bar_min15_dict[id]
        else:
            bar = new_bar
            self.bar_min15_dict[id] = bar
            return

        # 更新数据
        if bar.high < new_bar.high:
            bar.high = new_bar.high
        if bar.low > new_bar.low:
            bar.low =  new_bar.low
        bar.close = new_bar.close

        if new_bar.time[3:5] in ['15','30','45','00']:
            # 将 bar的分钟改为整点，推送并保存bar
            bar.time = new_bar.time
            self.put_bar(bar, 'min15')

            fname = self.dss + 'fut/put/rec/min15_' + self.tradeDay + '_' + id + '.csv'
            if os.path.exists(fname):
                df.to_csv(fname, index=False, mode='a', header=False)
            else:
                df.to_csv(fname, index=False, mode='a')

        self.bar_min15_dict[id] = bar


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

    def change_day(self):
        #清空，以免第二日重复保存
        self.q.bar_min1_dict = {}
        self.q.bar_min5_dict = {}
        self.q.bar_min15_dict = {}


    #----------------------------------------------------------------------
    def daily_worker(self):
        """运行"""
        schedule.every().day.at("20:28").do(self.run)
        schedule.every().day.at("05:50").do(self.release)
        schedule.every().day.at("08:28").do(self.run)
        schedule.every().day.at("15:50").do(self.release)
        schedule.every().day.at("15:52").do(self.change_day)

        print(u'行情接收器开始运行')
        while True:
            schedule.run_pending()
            time.sleep(10)

if __name__ == "__main__":
    try:
        # # 海通
        # front_trade = 'tcp://180.168.212.75:41305'
        # front_quote = 'tcp://180.168.212.75:41313'
        # broker = '8000'

        # 加载配置
        config = open(get_dss()+'fut/cfg/config.json')
        setting = json.load(config)
        front_trade = setting['front_trade']
        front_quote = setting['front_quote']
        broker = setting['broker']

        # # CTP
        # front_trade = 'tcp://180.168.146.187:10101'
        # front_quote = 'tcp://180.168.146.187:10111'
        # broker = '9999'

        investor = '71081980'
        pwd = 'zhenhu123'


        time.sleep(3)
        qq = TestQuote(front_quote, broker, investor, pwd)

        # qq.daily_worker()

        qq.run()
        input()
        qq.release()
        input()
    except Exception as e:
        print('error')
        print(e)
        while True:
            time.sleep(300)
