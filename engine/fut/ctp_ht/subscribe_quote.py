#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = 'test py ctp of se'
__author__ = 'HaiFeng'
__mtime__ = '20190506'

import os
import time
import datetime
import json
import pandas as pd
import schedule
import threading
from multiprocessing.connection import Client
import traceback
from csv import DictReader



from nature import CtpTrade
from nature import CtpQuote
from nature import Tick

from nature import VtBarData
from nature import SOCKET_BAR, get_dss, to_log, get_contract


class HuQuote(CtpQuote):
    #----------------------------------------------------------------------

    def __init__(self):
        """Constructor"""
        CtpQuote.__init__(self)

        # 加载配置
        config = open(get_dss()+'fut/cfg/config.json')
        setting = json.load(config)
        symbols = setting['symbols_quote']
        self.id_list = symbols.split(',')

        self.dss = get_dss()
        self.tradeDay = ''
        self.night_day = ''
        self.temp_tradeDay = ''
        self.bar_min1_dict = {}

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

        # 只做为临时变量，因郑交所与其他不一样!!!
        self.temp_tradeDay = pDepthMarketData.getTradingDay()

        tick.UpdateTime = pDepthMarketData.getUpdateTime()
        tick.UpdateMillisec = pDepthMarketData.getUpdateMillisec()
        tick.UpperLimitPrice = pDepthMarketData.getUpperLimitPrice()
        tick.LowerLimitPrice = pDepthMarketData.getLowerLimitPrice()
        tick.PreOpenInterest = pDepthMarketData.getPreOpenInterest()

        if (tick.UpdateTime>='08:59:59' and tick.UpdateTime <= '15:00:01') or \
        (tick.UpdateTime>='20:59:59' and tick.UpdateTime <= '23:59:59') or \
        (tick.UpdateTime>='00:00:00' and tick.UpdateTime <= '02:30:01') :
            #threading.Thread( target=self.OnTick, args=(tick,) ).start()
            #多线程容易出错。
            self.OnTick(tick)

    #----------------------------------------------------------------------
    def OnTick(self, f: Tick):
        """"""
        # 保存Tick到文件
        now = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        df = pd.DataFrame([f.__dict__])

        # 以白银作为首笔，确定当前的self.tradeDay
        if self.tradeDay == '':
            if f.Instrument[:2] == 'ag':
                self.tradeDay = self.temp_tradeDay
                # 赋值后，在此交易时段内保持不变。
            else:
                # 等待首笔Tick品种为白银
                return

        UpdateDate = self.tradeDay[:4] + '-' + self.tradeDay[4:6] + '-' + self.tradeDay[6:8]
        if f.UpdateTime >= '20:59:59' and self.night_day == '':
            # 夜盘时段，零点前仍为当日日期。 早期即确定初始值，值是确定的。
            self.night_day = time.strftime('%Y-%m-%d',time.localtime())

        if f.UpdateTime >= '20:59:59':
            UpdateDate = self.night_day

        df['Localtime'] = now
        df['UpdateDate'] = UpdateDate
        cols = ['Localtime','LastPrice','AveragePrice','Volume',
                'OpenInterest','PreOpenInterest','UpdateMillisec','UpdateDate','UpdateTime']
        df = df[cols]

        fname = self.dss + 'fut/tick/tick_' + self.tradeDay + '_' + f.Instrument + '.csv'
        if os.path.exists(fname):
            df.to_csv(fname, index=False, mode='a', header=False)
        else:
            df.to_csv(fname, index=False, mode='a')

        # 处理Bar
        #self._Generate_Bar_Min1(f)
        self._Generate_Bar_MinOne(f, UpdateDate)

    #----------------------------------------------------------------------
    def put_bar(self, bar, minx):
        df = pd.DataFrame([bar.__dict__])
        cols = ['date','time','open','high','low','close','volume']
        df = df[cols]

        fname = self.dss + 'fut/put/' + minx + '_' + bar.vtSymbol + '.csv'
        df.to_csv(fname, index=False)

    #----------------------------------------------------------------------
    def _Generate_Bar_MinOne(self, tick, UpdateDate):
        """生成、推送、保存Bar"""
        new_bar = VtBarData()
        new_bar.date = UpdateDate
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

        if bar.time[3:5] != new_bar.time[3:5] :
            # 将 bar的分钟改为整点，推送并保存bar
            bar.date = new_bar.date
            bar.time = new_bar.time[:-2] + '00'
            self.put_bar(bar, 'min1')

            bar.open = new_bar.open
            bar.high = new_bar.high
            bar.low = new_bar.low
            bar.close = new_bar.close
        else:
            # 更新数据
            if bar.high < new_bar.high:
                bar.high = new_bar.high
            if bar.low > new_bar.low:
                bar.low =  new_bar.low
            bar.close = new_bar.close

        self.bar_min1_dict[id] = bar

        # 收盘前，确保委托单能够成功发出！
        if tick.UpdateTime in ['14:58:57','14:58:58','14:58:59']:
            bar.time = '15:00:00'
            self.put_bar(bar, 'min1')

        c = get_contract(bar.vtSymbol)
        if c.exchangeID == 'CZCE':
            if c.pz in ['CF', 'SR'] and tick.UpdateTime in ['22:58:57','22:58:58','22:58:59']:
                bar.time = '23:00:00'
                self.put_bar(bar, 'min1')

        if c.exchangeID == 'DCE':
            if c.pz in ['m'] and tick.UpdateTime in ['22:58:57','22:58:58','22:58:59']:
                bar.time = '23:00:00'
                self.put_bar(bar, 'min1')

        if c.exchangeID == 'SHFE':
            if c.pz in ['rb'] and tick.UpdateTime in ['22:58:57','22:58:58','22:58:59']:
                bar.time = '23:00:00'
                self.put_bar(bar, 'min1')


class TestQuote(object):
    """TestQuote"""

    def __init__(self, addr: str, broker: str, investor: str, pwd: str):
        """"""
        self.front = addr
        self.broker = broker
        self.investor = investor
        self.pwd = pwd
        self.q = None

    def run(self):
        time.sleep(3)
        del self.q
        time.sleep(3)

        self.q = HuQuote()
        self.q.OnConnected = lambda x: self.q.ReqUserLogin(self.investor, self.pwd, self.broker)
        self.q.OnUserLogin = lambda o, i: self.subscribe_ids(self.q.id_list)

        self.q.ReqConnect(self.front)

    def subscribe_ids(self, ids):
        for id in ids:
            self.q.ReqSubscribeMarketData(id)

    def release(self):
        # 对quote的 ReqUserLogout方法做了修改
        self.q.ReqUserLogout()


    #----------------------------------------------------------------------
    def daily_worker(self):
        """运行"""
        schedule.every().day.at("08:48").do(self.run)
        schedule.every().day.at("15:02").do(self.release)
        schedule.every().day.at("20:48").do(self.run)
        schedule.every().day.at("02:32").do(self.release)

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
        # investor = '71081980'
        # pwd = 'zhenhu123'

        # 加载配置
        config = open(get_dss()+'fut/cfg/config.json')
        setting = json.load(config)
        front_trade = setting['front_trade']
        front_quote = setting['front_quote']
        broker = setting['broker']

        # CTP
        # front_trade = 'tcp://180.168.146.187:10101'
        # front_quote = 'tcp://180.168.146.187:10111'
        # broker = '9999'
        investor = ''
        pwd = ''

        time.sleep(3)
        qq = TestQuote(front_quote, broker, investor, pwd)

        qq.daily_worker()

        # print('begin')
        # qq.run()
        # print('wait')
        # input()

    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

        qq.run()
        input()
