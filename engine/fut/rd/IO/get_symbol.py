import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import os
import time
import json

from nature import CtpTrade
from nature import CtpQuote
from nature import Tick

from nature import VtBarData
from nature import SOCKET_BAR
from nature import get_dss

year = '2020'
dt = '20200506'

def cffex():
    fn = get_dss() + 'opt/' + year + '/day/cffex' + dt + '.csv'
    df = pd.read_csv(fn, encoding='gbk', error_bad_lines = False,)
    df = df[df['合约代码'].str.startswith('IO')]
    df['合约代码'] = df['合约代码'].str.strip()
    symbol_set = set(df['合约代码'])
    # print(symbol_set)
    symbol_list = list(symbol_set)

    return symbol_list

def czce():
    fn = get_dss() + 'opt/' + year + '/day/czce' + dt + '.xls'
    df = pd.read_excel(fn, skiprows=1)
    df['品种代码'] = df['品种代码'].str.strip()
    symbol_set = set(df['品种代码'])
    # print(symbol_set)
    # print(len(symbol_set))

    symbol_list = [s for s in symbol_set if len(s) >= 9]
    # print(symbol_list)
    # print(len(symbol_list))

    return symbol_list

def dce():
    fn = get_dss() + 'opt/' + year + '/day/dce' + dt + '.xls'
    df = pd.read_excel(fn)
    df['合约名称'] = df['合约名称'].str.strip()
    symbol_set = set(df['合约名称'])
    # print(symbol_set)
    # print(len(symbol_set))
    symbol_list = [s for s in symbol_set if s == s]
    symbol_list = [s for s in symbol_list if len(s) >= 9]
    # print(symbol_list)
    # print(len(symbol_list))

    return symbol_list

def shfe():
    fn = get_dss() + 'opt/' + year + '/day/shfe' + dt + '.csv'
    # df = pd.read_csv(fn, encoding='gbk', error_bad_lines = False,)
    df = pd.read_csv(fn, encoding='gbk',)
    # print(df.head())
    df['合约代码'] = df['合约代码'].str.strip()
    symbol_set = set(df['合约代码'])
    # print(symbol_set)
    # print(len(symbol_set))
    symbol_list = [s for s in symbol_set if 'C' in s or 'P' in s]
    # print(symbol_list)
    # print(len(symbol_list))

    return symbol_list

class HuQuote(CtpQuote):

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""

        CtpQuote.__init__(self)
        # self.id_list = ['c1909','c2001']

        self.id_list = cffex() + czce() + dce() + shfe()
        # cffex()
        # czce()
        # dce()
        # shfe()

        self.dss = get_dss()
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

        tick.OpenPrice = pDepthMarketData.getOpenPrice()
        tick.HighestPrice = pDepthMarketData.getHighestPrice()
        tick.LowestPrice = pDepthMarketData.getLowestPrice()
        tick.CurrDelta = pDepthMarketData.getCurrDelta()

        self.tradeDay = pDepthMarketData.getTradingDay()

        tick.UpdateTime = pDepthMarketData.getUpdateTime()
        tick.UpdateMillisec = pDepthMarketData.getUpdateMillisec()
        tick.UpperLimitPrice = pDepthMarketData.getUpperLimitPrice()
        tick.LowerLimitPrice = pDepthMarketData.getLowerLimitPrice()
        tick.PreOpenInterest = pDepthMarketData.getPreOpenInterest()

        self.OnTick(tick)

    #----------------------------------------------------------------------
    def OnTick(self, f: Tick):
        # 保存Tick到文件
        now = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        df = pd.DataFrame([f.__dict__])
        df['Localtime'] = now
        cols = ['Localtime','Instrument','OpenPrice','HighestPrice','LowestPrice','LastPrice','Volume','OpenInterest',
                'AskPrice','AskVolume','BidPrice','BidVolume',]
        df = df[cols]

        fn = get_dss() + 'opt/' + year + '/' + now[:7] + '.csv'
        if os.path.exists(fn):
            df.to_csv(fn, index=False, mode='a', header=False)
        else:
            df.to_csv(fn, index=False, mode='a')

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

if __name__ == "__main__":
    # 加载配置
    config = open(get_dss()+'fut/cfg/config.json')
    setting = json.load(config)
    front_trade = setting['front_trade']
    front_quote = setting['front_quote']
    broker = setting['broker']

    investor = ''
    pwd = ''

    print('here')
    qq = TestQuote(front_quote, broker, investor, pwd)

    qq.run()
    input()
    qq.release()
    input()
