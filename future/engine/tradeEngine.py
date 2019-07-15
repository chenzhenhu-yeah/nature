# encoding: UTF-8
from __future__ import print_function

from csv import DictReader
from datetime import datetime
from collections import OrderedDict, defaultdict


import schedule
import time
from datetime import datetime
import numpy as np
import pandas as pd
import tushare as ts
import json

from nature import to_log, is_trade_day
from nature import VtBarData, DIRECTION_LONG, DIRECTION_SHORT
from nature import Book
from nature import get_stk_hfq, get_trading_dates

SIZE_DICT = {}
PRICETICK_DICT = {}
VARIABLE_COMMISSION_DICT = {}
FIXED_COMMISSION_DICT = {}
SLIPPAGE_DICT = {}


########################################################################
class TradeEngine(object):
    """
    交易引擎不间断运行。开市前，重新初始化引擎，并加载数据；闭市后，保存数据到文件。
    收到交易指令后，传给交易路由，完成实际下单交易。
    """

    #----------------------------------------------------------------------
    def __init__(self,dss,gateway):
        """Constructor"""
        to_log('in TradeEngine.__init__')
        self.dss = dss
        self.gateway = gateway

        self.init_daily()


    #----------------------------------------------------------------------
    def init_daily(self):
        """每日初始化交易引擎"""
        to_log('in TradeEngine.init_engine')

        self.portfolio = None

        # 合约配置信息
        self.vtSymbolList = []


        self.cash = 0
        self.portfolioValue = 100E4
        self.currentDt = None

        self.dataDict = OrderedDict()
        self.tradeDict = OrderedDict()

    #----------------------------------------------------------------------
    def loadPortfolio(self, PortfolioClass, name):
        """每日重新加载投资组合"""
        to_log('in TradeEngine.loadPortfolio')

        self.portfolio = PortfolioClass(self)
        self.portfolio.name = name
        filename = self.dss + 'csv/setting.csv'

        with open(filename,encoding='utf-8') as f:
            r = DictReader(f)
            for d in r:
                self.vtSymbolList.append(d['vtSymbol'])

                SIZE_DICT[d['vtSymbol']] = int(d['size'])
                PRICETICK_DICT[d['vtSymbol']] = float(d['priceTick'])
                VARIABLE_COMMISSION_DICT[d['vtSymbol']] = float(d['variableCommission'])
                FIXED_COMMISSION_DICT[d['vtSymbol']] = float(d['fixedCommission'])
                SLIPPAGE_DICT[d['vtSymbol']] = float(d['slippage'])

        self.portfolio.init(self.portfolioValue, self.vtSymbolList, SIZE_DICT)

        self.output(u'投资组合的合约代码%s' %(self.vtSymbolList))

    #----------------------------------------------------------------------
    def loadHold(self):
        """每日重新加载持仓"""
        to_log('in TradeEngine.loadHold')

        b1 = Book(self.dss)
        self.cash = b1.cash

        for tactic in b1.tactic_List:
            if tactic.tacticName == self.portfolio:
                for hold in tactic.hold_Array:
                    code = hold[0]
                    num = hold[2]
                    self.portfolio.posDict[code] = num

    #----------------------------------------------------------------------
    def loadData(self):
        """每日重新加载数据"""
        to_log('in TradeEngine.loadData')

        self.dataDict = OrderedDict()

        for vtSymbol in self.vtSymbolList:
            df = get_stk_hfq(self.dss, vtSymbol)
            df = df.sort_values(['date'])
            for i, d in df.iterrows():
                #print(d)
                #set_trace()

                bar = VtBarData()
                bar.vtSymbol = vtSymbol
                bar.symbol = vtSymbol
                bar.open = float(d['open'])
                bar.high = float(d['high'])
                bar.low = float(d['low'])
                bar.close = float(d['close'])
                date = d['date'].split('-')             #去掉字符串中间的'-'
                date = ''.join(date)
                bar.date = date
                bar.time = '00:00:00'
                bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M:%S')
                bar.volume = float(d['volume'])
                #print(bar.datetime)
                #return

                barDict = self.dataDict.setdefault(bar.datetime, OrderedDict())
                barDict[bar.vtSymbol] = bar

                for signal in self.portfolio.signalDict[bar.vtSymbol]:
                    signal.am.updateBar(bar)

        self.output(u'全部数据加载完成')


    #----------------------------------------------------------------------
    def load_signal_param(self):
        """每日重新加载信号参数"""
        to_log('in TradeEngine.load_signal_param')

        df = pd.read_csv('signal_param.csv', dtype={'code':'str'})
        for i, row in df.iterrows():
            signal_list = self.portfolio.signalDict[row.code]
            if len(signal_list) > 0:
                signal = signal_list[0]
                signal.buyPrice = row.buyPrice
                signal.intraTradeLow = row.intraTradeLow
                signal.longStop = row.longStop

    #----------------------------------------------------------------------
    def sendOrder(self, vtSymbol, direction, offset, price, volume):
        """记录交易数据（由portfolio调用）"""

        # 对价格四舍五入
        priceTick = PRICETICK_DICT[vtSymbol]
        price = int(round(price/priceTick, 0)) * priceTick

        # 记录成交数据
        trade = TradeData(vtSymbol, direction, offset, price, volume)
        l = self.tradeDict.setdefault(self.currentDt, [])
        l.append(trade)

        print('send order: ', vtSymbol, direction, offset, price, volume )# 此处还应判断cash
        self.gateway(vtSymbol, direction, offset, price, volume, self.portfolio.name) #发单到真实交易路由


    #----------------------------------------------------------------------
    def output(self, content):
        """输出信息"""
        print(content)

    #----------------------------------------------------------------------
    def getTradeData(self, vtSymbol=''):
        """获取交易数据"""
        tradeList = []

        for l in self.tradeDict.values():
            for trade in l:
                if not vtSymbol:
                    tradeList.append(trade)
                elif trade.vtSymbol == vtSymbol:
                    tradeList.append(trade)

        return tradeList


########################################################################
class TradeData(object):
    """实际成交信息"""

    #----------------------------------------------------------------------
    def __init__(self, vtSymbol, direction, offset, price, volume):
        """Constructor"""
        self.vtSymbol = vtSymbol
        self.direction = direction
        self.offset = offset
        self.price = price
        self.volume = volume

    def print_tradedata(self):
        print(self.vtSymbol, self.direction, self.offset,self.price,self.volume)
