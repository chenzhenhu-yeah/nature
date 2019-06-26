# encoding: UTF-8

from collections import defaultdict

from nature import ArrayManager
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY

########################################################################
class Signal(object):
    def __init__(self, portfolio, vtSymbol):
        """Constructor"""
        self.portfolio = portfolio      # 投资组合

        self.vtSymbol = vtSymbol        # 合约代码
        self.am = ArrayManager()      # K线容器
        self.bar = None                 # 最新K线

    #----------------------------------------------------------------------
    def newSignal(self, direction, offset, price, volume):
        """"""
        self.portfolio.newSignal(self, direction, offset, price, volume)

    #----------------------------------------------------------------------
    def buy(self, price, volume):
        """买入开仓"""
        self.newSignal(DIRECTION_LONG, OFFSET_OPEN, price, volume)


    #----------------------------------------------------------------------
    def sell(self, price,volume):
        """卖出平仓"""

        self.newSignal(DIRECTION_SHORT, OFFSET_CLOSE, price, volume)

    #----------------------------------------------------------------------
    def short(self, price, volume):
        """卖出开仓"""
        self.newSignal(DIRECTION_SHORT, OFFSET_OPEN, price, volume)

    #----------------------------------------------------------------------
    def cover(self, price,volume):
        """买入平仓"""
        self.newSignal(DIRECTION_LONG, OFFSET_CLOSE, price, volume)

########################################################################
class Portfolio(object):

    #----------------------------------------------------------------------
    def __init__(self, engine):
        """Constructor"""
        self.engine = engine

        self.signalDict = defaultdict(list)

        self.tradingDict = {}       # 交易中的信号字典

        self.sizeDict = {}          # 合约大小字典
        self.posDict = {}           # 真实持仓量字典

        self.portfolioValue = 0     # 组合市值

    #----------------------------------------------------------------------
    def print_portfolio(self):
        print(self.tradingDict)       # 交易中的信号字典

        print(self.sizeDict)          # 合约大小字典
        print(self.posDict)        # 真实持仓量字典

        print(self.portfolioValue)     # 组合市值

    #----------------------------------------------------------------------
    def onBar(self, bar):
        """"""
        for signal in self.signalDict[bar.vtSymbol]:
            signal.onBar(bar)

    #----------------------------------------------------------------------
    def sendOrder(self, vtSymbol, direction, offset, price, volume, multiplier):
        """"""
        # 计算合约持仓
        if direction == DIRECTION_LONG:
            self.posDict[vtSymbol] += volume
        else:
            self.posDict[vtSymbol] -= volume

        # 向回测引擎中发单记录
        self.engine.sendOrder(vtSymbol, direction, offset, price, volume*multiplier)
        print('\n',str(self.engine.currentDt) + ' sendOrder...')
        print(vtSymbol, direction, offset, price, volume*multiplier)
        #self.print_portfolio()
