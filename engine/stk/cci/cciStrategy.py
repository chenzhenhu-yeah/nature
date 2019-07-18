# encoding: UTF-8

from collections import defaultdict


from nature import to_log
from nature import ArrayManager
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import Signal, Portfolio

########################################################################
class CciSignal(Signal):

    # 策略参数
    # initDays = 10                       # 初始化数据所用的天数
    fixedSize = 1                         # 每次交易的数量
    singlePosition = 6E4

    # 策略参数
    cciWindow = 20
    cciLong = 100
    cciShort = -100

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        Signal.__init__(self, portfolio, vtSymbol)

        # 策略变量
        self.cciValue = None

        # 需要持久化保存的参数
        self.counter = 0
        self.buyPrice = 0
        self.intraTradeLow = 100E4                   # 持仓期内的最低点
        self.longStop = 100E4                        # 多头止损

    #----------------------------------------------------------------------
    def onBar(self, bar):
        """"""
        #print(bar.date)

        self.bar = bar
        self.am.updateBar(bar)
        if not self.am.inited:
            return

        self.calculateIndicator()
        self.generateSignal(bar)

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""
        self.cciValue = self.am.cci(self.cciWindow, True)

    #----------------------------------------------------------------------
    def generateSignal(self, bar):
        """
        cci>100，买入；
        买入后，6日内cci<100，卖出;
        cci<-100，卖出；
        """

        # cci>100触发买入，停留其上的天数用counter表示。
        if pos > 0 and self.cciValue[-1] >= self.cciLong:
            self.counter += 1

        pos = self.portfolio.posDict[self.vtSymbol]
        # 当前无仓位，发送开仓委托
        if pos == 0:
            if self.cciValue[-1] >= self.cciLong and self.cciValue[-2] < self.cciLong :
                self.buy(bar.close, 1)
        # 持有多头仓位
        elif pos > 0:
            if self.cciValue[-1] < self.cciLong and self.counter < 6:
                self.sell(bar.close, 1)
            elif self.cciValue[-1] < self.cciShort:
                self.sell(bar.close, 1)

########################################################################
class CciPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, name):
        Portfolio.__init__(self, engine, name)

    #----------------------------------------------------------------------
    def init(self, portfolioValue, vtSymbolList, sizeDict):
        """"""
        self.portfolioValue = portfolioValue
        self.sizeDict = sizeDict

        for vtSymbol in vtSymbolList:
            signal1 = CciSignal(self, vtSymbol)
            l = self.signalDict[vtSymbol]
            l.append(signal1)

            self.posDict[vtSymbol] = 0

    #----------------------------------------------------------------------
    def newSignal(self, signal, direction, offset, price, volume):
        """
        对交易信号进行过滤，符合条件的才发单执行。
        计算真实交易价格和数量。
        """
        multiplier = 1

        # 计算合约持仓
        if direction == DIRECTION_LONG:
            self.posDict[vtSymbol] += volume
        else:
            self.posDict[vtSymbol] -= volume

        self.sendOrder(signal.vtSymbol, direction, offset, price, volume, multiplier)
