# encoding: UTF-8

from collections import defaultdict



from vtUtility import ArrayManager
from vtUtility import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from strategy import Signal, Portfolio

########################################################################
class UpBollSignal(Signal):

    # 策略参数
    # initDays = 10                       # 初始化数据所用的天数
    fixedSize = 1                         # 每次交易的数量
    singlePosition = 3E4

    # 策略参数
    bollWindow = 20                     # 布林通道窗口数
    bollDev = 2                         # 布林通道的偏差

    # 策略变量
    bollUp = 0                          # 布林通道上轨
    bollDown = 0                        # 布林通道下轨

    intraTradeHigh = 0                  # 持仓期内的最高点
    intraTradeLow = 0                   # 持仓期内的最低点
    buyPrice = 0
    longStop = 100E4                        # 多头止损

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        Signal.__init__(self, portfolio, vtSymbol)

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
        self.bollUp, self.bollDown = self.am.boll(self.bollWindow, self.bollDev, True)

    #----------------------------------------------------------------------
    def generateSignal(self, bar):
        T1_bollUp, T1_bollDown = self.bollUp[-1], self.bollDown[-1]
        T1_bollMid = (T1_bollUp+T1_bollDown)/2
        T1_close = self.am.closeArray[-1]
        T1_open = self.am.openArray[-1]
        T1_high = self.am.highArray[-1]
        T1_low = self.am.lowArray[-1]


        if T1_close > T1_bollUp:
            print(bar.date, self.vtSymbol, 'rise boll up five days before')

########################################################################
class UpBollPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine):
        Portfolio.__init__(self, engine)

    #----------------------------------------------------------------------
    def init(self, portfolioValue, vtSymbolList, sizeDict):
        """"""
        self.portfolioValue = portfolioValue
        self.sizeDict = sizeDict

        for vtSymbol in vtSymbolList:
            signal1 = UpBollSignal(self, vtSymbol)
            l = self.signalDict[vtSymbol]
            l.append(signal1)

            self.posDict[vtSymbol] = 0

    #----------------------------------------------------------------------
    def newSignal(self, signal, direction, offset, price, volume):
        """对交易信号进行过滤，符合条件的才发单执行"""
        multiplier = 1
        self.sendOrder(signal.vtSymbol, direction, offset, price, volume, multiplier)
