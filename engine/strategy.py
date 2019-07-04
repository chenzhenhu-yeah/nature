# encoding: UTF-8

from collections import defaultdict

from nature import ArrayManager
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY

########################################################################
class Signal(object):
    """
    策略信号，实现策略逻辑，实例对象对应一支具体证券，属于且仅属于某个组合。
    包含K线容器，此容器用于计算指标。
    """
    def __init__(self, portfolio, vtSymbol):
        """Constructor"""
        self.portfolio = portfolio      # 投资组合

        self.vtSymbol = vtSymbol        # 合约代码
        self.am = ArrayManager()        # K线容器
        self.bar = None                 # 最新K线

    #----------------------------------------------------------------------
    def newSignal(self, direction, offset, price, volume):
        """调用组合中的接口，传递下单指令"""
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
    """
    证券组合，包含多个证券标的，每个证券对应一个signal。
    负责在engine与signal之间进行上传下达，并做组合层面的风控。
    。
    """

    #----------------------------------------------------------------------
    def __init__(self, engine):
        """Constructor"""
        self.engine = engine
        self.name = ''
        self.signalDict = defaultdict(list)  # 存储信号，code为键, signal列表为值

        self.sizeDict = {}          # 合约大小字典，code为键,size为值
        self.posDict = {}           # 真实持仓量字典，code为键,pos为值

        self.portfolioValue = 0     # 组合市值

    #----------------------------------------------------------------------
    def print_portfolio(self):
        print(self.sizeDict)          # 合约大小字典
        print(self.posDict)        # 真实持仓量字典
        print(self.portfolioValue)     # 组合市值

    #----------------------------------------------------------------------
    def onBar(self, bar):
        """引擎新推送过来bar，传递给每个signal"""
        for signal in self.signalDict[bar.vtSymbol]:
            signal.onBar(bar)

    #----------------------------------------------------------------------
    def sendOrder(self, vtSymbol, direction, offset, price, volume, multiplier):
        """向引擎发单"""

        # 向回测引擎中发单记录
        self.engine.sendOrder(vtSymbol, direction, offset, price, volume*multiplier)
        print('\n',str(self.engine.currentDt) + ' sendOrder...')
        print(vtSymbol, direction, offset, price, volume*multiplier)
        #self.print_portfolio()
