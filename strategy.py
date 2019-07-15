# encoding: UTF-8

from collections import defaultdict
import numpy as np
import talib

from nature import send_instruction

DIRECTION_LONG = u'多'
DIRECTION_SHORT = u'空'

OFFSET_OPEN = u'开仓'
OFFSET_CLOSE = u'平仓'
OFFSET_CLOSETODAY = u'平今'
OFFSET_CLOSEYESTERDAY = u'平昨'

# 默认空值
EMPTY_STRING = ''
EMPTY_UNICODE = u''
EMPTY_INT = 0
EMPTY_FLOAT = 0.0

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


########################################################################
class Gateway(object):
    def __init__(self):
        pass

class GatewayPingan(Gateway):
    def __init__(self):
        pass

    def sendOrder(self, code, direction, offset, price, volume, portfolio):
        cost = int(price*volume)
        df = ts.get_realtime_quotes(code)
        name = df.at[0,'name']
        ins_dict = {}

        if direction == DIRECTION_LONG:
            ins_dict = {'ins':'buy_order','portfolio':portfolio,'code':code,'num':volume,'price':price,'cost':cost,'agent':'pingan','name':name}
        if direction == DIRECTION_SHORT:
            ins_dict = {'ins':'sell_order','portfolio':portfolio,'code':code,'num':volume,'price':price,'cost':cost,'agent':'pingan','name':name}
        if ins_dict != {}:
            send_instruction(ins_dict)

########################################################################
class VtBaseData(object):
    """回调函数推送数据的基础类，其他数据类继承于此"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.gatewayName = EMPTY_STRING         # Gateway名称
        self.rawData = None                     # 原始数据

########################################################################
class VtBarData(VtBaseData):
    """K线数据"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtBarData, self).__init__()

        self.vtSymbol = EMPTY_STRING        # vt系统代码
        self.symbol = EMPTY_STRING          # 代码
        self.exchange = EMPTY_STRING        # 交易所

        self.open = EMPTY_FLOAT             # OHLC
        self.high = EMPTY_FLOAT
        self.low = EMPTY_FLOAT
        self.close = EMPTY_FLOAT
        self.close_bfq = EMPTY_FLOAT

        self.date = EMPTY_STRING            # bar开始的时间，日期
        self.time = EMPTY_STRING            # 时间
        self.datetime = None                # python的datetime时间对象

        self.volume = EMPTY_INT             # 成交量
        self.openInterest = EMPTY_INT       # 持仓量
        self.interval = EMPTY_UNICODE       # K线周期


    def print_bar(self):
        print(self.vtSymbol)
        print(self.date)
        print(self.open)
        print(self.high)
        print(self.low)
        print(self.close)


########################################################################
class ArrayManager(object):
    """
    K线序列管理工具，负责：
    1. K线时间序列的维护
    2. 常用技术指标的计算
    """

    #----------------------------------------------------------------------
    def __init__(self, initDays=90, size=100):
        """Constructor"""
        self.count = 0                      # 缓存计数
        self.size = size                    # 缓存大小
        self.initDays = initDays
        self.inited = False                 # True if count>=size

        self.openArray = np.zeros(size)     # OHLC
        self.highArray = np.zeros(size)
        self.lowArray = np.zeros(size)
        self.closeArray = np.zeros(size)
        self.volumeArray = np.zeros(size)

    #----------------------------------------------------------------------
    def updateBar(self, bar):
        """更新K线"""
        self.openArray[:-1] = self.openArray[1:]
        self.highArray[:-1] = self.highArray[1:]
        self.lowArray[:-1] = self.lowArray[1:]
        self.closeArray[:-1] = self.closeArray[1:]
        self.volumeArray[:-1] = self.volumeArray[1:]

        self.openArray[-1] = bar.open
        self.highArray[-1] = bar.high
        self.lowArray[-1] = bar.low
        self.closeArray[-1] = bar.close
        self.volumeArray[-1] = bar.volume

        self.count += 1
        if not self.inited and self.count >= self.initDays:
            self.inited = True

    #----------------------------------------------------------------------
    @property
    def open(self):
        """获取开盘价序列"""
        return self.openArray

    #----------------------------------------------------------------------
    @property
    def high(self):
        """获取最高价序列"""
        return self.highArray

    #----------------------------------------------------------------------
    @property
    def low(self):
        """获取最低价序列"""
        return self.lowArray

    #----------------------------------------------------------------------
    @property
    def close(self):
        """获取收盘价序列"""
        return self.closeArray

    #----------------------------------------------------------------------
    @property
    def volume(self):
        """获取成交量序列"""
        return self.volumeArray

    #----------------------------------------------------------------------
    def sma(self, n, array=False):
        """简单均线"""
        result = talib.SMA(self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def ema(self, n, array=False):
        """指数平均数指标"""
        result = talib.EMA(self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def std(self, n, array=False):
        """标准差"""
        result = talib.STDDEV(self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def cci(self, n, array=False):
        """CCI指标"""
        result = talib.CCI(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def atr(self, n, array=False):
        """ATR指标"""
        result = talib.ATR(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def rsi(self, n, array=False):
        """RSI指标"""
        result = talib.RSI(self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def macd(self, fastPeriod, slowPeriod, signalPeriod, array=False):
        """MACD指标"""
        macd, signal, hist = talib.MACD(self.close, fastPeriod,
                                        slowPeriod, signalPeriod)
        if array:
            return macd, signal, hist
        return macd[-1], signal[-1], hist[-1]

    #----------------------------------------------------------------------
    def adx(self, n, array=False):
        """ADX指标"""
        result = talib.ADX(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def boll(self, n, dev, array=False):
        """布林通道"""
        mid = self.sma(n, array)
        std = self.std(n, array)

        up = mid + std * dev
        down = mid - std * dev

        return up, down

    #----------------------------------------------------------------------
    def keltner(self, n, dev, array=False):
        """肯特纳通道"""
        mid = self.sma(n, array)
        atr = self.atr(n, array)

        up = mid + atr * dev
        down = mid - atr * dev

        return up, down

    #----------------------------------------------------------------------
    def donchian(self, n, array=False):
        """唐奇安通道"""
        up = talib.MAX(self.high, n)
        down = talib.MIN(self.low, n)

        if array:
            return up, down
        return up[-1], down[-1]
