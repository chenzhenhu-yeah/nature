# encoding: UTF-8

import pandas as pd
from csv import DictReader
from collections import defaultdict

from nature import to_log
from nature import ArrayManager
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import Signal, Portfolio

########################################################################
class Fut_AtrRsiSignal(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        Signal.__init__(self, portfolio, vtSymbol)

        # 策略参数
        self.atrLength = 22          # 计算ATR指标的窗口数
        self.atrMaLength = 10        # 计算ATR均线的窗口数
        self.rsiLength = 5           # 计算RSI的窗口数
        self.rsiEntry = 16           # RSI的开仓信号
        self.trailingPercent = 0.3   # 百分比移动止损
        self.initBars = 90           # 初始化数据所用的天数
        self.fixedSize = 1           # 每次交易的数量

        # 策略变量
        self.atrValue = 0                        # 最新的ATR指标数值
        self.atrMa = 0                           # ATR移动平均的数值
        self.rsiValue = 0                        # RSI指标的数值

        # 需要持久化保存的参数
        self.buyPrice = 0
        self.intraTradeHigh = 0                  # 移动止损用的持仓期内最高价
        self.intraTradeLow = 100E4                   # 持仓期内的最低点
        self.longStop = 100E4                        # 多头止损
        self.cost = 0

        # 初始化RSI入场阈值
        self.rsiBuy = 50 + self.rsiEntry
        self.rsiSell = 50 - self.rsiEntry

        # 载入历史数据，并采用回放计算的方式初始化策略数值
        initData = self.portfolio.engine._bc_loadInitBar(self.vtSymbol, self.initBars)
        for bar in initData:
            self.bar = bar
            self.am.updateBar(bar)

    #----------------------------------------------------------------------
    def onBar(self, bar):
        """新推送过来一个bar，进行处理"""
        print(bar.time, self.vtSymbol)

        self.bar = bar
        self.am.updateBar(bar)
        if not self.am.inited:
            return

        #print('here')
        self.calculateIndicator()     # 计算指标
        self.generateSignal(bar)    # 触发信号，产生交易指令

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""
        atrArray = self.am.atr(self.atrLength, array=True)
        self.atrValue = atrArray[-1]
        self.atrMa = atrArray[-self.atrMaLength:].mean()

        self.rsiValue = self.am.rsi(self.rsiLength)

    #----------------------------------------------------------------------
    def generateSignal(self, bar):
        # 判断是否要进行交易

        pos = self.portfolio.posDict[self.vtSymbol]
        # 当前无仓位
        if pos == 0:
            self.intraTradeHigh = bar.high
            self.intraTradeLow = bar.low

            # ATR数值上穿其移动平均线，说明行情短期内波动加大
            # 即处于趋势的概率较大，适合CTA开仓
            if self.atrValue > self.atrMa:
                # 使用RSI指标的趋势行情时，会在超买超卖区钝化特征，作为开仓信号
                if self.rsiValue > self.rsiBuy:
                    # 这里为了保证成交，选择超价5个整指数点下单
                    self.buy(bar.close, self.fixedSize)
                    self.cost = bar.close
                    self.longStop = 0
                elif self.rsiValue < self.rsiSell:
                    self.short(bar.close, self.fixedSize)
                    self.cost = bar.close
                    self.longStop = 100E4

        # 持有多头仓位
        elif pos > 0:
            # 计算多头持有期内的最高价，以及重置最低价
            self.intraTradeHigh = max(self.intraTradeHigh, bar.high)

            # 计算多头移动止损
            if self.rsiValue < 35:
                self.longStop = bar.close
            self.longStop = max( self.longStop, self.intraTradeHigh * (1-self.trailingPercent/100) )

            if bar.close <= self.longStop:
                self.sell(bar.close, abs(pos))


        # 持有空头仓位
        elif pos < 0:
            self.intraTradeLow = min(self.intraTradeLow, bar.low)

            if self.rsiValue > 65:
                self.longStop = bar.close
            self.longStop = min( self.longStop, self.intraTradeLow * (1+self.trailingPercent/100) )

            if bar.close >= self.longStop:
                self.cover(bar.close, abs(pos))


class Fut_AtrRsiPortfolio(Portfolio):
    #----------------------------------------------------------------------
    def __init__(self, engine, name):
        Portfolio.__init__(self, engine)

        self.name = name
        self.vtSymbolList = []
        self.SIZE_DICT = {}
        self.PRICETICK_DICT = {}
        self.VARIABLE_COMMISSION_DICT = {}
        self.FIXED_COMMISSION_DICT = {}
        self.SLIPPAGE_DICT = {}

    #----------------------------------------------------------------------
    def init(self):
        """初始化信号字典、持仓字典"""
        filename = self.engine.dss + 'fut/cfg/setting_fut_' + self.name + '.csv'

        with open(filename,encoding='utf-8') as f:
            r = DictReader(f)
            for d in r:
                # 当前可做的品种必须是引擎中有数据的品种
                if d['vtSymbol'] in self.engine.vtSymbol_list:
                    self.vtSymbolList.append(d['vtSymbol'])
                    self.SIZE_DICT[d['vtSymbol']] = int(d['size'])
                    self.PRICETICK_DICT[d['vtSymbol']] = float(d['priceTick'])
                    self.VARIABLE_COMMISSION_DICT[d['vtSymbol']] = float(d['variableCommission'])
                    self.FIXED_COMMISSION_DICT[d['vtSymbol']] = float(d['fixedCommission'])
                    self.SLIPPAGE_DICT[d['vtSymbol']] = float(d['slippage'])

        self.portfolioValue = 100E4

        for vtSymbol in self.vtSymbolList:
            self.posDict[vtSymbol] = 0
            # 每个portfolio可以管理多种类型signal,暂只管理同一种类型的signal
            signal1 = Fut_AtrRsiSignal(self, vtSymbol)
            l = self.signalDict[vtSymbol]
            l.append(signal1)

        print(u'投资组合的合约代码%s' %(self.vtSymbolList))

    #----------------------------------------------------------------------
    def _bc_newSignal(self, signal, direction, offset, price, volume):
        """
        对交易信号进行过滤，符合条件的才发单执行。
        计算真实交易价格和数量。
        """
        multiplier = 1

        # 计算合约持仓
        if direction == DIRECTION_LONG:
            self.posDict[signal.vtSymbol] += volume
        else:
            self.posDict[signal.vtSymbol] -= volume

        # 对价格四舍五入
        priceTick = self.PRICETICK_DICT[signal.vtSymbol]
        price = int(round(price/priceTick, 0)) * priceTick

        self.engine._bc_sendOrder(signal.vtSymbol, direction, offset, price, volume*multiplier, self.name)

    #----------------------------------------------------------------------
    def loadParam(self):
        filename = self.engine.dss + 'fut/cfg/AtrRsi_param.csv'
        df = pd.read_csv(filename)
        for i, row in df.iterrows():
            code = row.vtSymbol
            for signal in self.signalDict[code]:
                signal.buyPrice = row.buyPrice,
                signal.intraTradeLow = row.intraTradeLow
                signal.longStop = row.longStop

    #----------------------------------------------------------------------
    def saveParam(self):
        r = []
        for code in self.vtSymbolList:
            if self.posDict[code] != 0:   #有持仓需保存参数
                for signal in self.signalDict[code]:
                    r.append([code, signal.buyPrice, signal.intraTradeLow, signal.longStop])

        df = pd.DataFrame(r, columns=['vtSymbol','buyPrice','intraTradeLow','longStop'])
        filename = self.engine.dss + 'fut/cfg/AtrRsi_param.csv'
        df.to_csv(filename, index=False)
