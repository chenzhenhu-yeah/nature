# encoding: UTF-8

from collections import defaultdict



from nature import ArrayManager
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import Signal, Portfolio

########################################################################
class NearBollSignal(Signal):

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
        #self.generateSignal_A(bar)
        self.generateSignal_B(bar)

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""
        self.bollUp, self.bollDown = self.am.boll(self.bollWindow, self.bollDev, True)

    #----------------------------------------------------------------------
    def generateSignal_B(self, bar):
        T1_bollUp, T1_bollDown = self.bollUp[-1], self.bollDown[-1]
        T1_bollMid = (T1_bollUp+T1_bollDown)/2
        T1_close = self.am.closeArray[-1]

        T2_bollUp, T2_bollDown = self.bollUp[-2], self.bollDown[-2]
        T2_bollMid = (T2_bollUp+T2_bollDown)/2
        T2_close = self.am.closeArray[-2]

        T3_bollUp, T3_bollDown = self.bollUp[-3], self.bollDown[-3]
        T3_bollMid = (T3_bollUp+T3_bollDown)/2
        T3_close = self.am.closeArray[-3]

        T4_bollUp, T4_bollDown = self.bollUp[-4], self.bollDown[-4]
        T4_bollMid = (T4_bollUp+T4_bollDown)/2
        T4_close = self.am.closeArray[-4]

        T5_bollUp, T5_bollDown = self.bollUp[-5], self.bollDown[-5]
        T5_bollMid = (T5_bollUp+T5_bollDown)/2
        T5_close = self.am.closeArray[-5]
        T5_open = self.am.openArray[-5]
        T5_high = self.am.highArray[-5]
        T5_low = self.am.lowArray[-5]

        T30_close = self.am.lowArray[-30]
        h = min(T5_close,T4_close)

        if T5_close > T5_bollUp:
            print(bar.date, self.vtSymbol, 'rise boll up five days before')

        pos = self.portfolio.posDict[self.vtSymbol]
        # 当前无仓位，发送开仓委托
        if pos == 0:
            if T5_close > T5_bollUp and (T5_close-T5_open)/(T5_high+0.01-T5_low) > 0.618:    # T5日收盘突破上轨, T5日应是阳线
                if T5_close/T30_close < 1.15:   # 当前不是处于阶段性顶部
                    if h/T5_open < 1.12:         # 短期不能冲高太猛
                        if T1_close/h < 0.97 and T2_close<h and T3_close<h:   # 逐日回调，且距最高点回落3个百分点以上
                            if T1_close > T1_bollMid and T2_close > T2_bollMid and T3_close > T3_bollMid and T4_close > T4_bollMid:
                                if T1_close < T3_close or T1_close < T4_close :
                                    self.buyPrice = bar.close
                                    self.longStop = min(T5_open,T1_bollMid)
                                    self.intraTradeLow = self.longStop

                                    if bar.close_bfq == 0:
                                        self.buy(bar.close, 1000)
                                    else:
                                        volume = int(self.singlePosition/bar.close_bfq/100)*100
                                        self.buy(bar.close_bfq, volume)


        # 持有多头仓位
        elif pos > 0:
            # if bar.vtSymbol == '300399':
            #     print(self.longStop,self.intraTradeLow,T1_close,self.am.lowArray[-2], self.am.lowArray[-3])

            if T1_close > self.buyPrice and self.longStop < self.buyPrice:
                self.longStop = self.buyPrice*1.002          # 如果当前价大于成本价，移动止损到成本价

            if T1_close <= self.longStop:
                if bar.close_bfq == 0:
                    self.sell(bar.close-0.05, abs(pos)) # 确保成交
                else:
                    self.sell(bar.close_bfq-0.05, abs(pos)) # 确保成交
            elif self.longStop > self.intraTradeLow and T1_close < min(self.am.lowArray[-2], self.am.lowArray[-3]):
                if bar.close_bfq == 0:
                    self.sell(bar.close-0.05, abs(pos)) # 确保成交
                else:
                    self.sell(bar.close_bfq-0.05, abs(pos)) # 确保成交

                #print('here2:',bar.close)

    #----------------------------------------------------------------------
    def generateSignal_A(self, bar):
        T1_bollUp, T1_bollDown = self.bollUp[-1], self.bollDown[-1]
        T1_bollMid = (T1_bollUp+T1_bollDown)/2
        T1_close = self.am.closeArray[-1]

        T2_bollUp, T2_bollDown = self.bollUp[-2], self.bollDown[-2]
        T2_bollMid = (T2_bollUp+T2_bollDown)/2
        T2_close = self.am.closeArray[-2]

        T3_bollUp, T3_bollDown = self.bollUp[-3], self.bollDown[-3]
        T3_bollMid = (T3_bollUp+T3_bollDown)/2
        T3_close = self.am.closeArray[-3]

        T4_bollUp, T4_bollDown = self.bollUp[-4], self.bollDown[-4]
        T4_bollMid = (T4_bollUp+T4_bollDown)/2
        T4_close = self.am.closeArray[-4]

        T5_bollUp, T5_bollDown = self.bollUp[-5], self.bollDown[-5]
        T5_bollMid = (T5_bollUp+T5_bollDown)/2
        T5_close = self.am.closeArray[-5]

        T6_bollUp, T6_bollDown = self.bollUp[-6], self.bollDown[-6]
        T6_bollMid = (T6_bollUp+T6_bollDown)/2
        T6_close = self.am.closeArray[-6]

        pos = self.portfolio.posDict[self.vtSymbol]
        # 当前无仓位，发送开仓委托
        if pos == 0:
            if T1_close > T1_bollMid :
                if T2_close > T2_bollMid :
                    if T3_close > T3_bollMid and self.am.openArray[-3] < T3_bollMid:
                        if bar.close_bfq == 0:
                            self.buy(bar.close, 1000)
                        else:
                            volume = int(self.singlePosition/bar.close_bfq/100)*100
                            self.buy(bar.close_bfq, volume)

        # 持有多头仓位
        elif pos > 0:
            if T1_close < min(self.am.lowArray[-2], self.am.lowArray[-3]):
                if bar.close_bfq == 0:
                    self.sell(bar.close-0.05, abs(self.pos)) # 确保成交
                else:
                    self.sell(bar.close_bfq-0.05, abs(self.pos)) # 确保成交



########################################################################
class NearBollPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine):
        Portfolio.__init__(self, engine)

    #----------------------------------------------------------------------
    def init(self, portfolioValue, vtSymbolList, sizeDict):
        """"""
        self.portfolioValue = portfolioValue
        self.sizeDict = sizeDict

        for vtSymbol in vtSymbolList:
            signal1 = NearBollSignal(self, vtSymbol)
            l = self.signalDict[vtSymbol]
            l.append(signal1)

            self.posDict[vtSymbol] = 0

    #----------------------------------------------------------------------
    def newSignal(self, signal, direction, offset, price, volume):
        """对交易信号进行过滤，符合条件的才发单执行"""
        multiplier = 1
        self.sendOrder(signal.vtSymbol, direction, offset, price, volume, multiplier)
