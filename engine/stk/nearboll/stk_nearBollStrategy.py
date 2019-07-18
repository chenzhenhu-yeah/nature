# encoding: UTF-8

from csv import DictReader
from collections import defaultdict
import pandas as pd

from nature import to_log, get_nature_day
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import ArrayManager, Signal, Portfolio, Book

########################################################################
class stk_NearBollSignal(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        Signal.__init__(self, portfolio, vtSymbol)

        # 策略参数
        self.initBars = 90                    # 初始化am所用的数目
        self.fixedSize = 1                         # 每次交易的数量
        self.singlePosition = 3E4

        # 策略参数
        self.bollWindow = 20                     # 布林通道窗口数
        self.bollDev = 2                         # 布林通道的偏差

        # 策略变量
        self.bollUp = None                          # 布林通道上轨
        self.bollDown = None                        # 布林通道下轨

        # 需要持久化保存的参数
        self.buyPrice = 0
        self.intraTradeLow = 100E4                   # 持仓期内的最低点
        self.longStop = 100E4                        # 多头止损

        # 载入历史数据，并采用回放计算的方式初始化策略数值
        initData = self.portfolio.engine._backcall_loadInitBar(self.vtSymbol, self.initBars)
        for bar in initData:
            if not self.am.inited:
                self.onBar(bar)

    #----------------------------------------------------------------------
    def onBar(self, bar):
        """新推送过来一个bar，进行处理"""
        #print(bar.date, self.vtSymbol)

        self.bar = bar
        self.am.updateBar(bar)
        if not self.am.inited:
            return

        #print('here')
        self.calculateIndicator()     # 计算指标
        #self.generateSignal_A(bar)
        self.generateSignal_B(bar)    # 触发信号，产生交易指令

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

        to_log(self.vtSymbol)

        if T5_close > T5_bollUp:
            to_log(bar.date+' rise boll up five days before ')
            #print(T5_close,T5_open,T5_high,T5_low)

        pos = self.portfolio.posDict[self.vtSymbol]
        # 当前无仓位，发送开仓委托
        if pos == 0:
            if T5_close > T5_bollUp and (T5_close-T5_open)/(T5_high+0.01-T5_open) > 0.48:    # T5日收盘突破上轨, T5日应是阳线
                to_log('here1')
                if T5_close/T30_close < 1.15:   # 当前不是处于阶段性顶部
                    to_log('here2')
                    if h/T5_open < 1.12:         # 短期不能冲高太猛
                        to_log('here3')
                        if T1_close/h < 0.97 and T2_close<h and T3_close<h:   # 逐日回调，且距最高点回落3个百分点以上
                            to_log('here4')
                            if T1_close > T1_bollMid and T2_close > T2_bollMid and T3_close > T3_bollMid and T4_close > T4_bollMid:
                                to_log('here5')
                                if T1_close < T3_close or T1_close < T4_close :
                                    to_log('here6')
                                    self.buyPrice = bar.close
                                    self.longStop = min(T5_open,T1_bollMid)
                                    self.intraTradeLow = self.longStop

                                    if bar.close_bfq == 0:            # 回测
                                        self.buy(bar.close, 1000)
                                    else:                             # 实盘
                                        volume = int(self.singlePosition/bar.close_bfq/100)*100
                                        self.buy(bar.close_bfq, volume)

        # 持有多头仓位
        elif pos > 0:
            # if bar.vtSymbol == '300399':
            #     print(self.longStop,self.intraTradeLow,T1_close,self.am.lowArray[-2], self.am.lowArray[-3])

            if T1_close > self.buyPrice and self.longStop < self.buyPrice:
                self.longStop = self.buyPrice*1.002          # 如果当前价大于成本价，移动止损到成本价

            if T1_close <= self.longStop:
                if bar.close_bfq == 0:                  # 回测
                    self.sell(bar.close-0.05, abs(pos)) # 确保成交
                else:                                   # 实盘
                    self.sell(bar.close_bfq-0.05, abs(pos)) # 确保成交
            elif self.longStop > self.intraTradeLow and T1_close < min(self.am.lowArray[-2], self.am.lowArray[-3]):
                if bar.close_bfq == 0:                  # 回测
                    self.sell(bar.close-0.05, abs(pos)) # 确保成交
                else:                                   # 实盘
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
                if bar.close_bfq == 0:        # 回测
                    self.sell(bar.close-0.05, abs(self.pos)) # 确保成交
                else:                         # 实盘
                    self.sell(bar.close_bfq-0.05, abs(self.pos)) # 确保成交


class stk_NearBollPortfolio(Portfolio):
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
        #filename = self.engine.dss + 'csv/setting.csv'
        filename = self.engine.dss + 'csv/setting_stk_' + self.name + '.csv'

        # 从配置文件中读取组合所管理的合约
        with open(filename,encoding='utf-8') as f:
            r = DictReader(f)
            for d in r:
                self.vtSymbolList.append(d['vtSymbol'])
                self.SIZE_DICT[d['vtSymbol']] = int(d['size'])
                self.PRICETICK_DICT[d['vtSymbol']] = float(d['priceTick'])
                self.VARIABLE_COMMISSION_DICT[d['vtSymbol']] = float(d['variableCommission'])
                self.FIXED_COMMISSION_DICT[d['vtSymbol']] = float(d['fixedCommission'])
                self.SLIPPAGE_DICT[d['vtSymbol']] = float(d['slippage'])

        print(u'投资组合的合约代码%s' %(self.vtSymbolList))
        self.portfolioValue = 100E4

        # 加载Signal字典
        for vtSymbol in self.vtSymbolList:
            self.posDict[vtSymbol] = 0
            signal1 = stk_NearBollSignal(self, vtSymbol)
            l = self.signalDict[vtSymbol]
            l.append(signal1)


        self.loadHold()
        self.loadParam()

    #----------------------------------------------------------------------
    def newSignal(self, signal, direction, offset, price, volume):
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

        self.sendOrder(signal.vtSymbol, direction, offset, price, volume, multiplier)

    #----------------------------------------------------------------------
    def loadHold(self):
        """每日重新加载持仓"""

        b1 = Book(self.engine.dss)
        for tactic in b1.tactic_List:
            if tactic.tacticName == self.name:
                for hold in tactic.hold_Array:
                    code = hold[0]
                    num = hold[2]
                    self.posDict[code] = num


    #----------------------------------------------------------------------
    def loadParam(self):
        """每日重新加载信号参数"""

        filename = self.engine.dss + 'csv/stk_strategy_param.csv'
        df = pd.read_csv(filename, dtype='str')
        df = df[df.name==self.name]
        #df = df.sort_values('date', ascending=False)
        lastday= str(df['date'].max())
        print('lastday: ' + lastday)

        for i, row in df.iterrows():
            if lastday == row.date and row.A in self.signalDict:
                signal_list = self.signalDict[row.A]
                if len(signal_list) > 0:
                    signal = signal_list[0]
                    signal.buyPrice = row.B
                    signal.intraTradeLow = row.C
                    signal.longStop = row.D

    #----------------------------------------------------------------------
    def saveParam(self):
        r = []
        today = get_nature_day()
        for code in self.vtSymbolList:
            for signal in self.signalDict[code]:
                r.append([today,self.name, code, signal.buyPrice, signal.intraTradeLow, signal.longStop,'','',''])

        #['date', 'name', 'vtSymbol','buyPrice','intraTradeLow','longStop']
        df = pd.DataFrame(r, columns=['date','name','A','B','C','D','E','F','G'])
        filename = self.engine.dss + 'csv/stk_strategy_param.csv'
        df.to_csv(filename, index=False, mode='a', header=False)
