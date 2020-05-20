# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss, get_contract
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import VtBarData, ArrayManager, Signal, Portfolio, TradeData, SignalResult, DailyResult


########################################################################
class Fut_YueSignal(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        self.type = 'mix'

        # 策略参数
        self.fixedSize = 1            # 每次交易的数量
        self.initBars = 0           # 初始化数据所用的天数
        self.minx = 'min5'

        # 策略临时变量
        self.can_buy = False
        self.can_short = False
        self.can_sell = False
        self.can_cover = False


        # 需要持久化保存的变量

        Signal.__init__(self, portfolio, vtSymbol)

    #----------------------------------------------------------------------
    def load_param(self):
        pass

    #----------------------------------------------------------------------
    def set_param(self, param_dict):
        if 'fixedSize' in param_dict:
            # self.fixedSize = param_dict['fixedSize']
            # if self.fixedSize > 1:
            #     self.type = 'multi'
            # print('成功设置策略参数 self.fixedSize: ',self.fixedSize)
            pass

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min1'):
        """新推送过来一个bar，进行处理"""
        self.bar = bar
        if minx == 'min5':
            self.on_bar_minx(bar)

    def on_bar_minx(self, bar):
        if self.paused == True:
            return

        self.am.updateBar(bar)
        if not self.am.inited:
            return

        #print('here')

        self.calculateIndicator()     # 计算指标
        self.generateSignal(bar)      # 触发信号，产生交易指令

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""

        # 告知组合层，已获得最新行情
        self.portfolio.got_dict[self.vtSymbol] = True

        self.can_buy = False
        self.can_short = False
        self.can_sell = False
        self.can_cover = False

        if '_' in self.vtSymbol:
            # 此处对价差进行分析，产生交易信号
            if self.bar.low <= -200 and self.unit == 0:
                self.can_buy = True

            if self.bar.high >= 0 and self.unit > 0:
                self.can_sell = True


        if '_' in self.vtSymbol:
            r = [[self.bar.date,self.bar.time,self.bar.high,self.bar.low,self.can_buy,self.can_sell]]
        else:
            r = [[self.bar.date,self.bar.time,self.bar.close,self.bar.AskPrice,self.bar.BidPrice,self.can_buy,self.can_sell]]
        df = pd.DataFrame(r)
        filename = get_dss() +  'fut/engine/yue/bar_yue_'+self.type+ '_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

    #----------------------------------------------------------------------
    def generateSignal(self, bar):
        # 若触发交易，调用：self.portfolio._bc_dual_signal()

        # 开多仓
        if self.can_buy == True:
            self.unit += 1
            self.portfolio._bc_dual_signal(DIRECTION_LONG, OFFSET_OPEN, self.fixedSize)

        # 平多仓
        if self.can_sell == True:
            self.portfolio._bc_dual_signal(DIRECTION_SHORT, OFFSET_CLOSE, self.unit * self.fixedSize)
            self.unit = 0

        # 开空仓
        if self.can_short == True:
            self.unit -= 1
            self.portfolio._bc_dual_signal(DIRECTION_SHORT, OFFSET_OPEN, self.fixedSize)

        # 平空仓
        if self.can_cover == True:
            self.portfolio._bc_dual_signal(DIRECTION_LONG, OFFSET_CLOSE, self.unit * self.fixedSize)
            self.unit = 0

    #----------------------------------------------------------------------
    def load_var(self):
        pass

    #----------------------------------------------------------------------
    def save_var(self):
        pass
        if self.paused == True:
            return

    #----------------------------------------------------------------------
    def open(self, price, change):
        pass
    #----------------------------------------------------------------------
    def close(self, price):
        pass

########################################################################
class Fut_YuePortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'yue'

        assert len(symbol_list) == 2
        self.symbol_a = symbol_list[0]
        self.symbol_b = symbol_list[1]
        self.dual_name = self.symbol_a + '_' + self.symbol_b
        symbol_list.append(self.dual_name)
        self.got_dict = {}
        self.got_dict[self.symbol_a] = False
        self.got_dict[self.symbol_b] = False

        df = self.load_param()
        if df is not None:
            for i, row in df.iterrows():
                if row.symbol_a in symbol_list and row.symbol_b in symbol_list:
                    pass

        Portfolio.__init__(self, Fut_YueSignal, engine, symbol_list, signal_param)

        self.name_second = 'yue_' + self.dual_name

        # print(symbol_list, self.dual_name)


    #----------------------------------------------------------------------
    def load_param(self):
        fn = get_dss() +  'fut/engine/yue/portfolio_yue_param.csv'
        df = None
        if os.path.exists(fn):
            df = pd.read_csv(fn)

        return df

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min1'):
        """引擎新推送过来bar，传递给每个signal"""

        # 不处理不相关的品种
        if bar.vtSymbol not in self.vtSymbolList:
            return

        if minx != 'min5':               # 本策略为min5
            return

        if self.result.date != bar.date + ' ' + bar.time:
            previousResult = self.result
            self.result = DailyResult(bar.date + ' ' + bar.time)
            self.resultList.append(self.result)
            if previousResult:
                self.result.updateClose(previousResult.closeDict)

        # 将bar推送给signal
        for signal in self.signalDict[bar.vtSymbol]:
            signal.onBar(bar, minx)
            # print(bar.vtSymbol, bar.time)

        """
        在此实现P层的业务控制逻辑
        为每一个品种来检查是否触发下单条件
        # 开始处理组合关心的bar , 尤其是品种对价差的加工和处理
        """


        if self.got_dict[self.symbol_a] == True and self.got_dict[self.symbol_b] == True:
            for signal in self.signalDict[self.dual_name]:
                for s_a in self.signalDict[self.symbol_a]:
                    ask_price_a = float(s_a.bar.AskPrice)           # 挂卖价
                    bid_price_a = float(s_a.bar.BidPrice)           # 挂买价
                for s_b in self.signalDict[self.symbol_b]:
                    ask_price_b = float(s_b.bar.AskPrice)           # 挂卖价
                    bid_price_b = float(s_b.bar.BidPrice)           # 挂买价

                long_price  = ask_price_a - bid_price_b
                short_price = bid_price_a - ask_price_b
                bar_dual = VtBarData()
                bar_dual.vtSymbol = self.dual_name
                bar_dual.high = short_price
                bar_dual.low = long_price
                bar_dual.date = bar.date
                bar_dual.time = bar.time
                bar_dual.datetime = bar.date + ' ' + bar.time

                # 将bar推送给signal
                signal.onBar(bar_dual, minx)
                self.result.updateBar(bar_dual)

                # print(bar_dual.vtSymbol, bar_dual.datetime )

            self.got_dict[self.symbol_a] = False
            self.got_dict[self.symbol_b] = False


        self.result.updateBar(bar)
        self.result.updatePos(self.posDict)


    #----------------------------------------------------------------------
    def _bc_dual_signal(self, direction, offset, volume):
        for signal in self.signalDict[self.symbol_a]:
            signal_a = signal
        for signal in self.signalDict[self.symbol_b]:
            signal_b = signal

        # 此处应使用挂买、挂卖价
        if direction == DIRECTION_LONG and offset == OFFSET_OPEN:
            signal_a.buy(signal_a.bar.AskPrice, volume)
            signal_b.short(signal_b.bar.BidPrice, volume)
        if direction == DIRECTION_SHORT and offset == OFFSET_OPEN:
            signal_a.short(signal_a.bar.BidPrice, volume)
            signal_b.buy(signal_b.bar.AskPrice, volume)
        if direction == DIRECTION_LONG and offset == OFFSET_CLOSE:
            signal_a.cover(signal_a.bar.AskPrice, volume)
            signal_b.sell(signal_b.bar.BidPrice, volume)
        if direction == DIRECTION_SHORT and offset == OFFSET_CLOSE:
            signal_a.sell(signal_a.bar.BidPrice, volume)
            signal_b.cover(signal_b.bar.AskPrice, volume)
