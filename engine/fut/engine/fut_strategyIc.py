# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss, get_contract
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import VtBarData, ArrayManager, Signal, Portfolio, TradeData, SignalResult, DailyResult


########################################################################
class Fut_IcSignal(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        self.type = 'mix' 

        # 策略参数
        self.fixedSize = 1            # 每次交易的数量
        self.initBars = 0           # 初始化数据所用的天数
        self.minx = 'min15'

        # 策略临时变量
        self.can_buy = False
        self.can_short = False

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
            print('成功设置策略参数 self.fixedSize: ',self.fixedSize)

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min1'):
        """新推送过来一个bar，进行处理"""
        self.bar = bar
        if minx == 'min15':
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

        self.portfolio.got_dict[self.vtSymbol] = True

        if '_' in self.vtSymbol:
            # 此处对价差进行分析，产生交易信号
            # 若触发交易，调用：self.portfolio._bc_dual_signal()
            pass


        self.can_buy = False
        self.can_short = False

        r = [[self.bar.date,self.bar.time,self.bar.close,self.can_buy,self.can_short]]
        df = pd.DataFrame(r)
        filename = get_dss() +  'fut/engine/ic/bar_ic_'+self.type+ '_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

    #----------------------------------------------------------------------
    def generateSignal(self, bar):
        # 平空仓、开多仓
        if self.can_buy == True:
            pass

        # 平多仓、开空仓
        if self.can_short == True:
            pass

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
class Fut_IcPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'ic'

        assert len(symbol_list) == 2
        self.symbol_g = symbol_list[0]
        self.symbol_d = symbol_list[1]
        self.dual_name = self.symbol_g + '_' + self.symbol_d
        symbol_list.append(self.dual_name)
        self.direction_g = 'direction_g'
        self.num_g = 0
        self.direction_d = 'direction_d'
        self.num_d = 0
        self.got_dict = {}
        self.got_dict[self.symbol_g] = False
        self.got_dict[self.symbol_d] = False

        df = self.load_param()
        if df is not None:
            for i, row in df.iterrows():
                if row.symbol_g in symbol_list and row.symbol_d in symbol_list:
                    self.direction_g = row.symbol_g
                    self.num_g = row.direction_g
                    self.direction_d = row.num_g
                    self.num_d = row.num_d

        Portfolio.__init__(self, Fut_IcSignal, engine, symbol_list, signal_param)

        self.name_second = 'ic_' + self.dual_name

        # print(symbol_list, self.dual_name)


    #----------------------------------------------------------------------
    def load_param(self):
        fn = get_dss() +  'fut/engine/ic/portfolio_ic_param.csv'
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

        if minx != 'min15':               # 本策略为min15
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


        if self.got_dict[self.symbol_g] == True and self.got_dict[self.symbol_d] == True:
            for signal in self.signalDict[self.dual_name]:
                for s_g in self.signalDict[self.symbol_g]:
                    price_g = s_g.bar.close
                for s_d in self.signalDict[self.symbol_d]:
                    price_d = s_d.bar.close

                price_gap = price_g - price_d
                bar_dual = VtBarData()
                bar_dual.vtSymbol = self.dual_name
                bar_dual.open = price_gap
                bar_dual.high = price_gap
                bar_dual.low = price_gap
                bar_dual.close = price_gap
                bar_dual.date = bar.date
                bar_dual.time = bar.time
                bar_dual.datetime = bar.date + ' ' + bar.time

                # 将bar推送给signal
                signal.onBar(bar_dual, minx)
                self.result.updateBar(bar_dual)

                # print(bar_dual.vtSymbol, bar_dual.datetime )

            self.got_dict[self.symbol_g] = False
            self.got_dict[self.symbol_d] = False


        self.result.updateBar(bar)
        self.result.updatePos(self.posDict)


    #----------------------------------------------------------------------
    def _bc_dual_signal(self, direction, offset, volume_g, volume_d):
        for signal in self.signalDict[self.symbol_g]:
            signal_g = signal
        for signal in self.signalDict[self.symbol_d]:
            signal_d = signal

        if direction == DIRECTION_LONG and offset == OFFSET_OPEN:
            signal_g.buy(signal_g.bar.close, volume_g)
            signal_d.short(signal_d.bar.close, volume_d)
        if direction == DIRECTION_SHORT and offset == OFFSET_OPEN:
            signal_g.short(signal_g.bar.close, volume_g)
            signal_d.buy(signal_d.bar.close, volume_d)
        if direction == DIRECTION_LONG and offset == OFFSET_CLOSE:
            signal_g.cover(signal_g.bar.close, volume_g)
            signal_d.sell(signal_d.bar.close, volume_d)
        if direction == DIRECTION_SHORT and offset == OFFSET_CLOSE:
            signal_g.sell(signal_g.bar.close, volume_g)
            signal_d.cover(signal_d.bar.close, volume_d)
