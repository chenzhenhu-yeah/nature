# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss, get_contract
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import VtBarData, ArrayManager, Signal, Portfolio, TradeData, SignalResult, DailyResult


########################################################################
class Fut_AvengerSignal(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        self.type = 'mix'

        # 策略参数
        self.fixedSize = 1            # 每次交易的数量
        self.initBars = 0           # 初始化数据所用的天数
        self.minx = 'min1'

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
        if minx == 'min1':
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

        # if self.vtSymbol == self.portfolio.symbol_c and self.portfolio.hold_c != 0:
        if self.vtSymbol == self.portfolio.symbol_c:
            self.portfolio.profit_c = self.portfolio.hold_c * (self.bar.close - self.portfolio.price_c)

        # if self.vtSymbol == self.portfolio.symbol_p and self.portfolio.hold_p != 0:
        if self.vtSymbol == self.portfolio.symbol_p:
            self.portfolio.profit_p = self.portfolio.hold_p * (self.bar.close - self.portfolio.price_p)

        self.can_buy = False
        self.can_short = False
        self.can_sell = False
        self.can_cover = False

        # 记录数据

        r = [[self.bar.date,self.bar.time,self.bar.close,self.bar.AskPrice,self.bar.BidPrice,self.can_buy,self.can_sell,self.can_short,self.can_cover]]
        df = pd.DataFrame(r)

        filename = get_dss() +  'fut/engine/avenger/bar_avenger_'+self.type+ '_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

    #----------------------------------------------------------------------
    def generateSignal(self, bar):

        # 开多仓
        if self.can_buy == True:
            self.buy(bar.close, self.fixedSize)

        # 平多仓
        if self.can_sell == True:
            self.sell(bar.close, self.fixedSize)

        # 开空仓
        if self.can_short == True:
            self.short(bar.close, self.fixedSize)

        # 平空仓
        if self.can_cover == True:
            self.cover(bar.close, self.fixedSize)

    #----------------------------------------------------------------------
    def load_var(self):
        fn = get_dss() +  'fut/engine/avenger/signal_avenger_var.csv'
        if os.path.exists(fn):
            df = pd.read_csv(fn)
            df = df[df.vtSymbol == self.vtSymbol]
            df = df.sort_values(by='datetime')
            df = df.reset_index()
            if len(df) > 0:
                rec = df.iloc[-1,:]            # 取最近日期的记录
                if self.vtSymbol == self.portfolio.symbol_o:
                    self.portfolio.price_o = rec.price
                    self.portfolio.hold_o = rec.hold
                    self.portfolio.profit_o = rec.profit

                    self.portfolio.price_o_high = 1.01 * rec.price
                    self.portfolio.price_o_low  = 0.99 * rec.price

                if self.vtSymbol == self.portfolio.symbol_c:
                    self.portfolio.price_c = rec.price
                    self.portfolio.hold_c = rec.hold
                    self.portfolio.profit_c = rec.profit

                if self.vtSymbol == self.portfolio.symbol_p:
                    self.portfolio.price_p = rec.price
                    self.portfolio.hold_p = rec.hold
                    self.portfolio.profit_p = rec.profit

    #----------------------------------------------------------------------
    def save_var(self):
        r = []
        if self.vtSymbol == self.portfolio.symbol_o:
            r = [ [self.portfolio.result.date, self.vtSymbol, \
                   self.portfolio.price_o, self.portfolio.hold_o, self.portfolio.profit_o] ]

        if self.vtSymbol == self.portfolio.symbol_c:
            r = [ [self.portfolio.result.date, self.vtSymbol, \
                   self.portfolio.price_c, self.portfolio.hold_c, self.portfolio.profit_c] ]

        if self.vtSymbol == self.portfolio.symbol_p:
            r = [ [self.portfolio.result.date, self.vtSymbol, \
                   self.portfolio.price_p, self.portfolio.hold_p, self.portfolio.profit_p] ]


        if r != []:
            df = pd.DataFrame(r, columns=['datetime','vtSymbol','price','hold','profit'])
            fn = get_dss() +  'fut/engine/avenger/signal_avenger_var.csv'
            if os.path.exists(fn):
                df.to_csv(fn, index=False, mode='a', header=False)
            else:
                df.to_csv(fn, index=False)


    #----------------------------------------------------------------------
    def open(self, price, change):
        pass
        # print('come here open !')

    #----------------------------------------------------------------------
    def close(self, price, change):
        pass
        # print('come here close !')

########################################################################
class Fut_AvengerPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'avenger'

        assert len(symbol_list) == 3
        self.symbol_o = symbol_list[0]
        self.symbol_c = symbol_list[1]
        self.symbol_p = symbol_list[2]

        self.got_dict = {}
        self.got_dict[self.symbol_o] = False
        self.got_dict[self.symbol_c] = False
        self.got_dict[self.symbol_p] = False

        self.price_o = 0
        self.price_o_high = 0
        self.price_o_low  = 0

        self.price_c = 0
        self.price_p = 0

        self.hold_o = 0
        self.hold_c = 0
        self.hold_p = 0

        self.profit_o = 0
        self.profit_c = 0
        self.profit_p = 0

        self.switch_state = 'off'
        pz = str(get_contract(self.symbol_c).pz)
        fn = get_dss() +  'fut/engine/avenger/avenger_switch_' + pz + '.csv'
        if os.path.exists(fn):
            df = pd.read_csv(fn)
            if len(df) > 0:
                rec = df.iloc[-1,:]
                if rec.state == 'on':
                    self.switch_state = 'on'
                    rec.state = 'off'
                    df2 = pd.DataFrame([rec])
                    df2.to_csv(fn, index=False)                      # 回写文件

        Portfolio.__init__(self, Fut_AvengerSignal, engine, symbol_list, signal_param)

#----------------------------------------------------------------------
    def onBar(self, bar, minx='min1'):
        """引擎新推送过来bar，传递给每个signal"""

        # 不处理不相关的品种
        if bar.vtSymbol not in self.vtSymbolList:
            return

        if minx != 'min1':               # 本策略为min1
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

        if bar.time > '09:35:00' and self.got_dict[self.symbol_o] == True and self.got_dict[self.symbol_c] == True and self.got_dict[self.symbol_p] == True:
            s_o = self.signalDict[self.symbol_o][0]
            s_c = self.signalDict[self.symbol_c][0]
            s_p = self.signalDict[self.symbol_p][0]

            # 开仓
            if self.switch_state == 'on':
                s_c.short(s_c.bar.BidPrice, 1)                  # 挂买价
                s_p.short(s_p.bar.BidPrice, 1)                  # 挂买价

                self.price_o = s_o.bar.close
                self.price_o_high = 1.01 * self.price_o
                self.price_o_low  = 0.99 * self.price_o

                self.price_c = s_c.bar.BidPrice
                self.price_p = s_p.bar.BidPrice

                self.hold_c = -1
                self.hold_p = -1

                self.profit_o = (self.price_c + self.price_p) / 2    # 止盈止损点
                self.switch_state = 'off'                            # 不再开仓

            # 盈亏离场
            elif abs(self.profit_c + self.profit_p) > self.profit_o:
                if self.hold_c == -1 and self.hold_p == -1:
                    s_c.cover(s_c.bar.AskPrice, 1)                  # 挂卖价
                    s_p.cover(s_p.bar.AskPrice, 1)                  # 挂卖价
                    self.hold_c = 0
                    self.hold_p = 0
                if self.hold_c == 0 and self.hold_p == -2:
                    s_p.cover(s_p.bar.AskPrice, 2)                  # 挂卖价
                    self.hold_p = 0
                if self.hold_c == -2 and self.hold_p == 0:
                    s_c.cover(s_c.bar.AskPrice, 2)                  # 挂卖价
                    self.hold_c = 0

            # 已持仓
            elif self.hold_c != 0 or self.hold_p != 0:
                # 复仇
                if self.hold_c == -1 and self.hold_p == -1:
                    # 上涨1%
                    if s_o.bar.close > self.price_o_high:
                        s_c.cover(s_c.bar.AskPrice, 1)                  # 挂卖价
                        s_p.short(s_p.bar.BidPrice, 1)                  # 挂买价

                        self.price_c = self.price_c - s_c.bar.AskPrice
                        self.price_p = (s_p.bar.BidPrice + self.price_p) / 2
                        self.hold_c = 0
                        self.hold_p = -2

                    # 下跌1%
                    if s_o.bar.close < self.price_o_low:
                        s_p.cover(s_p.bar.AskPrice, 1)                  # 挂卖价
                        s_c.short(s_c.bar.BidPrice, 1)                  # 挂买价

                        self.price_p = self.price_p - s_p.bar.AskPrice
                        self.price_c = (s_c.bar.BidPrice + self.price_c) / 2
                        self.hold_p = 0
                        self.hold_c = -2

                # 复仇失败，上涨后又回原点
                if self.hold_c == 0 and self.hold_p == -2 and s_o.bar.close < self.price_o:
                    s_c.short(s_c.bar.BidPrice, 1)                     # 挂买价
                    s_p.cover(s_p.bar.AskPrice, 1)                     # 挂卖价

                    self.price_c = s_c.bar.BidPrice + self.price_c
                    self.price_p = 2*self.price_p - s_p.bar.AskPrice
                    self.hold_p = -1
                    self.hold_c = -1


                # 复仇失败，下跌后又回原点
                if self.hold_c == -2 and self.hold_p == 0 and s_o.bar.close > self.price_o:
                    s_p.short(s_p.bar.BidPrice, 1)                     # 挂买价
                    s_c.cover(s_c.bar.AskPrice, 1)                     # 挂卖价
                    self.price_p = s_p.bar.BidPrice + self.price_p
                    self.price_c = 2*self.price_c - s_c.bar.AskPrice
                    self.hold_p = -1
                    self.hold_c = -1


            self.got_dict[self.symbol_o] = False
            self.got_dict[self.symbol_c] = False
            self.got_dict[self.symbol_p] = False

        self.result.updateBar(bar)
        self.result.updatePos(self.posDict)
