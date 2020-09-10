# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss, get_contract
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import VtBarData, ArrayManager, Signal, Portfolio, TradeData, SignalResult, DailyResult


########################################################################
class Fut_RatioSignal(Signal):

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
        filename = get_dss() + 'fut/cfg/signal_pause_var.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = df[df.signal == self.portfolio.name]
            if len(df) > 0:
                symbol_list = str(df.iat[0,1]).split(',')
                if self.vtSymbol in symbol_list:
                    self.paused = True
                    # print(self.vtSymbol + ' right now paused in ' + self.portfolio.name)
                    return
        self.paused = False

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
        if self.bar.AskPrice - self.bar.BidPrice < 3:
            self.portfolio.got_dict[self.vtSymbol] = True

            if self.vtSymbol == self.portfolio.symbol_c:
                if self.portfolio.engine.type == 'backtest':
                    self.portfolio.profit_c = self.portfolio.hold_c * (self.bar.close - self.portfolio.price_c)
                else:
                    self.portfolio.profit_c = self.portfolio.hold_c * (self.bar.BidPrice - self.portfolio.price_c)

            if self.vtSymbol == self.portfolio.symbol_p:
                if self.portfolio.engine.type == 'backtest':
                    self.portfolio.profit_p = self.portfolio.hold_p * (self.bar.close - self.portfolio.price_p)
                else:
                    self.portfolio.profit_p = self.portfolio.hold_p * (self.bar.AskPrice - self.portfolio.price_p)

        self.can_buy = False
        self.can_short = False
        self.can_sell = False
        self.can_cover = False

        # 记录数据
        if self.vtSymbol in [self.portfolio.symbol_c, self.portfolio.symbol_p]:
            r = [[self.bar.date,self.bar.time,self.bar.close,self.bar.AskPrice,self.bar.BidPrice,self.portfolio.profit,self.portfolio.profit_c,self.portfolio.profit_p,self.portfolio.profit_c+self.portfolio.profit_p]]
            df = pd.DataFrame(r)

            filename = get_dss() +  'fut/engine/ratio/bar_ratio_'+self.type+ '_' + self.vtSymbol + '.csv'
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
        pass

    #----------------------------------------------------------------------
    def save_var(self):
        pass

    #----------------------------------------------------------------------
    def open(self, price, change):
        pass
        # print('come here open !')

    #----------------------------------------------------------------------
    def close(self, price, change):
        pass
        # print('come here close !')



########################################################################
class Fut_RatioPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'ratio'

        assert len(symbol_list) == 2
        self.symbol_c = symbol_list[0]
        self.symbol_p = symbol_list[1]
        self.dual_name = self.symbol_c + '_' + self.symbol_p

        self.got_dict = {}
        self.got_dict[self.symbol_c] = False
        self.got_dict[self.symbol_p] = False


        self.price_c = 0
        self.price_p = 0

        self.hold_c = 0
        self.hold_p = 0

        self.profit_c = 0
        self.profit_p = 0

        self.fixed_size, self.gap, self.profit = self.load_param(self.symbol_c, self.symbol_p)

        Portfolio.__init__(self, Fut_RatioSignal, engine, symbol_list, signal_param)

        self.name_second = 'ratio_' + self.dual_name


    #----------------------------------------------------------------------
    def load_param(self, s_c, s_p):
        fn = get_dss() +  'fut/engine/ratio/portfolio_ratio_param.csv'
        df = pd.read_csv(fn)
        df = df[(df.symbol_c == s_c) & (df.symbol_p == s_p)]
        row = df.iloc[-1,:]

        return int(row.fixed_size), int(row.gap), int(row.profit)

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
        if (bar.time > '09:35:00' and bar.time < '11:25:00' and bar.vtSymbol[:2] in ['IF','IO']) or \
           (bar.time > '13:05:00' and bar.time < '14:55:00' and bar.vtSymbol[:2] in ['IF','IO']) or \
           (bar.time > '09:05:00' and bar.time < '11:25:00' and bar.vtSymbol[:2] not in ['IF','IO']) or \
           (bar.time > '13:35:00' and bar.time < '14:55:00' and bar.vtSymbol[:2] not in ['IF','IO']) or \
           (bar.time > '21:05:00' and bar.time < '22:55:00' and bar.vtSymbol[:2] not in ['IF','IO']) :    # 因第一根K线的价格为0
            # 开仓
            if self.hold_c == 0 and self.hold_p == 0 :
                self.fixed_size, self.gap, self.profit = self.load_param(self.symbol_c, self.symbol_p)    # 加载最新参数
                if self.got_dict[self.symbol_c] == True and self.got_dict[self.symbol_p] == True:
                    self.got_dict[self.symbol_c] = False
                    self.got_dict[self.symbol_p] = False

                    s_c = self.signalDict[self.symbol_c][0]
                    s_p = self.signalDict[self.symbol_p][0]

                    if 2*s_p.bar.close - s_c.bar.close >= self.gap:
                        if self.engine.type == 'backtest':
                            s_c.buy(s_c.bar.close, self.fixed_size)
                            s_p.short(s_p.bar.close, 2*self.fixed_size)
                            self.price_c = s_c.bar.close
                            self.price_p = s_p.bar.close
                        else:
                            s_c.buy(s_c.bar.AskPrice, self.fixed_size)              # 挂卖价
                            s_p.short(s_p.bar.BidPrice, 2*self.fixed_size)          # 挂买价
                            self.price_c = s_c.bar.AskPrice
                            self.price_p = s_p.bar.BidPrice

                        self.hold_c = 1
                        self.hold_p = -2

            # 获利平仓
            if self.hold_c == 1 and self.hold_p == -2:
                self.fixed_size, self.gap, self.profit = self.load_param(self.symbol_c, self.symbol_p)    # 加载最新参数
                if self.got_dict[self.symbol_c] == True and self.got_dict[self.symbol_p] == True:
                    self.got_dict[self.symbol_c] = False
                    self.got_dict[self.symbol_p] = False

                    s_c = self.signalDict[self.symbol_c][0]
                    s_p = self.signalDict[self.symbol_p][0]

                    if self.profit_c + self.profit_p >= self.profit:
                        if self.engine.type == 'backtest':
                            s_c.sell(s_c.bar.close, self.fixed_size)
                            s_p.cover(s_p.bar.close, 2*self.fixed_size)
                        else:
                            s_c.sell(s_c.bar.BidPrice, self.fixed_size)                          # 挂买价
                            s_p.cover(s_p.bar.AskPrice, 2*self.fixed_size)                         # 挂卖价

                        self.hold_c = 0
                        self.hold_p = 0

        self.result.updateBar(bar)
        self.result.updatePos(self.posDict)

    #----------------------------------------------------------------------
    def daily_open(self):
        Portfolio.daily_open(self)

        fn = get_dss() +  'fut/engine/ratio/portfolio_' + self.name_second + '_save.csv'
        if os.path.exists(fn):
            df = pd.read_csv(fn)
            df = df[(df.symbol_c == self.symbol_c) & (df.symbol_p == self.symbol_p)]
            if len(df) > 0:
                rec = df.iloc[-1,:]            # 取最近日期的记录
                self.price_c = rec.price_c
                self.price_p = rec.price_p
                self.hold_c = rec.hold_c
                self.hold_p = rec.hold_p

    #----------------------------------------------------------------------
    def daily_close(self):
        Portfolio.daily_close(self)

        r = [ [self.result.date, self.symbol_c, self.symbol_p, self.price_c, self.price_p, self.hold_c, self.hold_p] ]

        df = pd.DataFrame(r, columns=['datetime','symbol_c', 'symbol_p', 'price_c', 'price_p', 'hold_c', 'hold_p'])
        fn = get_dss() +  'fut/engine/ratio/portfolio_' + self.name_second + '_save.csv'
        if os.path.exists(fn):
            df.to_csv(fn, index=False, mode='a', header=False)
        else:
            df.to_csv(fn, index=False)
