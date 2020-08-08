# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss, get_contract
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import VtBarData, ArrayManager, Signal, Portfolio, TradeData, SignalResult, DailyResult


########################################################################
class Fut_SwapSignal(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        self.type = 'mix'

        # 策略参数
        self.fixedSize = 1            # 每次交易的数量
        self.initBars = 0           # 初始化数据所用的天数
        self.minx = 'min30'

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
        if minx == 'min30':
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

        filename = get_dss() +  'fut/engine/swap/bar_swap_'+self.type+ '_' + self.vtSymbol + '.csv'
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
        fn = get_dss() +  'fut/engine/swap/signal_swap_var.csv'
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
            fn = get_dss() +  'fut/engine/swap/signal_swap_var.csv'
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
class Fut_SwapPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'swap'

        self.symbol_o = ''
        self.symbol_a = ''
        for symbol in symbol_list:
            if len(symbol) == 6:
                self.symbol_o = symbol
                break
        assert self.symbol_o != ''

        self.got_dict = {}

        self.price_o = 0
        self.price_a = 0

        self.hold_o = 0
        self.hold_a = 0

        self.profit_o = 0
        self.profit_a = 0

        self.can_duo = False
        self.can_kong = False

        self.switch_state = 'on'
        pz = str(get_contract(self.symbol_c).pz)
        fn = get_dss() +  'fut/engine/swap/swap_switch_' + pz + '.csv'
        if os.path.exists(fn):
            df = pd.read_csv(fn)
            if len(df) > 0:
                rec = df.iloc[-1,:]
                if rec.state == 'off':
                    self.switch_state = 'off'

        Portfolio.__init__(self, Fut_SwapSignal, engine, symbol_list, signal_param)

    #----------------------------------------------------------------------
    def rec_profit(self, dt, tm, price_o, price_c, price_p, note):
        r = [[dt, tm, price_o, price_c, self.hold_c, self.price_c, self.profit_c, price_p, self.hold_p, self.price_p, self.profit_p, note, 60, self.profit_c + self.profit_p]]
        df = pd.DataFrame(r, columns=['date','time','price_o','price_c','hold_c','cost_c','profit_c','price_p','hold_p','cost_p','profit_p','note','commission','profit'])
        pz = str(get_contract(self.symbol_c).pz)
        fn = get_dss() +  'fut/engine/swap/portfolio_profit_' + pz + '.csv'
        if os.path.exists(fn):
            df.to_csv(fn, index=False, mode='a', header=False)
        else:
            df.to_csv(fn, index=False)

    #----------------------------------------------------------------------
    def got_all_bar(self):
        for symbol in self.vtSymbolList:
            if symbol not in self.got_dict:
                return False
            if self.got_dict[symbol] == False:
                return False

        for symbol in self.vtSymbolList:
            self.got_dict[symbol] = False

        return True

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min1'):
        """引擎新推送过来bar，传递给每个signal"""
        # 策略未启用
        if self.switch_state == 'off':
            return

        # 不处理不相关的品种
        if bar.vtSymbol not in self.vtSymbolList:
            return

        if minx != 'min30':               # 本策略为min30
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

        """
        在此实现P层的业务控制逻辑
        为每一个品种来检查是否触发下单条件
        # 开始处理组合关心的bar , 尤其是品种对价差的加工和处理
        """

        if bar.time > '09:35:00' and self.got_all_bar() == True:
            s_o = self.signalDict[self.symbol_o][0]

            # 做多
            if self.can_duo == True:
                if self.hold_a == -1:
                    s_a = self.signalDict[self.symbol_a][0]
                    s_a.cover(s_a.bar.close, 1)
                    # s_a.cover(s_a.bar.AskPrice, 1)                  # 挂卖价
                    self.hold_a = 0
                    self.rec_profit(bar.date, bar.time, s_o.bar.close, s_c.bar.close, s_p.bar.close, '平仓')

                strike = int( round((self.price_o - 150)/100, 0) * 100 )
                self.symbol_a = 'IO' + self.symbol_o[2:6] + '-P-' + str(strike)
                s_a = self.signalDict[self.symbol_a][0]
                s_a.short(s_a.bar.close, 1)
                # s_a.short(s_a.bar.BidPrice, 1)                  # 挂买价

                self.price_o = s_o.bar.close
                self.price_a = s_a.bar.close
                # self.price_a = s_a.bar.BidPrice

                self.hold_a = -1
                self.rec_profit(bar.date, bar.time, s_o.bar.close, s_c.bar.close, s_p.bar.close, '做多开仓')

                self.can_duo = False

            # 做空
            elif self.can_kong == True:
                if self.hold_a == -1:
                    s_a = self.signalDict[self.symbol_a][0]
                    s_a.cover(s_a.bar.close, 1)
                    # s_a.cover(s_a.bar.AskPrice, 1)                  # 挂卖价
                    self.hold_a = 0
                    self.rec_profit(bar.date, bar.time, s_o.bar.close, s_c.bar.close, s_p.bar.close, '平仓')

                strike = int( round((self.price_o + 150)/100, 0) * 100 )
                self.symbol_a = 'IO' + self.symbol_o[2:6] + '-C-' + str(strike)
                s_a = self.signalDict[self.symbol_a][0]
                s_a.short(s_a.bar.close, 1)
                # s_a.short(s_a.bar.BidPrice, 1)                      # 挂买价

                self.price_o = s_o.bar.close
                self.price_a = s_a.bar.close
                # self.price_a = s_a.bar.BidPrice

                self.hold_a = -1
                self.rec_profit(bar.date, bar.time, s_o.bar.close, s_c.bar.close, s_p.bar.close, '做空开仓')

                self.can_kong = False

        self.result.updateBar(bar)
        self.result.updatePos(self.posDict)
