# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss, get_contract
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import VtBarData, ArrayManager, Signal, Portfolio, TradeData, SignalResult, DailyResult


########################################################################
class Fut_FollowSignal(Signal):

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
        if self.vtSymbol in [self.portfolio.symbol_o, self.portfolio.symbol_c, self.portfolio.symbol_p]:
            r = [[self.bar.date,self.bar.time,self.bar.close,self.bar.AskPrice,self.bar.BidPrice,self.can_buy,self.can_sell,self.can_short,self.can_cover]]
            df = pd.DataFrame(r)

            filename = get_dss() +  'fut/engine/follow/bar_follow_'+self.type+ '_' + self.vtSymbol + '.csv'
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
class Fut_FollowPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'follow'

        assert len(symbol_list) == 3
        self.symbol_o = symbol_list[0]
        self.symbol_c = symbol_list[1]
        self.symbol_p = symbol_list[2]

        if self.symbol_o[:2] == 'IF':
            self.symbol_future = 'IO' + self.symbol_o[2:]
        else:
            self.symbol_future = self.symbol_o
        self.dual_name = self.symbol_future

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

        self.flag_c, self.flag_p, self.strike_high, self.strike_low, self.fixed_size, self.switch_state, self.percent, self.gap = self.load_param(self.symbol_c, self.symbol_p)

        Portfolio.__init__(self, Fut_FollowSignal, engine, symbol_list, signal_param)

        self.name_second = 'follow_' + self.dual_name

    #----------------------------------------------------------------------
    def load_param(self, s_c, s_p):
        fn = get_dss() +  'fut/engine/follow/portfolio_follow_param.csv'
        df = pd.read_csv(fn)
        df = df[(df.symbol_c == s_c) & (df.symbol_p == s_p)]
        row = df.iloc[-1,:]

        return row.flag_c, row.flag_p, int(row.strike_high), int(row.strike_low), int(row.fixed_size), row.switch_state, float(row.percent), int(row.gap)

    #----------------------------------------------------------------------
    def set_param(self, rec):
        fn = get_dss() +  'fut/engine/follow/portfolio_follow_param.csv'
        df = pd.read_csv(fn)
        df = df[(df.symbol_c != rec[1]) & (df.symbol_p != rec[2])]
        df.to_csv(fn, index=False)

        df = pd.DataFrame([rec], columns=['symbol_o','symbol_c','symbol_p','flag_c','flag_p','strike_high','strike_low','fixed_size','switch_state','percent','gap'])
        df.to_csv(fn, index=False, mode='a', header=False)

    #----------------------------------------------------------------------
    def rec_profit(self, dt, tm, price_o, price_c, price_p, note):
        r = [[dt, tm, price_o, price_c, self.hold_c, self.price_c, self.profit_c, price_p, self.hold_p, self.price_p, self.profit_p, note, 60, self.profit_c + self.profit_p]]
        df = pd.DataFrame(r, columns=['date','time','price_o','price_c','hold_c','cost_c','profit_c','price_p','hold_p','cost_p','profit_p','note','commission','profit'])
        pz = str(get_contract(self.symbol_c).pz)
        fn = get_dss() +  'fut/engine/follow/portfolio_' + self.name_second + '_profit.csv'
        if os.path.exists(fn):
            df.to_csv(fn, index=False, mode='a', header=False)
        else:
            df.to_csv(fn, index=False)


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
        if (bar.time > '09:35:00' and bar.time < '15:00:00') or  (bar.time > '21:05:00' and bar.time < '23:00:00') :    # 因第一根K线的价格为0
            # 开仓
            if self.switch_state == 'on' and self.hold_c == 0 and self.hold_p == 0:
                if self.got_dict[self.symbol_o] == True and self.got_dict[self.symbol_c] == True and self.got_dict[self.symbol_p] == True:
                    self.got_dict[self.symbol_o] = False
                    self.got_dict[self.symbol_c] = False
                    self.got_dict[self.symbol_p] = False

                    s_o = self.signalDict[self.symbol_o][0]
                    s_c = self.signalDict[self.symbol_c][0]
                    s_p = self.signalDict[self.symbol_p][0]

                    self.price_o = s_o.bar.close
                    self.price_o_high = (1+self.percent) * self.price_o
                    self.price_o_low  = (1-self.percent) * self.price_o

                    if self.engine.type == 'backtest':
                        s_c.short(s_c.bar.close, self.fixed_size)
                        s_p.short(s_p.bar.close, self.fixed_size)
                    else:
                        s_c.short(s_c.bar.BidPrice, self.fixed_size)                     # 挂买价
                        s_p.short(s_p.bar.BidPrice, self.fixed_size)                     # 挂买价

                    if self.engine.type == 'backtest':
                        self.price_c = s_c.bar.close
                        self.price_p = s_p.bar.close
                    else:
                        self.price_c = s_c.bar.BidPrice
                        self.price_p = s_p.bar.BidPrice

                    self.hold_c = -1
                    self.hold_p = -1

                    self.profit_o = self.price_c + self.price_p          # 止盈止损点
                    self.switch_state = 'off'                            # 不再开仓
                    self.set_param([self.symbol_o,self.symbol_c,self.symbol_p,self.flag_c,self.flag_p,self.strike_high,self.strike_low,self.fixed_size,self.switch_state,self.percent,self.gap])

                    self.rec_profit(bar.date, bar.time, s_o.bar.close, s_c.bar.close, s_p.bar.close, '开仓')

            if self.hold_c == -1 and self.hold_p == -1 :
                if self.got_dict[self.symbol_o] == True and self.got_dict[self.symbol_c] == True and self.got_dict[self.symbol_p] == True:
                    s_o = self.signalDict[self.symbol_o][0]
                    s_c = self.signalDict[self.symbol_c][0]
                    s_p = self.signalDict[self.symbol_p][0]

                    # 盈亏离场 或 无剩余价值离场
                    if self.profit_c + self.profit_p > 0.5*self.profit_o or self.profit_c + self.profit_p < -0.3*self.profit_o or abs(s_c.bar.close + s_p.bar.close) <= 3:
                        if self.engine.type == 'backtest':
                            s_c.cover(s_c.bar.close, self.fixed_size)
                            s_p.cover(s_p.bar.close, self.fixed_size)
                        else:
                            s_c.cover(s_c.bar.AskPrice, self.fixed_size)           # 挂卖价
                            s_p.cover(s_p.bar.AskPrice, self.fixed_size)           # 挂卖价

                        self.hold_c = 0
                        self.hold_p = 0

                        self.rec_profit(bar.date, bar.time, s_o.bar.close, s_c.bar.close, s_p.bar.close, '清仓')

                    # 已持仓
                    elif self.hold_c == -1 or self.hold_p == -1:
                        # 上涨4% 或 离行权价小于50点时
                        if s_o.bar.close > self.price_o_high or self.strike_high - s_o.bar.close < self.gap:
                            if self.engine.type == 'backtest':
                                s_c.cover(s_c.bar.close, self.fixed_size)
                                s_p.cover(s_p.bar.close, self.fixed_size)
                            else:
                                s_c.cover(s_c.bar.AskPrice, self.fixed_size)           # 挂卖价
                                s_p.cover(s_p.bar.AskPrice, self.fixed_size)           # 挂卖价

                            if self.engine.type == 'backtest':
                                self.price_c = self.price_c - s_c.bar.close
                                self.price_p = self.price_p - s_p.bar.close
                            else:
                                self.price_c = self.price_c - s_c.bar.AskPrice
                                self.price_p = self.price_p - s_p.bar.AskPrice

                            self.hold_c = 0
                            self.hold_p = 0
                            self.rec_profit(bar.date, bar.time, s_o.bar.close, s_c.bar.close, s_p.bar.close, '移仓')

                            self.price_o = s_o.bar.close
                            self.price_o_high = (1+self.percent) * self.price_o
                            self.price_o_low  = (1-self.percent) * self.price_o

                            self.strike_high += 2*self.gap
                            self.strike_low  += 2*self.gap

                            self.symbol_c = self.symbol_future + self.flag_c + str(self.strike_high)
                            self.symbol_p = self.symbol_future + self.flag_p + str(self.strike_low)

                            self.set_param([self.symbol_o,self.symbol_c,self.symbol_p,self.flag_c,self.flag_p,self.strike_high,self.strike_low,self.fixed_size,self.switch_state,self.percent,self.gap])
                            self.switch_state = 'on'
                            self.vtSymbolList = [self.symbol_o,self.symbol_c,self.symbol_p]
                            for s in [self.symbol_c,self.symbol_p]:
                                self.posDict[s] = 0
                                signal1 = Fut_FollowSignal(self, s)
                                l = self.signalDict[s]
                                l.append(signal1)

                        # 下跌4% 或 离行权价小于50点时
                    elif s_o.bar.close < self.price_o_low or s_o.bar.close - self.strike_low < self.gap:
                            if self.engine.type == 'backtest':
                                s_c.cover(s_c.bar.close, self.fixed_size)
                                s_p.cover(s_p.bar.close, self.fixed_size)
                            else:
                                s_c.cover(s_c.bar.AskPrice, self.fixed_size)           # 挂卖价
                                s_p.cover(s_p.bar.AskPrice, self.fixed_size)           # 挂卖价

                            if self.engine.type == 'backtest':
                                self.price_c = self.price_c - s_c.bar.close
                                self.price_p = self.price_p - s_p.bar.close
                            else:
                                self.price_c = self.price_c - s_c.bar.AskPrice
                                self.price_p = self.price_p - s_p.bar.AskPrice

                            self.hold_c = 0
                            self.hold_p = 0
                            self.rec_profit(bar.date, bar.time, s_o.bar.close, s_c.bar.close, s_p.bar.close, '移仓')

                            self.price_o = s_o.bar.close
                            self.price_o_high = (1+self.percent) * self.price_o
                            self.price_o_low  = (1-self.percent) * self.price_o

                            self.strike_high -= 2*self.gap
                            self.strike_low  -= 2*self.gap

                            self.symbol_c = self.symbol_future + self.flag_c + str(self.strike_high)
                            self.symbol_p = self.symbol_future + self.flag_p + str(self.strike_low)

                            self.set_param([self.symbol_o,self.symbol_c,self.symbol_p,self.flag_c,self.flag_p,self.strike_high,self.strike_low,self.fixed_size,self.switch_state,self.percent,self.gap])
                            self.switch_state = 'on'
                            self.vtSymbolList = [self.symbol_o,self.symbol_c,self.symbol_p]
                            for s in [self.symbol_c, self.symbol_p]:
                                self.posDict[s] = 0
                                signal1 = Fut_FollowSignal(self, s)
                                l = self.signalDict[s]
                                l.append(signal1)

                    self.got_dict[self.symbol_o] = False
                    self.got_dict[self.symbol_c] = False
                    self.got_dict[self.symbol_p] = False

        self.result.updateBar(bar)
        self.result.updatePos(self.posDict)

    #----------------------------------------------------------------------
    def daily_open(self):
        Portfolio.daily_open(self)

        fn = get_dss() +  'fut/engine/follow/portfolio_' + self.name_second + '_save.csv'
        if os.path.exists(fn):
            df = pd.read_csv(fn)
            if len(df) > 0:
                rec = df.iloc[-1,:]            # 取最近日期的记录

                self.profit_o = rec.profit_o
                self.price_o = rec.price_o
                self.price_o_high = (1+self.percent) * self.price_o
                self.price_o_low  = (1-self.percent) * self.price_o

                self.price_c = rec.price_c
                self.price_p = rec.price_p

                self.hold_c = rec.hold_c
                self.hold_p = rec.hold_p

    #----------------------------------------------------------------------
    def daily_close(self):
        Portfolio.daily_close(self)

        r = [ [self.result.date, self.profit_o, self.price_o, self.price_c, self.price_p, self.hold_c, self.hold_p] ]

        df = pd.DataFrame(r, columns=['datetime', 'profit_o','price_o', 'price_c', 'price_p', 'hold_c', 'hold_p'])
        fn = get_dss() +  'fut/engine/follow/portfolio_' + self.name_second + '_save.csv'
        if os.path.exists(fn):
            df.to_csv(fn, index=False, mode='a', header=False)
        else:
            df.to_csv(fn, index=False)
