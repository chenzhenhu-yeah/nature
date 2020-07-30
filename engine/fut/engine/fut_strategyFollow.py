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
            self.portfolio.profit_c = self.portfolio.hold_c * (self.bar.close - self.portfolio.price_c)

        if self.vtSymbol == self.portfolio.symbol_p:
            self.portfolio.profit_p = self.portfolio.hold_p * (self.bar.close - self.portfolio.price_p)

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

        self.symbol_o = ''
        self.symbol_c = ''
        self.symbol_p = ''

        for symbol in symbol_list:
            if symbol[:2] == 'IF':
                self.symbol_o = symbol
                break

        self.got_dict = {}
        self.strike_high = 0
        self.strike_low  = 0

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
        fn = get_dss() +  'fut/engine/follow/follow_switch.csv'
        if os.path.exists(fn):
            df = pd.read_csv(fn)
            if len(df) > 0:
                rec = df.iloc[-1,:]
                if rec.state == 'on':
                    self.switch_state = 'on'

        Portfolio.__init__(self, Fut_FollowSignal, engine, symbol_list, signal_param)

#----------------------------------------------------------------------
    def rec_profit(self, dt, tm, price_o, price_c, price_p, note):
        r = [[dt, tm, price_o, price_c, self.hold_c, self.price_c, self.profit_c, price_p, self.hold_p, self.price_p, self.profit_p, note, 60, self.profit_c + self.profit_p]]
        df = pd.DataFrame(r, columns=['date','time','price_o','price_c','hold_c','cost_c','profit_c','price_p','hold_p','cost_p','profit_p','note','commission','profit'])
        pz = str(get_contract(self.symbol_c).pz)
        fn = get_dss() +  'fut/engine/follow/portfolio_profit_' + pz + '.csv'
        if os.path.exists(fn):
            df.to_csv(fn, index=False, mode='a', header=False)
        else:
            df.to_csv(fn, index=False)

#----------------------------------------------------------------------
    def get_open_signal(self):
        return True

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
        if bar.time > '09:35:00':    # 因第一根K线的价格为0
            if self.switch_state == 'on' and self.get_open_signal() == True:
                s_o = self.signalDict[self.symbol_o][0]
                self.price_o = s_o.bar.close
                self.price_o_high = 1.04 * self.price_o
                self.price_o_low  = 0.96 * self.price_o

                strike_mid = int( round(self.price_o/100, 0) * 100 )
                self.strike_high = strike_mid + 300
                self.strike_low  = strike_mid - 300

                self.symbol_c = 'IO' + self.symbol_o[2:6] + '-C-' + str(self.strike_high)
                self.symbol_p = 'IO' + self.symbol_o[2:6] + '-P-' + str(self.strike_low)

                s_c = self.signalDict[self.symbol_c][0]
                s_p = self.signalDict[self.symbol_p][0]

                if self.engine.type == 'backtest':
                    s_c.short(s_c.bar.close, 1)
                    s_p.short(s_p.bar.close, 1)
                else:
                    s_c.short(s_c.bar.BidPrice, 1)                     # 挂买价
                    s_p.short(s_p.bar.BidPrice, 1)                     # 挂买价

                self.got_dict[self.symbol_o] = False
                self.got_dict[self.symbol_c] = False
                self.got_dict[self.symbol_p] = False

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

                df2 = pd.DataFrame([{'state':'off'}])
                fn = get_dss() +  'fut/engine/follow/follow_switch.csv'
                df2.to_csv(fn, index=False)                          # 回写文件

                self.rec_profit(bar.date, bar.time, s_o.bar.close, s_c.bar.close, s_p.bar.close, '开仓')

            if self.hold_c == -1 and self.hold_p == -1 :
                if self.got_dict[self.symbol_o] == True and self.got_dict[self.symbol_c] == True and self.got_dict[self.symbol_p] == True:
                    self.got_dict[self.symbol_o] = False
                    self.got_dict[self.symbol_c] = False
                    self.got_dict[self.symbol_p] = False

                    s_o = self.signalDict[self.symbol_o][0]
                    s_c = self.signalDict[self.symbol_c][0]
                    s_p = self.signalDict[self.symbol_p][0]

                    # 盈亏离场 或 无剩余价值离场
                    if self.profit_c + self.profit_p > 0.5*self.profit_o or self.profit_c + self.profit_p < -0.3*self.profit_o or abs(self.hold_c*s_c.bar.close + self.hold_p*s_p.bar.close) <= 3:
                        if self.engine.type == 'backtest':
                            s_c.cover(s_c.bar.close, 1)
                            s_p.cover(s_p.bar.close, 1)
                        else:
                            s_c.cover(s_c.bar.AskPrice, 1)           # 挂卖价
                            s_p.cover(s_p.bar.AskPrice, 1)           # 挂卖价

                        self.hold_c = 0
                        self.hold_p = 0

                        self.rec_profit(bar.date, bar.time, s_o.bar.close, s_c.bar.close, s_p.bar.close, '清仓')

                    # 已持仓
                    elif self.hold_c == -1 or self.hold_p == -1:
                        # 上涨2% 或 离行权价小于50点时
                        if s_o.bar.close > self.price_o_high or self.strike_high - s_o.bar.close < 50:
                            if self.engine.type == 'backtest':
                                s_c.cover(s_c.bar.close, 1)
                                s_p.cover(s_p.bar.close, 1)
                            else:
                                s_c.cover(s_c.bar.AskPrice, 1)           # 挂卖价
                                s_p.cover(s_p.bar.AskPrice, 1)           # 挂卖价

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
                            self.price_o_high = 1.04 * self.price_o
                            self.price_o_low  = 0.96 * self.price_o

                            self.strike_high += 100
                            self.strike_low  += 100

                            self.symbol_c = 'IO' + self.symbol_o[2:6] + '-C-' + str(self.strike_high)
                            self.symbol_p = 'IO' + self.symbol_o[2:6] + '-P-' + str(self.strike_low)

                            s_c = self.signalDict[self.symbol_c][0]
                            s_p = self.signalDict[self.symbol_p][0]

                            if self.engine.type == 'backtest':
                                s_c.short(s_c.bar.close, 1)
                                s_p.short(s_p.bar.close, 1)
                            else:
                                s_c.short(s_c.bar.BidPrice, 1)                     # 挂买价
                                s_p.short(s_p.bar.BidPrice, 1)                     # 挂买价

                            self.got_dict[self.symbol_o] = False
                            self.got_dict[self.symbol_c] = False
                            self.got_dict[self.symbol_p] = False

                            if self.engine.type == 'backtest':
                                self.price_c += s_c.bar.close
                                self.price_p += s_p.bar.close
                            else:
                                self.price_c += s_c.bar.BidPrice
                                self.price_p += s_p.bar.BidPrice

                            self.hold_c = -1
                            self.hold_p = -1
                            self.rec_profit(bar.date, bar.time, s_o.bar.close, s_c.bar.close, s_p.bar.close, '移仓')

                        # 下跌2% 或 离行权价小于50点时
                        if s_o.bar.close < self.price_o_low or s_o.bar.close - self.strike_low < 50:
                            if self.engine.type == 'backtest':
                                s_c.cover(s_c.bar.close, 1)
                                s_p.cover(s_p.bar.close, 1)
                            else:
                                s_c.cover(s_c.bar.AskPrice, 1)           # 挂卖价
                                s_p.cover(s_p.bar.AskPrice, 1)           # 挂卖价

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
                            self.price_o_high = 1.04 * self.price_o
                            self.price_o_low  = 0.96 * self.price_o

                            self.strike_high -= 100
                            self.strike_low  -= 100

                            self.symbol_c = 'IO' + self.symbol_o[2:6] + '-C-' + str(self.strike_high)
                            self.symbol_p = 'IO' + self.symbol_o[2:6] + '-P-' + str(self.strike_low)

                            s_c = self.signalDict[self.symbol_c][0]
                            s_p = self.signalDict[self.symbol_p][0]
                            if self.engine.type == 'backtest':
                                s_c.short(s_c.bar.close, 1)
                                s_p.short(s_p.bar.close, 1)
                            else:
                                s_c.short(s_c.bar.BidPrice, 1)                     # 挂买价
                                s_p.short(s_p.bar.BidPrice, 1)                     # 挂买价


                            self.got_dict[self.symbol_o] = False
                            self.got_dict[self.symbol_c] = False
                            self.got_dict[self.symbol_p] = False

                            if self.engine.type == 'backtest':
                                self.price_c += s_c.bar.close
                                self.price_p += s_p.bar.close
                            else:
                                self.price_c += s_c.bar.BidPrice
                                self.price_p += s_p.bar.BidPrice

                            self.hold_c = -1
                            self.hold_p = -1
                            self.rec_profit(bar.date, bar.time, s_o.bar.close, s_c.bar.close, s_p.bar.close, '移仓')


        self.result.updateBar(bar)
        self.result.updatePos(self.posDict)

    #----------------------------------------------------------------------
    def daily_open(self):
        Portfolio.daily_open(self)

        fn = get_dss() +  'fut/engine/follow/portfolio_follow_param.csv'
        if os.path.exists(fn):
            df = pd.read_csv(fn)
            if len(df) > 0:
                rec = df.iloc[-1,:]            # 取最近日期的记录

                self.symbol_c = rec.symbol_c
                self.symbol_p = rec.symbol_p

                self.strike_high = rec.strike_high
                self.strike_low  = rec.strike_low

                self.price_o = rec.price_o
                self.price_o_high = 1.04 * self.price_o
                self.price_o_low  = 0.96 * self.price_o

                self.price_c = rec.price_c
                self.price_p = rec.price_p

                self.hold_c = rec.hold_c
                self.hold_p = rec.hold_p

    #----------------------------------------------------------------------
    def daily_close(self):
        Portfolio.daily_close(self)

        r = [ [self.result.date, self.symbol_c, self.symbol_p, self.strike_high, self.strike_low, \
               self.price_o, self.price_c, self.price_p, self.hold_c, self.hold_p] ]

        df = pd.DataFrame(r, columns=['datetime','symbol_c', 'symbol_p', 'strike_high', 'strike_low','price_o', 'price_c', 'price_p', 'hold_c', 'hold_p'])
        fn = get_dss() +  'fut/engine/follow/portfolio_follow_param.csv'
        if os.path.exists(fn):
            df.to_csv(fn, index=False, mode='a', header=False)
        else:
            df.to_csv(fn, index=False)
