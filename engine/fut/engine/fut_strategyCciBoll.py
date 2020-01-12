# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss, get_contract
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import ArrayManager, Signal, Portfolio, TradeData, SignalResult


########################################################################
class Fut_CciBollSignal_Duo(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        self.type = 'duo'

        # 策略参数
        self.bollWindow = 20                     # 布林通道窗口数
        self.bollDev = 3.1                         # 布林通道的偏差
        self.cciWindow = 10                      # CCI窗口数
        self.atrWindow = 30                      # ATR窗口数
        self.slMultiplier = 3                  # 计算止损距离的乘数

        self.fixedSize = 1           # 每次交易的数量
        self.initBars = 100           # 初始化数据所用的天数
        self.minx = 'min15'

        # 策略临时变量
        self.cciValue = 0                        # CCI指标数值
        self.atrValue = 0                        # ATR指标数值
        self.bollUp = 0
        self.bollDown = 0

        self.can_buy = False
        self.can_short = False

        # 需要持久化保存的变量
        self.cost = 0
        self.intraTradeHigh = 0                  # 移动止损用的持仓期内最高价
        self.intraTradeLow = 0                   # 持仓期内的最低点
        self.stop = 0                            # 多头止损

        Signal.__init__(self, portfolio, vtSymbol)

    #----------------------------------------------------------------------
    def load_param(self):
        filename = get_dss() +  'fut/cfg/signal_cciboll_param.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = df[ df.pz == get_contract(self.vtSymbol).pz ]
            if len(df) > 0:
                rec = df.iloc[0,:]
                self.rsiLength = rec.rsiLength
                self.trailingPercent = rec.trailingPercent
                self.victoryPercent = rec.victoryPercent
                print('成功加载策略参数', self.rsiLength, self.trailingPercent, self.victoryPercent)

    #----------------------------------------------------------------------
    def set_param(self, param_dict):
        if 'bollWindow' in param_dict:
            self.bollWindow = param_dict['bollWindow']
            print('成功设置策略参数 self.bollWindow: ',self.bollWindow)
        if 'bollDev' in param_dict:
            self.bollDev = param_dict['bollDev']
            print('成功设置策略参数 self.bollDev: ',self.bollDev)
        if 'slMultiplier' in param_dict:
            self.slMultiplier = param_dict['slMultiplier']
            print('成功设置策略参数 self.slMultiplier: ',self.slMultiplier)

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min5'):
        """新推送过来一个bar，进行处理"""
        self.bar = bar
        if minx == 'min1':
            self.on_bar_min1(bar)

        if minx == self.minx:
            self.on_bar_minx(bar)

    def on_bar_min1(self, bar):
        pass
        # 效果提升不明显。不能加开仓逻辑，大幅降低效果

        # if self.unit > 0:
        #     if bar.close <= self.stop:
        #         self.sell(bar.close, abs(self.unit))

    def on_bar_minx(self, bar):
        self.am.updateBar(bar)
        if not self.am.inited:
            return

        #print('here')
        self.calculateIndicator()     # 计算指标
        self.generateSignal(bar)    # 触发信号，产生交易指令

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""

        atrArray = self.am.atr(1, array=True)
        self.atrValue = atrArray[-self.atrWindow:].mean()

        self.bollUp, self.bollDown = self.am.boll(self.bollWindow, self.bollDev)
        boll_condition = True if self.bar.close > self.bollUp else False

        self.cciValue = self.am.cci(self.cciWindow)
        cci_condition  = True if self.cciValue > 100 else False

        self.can_buy = False
        if cci_condition and boll_condition:
        #if boll_condition:
            self.can_buy = True

        # r = [[self.bar.date,self.bar.time,self.bar.close,self.can_short,self.bollUp,self.bollDown,self.cciValue,self.atrValue,boll_condition, cci_condition]]
        # df = pd.DataFrame(r)
        # filename = get_dss() +  'fut/engine/cciboll/bar_cciboll_duo_' + self.vtSymbol + '.csv'
        # df.to_csv(filename, index=False, mode='a', header=False)

    # #----------------------------------------------------------------------
    def generateSignal(self, bar):

        # 当前无仓位
        if self.unit == 0:
            self.intraTradeHigh = bar.high
            self.intraTradeLow = bar.low

            if self.can_buy == True and self.paused == False:
                self.cost = bar.close
                self.stop = 0
                self.intraTradeHigh = bar.close

                self.buy(bar.close, self.fixedSize)

        # 持有多头仓位
        elif self.unit > 0:
            if bar.close <= self.stop:
                self.sell(bar.close, abs(self.unit))

            self.intraTradeHigh = max(self.intraTradeHigh, bar.high)
            self.stop = self.intraTradeHigh - self.atrValue * self.slMultiplier

    #----------------------------------------------------------------------
    def load_var(self):
        filename = get_dss() +  'fut/engine/cciboll/signal_cciboll_'+self.type+'_var.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename, sep='$')
            df = df[df.vtSymbol == self.vtSymbol]
            df = df.sort_values(by='datetime')
            df = df.reset_index()
            if len(df) > 0:
                rec = df.iloc[-1,:]            # 取最近日期的记录
                self.unit = rec.unit
                self.cost = rec.cost
                self.intraTradeHigh = rec.intraTradeHigh
                self.intraTradeLow = rec.intraTradeLow
                self.stop = rec.stop
                if rec.has_result == 1:
                    self.result = SignalResult()
                    self.result.unit = rec.result_unit
                    self.result.entry = rec.result_entry
                    self.result.exit = rec.result_exit
                    self.result.pnl = rec.result_pnl

    #----------------------------------------------------------------------
    def save_var(self):
        r = []
        if self.result is None:
            r = [ [self.portfolio.result.date,self.vtSymbol, self.unit, self.cost, \
                   self.intraTradeHigh, self.intraTradeLow, self.stop, \
                   0, 0, 0, 0, 0 ] ]
        else:
            r = [ [self.portfolio.result.date,self.vtSymbol, self.unit, self.cost, \
                   self.intraTradeHigh, self.intraTradeLow, self.stop, \
                   1, self.result.unit, self.result.entry, self.result.exit, self.result.pnl ] ]
        df = pd.DataFrame(r, columns=['datetime','vtSymbol','unit','cost', \
                                      'intraTradeHigh','intraTradeLow','stop', \
                                      'has_result','result_unit','result_entry','result_exit', 'result_pnl'])
        filename = get_dss() +  'fut/engine/cciboll/signal_cciboll_'+self.type+'_var.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, sep='$', mode='a', header=False)
        else:
            df.to_csv(filename, index=False, sep='$')

    #----------------------------------------------------------------------
    def open(self, price, change):
        """开仓"""
        self.unit += change

        if not self.result:
            self.result = SignalResult()
        self.result.open(price, change)

        r = [ [self.bar.date+' '+self.bar.time, '多' if change>0 else '空', '开',  \
               abs(change), price, 0, \
               self.bollUp, self.bollDown, self.cciValue, self.atrValue, \
               self.intraTradeHigh, self.intraTradeLow, self.stop] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl',  \
                                      'bollUp', 'bollDown', 'cciValue', 'atrValue', \
                                      'intraTradeHigh','intraTradeLow','stop'])
        filename = get_dss() +  'fut/engine/cciboll/signal_cciboll_'+self.type+ '_deal_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

    #----------------------------------------------------------------------
    def close(self, price):
        """平仓"""
        self.unit = 0
        self.result.close(price)

        r = [ [self.bar.date+' '+self.bar.time, '', '平',  \
               0, price, self.result.pnl, \
               self.bollUp, self.bollDown, self.cciValue, \
               self.intraTradeHigh, self.intraTradeLow, self.stop] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl',  \
                                      'bollUp', 'bollDown', 'cciValue', \
                                      'intraTradeHigh','intraTradeLow','stop'])
        filename = get_dss() +  'fut/engine/cciboll/signal_cciboll_'+self.type+ '_deal_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

        self.result = None

########################################################################
class Fut_CciBollSignal_Kong(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        self.type = 'kong'

        # 策略参数
        self.bollWindow = 20                     # 布林通道窗口数
        self.bollDev = 3.1                       # 布林通道的偏差
        self.cciWindow = 10                      # CCI窗口数
        self.atrWindow = 30                      # ATR窗口数
        self.slMultiplier = 3                  # 计算止损距离的乘数

        self.fixedSize = 1           # 每次交易的数量
        self.initBars = 100           # 初始化数据所用的天数
        self.minx = 'min15'

        # 策略临时变量
        self.cciValue = 0                        # CCI指标数值
        self.atrValue = 0                        # ATR指标数值
        self.bollUp = 0
        self.bollDown = 0

        self.can_buy = False
        self.can_short = False

        # 需要持久化保存的变量
        self.cost = 0
        self.intraTradeHigh = 0                  # 移动止损用的持仓期内最高价
        self.intraTradeLow = 0                   # 持仓期内的最低点
        self.stop = 0                            # 多头止损

        Signal.__init__(self, portfolio, vtSymbol)

    #----------------------------------------------------------------------
    def load_param(self):
        filename = get_dss() +  'fut/cfg/signal_cciboll_param.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = df[ df.pz == get_contract(self.vtSymbol).pz ]
            if len(df) > 0:
                rec = df.iloc[0,:]
                self.rsiLength = rec.rsiLength
                self.trailingPercent = rec.trailingPercent
                self.victoryPercent = rec.victoryPercent
                print('成功加载策略参数', self.rsiLength, self.trailingPercent, self.victoryPercent)

    #----------------------------------------------------------------------
    def set_param(self, param_dict):
        if 'bollWindow' in param_dict:
            self.bollWindow = param_dict['bollWindow']
            print('成功设置策略参数 self.bollWindow: ',self.bollWindow)
        if 'bollDev' in param_dict:
            self.bollDev = param_dict['bollDev']
            print('成功设置策略参数 self.bollDev: ',self.bollDev)
        if 'slMultiplier' in param_dict:
            self.slMultiplier = param_dict['slMultiplier']
            print('成功设置策略参数 self.slMultiplier: ',self.slMultiplier)

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min5'):
        """新推送过来一个bar，进行处理"""
        self.bar = bar
        if minx == 'min1':
            self.on_bar_min1(bar)

        if minx == self.minx:
            self.on_bar_minx(bar)

    def on_bar_min1(self, bar):
        pass
        # 效果提升不明显。不能加开仓逻辑，大幅降低效果

        # if self.unit < 0:
        #     if bar.close >= self.stop:
        #         self.cover(bar.close, abs(self.unit))


    def on_bar_minx(self, bar):
        self.am.updateBar(bar)
        if not self.am.inited:
            return

        #print('here')
        self.calculateIndicator()     # 计算指标
        self.generateSignal(bar)    # 触发信号，产生交易指令

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""
        atrArray = self.am.atr(1, array=True)
        self.atrValue = atrArray[-self.atrWindow:].mean()


        self.bollUp, self.bollDown = self.am.boll(self.bollWindow, self.bollDev)
        boll_condition = True if self.bar.close < self.bollDown else False

        self.cciValue = self.am.cci(self.cciWindow)
        cci_condition  = True if self.cciValue < -100 else False

        self.can_short = False
        if cci_condition and boll_condition:
        #if boll_condition:
            self.can_short = True

        # r = [[self.bar.date,self.bar.time,self.bar.close,self.can_short,self.bollUp,self.bollDown,self.cciValue,self.atrValue,boll_condition, cci_condition]]
        # df = pd.DataFrame(r)
        # filename = get_dss() +  'fut/engine/cciboll/bar_cciboll_kong_' + self.vtSymbol + '.csv'
        # df.to_csv(filename, index=False, mode='a', header=False)


    #----------------------------------------------------------------------
    def generateSignal(self, bar):

        # 当前无仓位
        if self.unit == 0:
            self.intraTradeHigh = bar.high
            self.intraTradeLow = bar.low

            if self.can_short == True and self.paused == False:
                self.cost = bar.close
                self.stop = 100E4
                self.intraTradeLow = bar.close

                self.short(bar.close, self.fixedSize)

        # 持有空头仓位
        elif self.unit < 0:
            if bar.close >= self.stop:
                self.cover(bar.close, abs(self.unit))

            self.intraTradeLow = min(self.intraTradeLow, bar.low)
            self.stop = self.intraTradeLow + self.atrValue * self.slMultiplier

    #----------------------------------------------------------------------
    def load_var(self):
        filename = get_dss() +  'fut/engine/cciboll/signal_cciboll_'+self.type+'_var.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename, sep='$')
            df = df[df.vtSymbol == self.vtSymbol]
            df = df.sort_values(by='datetime')
            df = df.reset_index()
            if len(df) > 0:
                rec = df.iloc[-1,:]            # 取最近日期的记录
                self.unit = rec.unit
                self.cost = rec.cost
                self.intraTradeHigh = rec.intraTradeHigh
                self.intraTradeLow = rec.intraTradeLow
                self.stop = rec.stop
                if rec.has_result == 1:
                    self.result = SignalResult()
                    self.result.unit = rec.result_unit
                    self.result.entry = rec.result_entry
                    self.result.exit = rec.result_exit
                    self.result.pnl = rec.result_pnl

    #----------------------------------------------------------------------
    def save_var(self):
        r = []
        if self.result is None:
            r = [ [self.portfolio.result.date,self.vtSymbol, self.unit, self.cost, \
                   self.intraTradeHigh, self.intraTradeLow, self.stop, \
                   0, 0, 0, 0, 0 ] ]
        else:
            r = [ [self.portfolio.result.date,self.vtSymbol, self.unit, self.cost, \
                   self.intraTradeHigh, self.intraTradeLow, self.stop, \
                   1, self.result.unit, self.result.entry, self.result.exit, self.result.pnl ] ]
        df = pd.DataFrame(r, columns=['datetime','vtSymbol','unit','cost', \
                                      'intraTradeHigh','intraTradeLow','stop', \
                                      'has_result','result_unit','result_entry','result_exit', 'result_pnl'])
        filename = get_dss() +  'fut/engine/cciboll/signal_cciboll_'+self.type+'_var.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, sep='$', mode='a', header=False)
        else:
            df.to_csv(filename, index=False, sep='$')

    #----------------------------------------------------------------------
    def open(self, price, change):
        """开仓"""
        self.unit += change

        if not self.result:
            self.result = SignalResult()
        self.result.open(price, change)

        r = [ [self.bar.date+' '+self.bar.time, '多' if change>0 else '空', '开',  \
               abs(change), price, 0, \
               self.bollUp, self.bollDown, self.cciValue, self.atrValue,\
               self.intraTradeHigh, self.intraTradeLow, self.stop] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl',  \
                                      'bollUp', 'bollDown', 'cciValue', 'atrValue',\
                                      'intraTradeHigh','intraTradeLow','stop'])
        filename = get_dss() +  'fut/engine/cciboll/signal_cciboll_'+self.type+ '_deal_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)


    #----------------------------------------------------------------------
    def close(self, price):
        """平仓"""
        self.unit = 0
        self.result.close(price)

        r = [ [self.bar.date+' '+self.bar.time, '', '平',  \
               0, price, self.result.pnl, \
               self.bollUp, self.bollDown, self.cciValue, self.atrValue, \
               self.intraTradeHigh, self.intraTradeLow, self.stop] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl',  \
                                      'bollUp', 'bollDown', 'cciValue', 'atrValue', \
                                      'intraTradeHigh','intraTradeLow','stop'])
        filename = get_dss() +  'fut/engine/cciboll/signal_cciboll_'+self.type+ '_deal_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

        self.result = None

########################################################################
class Fut_CciBollPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'cciboll'
        Portfolio.__init__(self, Fut_CciBollSignal_Duo, engine, symbol_list, signal_param, Fut_CciBollSignal_Kong, signal_param)
        #Portfolio.__init__(self, Fut_CciBollSignal_Duo, engine, symbol_list, signal_param, None, None)
        #Portfolio.__init__(self, Fut_CciBollSignal_Kong, engine, symbol_list, signal_param, None, None)



    #----------------------------------------------------------------------
    def _bc_newSignal(self, signal, direction, offset, price, volume):
        """
        对交易信号进行过滤，符合条件的才发单执行。
        计算真实交易价格和数量。
        """
        multiplier = self.portfolioValue * 0.01 / get_contract(signal.vtSymbol).size
        multiplier = int(round(multiplier, 0))
        #print(multiplier)
        multiplier = 1

        #print(self.posDict)
        # 计算合约持仓
        if direction == DIRECTION_LONG:
            self.posDict[signal.vtSymbol] += volume*multiplier
        else:
            self.posDict[signal.vtSymbol] -= volume*multiplier

        #print(self.posDict)

        # 对价格四舍五入
        priceTick = get_contract(signal.vtSymbol).price_tick
        price = int(round(price/priceTick, 0)) * priceTick
        price_deal = price
        if direction == DIRECTION_LONG:
            price_deal += 3*priceTick
        if direction == DIRECTION_SHORT:
            price_deal -= 3*priceTick

        self.engine._bc_sendOrder(signal.vtSymbol, direction, offset, price_deal, volume*multiplier, self.name)

        # 记录成交数据
        trade = TradeData(self.result.date, signal.vtSymbol, direction, offset, price, volume*multiplier)
        # l = self.tradeDict.setdefault(self.result.date, [])
        # l.append(trade)

        self.result.updateTrade(trade)
