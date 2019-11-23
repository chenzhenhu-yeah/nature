# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss, get_contract
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import ArrayManager, Signal, Portfolio, TradeData, SignalResult


########################################################################
class Fut_RsiBollSignal_Duo(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        self.type = 'duo'

        # 策略参数
        self.rsiLength = 5           # 计算RSI的窗口数
        self.rsiEntry = 16           # RSI的开仓信号
        self.trailingPercent = 0.7   # 百分比移动止损
        self.victoryPercent = 0.3
        self.fixedSize = 1           # 每次交易的数量

        self.initBars = 90           # 初始化数据所用的天数
        self.minx = 'min5'
        # 初始化RSI入场阈值
        self.rsiBuy = 50 + self.rsiEntry
        self.rsiSell = 50 - self.rsiEntry

        # 策略临时变量
        self.atr_short = 0
        self.atr_mid = 0
        self.atr_long = 0

        self.rsi_value = 0                        # RSI指标的数值
        self.rsi_ma = 0
        self.can_buy = False
        self.can_short = False

        self.bollUp = 0
        self.bollDown = 0

        # 需要持久化保存的变量
        self.cost = 0
        self.intraTradeHigh = 0                  # 移动止损用的持仓期内最高价
        self.intraTradeLow = 0                   # 持仓期内的最低点
        self.stop = 0                            # 多头止损

        Signal.__init__(self, portfolio, vtSymbol)

    #----------------------------------------------------------------------
    def load_param(self):
        filename = get_dss() +  'fut/cfg/signal_rsiboll_param.csv'
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
        if 'atrMaLength' in param_dict:
            self.atrMaLength = param_dict['atrMaLength']
            print('成功设置策略参数 self.atrMaLength: ',self.atrMaLength)
        if 'rsiLength' in param_dict:
            self.rsiLength = param_dict['rsiLength']
            print('成功设置策略参数 self.rsiLength: ',self.rsiLength)
        if 'trailingPercent' in param_dict:
            self.trailingPercent = param_dict['trailingPercent']
            print('成功设置策略参数 self.trailingPercent: ',self.trailingPercent)
        if 'victoryPercent' in param_dict:
            self.victoryPercent = param_dict['victoryPercent']
            print('成功设置策略参数 self.victoryPercent: ',self.victoryPercent)

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min5'):
        """新推送过来一个bar，进行处理"""

        self.bar = bar
        if minx == 'min1':
            self.on_bar_min1(bar)

        if minx == self.minx:
            self.on_bar_minx(bar)

        # r = [[minx,bar.date,bar.time,bar.open,bar.close]]
        # df = pd.DataFrame(r)
        # filename = get_dss() +  'fut/check/bar_' + self.vtSymbol + '.csv'
        # df.to_csv(filename, index=False, mode='a', header=False)


    def on_bar_min1(self, bar):
        # 持有多头仓位
        if self.unit > 0:
            if bar.close <= self.stop:
                # print('平多: ', bar.datetime, self.intraTradeHigh, self.stop, bar.close)
                self.sell(bar.close, abs(self.unit))

        # 持有空头仓位
        elif self.unit < 0:
            if bar.close >= self.stop:
                # print('平空: ', bar.datetime, self.intraTradeLow, self.stop, bar.close)
                self.cover(bar.close, abs(self.unit))

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
        atrArray = self.am.atr(1, array=True)  # 长度为100，有效数据为initBars        # print(len(atrArray));  print(atrArray);  assert False
        self.atr_short = atrArray[-3:].mean()
        self.atr_mid = atrArray[-20:].mean()
        self.atr_long = atrArray[-50:].mean()
        atr_condition = True if self.atr_short > self.atr_mid else False

        self.bollUp, self.bollDown = self.am.boll(80, 2)
        boll_condition = True if self.bar.close > self.bollUp else False

        rsiArray = self.am.rsi(self.rsiLength, array=True)
        self.rsi_value = rsiArray[-1]
        self.rsi_ma= rsiArray[-35:].mean()
        rsi_condition  = True if self.rsi_value > self.rsiBuy and self.rsi_ma < 60 else False

        self.can_buy = False
        if rsi_condition and boll_condition and atr_condition:
            self.can_buy = True

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
            # 计算多头持有期内的最高价，以及重置最低价
            self.intraTradeHigh = max(self.intraTradeHigh, bar.high)

            #self.stop = max( self.stop, self.intraTradeHigh * (1-self.victoryPercent/100) )
            if self.stop < self.cost:
                self.stop = max( self.stop, self.intraTradeHigh * (1-self.trailingPercent/100) )
            else:
                self.stop = max( self.stop, self.intraTradeHigh * (1-self.victoryPercent/100) )

            if bar.close <= self.stop:
                # print('平多: ', bar.datetime, self.intraTradeHigh, self.stop, bar.close)
                self.sell(bar.close, abs(self.unit))

    #----------------------------------------------------------------------
    def load_var(self):
        filename = get_dss() +  'fut/check/signal_rsiboll_'+self.type+'_var.csv'
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
        filename = get_dss() +  'fut/check/signal_rsiboll_'+self.type+'_var.csv'
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
               self.bollUp,self.bollDown,self.rsi_value,self.rsi_ma,self.atr_short,self.atr_mid, \
               self.intraTradeHigh, self.intraTradeLow, self.stop] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl',  \
                                      'bollUp','bollDown','rsi_value','rsi_ma','atr_short','atr_mid', \
                                      'intraTradeHigh','intraTradeLow','stop'])
        filename = get_dss() +  'fut/deal/signal_rsiboll_'+self.type+'_' + self.vtSymbol + '.csv'
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
               self.bollUp,self.bollDown,self.rsi_value,self.rsi_ma,self.atr_short,self.atr_mid, \
               self.intraTradeHigh, self.intraTradeLow, self.stop] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl',  \
                                      'bollUp','bollDown','rsi_value','rsi_ma','atr_short','atr_mid', \
                                      'intraTradeHigh','intraTradeLow','stop'])
        filename = get_dss() +  'fut/deal/signal_rsiboll_'+self.type+'_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

        self.result = None


########################################################################
class Fut_RsiBollSignal_Kong(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        self.type = 'kong'

        # 策略参数
        self.rsiLength = 5           # 计算RSI的窗口数
        self.rsiEntry = 16           # RSI的开仓信号
        self.trailingPercent = 0.7   # 百分比移动止损
        self.victoryPercent = 0.3
        self.fixedSize = 1           # 每次交易的数量

        self.initBars = 90           # 初始化数据所用的天数
        self.minx = 'min5'
        # 初始化RSI入场阈值
        self.rsiBuy = 50 + self.rsiEntry
        self.rsiSell = 50 - self.rsiEntry

        # 策略临时变量
        self.atr_short = 0
        self.atr_mid = 0
        self.atr_long = 0

        self.rsi_value = 0                        # RSI指标的数值
        self.rsi_ma = 0
        self.can_buy = False
        self.can_short = False

        self.bollUp = 0
        self.bollDown = 0

        # 需要持久化保存的变量
        self.cost = 0
        self.intraTradeHigh = 0                  # 移动止损用的持仓期内最高价
        self.intraTradeLow = 0                   # 持仓期内的最低点
        self.stop = 0                            # 多头止损

        Signal.__init__(self, portfolio, vtSymbol)

    #----------------------------------------------------------------------
    def load_param(self):
        filename = get_dss() +  'fut/cfg/signal_rsiboll_param.csv'
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
        if 'atrMaLength' in param_dict:
            self.atrMaLength = param_dict['atrMaLength']
            print('成功设置策略参数 self.atrMaLength: ',self.atrMaLength)
        if 'rsiLength' in param_dict:
            self.rsiLength = param_dict['rsiLength']
            print('成功设置策略参数 self.rsiLength: ',self.rsiLength)
        if 'trailingPercent' in param_dict:
            self.trailingPercent = param_dict['trailingPercent']
            print('成功设置策略参数 self.trailingPercent: ',self.trailingPercent)
        if 'victoryPercent' in param_dict:
            self.victoryPercent = param_dict['victoryPercent']
            print('成功设置策略参数 self.victoryPercent: ',self.victoryPercent)

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min5'):
        """新推送过来一个bar，进行处理"""

        self.bar = bar
        if minx == 'min1':
            self.on_bar_min1(bar)

        if minx == self.minx:
            self.on_bar_minx(bar)

        # r = [[minx,bar.date,bar.time,bar.open,bar.close]]
        # df = pd.DataFrame(r)
        # filename = get_dss() +  'fut/check/bar_' + self.vtSymbol + '.csv'
        # df.to_csv(filename, index=False, mode='a', header=False)


    def on_bar_min1(self, bar):
        # 持有多头仓位
        if self.unit > 0:
            if bar.close <= self.stop:
                # print('平多: ', bar.datetime, self.intraTradeHigh, self.stop, bar.close)
                self.sell(bar.close, abs(self.unit))

        # 持有空头仓位
        elif self.unit < 0:
            if bar.close >= self.stop:
                # print('平空: ', bar.datetime, self.intraTradeLow, self.stop, bar.close)
                self.cover(bar.close, abs(self.unit))

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
        atrArray = self.am.atr(1, array=True)  # 长度为100，有效数据为initBars        # print(len(atrArray));  print(atrArray);  assert False
        self.atr_short = atrArray[-3:].mean()
        self.atr_mid = atrArray[-20:].mean()
        self.atr_long = atrArray[-50:].mean()
        atr_condition = True if self.atr_short > self.atr_mid else False

        self.bollUp, self.bollDown = self.am.boll(80, 2)
        boll_condition = True if self.bar.close < self.bollDown else False

        rsiArray = self.am.rsi(self.rsiLength, array=True)
        self.rsi_value = rsiArray[-1]
        self.rsi_ma= rsiArray[-35:].mean()
        rsi_condition  = True if self.rsi_value < self.rsiSell and self.rsi_ma > 40 else False

        self.can_short = False
        if rsi_condition and boll_condition and atr_condition:
            self.can_short = True

        r = [[self.bar.date,self.bar.time,self.bar.close,self.can_short,self.bollDown,self.rsi_value,self.rsi_ma,self.atr_short,self.atr_mid,rsi_condition, boll_condition, atr_condition]]
        df = pd.DataFrame(r)
        filename = get_dss() +  'fut/check/bar_rsiboll_kong_' + self.vtSymbol + '.csv'
        df.to_csv(filename, index=False, mode='a', header=False)


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
            self.intraTradeLow = min(self.intraTradeLow, bar.low)

            #self.stop = min( self.stop, self.intraTradeLow * (1+self.victoryPercent/100) )
            if self.stop > self.cost:
                self.stop = min( self.stop, self.intraTradeLow * (1+self.trailingPercent/100) )
            else:
                self.stop = min( self.stop, self.intraTradeLow * (1+self.victoryPercent/100) )

            #print(self.stop)

            if bar.close >= self.stop:
                # print('平空: ', bar.datetime, self.intraTradeLow, self.stop, bar.close)
                self.cover(bar.close, abs(self.unit))

    #----------------------------------------------------------------------
    def load_var(self):
        filename = get_dss() +  'fut/check/signal_rsiboll_'+self.type+'_var.csv'
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
        filename = get_dss() +  'fut/check/signal_rsiboll_'+self.type+'_var.csv'
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
               self.bollUp,self.bollDown,self.rsi_value,self.rsi_ma,self.atr_short,self.atr_mid, \
               self.intraTradeHigh, self.intraTradeLow, self.stop] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl',  \
                                      'bollUp','bollDown','rsi_value','rsi_ma','atr_short','atr_mid', \
                                      'intraTradeHigh','intraTradeLow','stop'])
        filename = get_dss() +  'fut/deal/signal_rsiboll_'+self.type+'_' + self.vtSymbol + '.csv'
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
               self.bollUp,self.bollDown,self.rsi_value,self.rsi_ma,self.atr_short,self.atr_mid, \
               self.intraTradeHigh, self.intraTradeLow, self.stop] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl',  \
                                      'bollUp','bollDown','rsi_value','rsi_ma','atr_short','atr_mid', \
                                      'intraTradeHigh','intraTradeLow','stop'])
        filename = get_dss() +  'fut/deal/signal_rsiboll_'+self.type+'_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

        self.result = None

########################################################################
class Fut_RsiBollPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'rsiboll'
        Portfolio.__init__(self, Fut_RsiBollSignal_Duo, engine, symbol_list, signal_param, Fut_RsiBollSignal_Kong, signal_param)
        #Portfolio.__init__(self, Fut_RsiBollSignal_Duo, engine, symbol_list, signal_param, None, None)
        #Portfolio.__init__(self, Fut_RsiBollSignal_Kong, engine, symbol_list, signal_param, None, None)



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

        self.engine._bc_sendOrder(signal.vtSymbol, direction, offset, price, volume*multiplier, self.name)

        # 记录成交数据
        trade = TradeData(self.result.date, signal.vtSymbol, direction, offset, price, volume*multiplier)
        # l = self.tradeDict.setdefault(self.result.date, [])
        # l.append(trade)

        self.result.updateTrade(trade)
