# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss, get_contract
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import ArrayManager, Signal, Portfolio, TradeData, SignalResult


########################################################################
class Fut_AtrRsiSignal(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):

        # 策略参数
        self.atrLength = 1           # 计算ATR指标的窗口数
        self.atrMaLength = 14       # 计算ATR均线的窗口数
        self.rsiLength = 5           # 计算RSI的窗口数
        self.rsiEntry = 16           # RSI的开仓信号
        self.trailingPercent = 0.7   # 百分比移动止损
        self.victoryPercent = 0.3
        self.fixedSize = 1           # 每次交易的数量
        self.ratio_atrMa = 0.8

        self.initBars = 60           # 初始化数据所用的天数
        self.minx = 'min5'
        # 初始化RSI入场阈值
        self.rsiBuy = 50 + self.rsiEntry
        self.rsiSell = 50 - self.rsiEntry

        # 策略临时变量
        self.atrValue = 0                        # 最新的ATR指标数值
        self.atrMa = 0                           # ATR移动平均的数值
        self.rsiValue = 0                        # RSI指标的数值
        self.iswave = True

        self.can_buy = False
        self.can_short = False

        # 需要持久化保存的变量
        self.cost = 0
        self.intraTradeHigh = 0                  # 移动止损用的持仓期内最高价
        self.intraTradeLow = 0                   # 持仓期内的最低点
        self.stop = 0                            # 多头止损

        self.serialize_list = [self.atrValue,self.atrMa,self.rsiValue,self.iswave,self.intraTradeHigh,self.intraTradeLow,self.stop]

        Signal.__init__(self, portfolio, vtSymbol)

    #----------------------------------------------------------------------
    def load_param(self):
        filename = get_dss() +  'fut/cfg/signal_atrrsi_param.csv'
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
        # filename = get_dss() +  'fut/engine/atrrsi/bar_' + self.vtSymbol + '.csv'
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
        atrArray = self.am.atr(self.atrLength, array=True)
        # print(len(atrArray))
        self.atrValue = atrArray[-1]
        self.atrMa = atrArray[-self.atrMaLength:].mean()
        atrMa10 = atrArray[-10:].mean()
        atrMa50 = atrArray[-50:].mean()
        #self.iswave = False if atrMa10 < self.ratio_atrMa * atrMa50  else True
        self.iswave = False if self.atrMa < self.ratio_atrMa * atrMa50  else True

        self.rsiValue = self.am.rsi(self.rsiLength)
        rsiArray20 = self.am.rsi(20, array=True)
        self.can_buy = False
        if rsiArray20[-1]>50 and rsiArray20[-2]>50 and rsiArray20[-3]>50:
            self.can_buy = True
        self.can_short = False
        if rsiArray20[-1]<50 and rsiArray20[-2]<50 and rsiArray20[-3]<50:
            self.can_short = True


    #----------------------------------------------------------------------
    def generateSignal(self, bar):

        # 当前无仓位
        if self.unit == 0:
            self.intraTradeHigh = bar.high
            self.intraTradeLow = bar.low

            # ATR数值上穿其移动平均线，说明行情短期内波动加大
            # 即处于趋势的概率较大，适合CTA开仓
            if self.atrValue > self.atrMa and self.iswave == False and self.paused == False:
                # 使用RSI指标的趋势行情时，会在超买超卖区钝化特征，作为开仓信号
                #if self.rsiValue > self.rsiBuy and self.can_buy == True:
                if self.rsiValue > self.rsiBuy:
                    # 这里为了保证成交，选择超价5个整指数点下单
                    self.cost = bar.close
                    self.stop = 0
                    self.intraTradeHigh = bar.close

                    self.buy(bar.close, self.fixedSize)

                #elif self.rsiValue < self.rsiSell and self.can_short == True:
                elif self.rsiValue < self.rsiSell:
                    self.cost = bar.close
                    self.stop = 100E4
                    self.intraTradeLow = bar.close

                    self.short(bar.close, self.fixedSize)

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
        filename = get_dss() +  'fut/engine/atrrsi/signal_atrrsi_var.csv'
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
        filename = get_dss() +  'fut/engine/atrrsi/signal_atrrsi_var.csv'
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
               self.atrValue, self.atrMa, self.rsiValue, \
               self.iswave, self.intraTradeHigh, self.intraTradeLow, \
               self.stop] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl',  \
                                      'atrValue', 'atrMa', 'rsiValue', 'iswave', \
                                      'intraTradeHigh','intraTradeLow','stop'])
        filename = get_dss() +  'fut/engine/atrrsi/signal_atrrsi_' + self.vtSymbol + '.csv'
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
               self.atrValue, self.atrMa, self.rsiValue, \
               self.iswave, self.intraTradeHigh, self.intraTradeLow, \
               self.stop] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl',  \
                                      'atrValue', 'atrMa', 'rsiValue', 'iswave', \
                                      'intraTradeHigh','intraTradeLow','stop'])
        filename = get_dss() +  'fut/engine/atrrsi/signal_atrrsi_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

        self.result = None


########################################################################
class Fut_AtrRsiPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'atrrsi'
        Portfolio.__init__(self, Fut_AtrRsiSignal, engine, symbol_list, signal_param)

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
