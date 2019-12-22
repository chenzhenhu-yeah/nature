# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss, get_contract
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import ArrayManager, Signal, Portfolio, TradeData, SignalResult

MAX_PRODUCT_POS = 4         # 单品种最大持仓
MAX_DIRECTION_POS = 10      # 单方向最大持仓


########################################################################
class Fut_TurtleSignal(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):

        # 策略参数
        self.entryWindow = 100           # 入场通道周期数
        self.exitWindow = 50            # 出场通道周期数
        self.atrWindow = 20              # 计算ATR周期数
        self.profitCheck = True         # 是否检查上一笔盈利

        self.initBars = 100              # 初始化数据所用的天数
        self.minx = 'min15'

        # 策略临时变量
        self.atrVolatility = 0          # ATR波动率
        self.entryUp = 0                # 入场通道
        self.entryDown = 0
        self.exitUp = 0                 # 出场通道
        self.exitDown = 0

        self.longEntry1 = 0             # 多头入场位
        self.longEntry2 = 0
        self.longEntry3 = 0
        self.longEntry4 = 0
        self.longStop = 0               # 多头止损位

        self.shortEntry1 = 0            # 空头入场位
        self.shortEntry2 = 0
        self.shortEntry3 = 0
        self.shortEntry4 = 0
        self.shortStop = 0              # 空头止损位

        self.result_list = []

        Signal.__init__(self, portfolio, vtSymbol)

    #----------------------------------------------------------------------
    def load_param(self):
        filename = get_dss() +  'fut/engine/turtle/signal_turtle_param.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = df[ df.pz == get_contract(self.vtSymbol).pz ]
            if len(df) > 0:
                rec = df.iloc[0,:]
                self.entryWindow = rec.entryWindow
                self.exitWindow = rec.exitWindow

    #----------------------------------------------------------------------
    def set_param(self, param_dict):
        if 'entryWindow' in param_dict:
            self.entryWindow = param_dict['entryWindow']
        if 'exitWindow' in param_dict:
            self.exitWindow = param_dict['exitWindow']

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min15'):
        """新推送过来一个bar，进行处理"""
        self.bar = bar
        if minx == 'min1':
            self.on_bar_min1(bar)

        if minx == 'min15':
            self.on_bar_minx(bar)

    #----------------------------------------------------------------------
    def on_bar_min1(self, bar):
        # self.generateSignal(bar)    # 在min1周期上，触发信号，产生交易指令
        pass

    #----------------------------------------------------------------------
    def on_bar_minx(self, bar):
        self.am.updateBar(bar)
        if not self.am.inited:
            return

        self.generateSignal(bar)      # 在minx周期上，触发信号，产生交易指令
        self.calculateIndicator()     # 在minx周期上，计算指标

    #----------------------------------------------------------------------
    def generateSignal(self, bar):
        """
        判断交易信号
        要注意在任何一个数据点：buy/sell/short/cover只允许执行一类动作
        """
        # 如果指标尚未初始化，则忽略
        if self.longEntry1 == 0:
            return

        # 优先检查平仓
        if self.unit > 0:
            longExit = max(self.longStop, self.exitDown)
            if bar.low <= longExit:
                self.sell( self.calculateTradePrice(DIRECTION_SHORT, bar.low), abs(self.unit) )
                return
        elif self.unit < 0:
            shortExit = min(self.shortStop, self.exitUp)
            if bar.high >= shortExit:
                self.cover( self.calculateTradePrice(DIRECTION_LONG, bar.high), abs(self.unit) )
                return

        # 没有仓位或者持有多头仓位的时候，可以做多（加仓）
        if self.unit >= 0:
            trade = False

            if bar.high >= self.longEntry1 and self.unit < 1:
                price = self.calculateTradePrice(DIRECTION_LONG, self.longEntry1)
                self.buy(price, 1)
                self.longStop = price - self.atrVolatility * 2
                trade = True

            if bar.high >= self.longEntry2 and self.unit < 2:
                price = self.calculateTradePrice(DIRECTION_LONG, self.longEntry2)
                self.buy(price, 1)
                self.longStop = price - self.atrVolatility * 2
                trade = True

            # if bar.high >= self.longEntry3 and self.unit < 3:
            #     price = self.calculateTradePrice(DIRECTION_LONG, self.longEntry3)
            #     self.buy(price, 1)
            #     self.longStop = price - self.atrVolatility * 2
            #     trade = True
            #
            # if bar.high >= self.longEntry4 and self.unit < 4:
            #     price = self.calculateTradePrice(DIRECTION_LONG, self.longEntry4)
            #     self.buy(price, 1)
            #     self.longStop = price - self.atrVolatility * 2
            #     trade = True

            if trade:
                return

        # 没有仓位或者持有空头仓位的时候，可以做空（加仓）
        if self.unit <= 0:
            if bar.low <= self.shortEntry1 and self.unit > -1:
                price = self.calculateTradePrice(DIRECTION_SHORT, self.shortEntry1)
                self.short( price, 1 )
                self.shortStop = price + self.atrVolatility * 2

            if bar.low <= self.shortEntry2 and self.unit > -2:
                price = self.calculateTradePrice(DIRECTION_SHORT, self.shortEntry2)
                self.short( price, 1 )
                self.shortStop = price + self.atrVolatility * 2

            # if bar.low <= self.shortEntry3 and self.unit > -3:
            #     price = self.calculateTradePrice(DIRECTION_SHORT, self.shortEntry3)
            #     self.short( price, 1 )
            #     self.shortStop = price + self.atrVolatility * 2
            #
            # if bar.low <= self.shortEntry4 and self.unit > -4:
            #     price = self.calculateTradePrice(DIRECTION_SHORT, self.shortEntry4)
            #     self.short( price, 1 )
            #     self.shortStop = price + self.atrVolatility * 2

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""
        self.entryUp, self.entryDown = self.am.donchian(self.entryWindow)
        self.exitUp, self.exitDown = self.am.donchian(self.exitWindow)

        # 有持仓后，ATR波动率和入场位等都不再变化
        if self.unit == 0:
            self.atrVolatility = self.am.atr(self.atrWindow)

            self.longEntry1 = self.entryUp
            self.longEntry2 = self.entryUp + self.atrVolatility * 10
            self.longEntry3 = self.entryUp + self.atrVolatility * 20
            self.longEntry4 = self.entryUp + self.atrVolatility * 30
            self.longStop = 0

            self.shortEntry1 = self.entryDown
            self.shortEntry2 = self.entryDown - self.atrVolatility * 10
            self.shortEntry3 = self.entryDown - self.atrVolatility * 20
            self.shortEntry4 = self.entryDown - self.atrVolatility * 30
            self.shortStop = 0

        r = [[self.bar.date,self.bar.time,self.bar.close,self.unit,self.entryUp,self.entryDown,self.atrVolatility, \
              self.longEntry1,self.longEntry2,self.longEntry3,self.longEntry4, self.longStop, \
              self.shortEntry1,self.shortEntry2,self.shortEntry3,self.shortEntry4, self.shortStop  ]]
        df = pd.DataFrame(r)
        filename = get_dss() +  'fut/engine/turtle/bar_turtle_' + self.vtSymbol + '.csv'
        df.to_csv(filename, index=False, mode='a', header=False)

    #----------------------------------------------------------------------
    def load_var(self):
        filename = get_dss() +  'fut/engine/turtle/signal_turtle_var.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = df[df.vtSymbol == self.vtSymbol]
            if len(df) > 0:
                rec = df.iloc[-1,:]            # 取最近日期的记录
                self.unit = rec.unit

                self.atrVolatility = rec.atrVolatility
                self.entryUp = rec.entryUp
                self.entryDown = rec.entryDown
                self.exitUp = rec.exitUp
                self.exitDown = rec.exitDown
                self.longEntry1 = rec.longEntry1
                self.longEntry2 = rec.longEntry2
                self.longEntry3 = rec.longEntry3
                self.longEntry4 = rec.longEntry4
                self.longStop = rec.longStop
                self.shortEntry1 = rec.shortEntry1
                self.shortEntry2 = rec.shortEntry2
                self.shortEntry3 = rec.shortEntry3
                self.shortEntry4 = rec.shortEntry4
                self.shortStop = rec.shortStop

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
            r = [ [self.portfolio.result.date, self.vtSymbol, self.unit, \
                   0, 0, 0, 0, 0, \
                   self.atrVolatility,self.entryUp,self.entryDown,self.exitUp,self.exitDown, \
                   self.longEntry1,self.longEntry2,self.longEntry3,self.longEntry4,self.longStop, \
                   self.shortEntry1,self.shortEntry2,self.shortEntry3,self.shortEntry4,self.shortStop
                  ]
                ]
        else:
            r = [ [self.portfolio.result.date, self.vtSymbol, self.unit, \
                   1, self.result.unit, self.result.entry, self.result.exit, self.result.pnl, \
                   self.atrVolatility,self.entryUp,self.entryDown,self.exitUp,self.exitDown, \
                   self.longEntry1,self.longEntry2,self.longEntry3,self.longEntry4,self.longStop, \
                   self.shortEntry1,self.shortEntry2,self.shortEntry3,self.shortEntry4,self.shortStop
                  ]
                ]

        df = pd.DataFrame(r, columns=['datetime','vtSymbol','unit', \
                                      'has_result','result_unit','result_entry','result_exit', 'result_pnl', \
                                      'atrVolatility','entryUp','entryDown','exitUp','exitDown', \
                                      'longEntry1','longEntry2','longEntry3','longEntry4','longStop', \
                                      'shortEntry1','shortEntry2','shortEntry3','shortEntry4','shortStop'
                                     ])
        filename = get_dss() +  'fut/engine/turtle/signal_turtle_var.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

    #----------------------------------------------------------------------
    def open(self, price, change):
        """开仓"""
        self.unit += change

        if not self.result:
            self.result = SignalResult()
        self.result.open(price, change)

        r = [ [self.bar.date+' '+self.bar.time, '多' if change>0 else '空', '开', abs(change), price, 0] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl'])
        filename = get_dss() +  'fut/engine/turtle/signal_deal_' + self.portfolio.name + '_' + self.vtSymbol + '.csv'
        df.to_csv(filename, index=False, mode='a', header=False)

    #----------------------------------------------------------------------
    def close(self, price):
        """平仓"""
        self.unit = 0
        self.result.close(price)

        r = [ [self.bar.date+' '+self.bar.time, '', '平', 0, price, self.result.pnl] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl'])
        filename = get_dss() +  'fut/engine/turtle/signal_deal_' + self.portfolio.name + '_' + self.vtSymbol + '.csv'
        df.to_csv(filename, index=False, mode='a', header=False)

        self.result_list.append(self.result)
        self.result = None

    #----------------------------------------------------------------------
    def getLastPnl(self):
        """获取上一笔交易的盈亏"""
        if not self.result_list:
            return 0

        result = self.result_list[-1]
        return result.pnl

    #----------------------------------------------------------------------
    def calculateTradePrice(self, direction, price):
        """计算成交价格"""
        # 买入时，停止单成交的最优价格不能低于当前K线开盘价
        if direction == DIRECTION_LONG:
            tradePrice = max(self.bar.open, price)
        # 卖出时，停止单成交的最优价格不能高于当前K线开盘价
        else:
            tradePrice = min(self.bar.open, price)

        return tradePrice


########################################################################
class Fut_TurtlePortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'turtle'
        self.multiplierDict = {}             # 按照波动幅度计算的委托量单位字典
        self.totalLong = 0          # 总的多头持仓
        self.totalShort = 0         # 总的空头持仓

        Portfolio.__init__(self, Fut_TurtleSignal, engine, symbol_list, signal_param)

    #----------------------------------------------------------------------
    def _bc_newSignal(self, signal, direction, offset, price, volume):
        """
        对交易信号进行过滤，符合条件的才发单执行。
        计算真实交易价格和数量。
        """
        pos = self.posDict.get(signal.vtSymbol, 0)

        # 如果当前无仓位，则重新根据波动幅度计算委托量单位
        # if pos == 0:
        #     size = get_contract(signal.vtSymbol).size
        #     riskValue = self.portfolioValue * 0.01
        #     multiplier = riskValue / (signal.atrVolatility * size)
        #     multiplier = int(round(multiplier, 0))
        #     self.multiplierDict[signal.vtSymbol] = multiplier
        # else:
        #     multiplier = self.multiplierDict[signal.vtSymbol]

        # 先简化
        multiplier = 1

        # 开仓
        if offset == OFFSET_OPEN:
            # 检查上一次是否为盈利
            if signal.profitCheck:
                pnl = signal.getLastPnl()
                if pnl > 0:
                    return

            # 买入
            if direction == DIRECTION_LONG:
                # 组合持仓不能超过上限
                if self.totalLong > MAX_DIRECTION_POS:
                    return

                # 单品种持仓不能超过上限
                if signal.unit > MAX_PRODUCT_POS:
                    return
            # 卖出
            else:
                if self.totalShort < -MAX_DIRECTION_POS:
                    return

                if signal.unit < -MAX_PRODUCT_POS:
                    return
        # # 平仓
        # else:
        #     if direction == DIRECTION_LONG:
        #         # 必须有空头持仓
        #         if signal.unit >= 0:
        #             return
        #
        #         # 平仓数量不能超过空头持仓
        #         volume = min(volume, abs(signal.unit))
        #     else:
        #         if signal.unit <= 0:
        #             return
        #
        #         volume = min(volume, abs(signal.unit))

        # 更新总持仓
        if direction == DIRECTION_LONG and offset == OFFSET_OPEN:
            self.totalLong += 1                                      #多开
        if direction == DIRECTION_SHORT and offset != OFFSET_OPEN:
            self.totalLong -= 1                                      #空平
        if direction == DIRECTION_SHORT and offset == OFFSET_OPEN:
            self.totalShort += 1                                     #空开
        if direction == DIRECTION_LONG and offset != OFFSET_OPEN:
            self.totalShort -= 1                                     #多平

        # 更新合约持仓
        if direction == DIRECTION_LONG:
            self.posDict[signal.vtSymbol] += volume * multiplier
        else:
            self.posDict[signal.vtSymbol] -= volume * multiplier

        # 对价格四舍五入
        priceTick = get_contract(signal.vtSymbol).price_tick
        price = int(round(price/priceTick, 0)) * priceTick

        self.engine._bc_sendOrder(signal.vtSymbol, direction, offset, price, volume*multiplier, self.name)

        # 记录成交数据
        trade = TradeData(self.result.date, signal.vtSymbol, direction, offset, price, volume*multiplier)
        # l = self.tradeDict.setdefault(self.result.date, [])
        # l.append(trade)

        self.result.updateTrade(trade)
