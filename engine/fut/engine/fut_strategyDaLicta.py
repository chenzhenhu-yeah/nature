# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss, get_contract
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import ArrayManager, Signal, Portfolio, TradeData, SignalResult


########################################################################
class Fut_DaLictaSignal(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        self.type = 'one'

        # 策略参数
        self.fixedSize = 6            # 每次交易的数量
        self.initBars = 150           # 初始化数据所用的天数
        self.minx = 'min30'

        # 策略临时变量
        self.can_buy = False
        self.can_sell = False
        self.can_short = False
        self.can_cover = False

        self.ma_short_arr = None
        self.ma_mid_arr = None
        self.ma_long = None

        # 需要持久化保存的变量
        self.cost = 0

        Signal.__init__(self, portfolio, vtSymbol)

    #----------------------------------------------------------------------
    def load_param(self):
        filename = get_dss() +  'fut/engine/dalicta/signal_dalicta_param.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = df[ df.symbol == self.vtSymbol ]
            if len(df) > 0:
                rec = df.iloc[0,:]
                print('成功加载策略参数' )

    #----------------------------------------------------------------------
    def set_param(self, param_dict):
        if 'fixedSize' in param_dict:
            self.fixedSize = param_dict['fixedSize']
            if self.fixedSize > 1:
                self.type = 'multi'
            print('成功设置策略参数 self.fixedSize: ',self.fixedSize)

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min1'):
        """新推送过来一个bar，进行处理"""
        self.bar = bar
        if minx == 'min30':
            self.on_bar_minx(bar)

    def on_bar_minx(self, bar):
        self.am.updateBar(bar)
        if not self.am.inited:
            return

        self.calculateIndicator()     # 计算指标
        self.generateSignal(bar)      # 触发信号，产生交易指令

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""
        self.can_buy = False
        self.can_sell = False
        self.can_short = False
        self.can_cover = False

        self.ma_short_arr = self.am.sma(8, array=True)
        self.ma_mid_arr = self.am.sma(21, array=True)
        self.ma_long = self.am.sma(120)

        if self.unit == 0:
            if self.bar.close >= self.ma_long and self.paused == False:
                if self.ma_short_arr[-1] > self.ma_mid_arr[-1] and self.ma_short_arr[-2] <= self.ma_mid_arr[-2] :
                #if self.ma_short_arr[-1] > self.ma_mid_arr[-1]:
                    self.can_buy = True

            if self.bar.close <= self.ma_long and self.paused == False:
                if self.ma_short_arr[-1] < self.ma_mid_arr[-1] and self.ma_short_arr[-2] >= self.ma_mid_arr[-2] :
                #if self.ma_short_arr[-1] < self.ma_mid_arr[-1]:
                    self.can_short = True

        elif self.bar.close < self.ma_long and self.unit > 0:
            self.can_sell = True

        elif self.bar.close > self.ma_long and self.unit < 0:
            self.can_cover = True

        r = [[self.bar.date,self.bar.time,self.bar.close,self.can_buy,self.can_short,self.can_sell,self.can_cover,self.ma_short_arr[-1],self.ma_mid_arr[-1],self.ma_long]]
        df = pd.DataFrame(r)
        filename = get_dss() +  'fut/engine/dalicta/bar_dalicta_'+self.type+ '_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

    #----------------------------------------------------------------------
    def generateSignal(self, bar):
        # 开多仓
        if self.can_buy == True:
            self.buy(bar.close, self.fixedSize)
            self.cost = bar.close

        # 开空仓
        if self.can_short == True:
            self.short(bar.close, self.fixedSize)
            self.cost = bar.close

        # 平多仓
        if self.can_sell == True:
            self.sell(bar.close, self.fixedSize)
            self.cost = 0

        # 平空仓
        if self.can_cover == True:
            self.cover(bar.close, self.fixedSize)
            self.cost = 0

    #----------------------------------------------------------------------
    def load_var(self):
        filename = get_dss() +  'fut/engine/dalicta/signal_dalicta_'+self.type+ '_var_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename, sep='$')
            df = df[df.vtSymbol == self.vtSymbol]
            df = df.sort_values(by='datetime')
            df = df.reset_index()
            if len(df) > 0:
                rec = df.iloc[-1,:]            # 取最近日期的记录
                self.unit = rec.unit
                self.cost = rec.cost
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
                   0, 0, 0, 0, 0 ] ]
        else:
            r = [ [self.portfolio.result.date,self.vtSymbol, self.unit, self.cost, \
                   1, self.result.unit, self.result.entry, self.result.exit, self.result.pnl ] ]

        df = pd.DataFrame(r, columns=['datetime','vtSymbol','unit','cost', \
                                      'has_result','result_unit','result_entry','result_exit', 'result_pnl'])
        filename = get_dss() +  'fut/engine/dalicta/signal_dalicta_'+self.type+ '_var_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, sep='$', mode='a', header=False)
        else:
            df.to_csv(filename, index=False, sep='$')

    #----------------------------------------------------------------------
    def open(self, price, change):
        self.unit += change

        if not self.result:
            self.result = SignalResult()
        self.result.open(price, change)

        r = [ [self.bar.date+' '+self.bar.time, '多' if change>0 else '空', '开',  \
               abs(change), price, 0 ] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl' ])
        filename = get_dss() +  'fut/engine/dalicta/signal_dalicta_'+self.type+ '_deal_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

    #----------------------------------------------------------------------
    def close(self, price):
        self.unit = 0
        self.result.close(price)

        r = [ [self.bar.date+' '+self.bar.time, '', '平',  \
               0, price, self.result.pnl ] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl'])
        filename = get_dss() +  'fut/engine/dalicta/signal_dalicta_'+self.type+ '_deal_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

        self.result = None

########################################################################
class Fut_DaLictaPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'dalicta'

        s_param = {}
        for symbol in symbol_list:
            s_param[symbol] = {'fixedSize':6}
        Portfolio.__init__(self, Fut_DaLictaSignal, engine, symbol_list, s_param)
        #Portfolio.__init__(self, Fut_DaLictaSignal, engine, symbol_list, {}, Fut_DaLictaSignal, s_param)

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
        #print('here')
