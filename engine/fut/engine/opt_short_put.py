# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss, get_contract
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import ArrayManager, Signal, Portfolio, TradeData, SignalResult


########################################################################
class Opt_Short_PutSignal(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        self.type = 'duo'

        # 策略参数
        self.fixedSize = 1            # 每次交易的数量
        self.initBars = 240           # 初始化数据所用的天数
        self.minx = 'min30'

        # 策略临时变量
        self.can_short = False
        self.can_cover = False

        self.target = ''
        self.opt = True

        # 需要持久化保存的变量
        self.cost = 0

        size_am = 300
        assert self.initBars <= size_am
        Signal.__init__(self, portfolio, vtSymbol, size_am)

    #----------------------------------------------------------------------
    def load_param(self):
        filename = get_dss() +  'fut/engine/short_put/signal_short_put_param.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = df[ df.symbol == self.vtSymbol ]
            if len(df) > 0:
                rec = df.iloc[0,:]
                self.fixedSize = rec['fixed_size']
                self.opt = rec['opt']
                self.target = rec['target']

    #----------------------------------------------------------------------
    def set_param(self, param_dict):
        if 'fixedSize' in param_dict:
            self.fixedSize = param_dict['fixedSize']

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

        if self.paused == True:
            return

        self.calculateIndicator()     # 计算指标
        self.generateSignal(bar)      # 触发信号，产生交易指令

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""
        self.can_short = False
        self.can_cover = False

        if self.opt == False:
            ma_short_arr = self.am.sma(90, array=True)
            ma_mid_arr = self.am.sma(150, array=True)
            ma_long_arr = self.am.sma(240, array=True)

            # 90线穿240线，开仓
            if ma_short_arr[-2] <= ma_long_arr[-2] and ma_short_arr[-1] > ma_long_arr[-1]:
                self.portfolio.open_dict[self.target] = True

            # 价格穿90线，平仓
            if self.am.closeArray[-2] >= ma_short_arr[-2] and self.am.closeArray[-1] < ma_short_arr[-1]:
                self.portfolio.close_dict[self.target] = True

            r = [[self.bar.date,self.bar.time,self.bar.close,ma_short_arr[-1],ma_mid_arr[-1],ma_long_arr[-1]]]
            df = pd.DataFrame(r)
            filename = get_dss() +  'fut/engine/short_put/bar_short_put_'+self.type+ '_' + self.vtSymbol + '.csv'
            if os.path.exists(filename):
                df.to_csv(filename, index=False, mode='a', header=False)
            else:
                df.to_csv(filename, index=False)

        if self.opt == True:
            if self.unit == 0 and self.portfolio.open_dict[self.target] == True:
                self.short = True

            if self.unit > 0 and self.portfolio.close_dict[self.target] == True:
                self.cover = True

            self.portfolio.open_dict[self.target] = False
            self.portfolio.close_dict[self.target] = False


    #----------------------------------------------------------------------
    def generateSignal(self, bar):

        # 开空仓
        if self.can_short == True:
            self.short(bar.close, self.fixedSize)
            self.cost = bar.close

        # 平空仓
        if self.can_cover == True:
            self.cover(bar.close, self.fixedSize)
            self.cost = 0

    #----------------------------------------------------------------------
    def load_var(self):
        pz = str(get_contract(self.vtSymbol).pz)
        filename = get_dss() +  'fut/engine/short_put/signal_short_put_'+self.type+ '_var_' + pz + '.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
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
        if self.paused == True:
            return

        r = []
        if self.result is None:
            r = [ [self.portfolio.result.date,self.vtSymbol, self.unit, self.cost, \
                   0, 0, 0, 0, 0 ] ]
        else:
            r = [ [self.portfolio.result.date,self.vtSymbol, self.unit, self.cost, \
                   1, self.result.unit, self.result.entry, self.result.exit, self.result.pnl ] ]

        df = pd.DataFrame(r, columns=['datetime','vtSymbol','unit','cost', \
                                      'has_result','result_unit','result_entry','result_exit', 'result_pnl'])
        pz = str(get_contract(self.vtSymbol).pz)
        filename = get_dss() +  'fut/engine/short_put/signal_short_put_'+self.type+ '_var_' + pz + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

    #----------------------------------------------------------------------
    def open(self, price, change):
        self.unit += change

        if not self.result:
            self.result = SignalResult()
        self.result.open(price, change)

        r = [ [self.bar.date+' '+self.bar.time, '多' if change>0 else '空', '开',  \
               abs(change), price, 0, self.vtSymbol ] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl', 'symbol'])
        pz = str(get_contract(self.vtSymbol).pz)
        filename = get_dss() +  'fut/engine/short_put/signal_short_put_'+self.type+ '_deal_' + pz + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

    #----------------------------------------------------------------------
    def close(self, price):
        self.unit = 0
        self.result.close(price)

        r = [ [self.bar.date+' '+self.bar.time, '', '平',  \
               0, price, self.result.pnl, self.vtSymbol] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl','symbol'])
        pz = str(get_contract(self.vtSymbol).pz)
        filename = get_dss() +  'fut/engine/short_put/signal_short_put_'+self.type+ '_deal_' + pz + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

        self.result = None


########################################################################
class Opt_Short_PutPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'short_put'
        self.open_dict = {}
        self.close_dict = {}
        for symbol in symbol_list:
            self.open_dict[self.symbol] = False
            self.close_dict[self.symbol] = False

        Portfolio.__init__(self, Opt_Short_PutSignal, engine, symbol_list, signal_param)

        self.name_second = 'short_put_' + str(get_contract(symbol_list[0]).pz)
