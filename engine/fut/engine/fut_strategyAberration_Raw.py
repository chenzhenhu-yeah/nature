# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss, get_contract
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import ArrayManager, Signal, Portfolio, TradeData, SignalResult



########################################################################
class Fut_Aberration_RawSignal_Duo(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        self.type = 'duo'

        # 策略参数
        self.bollWindow = 10                     # 布林通道窗口数
        self.bollDev = 2.4                     # 布林通道的偏差

        self.fixedSize = 1           # 每次交易的数量
        self.initBars = 20           # 初始化数据所用的天数
        self.minx = 'day'

        # 策略临时变量
        self.bollUp = 0                          # 布林通道上轨
        self.bollDown = 0                        # 布林通道下轨
        self.stop = 0                            # 多头止损

        # 需要持久化保存的变量

        Signal.__init__(self, portfolio, vtSymbol)

    #----------------------------------------------------------------------
    def load_param(self):
        filename = get_dss() +  'fut/engine/aberration_raw/signal_aberration_raw_param.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = df[ df.pz == get_contract(self.vtSymbol).pz ]
            if len(df) > 0:
                rec = df.iloc[0,:]
                self.bollWindow = rec.bollWindow
                self.bollDev = rec.bollDev
                print('成功加载策略参数', self.bollWindow, self.bollDev)

    #----------------------------------------------------------------------
    def set_param(self, param_dict):
        if 'bollWindow' in param_dict:
            self.bollWindow = param_dict['bollWindow']
            print('成功设置策略参数 self.bollWindow: ',self.bollWindow)
        if 'bollDev' in param_dict:
            self.bollDev = param_dict['bollDev']
            print('成功设置策略参数 self.bollDev: ',self.bollDev)

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

    def on_bar_minx(self, bar):
        self.am.updateBar(bar)
        if not self.am.inited:
            return

        #print('here')
        self.calculateIndicator()     # 计算指标
        self.generateSignal(bar)      # 触发信号，产生交易指令

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""
        self.bollUp, self.bollDown = self.am.boll(self.bollWindow, self.bollDev)
        self.stop = (self.bollUp + self.bollDown)/2

    #----------------------------------------------------------------------
    def generateSignal(self, bar):

        # 当前无仓位
        if self.unit == 0:
            if bar.close > self.bollUp:
                self.buy(bar.close, self.fixedSize)

        # 持有多头仓位
        elif self.unit > 0:
            if bar.close < self.stop:
                self.sell(bar.close, abs(self.unit))

    #----------------------------------------------------------------------
    def load_var(self):
        filename = get_dss() +  'fut/engine/aberration_raw/signal_aberration_raw_'+self.type+'_var_' + pz + '.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = df[df.vtSymbol == self.vtSymbol]
            df = df.sort_values(by='datetime')
            df = df.reset_index()
            if len(df) > 0:
                rec = df.iloc[-1,:]            # 取最近日期的记录
                self.unit = rec.unit
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
            r = [ [self.portfolio.result.date,self.vtSymbol, self.unit,  \
                   0, 0, 0, 0, 0 ] ]
        else:
            r = [ [self.portfolio.result.date,self.vtSymbol, self.unit,  \
                   1, self.result.unit, self.result.entry, self.result.exit, self.result.pnl ] ]
        df = pd.DataFrame(r, columns=['datetime','vtSymbol','unit', \
                                      'has_result','result_unit','result_entry','result_exit', 'result_pnl'])
        pz = str(get_contract(self.vtSymbol).pz)
        filename = get_dss() +  'fut/engine/aberration_raw/signal_aberration_raw_'+self.type+'_var_' + pz + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

########################################################################
class Fut_Aberration_RawSignal_Kong(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        self.type = 'kong'

        # 策略参数
        self.bollWindow = 10                     # 布林通道窗口数
        self.bollDev = 2.4                     # 布林通道的偏差

        self.fixedSize = 1           # 每次交易的数量
        self.initBars = 20           # 初始化数据所用的天数
        self.minx = 'day'

        # 策略临时变量
        self.bollUp = 0                          # 布林通道上轨
        self.bollDown = 0                        # 布林通道下轨
        self.stop = 0                            # 多头止损

        # 需要持久化保存的变量

        Signal.__init__(self, portfolio, vtSymbol)

    #----------------------------------------------------------------------
    def load_param(self):
        filename = get_dss() +  'fut/cfg/signal_aberration_raw_'+self.type+'_param.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = df[ df.pz == get_contract(self.vtSymbol).pz ]
            if len(df) > 0:
                rec = df.iloc[0,:]
                self.bollWindow = rec.bollWindow
                self.bollDev = rec.bollDev
                print('成功加载策略参数', self.bollWindow, self.bollDev)

    #----------------------------------------------------------------------
    def set_param(self, param_dict):
        if 'bollWindow' in param_dict:
            self.bollWindow = param_dict['bollWindow']
            print('成功设置策略参数 self.bollWindow: ',self.bollWindow)
        if 'bollDev' in param_dict:
            self.bollDev = param_dict['bollDev']
            print('成功设置策略参数 self.bollDev: ',self.bollDev)

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

    def on_bar_minx(self, bar):
        self.am.updateBar(bar)
        if not self.am.inited:
            return

        #print('here')
        self.calculateIndicator()     # 计算指标
        self.generateSignal(bar)      # 触发信号，产生交易指令

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""
        self.bollUp, self.bollDown = self.am.boll(self.bollWindow, self.bollDev)
        self.stop = (self.bollUp + self.bollDown)/2

    #----------------------------------------------------------------------
    def generateSignal(self, bar):
        # 当前无仓位
        if self.unit == 0:
            if  bar.close < self.bollDown:
                self.short(bar.close, self.fixedSize)

        # 持有空头仓位
        elif self.unit < 0:
            if bar.close > self.stop:
                self.cover(bar.close, abs(self.unit))


    #----------------------------------------------------------------------
    def load_var(self):
        filename = get_dss() +  'fut/engine/aberration_raw/signal_aberration_raw_'+self.type+'_var_' + pz + '.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = df[df.vtSymbol == self.vtSymbol]
            df = df.sort_values(by='datetime')
            df = df.reset_index()
            if len(df) > 0:
                rec = df.iloc[-1,:]            # 取最近日期的记录
                self.unit = rec.unit
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
            r = [ [self.portfolio.result.date,self.vtSymbol, self.unit,  \
                   0, 0, 0, 0, 0 ] ]
        else:
            r = [ [self.portfolio.result.date,self.vtSymbol, self.unit,  \
                   1, self.result.unit, self.result.entry, self.result.exit, self.result.pnl ] ]
        df = pd.DataFrame(r, columns=['datetime','vtSymbol','unit', \
                                      'has_result','result_unit','result_entry','result_exit', 'result_pnl'])
        pz = str(get_contract(self.vtSymbol).pz)
        filename = get_dss() +  'fut/engine/aberration_raw/signal_aberration_raw_'+self.type+'_var_' + pz + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

########################################################################
class Fut_Aberration_RawPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'aberration_raw'

        Portfolio.__init__(self, Fut_Aberration_RawSignal_Duo, engine, symbol_list, signal_param, Fut_Aberration_RawSignal_Kong, signal_param)
        #Portfolio.__init__(self, Fut_Aberration_RawSignal_Duo, engine, symbol_list, signal_param, None, None)
        #Portfolio.__init__(self, Fut_Aberration_RawSignal_Kong, engine, symbol_list, signal_param, None, None)
