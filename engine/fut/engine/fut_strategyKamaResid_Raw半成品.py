# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss, get_contract
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import VtBarData, ArrayManager, Signal, Portfolio, TradeData, SignalResult



########################################################################
class Fut_KamaResid_RawSignal_Duo(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        self.type = 'duo'

        self.fixedSize = 1           # 每次交易的数量
        self.initBars = 50           # 初始化数据所用的天数
        self.minx = 'day'

        # 策略参数
        self.er_length = 20
        self.filter_ratio = 0.45
        self.atrWindow = 10
        self.slMultiplier = 0.5                  # CF

        # 策略临时变量
        self.stop = 0                            # 多头止损
        self.intraTradeHigh = 0
        self.intraTradeLow = 100E4

        self.can_buy = False
        self.can_sell = False

        # 需要持久化保存的变量
        self.cost = 0

        Signal.__init__(self, portfolio, vtSymbol)

        self.bm = ArrayManager(10)
        self.init_bm()

    #----------------------------------------------------------------------
    def init_bm(self):
        kama_arr = self.am.kama(self.er_length, array=True)
        resid_arr = self.am.close - kama_arr
        # print(kama_arr)
        # print(resid_arr)

        for i in [10,9,8,7,6,5,4,3,2,1]:
            resid = resid_arr[-i]
            bar = VtBarData()
            bar.open = resid
            bar.high = resid
            bar.low = resid
            bar.close = resid
            self.bm.updateBar(bar)

        # print(self.bm.close)
        # assert False

    #----------------------------------------------------------------------
    def get_resid_bar(self):
        kama = self.am.kama(self.er_length)
        close = self.bar.close
        resid = close - kama

        bar = VtBarData()
        bar.open = resid
        bar.high = resid
        bar.low = resid
        bar.close = resid
        bar.date = self.bar.date
        bar.time = self.bar.time

        return bar

    #----------------------------------------------------------------------
    def load_param(self):
        filename = get_dss() +  'fut/engine/kamaresid_raw/signal_kamaresid_raw_param.csv'
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

        self.resid_bar = self.get_resid_bar()
        self.bm.updateBar(self.resid_bar)


        kama = self.am.kama(self.er_length)

        boll_condition  = False

        self.can_buy = False
        if boll_condition:
            self.can_buy = True

        atrArray = self.am.atr(1, array=True)
        self.atrValue = atrArray[-self.atrWindow:].mean()

        self.can_sell = False
        self.intraTradeHigh = max(self.intraTradeHigh, self.bar.high)
        self.stop = self.intraTradeHigh - self.atrValue * self.slMultiplier
        if self.bar.close <= self.stop:
            self.can_sell = True

        r = [ [self.bar.date,self.bar.time,self.bar.close,self.can_buy,self.can_sell,self.bm.close[-1]] ]
        df = pd.DataFrame(r)
        filename = get_dss() +  'fut/engine/kamaresid_raw/bar_kamaresid_raw_'+self.type+ '_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

    #----------------------------------------------------------------------
    def generateSignal(self, bar):

        # 当前无仓位
        if self.unit == 0:
            if self.can_buy == True and self.paused == False:
                self.cost = bar.close
                self.buy(bar.close, self.fixedSize)

        # 持有多头仓位
        elif self.unit > 0:
            if self.can_sell == True:
                self.sell(bar.close, abs(self.unit))

    #----------------------------------------------------------------------
    def load_var(self):
        filename = get_dss() +  'fut/engine/kamaresid_raw/signal_kamaresid_raw_'+self.type+'_var_' + pz + '.csv'
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
        filename = get_dss() +  'fut/engine/kamaresid_raw/signal_kamaresid_raw_'+self.type+'_var_' + pz + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

########################################################################
class Fut_KamaResid_RawSignal_Kong(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        self.type = 'kong'

        self.fixedSize = 1           # 每次交易的数量
        self.initBars = 50           # 初始化数据所用的天数
        self.minx = 'day'

        # 策略参数
        self.er_length = 20
        self.filter_ratio = 0.45
        self.atrWindow = 10
        self.slMultiplier = 0.5                  # CF

        # 策略临时变量
        self.stop = 0                            # 多头止损
        self.intraTradeHigh = 0
        self.intraTradeLow = 100E4


        self.can_short = False
        self.can_cover = False

        # 需要持久化保存的变量
        self.cost = 0

        Signal.__init__(self, portfolio, vtSymbol)

    #----------------------------------------------------------------------
    def load_param(self):
        filename = get_dss() +  'fut/cfg/signal_kamaresid_raw_'+self.type+'_param.csv'
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

        kama_arr = self.am.kama(self.er_length, array=True)
        filter = self.filter_ratio * kama_arr[-self.er_length:].std(ddof=0)
        kama_condition  = True if kama_arr[-1] - kama_arr[-2] < -filter else False

        if self.bar.date == '2019-01-09':
            print(kama_arr)

        self.can_short = False
        if kama_condition:
            self.can_short = True

        atrArray = self.am.atr(1, array=True)
        atr_value = atrArray[-self.atrWindow:].mean()

        self.can_cover = False
        self.intraTradeLow = min(self.intraTradeLow, self.bar.low)
        self.stop = self.intraTradeLow + atr_value * self.slMultiplier
        if self.bar.close >= self.stop:
            self.can_cover = True

        r = [ [self.bar.date,self.bar.time,self.bar.close,self.can_short,self.can_cover,atr_value,self.intraTradeLow,self.stop,kama_arr[-2],kama_arr[-1],filter] ]
        df = pd.DataFrame(r)
        filename = get_dss() +  'fut/engine/kamaresid_raw/bar_kamaresid_raw_'+self.type+ '_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)


    #----------------------------------------------------------------------
    def generateSignal(self, bar):

        # 当前无仓位
        if self.unit == 0:
            if self.can_short == True and self.paused == False:
                self.cost = bar.close
                self.short(bar.close, self.fixedSize)

        # 持有多头仓位
        elif self.unit < 0:
            if self.can_cover == True:
                self.cover(bar.close, abs(self.unit))

    #----------------------------------------------------------------------
    def load_var(self):
        filename = get_dss() +  'fut/engine/kamaresid_raw/signal_kamaresid_raw_'+self.type+'_var_' + pz + '.csv'
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
        filename = get_dss() +  'fut/engine/kamaresid_raw/signal_kamaresid_raw_'+self.type+'_var_' + pz + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

########################################################################
class Fut_KamaResid_RawPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'kamaresid_raw'

        #Portfolio.__init__(self, Fut_KamaResid_RawSignal_Duo, engine, symbol_list, signal_param, Fut_KamaResid_RawSignal_Kong, signal_param)
        Portfolio.__init__(self, Fut_KamaResid_RawSignal_Duo, engine, symbol_list, signal_param, None, None)
        #Portfolio.__init__(self, Fut_KamaResid_RawSignal_Kong, engine, symbol_list, signal_param, None, None)
