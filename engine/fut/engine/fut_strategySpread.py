# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict
import traceback
import json

from nature import to_log, get_dss, get_contract, get_trade_preday
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import VtBarData, ArrayManager, Signal, Portfolio, TradeData, SignalResult, DailyResult


########################################################################
class Fut_SpreadSignal(Signal):

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
        self.am.updateBar(bar)
        if not self.am.inited:
            return

        #print('here')

        self.calculateIndicator()     # 计算指标

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""

        self.portfolio.got_dict[self.vtSymbol] = True

        # 记录数据
        r = [[self.bar.date,self.bar.time,self.bar.close,self.bar.AskPrice,self.bar.BidPrice]]
        df = pd.DataFrame(r)

        filename = get_dss() +  'fut/engine/spread/bar_spread_'+self.type+ '_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

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
class Fut_SpreadPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'spread'

        self.tm = '00:00:00'
        self.got_dict = {}
        for symbol in symbol_list:
            self.got_dict[symbol] = False

        self.spread_dict = {}
        self.process_dict = {}
        fn = get_dss() +  'fut/engine/spread/portfolio_spread_param.csv'
        df = pd.read_csv(fn)
        for i, row in df.iterrows():
            self.spread_dict[row.nm] = [row.s0, row.s1]
            self.process_dict[row.nm] = False

        Portfolio.__init__(self, Fut_SpreadSignal, engine, symbol_list, signal_param)

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min1'):
        """引擎新推送过来bar，传递给每个signal"""

        # 不处理不相关的品种
        if bar.vtSymbol not in self.vtSymbolList:
            return

        if minx != 'min1':               # 本策略为min1
            return

        if self.tm != bar.time:
            self.tm = bar.time
            for symbol in self.vtSymbolList:
                self.got_dict[symbol] = False
            for k in self.process_dict.keys():
                self.process_dict[k] = False

        # 将bar推送给signal
        for signal in self.signalDict[bar.vtSymbol]:
            signal.onBar(bar, minx)

        """
        在此实现P层的业务控制逻辑
        """
        self.control_in_p(bar)

    #----------------------------------------------------------------------
    def control_in_p(self, bar):
        try:
            for k in self.spread_dict.keys():
                s0 = self.spread_dict[k][0]
                s1 = self.spread_dict[k][1]
                if s0 not in self.got_dict or s1 not in self.got_dict:
                    continue 
                if self.got_dict[s0] and self.got_dict[s1] and self.process_dict[k] == False:
                    self.process_dict[k] = True
                    s0 = self.signalDict[s0][0]
                    s1 = self.signalDict[s1][0]

                    bar_s = VtBarData()
                    bar_s.vtSymbol = k
                    bar_s.symbol = k
                    bar_s.exchange = s0.bar.exchange
                    bar_s.date = s0.bar.date
                    bar_s.time = s0.bar.time

                    bar_s.close = s0.bar.close - s1.bar.close
                    bar_s.AskPrice = s0.bar.AskPrice - s1.bar.BidPrice
                    bar_s.BidPrice = s0.bar.BidPrice - s1.bar.AskPrice

                    print(bar_s.time, bar_s.vtSymbol, bar_s.close, bar_s.AskPrice, bar_s.BidPrice)

                    self.engine.lock.acquire()
                    self.engine.bar_list.append(bar_s)
                    self.engine.lock.release()

        except Exception as e:
            s = traceback.format_exc()
            to_log(s)
