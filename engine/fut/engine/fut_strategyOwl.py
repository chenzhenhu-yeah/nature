# encoding: UTF-8

import os
import json
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss, get_contract, send_email
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import VtBarData, ArrayManager, Signal, Portfolio, TradeData, SignalResult, DailyResult
from nature import a_file, rc_file


########################################################################
class Fut_OwlSignal(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        self.type = 'mix'

        # 策略参数
        self.fixedSize = 1            # 每次交易的数量
        self.initBars = 0             # 初始化数据所用的天数
        self.minx = 'min5'

        # 策略临时变量
        self.can_buy = False
        self.can_sell = False
        self.can_short = False
        self.can_cover = False

        # 需要持久化保存的变量
        self.cost = 0
        self.ins_list = []

        Signal.__init__(self, portfolio, vtSymbol)

        print('come here : ' + vtSymbol)

    #----------------------------------------------------------------------
    def load_param(self):
        filename = get_dss() +  'fut/engine/owl/signal_owl_param.csv'
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
            print('成功设置策略参数 self.fixedSize: ',self.fixedSize)

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min1'):
        """新推送过来一个bar，进行处理"""
        self.bar = bar
        if minx == 'min5':
            self.on_bar_minx(bar)

    def on_bar_minx(self, bar):
        if self.paused == True:
            return

        self.am.updateBar(bar)
        if not self.am.inited:
            return

        self.calculateIndicator()     # 计算指标

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""
        # 记录数据
        r = [[self.bar.date,self.bar.time,self.bar.close]]
        df = pd.DataFrame(r)
        filename = get_dss() +  'fut/engine/owl/bar_owl_'+self.type+ '_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

        r = []
        fn = get_dss() + 'fut/engine/owl/signal_owl_'+self.type+ '_var_' + self.vtSymbol + '.csv'
        if os.path.exists(fn):
            df = pd.read_csv(fn)
            for i, row in df.iterrows():
                self.can_buy = False
                self.can_sell = False
                self.can_short = False
                self.can_cover = False

                ins = row['ins']
                price = float( row['price'] )
                self.fixedSize = int( row['num'] )
                # print(ins, price, self.fixedSize)

                if ins == 'up_buy' and self.bar.close >= price:
                    self.can_buy = True
                elif ins == 'down_buy' and self.bar.close <= price:
                    self.can_buy = True
                elif ins == 'up_sell' and self.bar.close >= price:
                    self.can_sell = True
                elif ins == 'down_sell' and self.bar.close <= price:
                    self.can_sell = True
                elif ins == 'up_short' and self.bar.close >= price:
                    self.can_short = True
                elif ins == 'down_short' and self.bar.close <= price:
                    self.can_short = True
                elif ins == 'up_cover' and self.bar.close >= price:
                    self.can_cover = True
                elif ins == 'down_cover' and self.bar.close <= price:
                    self.can_cover = True
                elif ins == 'up_warn' and self.bar.close >= price:
                    send_email(get_dss(), self.bar.vtSymbol+' up_warn: '+str(price), '')
                elif ins == 'down_warn' and self.bar.close <= price:
                    send_email(get_dss(), self.bar.vtSymbol+' down_warn: '+str(price), '')
                else:
                    r.append([row.ins, row.price, row.num])

                self.generateSignal(self.bar)      # 触发信号，产生交易指令

        fn = get_dss() + 'fut/engine/owl/history.csv'
        df = pd.read_csv(fn)
        df1 = df[(df.code == self.bar.vtSymbol) & (df.got == 'no')]
        if len(df1) > 0:
            for i, row in df1.iterrows():
                r.append([row.ins, row.price, row.num])
                df.at[i,'got'] = 'yes'
            df.to_csv(fn, index=False)

        fn = get_dss() + 'fut/engine/owl/signal_owl_'+self.type+ '_var_' + self.vtSymbol + '.csv'
        df = pd.DataFrame(r, columns=['ins','price','num'])
        df.to_csv(fn, index=False)

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
        pass

    #----------------------------------------------------------------------
    def save_var(self):
        pass

    #----------------------------------------------------------------------
    def open(self, price, change):
        pass

    #----------------------------------------------------------------------
    def close(self, price, change):
        pass


########################################################################
class Fut_OwlPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'owl'
        Portfolio.__init__(self, Fut_OwlSignal, engine, list(set(symbol_list)), signal_param)

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min5'):
        """引擎新推送过来bar，传递给每个signal"""

        # 不处理不相关的品种
        if bar.vtSymbol not in self.vtSymbolList:
            return

        if minx != 'min5':
            return

        # 动态加载新维护的symbol
        config = open(get_dss()+'fut/cfg/config.json')
        setting = json.load(config)
        symbols = setting['symbols_owl']
        symbols_list = symbols.split(',')

        for vtSymbol in set(symbols_list):
            if vtSymbol not in self.vtSymbolList:
                self.vtSymbolList.append(vtSymbol)
                self.posDict[vtSymbol] = 0
                signal1 = Fut_OwlSignal(self, vtSymbol)

                l = self.signalDict[vtSymbol]
                l.append(signal1)

        if self.result.date != bar.date + ' ' + bar.time:
            previousResult = self.result
            self.result = DailyResult(bar.date + ' ' + bar.time)
            self.resultList.append(self.result)
            if previousResult:
                self.result.updateClose(previousResult.closeDict)

        # 将bar推送给signal
        for signal in self.signalDict[bar.vtSymbol]:
            signal.onBar(bar, minx)

        self.result.updateBar(bar)
        self.result.updatePos(self.posDict)
