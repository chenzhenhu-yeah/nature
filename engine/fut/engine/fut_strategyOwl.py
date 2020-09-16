# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss, get_contract, send_email
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import ArrayManager, Signal, Portfolio, TradeData, SignalResult

from nature import a_file, rc_file


########################################################################
class Fut_OwlSignal(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        self.type = 'mix'

        # 策略参数
        self.fixedSize = 1            # 每次交易的数量
        self.initBars = 10           # 初始化数据所用的天数
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
        r = []
        fn = get_dss() + 'fut/engine/owl/signal_owl_'+self.type+ '_var_' + self.vtSymbol + '.csv'
        # fn = get_dss() +  'fut/engine/owl/signal_owl_'+self.type+ '_var_' + self.vtSymbol + '.csv'
        self.ins_list += rc_file(fn)

        for row in self.ins_list:
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
                r.append(row)

            self.generateSignal(self.bar)      # 触发信号，产生交易指令

        self.ins_list = r

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
        fn = get_dss() + 'fut/engine/owl/signal_owl_'+self.type+ '_var_' + self.vtSymbol + '.csv'
        # fn = get_dss() +  'fut/engine/owl/signal_owl_'+self.type+ '_var_' + self.vtSymbol + '.csv'
        for ins in self.ins_list:
            a_file(fn, str(ins))

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
        Portfolio.__init__(self, Fut_OwlSignal, engine, symbol_list, signal_param)

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
