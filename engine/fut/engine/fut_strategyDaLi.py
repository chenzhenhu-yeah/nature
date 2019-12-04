# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss, get_contract
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import ArrayManager, Signal, Portfolio, TradeData, SignalResult


########################################################################
class Fut_DaLiSignal(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):

        # 策略参数
        self.fixedSize = 1           # 每次交易的数量
        self.initBars = 100           # 初始化数据所用的天数
        self.minx = 'min5'


        self.atrValue = 0
        self.atrWindow = 20

        self.gap = 30
        self.price_duo_list =  []
        self.price_kong_list = []

        # 策略临时变量
        self.can_buy = False
        self.can_short = False
        self.pnl = 0

        # 需要持久化保存的变量

        Signal.__init__(self, portfolio, vtSymbol)
    #----------------------------------------------------------------------
    def load_param(self):
        filename = get_dss() +  'fut/dali/signal_dali_param.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = df[ df.pz == get_contract(self.vtSymbol).pz ]
            if len(df) > 0:
                rec = df.iloc[0,:]
                self.gap = rec.gap
                print('成功加载策略参数', self.gap)

    #----------------------------------------------------------------------
    def set_param(self, param_dict):
        if 'gap' in param_dict:
            self.gap = param_dict['gap']
            print('成功设置策略参数 self.gap: ',self.gap)

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min1'):
        """新推送过来一个bar，进行处理"""
        self.bar = bar
        if minx == 'min5':
            self.on_bar_minx(bar)

        # r = [[minx,bar.date,bar.time,bar.open,bar.close]]
        # df = pd.DataFrame(r)
        # filename = get_dss() +  'fut/dali/bar_' + self.vtSymbol + '.csv'
        # df.to_csv(filename, index=False, mode='a', header=False)


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
        self.can_buy = False
        self.can_short = False

        if self.bar.close < 2600 or self.bar.close > 3100:
            return

        self.atrValue = self.am.atr(self.atrWindow)
        #print(self.atrValue)
        self.gap = max(12*self.atrValue, 15)

        if self.bar.close <= self.get_price_kong() - self.get_gap_minus() :
        #if self.bar.close <= self.get_price_kong() - self.gap:
            self.can_buy = True
            self.pnl = self.get_price_kong() - self.bar.close

        if self.bar.close >= self.get_price_duo() + self.get_gap_plus() :
        #if self.bar.close >= self.get_price_duo() + self.gap:
            self.can_short = True
            self.pnl = self.get_price_duo() - self.bar.close

    #----------------------------------------------------------------------
    def generateSignal(self, bar):
        if len(self.price_duo_list) == 0 or len(self.price_kong_list) == 0 :
            self.buy(bar.close, self.fixedSize)
            self.unit_buy(bar.close)
            self.short(bar.close, self.fixedSize)
            self.unit_short(bar.close)

        # 平空仓、开多仓
        if self.can_buy == True:
            if len(self.price_kong_list) == 1:
                self.buy(bar.close, 2*self.fixedSize)
                #self.buy(bar.close, self.fixedSize)
                self.unit_buy(bar.close)
                self.unit_buy(bar.close)

                self.short(bar.close, self.fixedSize)
                self.cover(bar.close, self.fixedSize)
                self.unit_cover()
                self.unit_short(bar.close)
            else:
                self.cover(bar.close, self.fixedSize)
                self.unit_cover()
                self.buy(bar.close, self.fixedSize)
                self.unit_buy(bar.close)

            if len(self.price_duo_list) >= 15 or len(self.price_kong_list) >= 15:
                print( len(self.price_duo_list), len(self.price_kong_list) )

        # 平多仓、开空仓
        if self.can_short == True:
            if len(self.price_duo_list) == 1:
                self.short(bar.close, 2*self.fixedSize)
                #self.short(bar.close, self.fixedSize)
                self.unit_short(bar.close)
                self.unit_short(bar.close)

                self.buy(bar.close, self.fixedSize)
                self.sell(bar.close, self.fixedSize)
                self.unit_sell()
                self.unit_buy(bar.close)
            else:
                self.sell(bar.close, self.fixedSize)
                self.unit_sell()
                self.short(bar.close, self.fixedSize)
                self.unit_short(bar.close)

            if len(self.price_duo_list) >= 15 or len(self.price_kong_list) >= 15:
                print( len(self.price_duo_list), len(self.price_kong_list) )

    #----------------------------------------------------------------------
    def get_gap_plus(self):
        # 当为上涨趋势时，空头持仓增加，要控制。
        cc = len(self.price_duo_list) - len(self.price_kong_list)
        if  cc >= 11:
            self.gap -= 20
        elif cc >= 9:
            self.gap -= 10
        elif cc >= 7:
            self.gap -= 5

        return max(self.gap, 20)
    #----------------------------------------------------------------------
    def get_gap_minus(self):
        cc = len(self.price_kong_list) - len(self.price_duo_list)
        if  cc >= 11:
            self.gap -= 20
        elif cc >= 9:
            self.gap -= 10
        elif cc >= 7:
            self.gap -= 5

        return max(self.gap, 20)

    #----------------------------------------------------------------------
    def get_price_duo(self):
        if len(self.price_duo_list) == 0:
            return 100E4

        self.price_duo_list = sorted(self.price_duo_list)
        return self.price_duo_list[0]

    #----------------------------------------------------------------------
    def get_price_kong(self):
        if len(self.price_kong_list) == 0:
            return 0

        self.price_kong_list = sorted(self.price_kong_list)
        return self.price_kong_list[-1]

    #----------------------------------------------------------------------
    def unit_buy(self, price):
        self.price_duo_list.append(price)

    #----------------------------------------------------------------------
    def unit_sell(self):
        self.price_duo_list = sorted(self.price_duo_list)
        self.price_duo_list.pop(0)

    #----------------------------------------------------------------------
    def unit_short(self, price):
        self.price_kong_list.append(price)

    #----------------------------------------------------------------------
    def unit_cover(self):
        self.price_kong_list = sorted(self.price_kong_list)
        self.price_kong_list.pop(-1)

    #----------------------------------------------------------------------
    def load_var(self):
        filename = get_dss() +  'fut/dali/signal_dali_var.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename, sep='$')
            df = df[df.vtSymbol == self.vtSymbol]
            df = df.sort_values(by='datetime')
            df = df.reset_index()
            if len(df) > 0:
                rec = df.iloc[-1,:]            # 取最近日期的记录
                self.unit = rec.unit
                self.price_duo_list = eval( rec.price_duo_list )
                self.price_kong_list = eval( rec.price_kong_list )

    #----------------------------------------------------------------------
    def save_var(self):
        r = [ [self.portfolio.result.date,self.vtSymbol, self.unit, \
               str(self.price_duo_list), str(self.price_kong_list)] ]

        df = pd.DataFrame(r, columns=['datetime','vtSymbol','unit', \
                                      'price_duo_list','price_kong_list'])
        filename = get_dss() +  'fut/dali/signal_dali_var.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, sep='$', mode='a', header=False)
        else:
            df.to_csv(filename, index=False, sep='$')

    #----------------------------------------------------------------------
    def open(self, price, change):
        """开仓"""
        self.unit += change

        r = [ [self.bar.date+' '+self.bar.time, '多' if change>0 else '空', '开',  \
               abs(change), price, 0] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl'])
        filename = get_dss() +  'fut/dali/signal_dali_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)


    #----------------------------------------------------------------------
    def close(self, price):
        """平仓"""
        if self.pnl < 0:
            self.unit -= 2*self.fixedSize
        if self.pnl > 0:
            self.unit += 2*self.fixedSize

        r = [ [self.bar.date+' '+self.bar.time, '', '平', self.fixedSize, price, abs(self.pnl)] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl'])
        filename = get_dss() +  'fut/dali/signal_dali_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

########################################################################
class Fut_DaLiPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'dali'
        Portfolio.__init__(self, Fut_DaLiSignal, engine, symbol_list, signal_param)

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
