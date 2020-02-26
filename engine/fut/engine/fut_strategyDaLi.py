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
        self.type = 'multi'

        # 策略参数
        self.fixedSize = 1            # 每次交易的数量
        self.initBars = 60           # 初始化数据所用的天数
        self.minx = 'min5'

        self.atrValue = 0
        self.atrWindow = 30
        self.atr_x = 8

        self.gap = 30
        self.gap_base = self.gap
        self.gap_min = 15
        self.gap_max = 40
        self.price_min = 2600
        self.price_max = 3100

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
        filename = get_dss() +  'fut/engine/dali/signal_dali_param.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = df[ df.symbol == self.vtSymbol ]
            if len(df) > 0:
                rec = df.iloc[0,:]
                self.fixedSize = rec.fixed_size
                self.gap = rec.gap
                self.gap_base = rec.gap
                self.gap_min = rec.gap_min
                self.gap_max = rec.gap_max
                self.atr_x = rec.atr_x
                self.price_min = rec.price_min
                self.price_max = rec.price_max
                #print('成功加载策略参数')

    #----------------------------------------------------------------------
    def set_param(self, param_dict):
        if 'gap' in param_dict:
            # self.gap = param_dict['gap']
            print('成功设置策略参数 self.gap: ',self.gap)
        if 'fixedSize' in param_dict:
            # self.fixedSize = param_dict['fixedSize']
            # if self.fixedSize > 1:
            #     self.type = 'multi'
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

        #print('here')
        if self.bar.close < self.price_min or self.bar.close > self.price_max:
            return

        self.calculateIndicator()     # 计算指标
        self.generateSignal(bar)      # 触发信号，产生交易指令

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""
        self.can_buy = False
        self.can_short = False

        atrArray = self.am.atr(1, array=True)
        self.atrValue = atrArray[-self.atrWindow:].mean()

        self.gap = self.atr_x * self.atrValue
        self.gap = max(self.gap, self.gap_min)
        self.gap = min(self.gap, self.gap_max)
        #self.gap = 20

        if self.bar.close <= self.get_price_kong() - self.get_gap_minus() :
        #if self.bar.close <= self.get_price_kong() - self.gap:
            self.can_buy = True
            self.pnl = (self.get_price_kong() - self.bar.close) * self.fixedSize

        if self.bar.close >= self.get_price_duo() + self.get_gap_plus() :
        #if self.bar.close >= self.get_price_duo() + self.gap:
            self.can_short = True
            self.pnl = (self.get_price_duo() - self.bar.close) * self.fixedSize

        r = [[self.bar.date,self.bar.time,self.bar.close,self.can_buy,self.can_short,self.atrValue,self.gap]]
        df = pd.DataFrame(r)
        filename = get_dss() +  'fut/engine/dali/bar_dali_'+self.type+ '_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

    #----------------------------------------------------------------------
    def generateSignal(self, bar):
        if len(self.price_duo_list) == 0 or len(self.price_kong_list) == 0 :
            self.buy(bar.close, self.fixedSize)
            self.short(bar.close, self.fixedSize)
            self.unit_buy(bar.close)
            self.unit_short(bar.close)

        # 平空仓、开多仓
        if self.can_buy == True:
            if len(self.price_kong_list) == 1:
                self.buy(bar.close, self.fixedSize)

                self.unit_cover()
                self.unit_short(bar.close)
                self.unit_buy(bar.close)

            else:
                self.cover(bar.close, self.fixedSize)
                self.buy(bar.close, self.fixedSize)

                self.unit_cover()
                self.unit_buy(bar.close)

        # 平多仓、开空仓
        if self.can_short == True:
            if len(self.price_duo_list) == 1:
                self.short(bar.close, self.fixedSize)

                self.unit_sell()
                self.unit_buy(bar.close)
                self.unit_short(bar.close)

            else:
                self.sell(bar.close, self.fixedSize)
                self.short(bar.close, self.fixedSize)

                self.unit_sell()
                self.unit_short(bar.close)

    #----------------------------------------------------------------------
    def get_gap_plus(self):
        # 当为上涨趋势时，空头持仓增加，要控制。
        g = self.gap
        cc = ( len(self.price_duo_list) - len(self.price_kong_list) ) * self.fixedSize

        if abs(cc) >= 20:
            g += self.gap_base
        elif abs(cc) >= 16:
            g += self.gap_base * 0.75
        elif abs(cc) >= 12:
            g += self.gap_base * 0.5
        elif abs(cc) >= 8:
            g += self.gap_base * 0.25

        if cc >= 4 and cc <= 6:
            g = self.gap_min

        return g

    #----------------------------------------------------------------------
    def get_gap_minus(self):
        g = self.gap
        cc = ( len(self.price_kong_list) - len(self.price_duo_list) ) * self.fixedSize


        if abs(cc) >= 20:
            g += self.gap_base
        elif abs(cc) >= 16:
            g += self.gap_base * 0.75
        elif abs(cc) >= 12:
            g += self.gap_base * 0.5
        elif abs(cc) >= 8:
            g += self.gap_base * 0.25

        if cc >= 4 and cc <= 6:
            g = self.gap_min

        return g

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
        if len(self.price_duo_list) >= 3:
            self.price_duo_list = sorted(self.price_duo_list)
            p_tail = self.price_duo_list.pop(-1)
            self.price_duo_list.append(p_tail - self.gap_base*0.5)
            self.price_duo_list.append(price  + self.gap_base*0.5)
        else:
            self.price_duo_list.append(price)

        self.unit_open(price, self.fixedSize)

    #----------------------------------------------------------------------
    def unit_short(self, price):
        if len(self.price_kong_list) >= 3:
            self.price_kong_list = sorted(self.price_kong_list)
            p0 = self.price_kong_list.pop(0)
            self.price_kong_list.append(p0 + self.gap_base*0.5)
            self.price_kong_list.append(price - self.gap_base*0.5)
        else:
            self.price_kong_list.append(price)

        self.unit_open(price, -self.fixedSize)

    #----------------------------------------------------------------------
    def unit_sell(self):
        self.price_duo_list = sorted(self.price_duo_list)
        self.price_duo_list.pop(0)

        self.unit_close(self.bar.close)

    #----------------------------------------------------------------------
    def unit_cover(self):
        self.price_kong_list = sorted(self.price_kong_list)
        self.price_kong_list.pop(-1)

        self.unit_close(self.bar.close)

    #----------------------------------------------------------------------
    def load_var(self):
        pz = str(get_contract(self.vtSymbol).pz)
        filename = get_dss() +  'fut/engine/dali/signal_dali_'+self.type+ '_var_' + pz + '.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = df[df.vtSymbol == self.vtSymbol]
            df = df.sort_values(by='datetime')
            df = df.reset_index()
            if len(df) > 0:
                rec = df.iloc[-1,:]            # 取最近日期的记录
                self.price_duo_list = eval( rec.price_duo_list )
                self.price_kong_list = eval( rec.price_kong_list )

    #----------------------------------------------------------------------
    def adjust_price_duo(self):
        r = []
        duo_list = self.price_duo_list
        n = len(duo_list)

        if n > 1:
            a1 = min(duo_list)
            A = sum(duo_list)
            x = int( (A-n*a1)/(0.5*n*(n-1)) )
            #print(x)

            for i in range(n):
                ai = a1 + i*x
                if i == n-1:
                    ai = A - sum(r)
                r.append(ai)
        else:
            r = duo_list

        return r

    #----------------------------------------------------------------------
    def adjust_price_kong(self):
        r = []
        kong_list = self.price_kong_list
        n = len(kong_list)

        if n > 1:
            b1 = max(kong_list)
            B = sum(kong_list)
            x = int( (n*b1-B)/(0.5*n*(n-1)) )
            # print(x)

            for i in range(n):
                bi = b1 - i*x
                if i == n-1:
                    bi = B - sum(r)
                r.append(bi)
        else:
            r = kong_list

        return r

    #----------------------------------------------------------------------
    def save_var(self):
        if self.paused == True:
            return

        self.price_duo_list = self.adjust_price_duo()
        self.price_kong_list = self.adjust_price_kong()

        pnl_trade = 0
        commission = 0
        slippage = 0
        pz = str(get_contract(self.vtSymbol).pz)
        filename = get_dss() + 'fut/engine/dali/signal_dali_'+self.type+ '_deal_' + pz + '.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            pnl_trade = df.pnl.sum()
            commission = df.commission.sum()
            slippage = df.slippage.sum()

        settle = self.bar.close
        pnl_hold = 0
        ct = get_contract(self.vtSymbol)
        size = ct.size
        for item in self.price_duo_list:
            pnl_hold += settle - item

        for item in self.price_kong_list:
            pnl_hold += item - settle
        pnl_hold = size * pnl_hold * self.fixedSize

        self.unit = len(self.price_duo_list) - len(self.price_kong_list)
        r = [ [self.portfolio.result.date,self.vtSymbol, self.unit, \
               pnl_trade+pnl_hold-commission-slippage, pnl_trade, pnl_hold, \
               commission, slippage, str(self.price_duo_list), str(self.price_kong_list)] ]

        df = pd.DataFrame(r, columns=['datetime','vtSymbol','unit', \
                                      'pnl_net','pnl_trade','pnl_hold', \
                                      'commission','slippage','price_duo_list','price_kong_list'])
        filename = get_dss() +  'fut/engine/dali/signal_dali_'+self.type+ '_var_' + pz + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

    #----------------------------------------------------------------------
    def open(self, price, change):
        pass

    #----------------------------------------------------------------------
    def unit_open(self, price, change):
        """开仓"""
        ct = get_contract(self.vtSymbol)
        size = ct.size
        slippage = ct.slippage
        variableCommission = ct.variable_commission
        fixedCommission = ct.fixed_commission

        commissionCost = self.fixedSize * fixedCommission + self.fixedSize * price * size * variableCommission
        slippageCost = self.fixedSize * size * slippage

        r = [ [self.bar.date+' '+self.bar.time, '多' if change>0 else '空', '开',  \
               abs(change), price, 0, commissionCost, slippageCost, self.vtSymbol] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl','commission', 'slippage','symbol'])
        pz = str(get_contract(self.vtSymbol).pz)
        filename = get_dss() +  'fut/engine/dali/signal_dali_'+self.type+ '_deal_' + pz + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

    #----------------------------------------------------------------------
    def close(self, price):
        pass

    #----------------------------------------------------------------------
    def unit_close(self, price):
        """平仓"""
        ct = get_contract(self.vtSymbol)
        size = ct.size
        slippage = ct.slippage
        variableCommission = ct.variable_commission
        fixedCommission = ct.fixed_commission

        commissionCost = self.fixedSize * fixedCommission + self.fixedSize * price * size * variableCommission
        slippageCost = self.fixedSize * size * slippage
        pnl = abs(self.pnl) * size

        r = [ [self.bar.date+' '+self.bar.time, '', '平', self.fixedSize, price, pnl, commissionCost, slippageCost, self.vtSymbol] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl','commission', 'slippage','symbol'])
        pz = str(get_contract(self.vtSymbol).pz)
        filename = get_dss() +  'fut/engine/dali/signal_dali_'+self.type+ '_deal_' + pz + '.csv'
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
        #Portfolio.__init__(self, Fut_DaLiSignal, engine, symbol_list, {}, Fut_DaLiSignal, {})
