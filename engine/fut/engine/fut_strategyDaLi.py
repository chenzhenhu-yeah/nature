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
        self.type = 'one'

        # 策略参数
        self.fixedSize = 1            # 每次交易的数量
        self.initBars = 100           # 初始化数据所用的天数
        self.minx = 'min5'

        self.atrValue = 0
        self.atrWindow = 30
        self.atr_x = 8

        self.gap = 30
        self.gap_min = 15
        self.gap_max = 40
        self.price_min_1 = 2600
        self.price_min_2 = 2730
        self.price_max_2 = 3000
        self.price_max_1 = 3100

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
                self.gap = rec.gap
                self.gap_min = rec.gap_min
                self.gap_max = rec.gap_max
                self.atr_x = rec.atr_x
                self.price_min_1 = rec.price_min_1
                self.price_min_2 = rec.price_min_2
                self.price_max_2 = rec.price_max_2
                self.price_max_1 = rec.price_max_1
                print('成功加载策略参数', self.vtSymbol,self.gap,self.gap_min,self.gap_max,self.atr_x, \
                      self.price_min_1,self.price_min_2,self.price_max_2,self.price_max_1)

    #----------------------------------------------------------------------
    def set_param(self, param_dict):
        if 'gap' in param_dict:
            self.gap = param_dict['gap']
            print('成功设置策略参数 self.gap: ',self.gap)
        if 'fixedSize' in param_dict:
            self.fixedSize = param_dict['fixedSize']
            if self.fixedSize > 1:
                self.type = 'multi'
            print('成功设置策略参数 self.fixedSize: ',self.fixedSize)

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min1'):
        """新推送过来一个bar，进行处理"""
        self.bar = bar
        if minx == 'min5':
            self.on_bar_minx(bar)

    def on_bar_minx(self, bar):
        self.am.updateBar(bar)
        if not self.am.inited:
            return

        #print('here')
        if self.fixedSize == 1:
            if self.bar.close <= self.price_min_1 or self.bar.close >= self.price_max_1:
                return
            if self.bar.close >= self.price_min_2 and self.bar.close <= self.price_max_2:
                return
        else:
            if self.bar.close < self.price_min_2 or self.bar.close > self.price_max_2:
                return

        self.calculateIndicator()     # 计算指标
        self.generateSignal(bar)      # 触发信号，产生交易指令

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""
        self.can_buy = False
        self.can_short = False

        self.atrValue = self.am.atr(self.atrWindow)
        # print(self.atrValue)
        self.gap = self.atr_x * self.atrValue
        self.gap = max(self.gap, self.gap_min)
        self.gap = min(self.gap, self.gap_max)
        #self.gap = 20

        if self.bar.close <= self.get_price_kong() - self.get_gap_minus() :
        #if self.bar.close <= self.get_price_kong() - self.gap:
            self.can_buy = True
            self.pnl = (self.get_price_kong() - self.bar.close)*self.fixedSize

        if self.bar.close >= self.get_price_duo() + self.get_gap_plus() :
        #if self.bar.close >= self.get_price_duo() + self.gap:
            self.can_short = True
            self.pnl = (self.get_price_duo() - self.bar.close)*self.fixedSize

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

            if len(self.price_duo_list) >= 10 or len(self.price_kong_list) >= 10:
                print( len(self.price_duo_list), len(self.price_kong_list) )

            # to_log( '买开仓：'+str(self.pnl)+ '  '+str(self.gap)+'  ' +str(self.bar.close) )
            # to_log( str(self.price_duo_list) )
            # to_log( str(self.price_kong_list) )

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


            if len(self.price_duo_list) >= 10 or len(self.price_kong_list) >= 10:
                print( len(self.price_duo_list), len(self.price_kong_list) )

            # to_log( '卖开仓：'+str(self.pnl)+ '  '+str(self.gap)+'  ' +str(self.bar.close) )
            # to_log( str(self.price_duo_list) )
            # to_log( str(self.price_kong_list) )

    #----------------------------------------------------------------------
    def get_gap_plus(self):
        # 当为上涨趋势时，空头持仓增加，要控制。
        g = self.gap

        cc = len(self.price_duo_list) - len(self.price_kong_list)
        # if  cc >= 11:
        #     g -= 20
        # elif cc >= 9:
        #     g -= 10
        # elif cc >= 7:
        #     g -= 5

        #if self.pnl > 40 and cc >= 1:
        if cc >= 2:
            g = self.gap_min

        if cc <= -12:
            g += 20
        elif cc <= -10:
            g += 15
        elif cc <= -8:
            g += 10
        elif cc <= -6:
            g += 5

        g = max(g, self.gap_min)
        g = min(g, self.gap_max+10)
        return g

    #----------------------------------------------------------------------
    def get_gap_minus(self):
        g = self.gap

        cc = len(self.price_kong_list) - len(self.price_duo_list)
        # if  cc >= 11:
        #     g -= 20
        # elif cc >= 9:
        #     g -= 10
        # elif cc >= 7:
        #     g -= 5

        #if self.pnl < -40 and cc >= 1:
        if cc >= 2:
            g = self.gap_min

        if cc <= -12:
            g += 20
        elif cc <= -10:
            g += 15
        elif cc <= -8:
            g += 10
        elif cc <= -6:
            g += 5

        g = max(g, self.gap_min)
        g = min(g, self.gap_max+10)
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
        self.price_duo_list.append(price)
        self.unit_open(price, self.fixedSize)

    #----------------------------------------------------------------------
    def unit_sell(self):
        self.price_duo_list = sorted(self.price_duo_list)
        self.price_duo_list.pop(0)

        self.unit_close(self.bar.close)

    #----------------------------------------------------------------------
    def unit_short(self, price):
        self.price_kong_list.append(price)
        self.unit_open(price, self.fixedSize)

    #----------------------------------------------------------------------
    def unit_cover(self):
        self.price_kong_list = sorted(self.price_kong_list)
        self.price_kong_list.pop(-1)

        self.unit_close(self.bar.close)

    #----------------------------------------------------------------------
    def load_var(self):
        filename = get_dss() +  'fut/engine/dali/signal_dali_'+self.type+ '_var_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename, sep='$')
            df = df[df.vtSymbol == self.vtSymbol]
            df = df.sort_values(by='datetime')
            df = df.reset_index()
            if len(df) > 0:
                rec = df.iloc[-1,:]            # 取最近日期的记录
                self.price_duo_list = eval( rec.price_duo_list )
                self.price_kong_list = eval( rec.price_kong_list )

    #----------------------------------------------------------------------
    def save_var(self):
        pnl_trade = 0
        filename = get_dss() + 'fut/engine/dali/signal_dali_'+self.type+ '_deal_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            pnl_trade = df.pnl.sum()

        settle = self.bar.close
        pnl_hold = 0
        for item in self.price_duo_list:
            pnl_hold += settle - item

        for item in self.price_kong_list:
            pnl_hold += item - settle
        pnl_hold = pnl_hold*self.fixedSize

        self.unit = len(self.price_duo_list) - len(self.price_kong_list)
        r = [ [self.portfolio.result.date,self.vtSymbol, self.unit, \
               pnl_trade+pnl_hold, pnl_trade, pnl_hold, str(self.price_duo_list), str(self.price_kong_list)] ]

        df = pd.DataFrame(r, columns=['datetime','vtSymbol','unit', \
                                      'pnl_net','pnl_trade','pnl_hold','price_duo_list','price_kong_list'])
        filename = get_dss() +  'fut/engine/dali/signal_dali_'+self.type+ '_var_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, sep='$', mode='a', header=False)
        else:
            df.to_csv(filename, index=False, sep='$')

    #----------------------------------------------------------------------
    def open(self, price, change):
        pass

    #----------------------------------------------------------------------
    def unit_open(self, price, change):
        """开仓"""

        r = [ [self.bar.date+' '+self.bar.time, '多' if change>0 else '空', '开',  \
               abs(change), price, 0] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl'])
        filename = get_dss() +  'fut/engine/dali/signal_dali_'+self.type+ '_deal_' + self.vtSymbol + '.csv'
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

        r = [ [self.bar.date+' '+self.bar.time, '', '平', self.fixedSize, price, abs(self.pnl)] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl'])
        filename = get_dss() +  'fut/engine/dali/signal_dali_'+self.type+ '_deal_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

########################################################################
class Fut_DaLiPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'dali'

        s_param = {}
        for symbol in symbol_list:
            s_param[symbol] = {'fixedSize':2}
        #Portfolio.__init__(self, Fut_DaLiSignal, engine, symbol_list, s_param)
        Portfolio.__init__(self, Fut_DaLiSignal, engine, symbol_list, {}, Fut_DaLiSignal, s_param)

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
