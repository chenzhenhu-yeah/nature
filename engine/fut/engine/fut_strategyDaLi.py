# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict
import traceback

from nature import to_log, get_dss, get_contract, send_email
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import ArrayManager, Signal, Portfolio, TradeData, SignalResult


########################################################################
class Fut_DaLiSignal(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        self.type = 'multi'

        # 策略参数
        self.fixed_size_dict = {10:1, 20:2, 30:3}            # 每次交易的数量
        self.initBars = 60            # 初始化数据所用的天数
        self.minx = 'min5'

        self.atrValue = 0
        self.atrWindow = 20
        self.atr_x = 8
        self.dual = 10
        self.atr_h = 5
        self.adjust_bp = 0
        self.price_tick = get_contract(vtSymbol).price_tick

        self.gap = 30
        self.gap_base = self.gap
        self.gap_min = 15
        self.gap_max = 40

        self.price_duo_list =  []
        self.price_kong_list = []
        self.duo_adjust_price = 0
        self.kong_adjust_price = 0
        self.size_duo_list =  []
        self.size_kong_list = []

        # 策略临时变量
        self.can_buy = False
        self.can_short = False
        self.pnl = 0
        self.first = True

        self.counter = 0

        Signal.__init__(self, portfolio, vtSymbol)

        self.backtest = True if self.portfolio.engine.type == 'backtest' else False
        # self.backtest = True               # 回测模式
        # self.backtest = False              # 实盘模式

    #----------------------------------------------------------------------
    def load_param(self):
        filename = get_dss() +  'fut/engine/dali/signal_dali_param.csv'
        pz = str(get_contract(self.vtSymbol).pz)
        df = pd.read_csv(filename)
        df = df[ df.pz == pz ]
        if len(df) > 0:
            rec = df.iloc[0,:]
            self.fixed_size_dict = eval(rec.fixed_size)
            print(self.fixed_size_dict)
            self.gap = rec.gap
            self.gap_base = rec.gap
            self.gap_min = rec.gap_min
            self.gap_max = rec.gap_max
            self.atr_x = rec.atr_x
            self.dual = rec.dual
            self.atr_h = rec.atr_h

            #print('成功加载策略参数')

    # 回测时用，实盘用不上----------------------------------------------------
    def set_param(self, param_dict):
        if 'price_duo_list' in param_dict:
            self.price_duo_list = param_dict['price_duo_list']
            print('成功设置策略参数 self.price_duo_list: ',self.price_duo_list)

        if 'price_kong_list' in param_dict:
            self.price_kong_list = param_dict['price_kong_list']
            print('成功设置策略参数 self.price_kong_list: ',self.price_kong_list)

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min1'):
        """新推送过来一个bar，进行处理"""
        self.lock.acquire()

        self.bar = bar
        if minx == 'min1':
            self.on_bar_min1(bar)
        if minx == 'min5':
            self.on_bar_minx(bar)

        self.lock.release()

    def on_bar_min1(self, bar):
        # 按照新逻辑，不再做跳空处理
        pass

    def on_bar_minx(self, bar):
        self.am.updateBar(bar)
        if not self.am.inited:
            return

        # 涨跌停时暂停交易
        if bar.close == bar.UpperLimitPrice:
            self.pause = True
            to_log(self.vtSymbol + ' 已涨停，该品种dali策略暂停交易')

        if bar.close == bar.LowerLimitPrice:
            self.pause = True
            to_log(self.vtSymbol + ' 已跌停，该品种dali策略暂停交易')

        if self.paused == True and self.backtest == False:
            self.counter += 1
            if self.counter == 3:
                send_email(get_dss(), self.vtSymbol + ' 挂单未成交！！！', '')

            return

        self.calculateIndicator()     # 计算指标
        self.generateSignal(bar)      # 触发信号，产生交易指令

    #----------------------------------------------------------------------
    def on_trade(self, t):
        # print(self.order_list)
        # print( '收到成交回报 ', str(t) )
        # print(self.paused)

        self.lock.acquire()

        b = True
        for o in self.order_list:
            if o['direction'] == t['direction'] and o['offset'] == t['offset'] and o['traded'] < o['volume']:
                o['traded'] += t['volume']
            if o['traded'] < o['volume']:
                b = False
        if b == True:
            self.paused = False
            self.counter = 0

        self.lock.release()
        # print(self.order_list)
        # print(self.paused)

    #----------------------------------------------------------------------
    def record(self, r):
        df = pd.DataFrame(r)
        filename = get_dss() +  'fut/engine/dali/bar_dali_'+self.type+ '_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

    #----------------------------------------------------------------------
    def get_fixed_size(self, act):
        r = 1
        if act == 'open':
            k = sorted(self.fixed_size_dict.keys())
            cc = abs(len(self.price_duo_list) - len(self.price_kong_list))
            if cc < k[0]:
                r = self.fixed_size_dict[k[0]]
            elif cc < k[1]:
                r = self.fixed_size_dict[k[1]]
            else:
                r = self.fixed_size_dict[k[2]]
        if act == 'close':
            if len(self.size_duo_list) > 0:
                r = self.size_duo_list[-1]
            if len(self.size_kong_list) > 0:
                r = self.size_kong_list[-1]
        return r

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

        if self.atrValue > self.atr_h:
            self.adjust_bp = self.price_tick
        else:
            self.adjust_bp = 0

        gap_minus = self.get_gap_minus()
        # 价格下跌，空队列占优平一仓，多队列占优开一仓（需满足最近价的间隔要求）
        if self.bar.close <= self.get_price_kong() - gap_minus:
            if len(self.price_duo_list) >= len(self.price_kong_list) and self.bar.close > self.get_price_duo() - self.gap:
                pass
            else:
                self.can_buy = True
                fixed_size = self.get_fixed_size('close')
                self.pnl = (self.get_price_kong() - self.bar.close) * fixed_size

        gap_plus = self.get_gap_plus()
        # 价格上涨，多队列占优平一仓，空队列占优开一仓（需满足最近价的间隔要求）
        if self.bar.close >= self.get_price_duo() + gap_plus:
            if len(self.price_kong_list) >= len(self.price_duo_list) and self.bar.close < self.get_price_kong() + self.gap:
                pass
            else:
                self.can_short = True
                fixed_size = self.get_fixed_size('close')
                self.pnl = (self.get_price_duo() - self.bar.close) * fixed_size

        r = [[self.bar.date,self.bar.time,self.bar.close,self.can_buy,self.can_short,self.atrValue,self.gap,gap_plus,gap_minus]]
        self.record(r)

    #----------------------------------------------------------------------
    def generateSignal(self, bar):
        if len(self.price_duo_list) == 0 or len(self.price_kong_list) == 0 :
            fixed_size = self.get_fixed_size('open')
            self.buy(bar.close, fixed_size)
            self.short(bar.close, fixed_size)
            self.unit_buy(bar.close, fixed_size)
            self.unit_short(bar.close, fixed_size)

        cc = len(self.price_duo_list) - len(self.price_kong_list)

        # 平空仓、开多仓
        if self.can_buy == True:
            self.record([[self.bar.date, self.bar.time, str(self.price_duo_list), str(sorted(self.price_kong_list,reverse=True))]])
            if cc < 0:
                # 价格从高位下跌，仓差收窄，收窄时只有一种方式，单减。
                # 平空仓，移多队列。此时队列中的数量是充足的。
                fixed_size = self.get_fixed_size('close')
                self.cover(bar.close + self.adjust_bp, fixed_size)
                self.unit_cover()

                # 多队列实时调整，队列数量保持不变
                self.price_duo_list = self.adjust_price_duo(bar.close)
            else:
                # 价格回归后继续下跌，仓差走扩，起步阶段用仓差单增的方式。
                # 价格下跌，买开仓
                fixed_size = self.get_fixed_size('open')
                self.buy(bar.close + self.adjust_bp, fixed_size)
                self.unit_buy(bar.close, fixed_size)

                # 空队列实时调整，队列数量保持不变
                self.price_kong_list = self.adjust_price_kong(bar.close)

            self.paused = True
            self.record([[self.bar.date, self.bar.time, str(self.price_duo_list), str(sorted(self.price_kong_list,reverse=True))]])

        # 平多仓、开空仓
        if self.can_short == True:
            self.record([[self.bar.date, self.bar.time, str(self.price_duo_list), str(sorted(self.price_kong_list,reverse=True))]])
            if cc > 0:
                # 价格从底部上涨，仓差收窄，收窄时只有一种方式，单减。
                # 平多仓，移空队列。此时队列中的数量是充足的。
                fixed_size = self.get_fixed_size('close')
                self.sell(bar.close - self.adjust_bp, fixed_size)
                self.unit_sell()

                # 空队列实时调整，队列数量保持不变
                self.price_kong_list = self.adjust_price_kong(bar.close)
            else:
                # 价格回归后继续上涨，仓差走扩，起步阶段用仓差单增的方式。
                # 价格上涨，开空仓，移多队列
                fixed_size = self.get_fixed_size('open')
                self.short(bar.close - self.adjust_bp, fixed_size)
                self.unit_short(bar.close, fixed_size)

                # 多队列实时调整，队列数量保持不变
                self.price_duo_list = self.adjust_price_duo(bar.close)

            self.paused = True
            self.record([[self.bar.date, self.bar.time, str(self.price_duo_list), str(sorted(self.price_kong_list,reverse=True))]])

    #----------------------------------------------------------------------
    def get_gap_plus(self):
        # 当为上涨趋势时，空头持仓增加，要控制。
        g = self.gap
        cc = len(self.price_kong_list) - len(self.price_duo_list)

        if cc >= 15:
            g += self.gap_base
        elif cc >= 12:
            g += self.gap_base * 0.75
        elif cc >= 9:
            g += self.gap_base * 0.5
        elif cc >= 6:
            g += self.gap_base * 0.25

        return g

    #----------------------------------------------------------------------
    def get_gap_minus(self):
        g = self.gap
        cc = len(self.price_duo_list) - len(self.price_kong_list)

        if cc >= 15:
            g += self.gap_base
        elif cc >= 12:
            g += self.gap_base * 0.75
        elif cc >= 9:
            g += self.gap_base * 0.5
        elif cc >= 6:
            g += self.gap_base * 0.25

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
    def unit_buy(self, price, fixed_size):
        self.price_duo_list.append(price)
        self.size_duo_list.append(fixed_size)
        self.unit_open(price, fixed_size)

    #----------------------------------------------------------------------
    def unit_short(self, price, fixed_size):
        self.price_kong_list.append(price)
        self.size_kong_list.append(fixed_size)
        self.unit_open(price, -fixed_size)

    #----------------------------------------------------------------------
    def unit_sell(self):
        self.price_duo_list = sorted(self.price_duo_list)
        self.price_duo_list.pop(0)
        fixed_size = self.size_duo_list.pop(-1)

        self.unit_close(self.bar.close, fixed_size)

    #----------------------------------------------------------------------
    def unit_cover(self):
        self.price_kong_list = sorted(self.price_kong_list)
        self.price_kong_list.pop(-1)
        fixed_size = self.size_kong_list.pop(-1)

        self.unit_close(self.bar.close, fixed_size)

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
                self.size_duo_list = eval( rec.size_duo_list )
                self.size_kong_list = eval( rec.size_kong_list )
                # print(self.price_duo_list)
                # print(self.price_kong_list)

    #----------------------------------------------------------------------
    def adjust_price_duo(self, head=None):
        r = []
        duo_list = self.price_duo_list
        n = len(duo_list)

        if n > 1:
            if head is None:
                a1 = min(duo_list)
            else:
                a1 = head
            A = sum(duo_list)
            A += self.duo_adjust_price
            x = int( (A-n*a1)/(0.5*n*(n-1)) + 0.5 )           # 四舍五入
            #print(x)

            for i in range(n):
                ai = a1 + i*x
                if i == n-1:
                    ai = A - sum(r)
                r.append(ai)
        else:
            r = [ duo_list[0] + self.duo_adjust_price ]

        self.duo_adjust_price = 0
        return r

    #----------------------------------------------------------------------
    def adjust_price_kong(self, head=None):
        r = []
        kong_list = self.price_kong_list
        n = len(kong_list)

        if n > 1:
            if head is None:
                b1 = max(kong_list)
            else:
                b1 = head
            B = sum(kong_list)
            B += self.kong_adjust_price
            x = int( (n*b1-B)/(0.5*n*(n-1)) + 0.5 )           # 四舍五入
            # print(x)

            for i in range(n):
                bi = b1 - i*x
                if i == n-1:
                    bi = B - sum(r)
                r.append(bi)
        else:
            r = [ kong_list[0] + self.kong_adjust_price ]

        self.kong_adjust_price = 0
        return r

    #----------------------------------------------------------------------
    def adjust_price_head(self):
        if len(self.price_duo_list) == len(self.price_kong_list):
            self.price_duo_list  = sorted( self.price_duo_list )
            self.price_kong_list = sorted( self.price_kong_list, reverse=True )
            head = int( (self.price_duo_list[0] + self.price_kong_list[0])/2 )
            self.price_duo_list = self.adjust_price_duo(head)
            self.price_kong_list = self.adjust_price_kong(head)

    #----------------------------------------------------------------------
    def save_var(self):
        try:
            # 按照新逻辑，收盘不再调整队列
            self.adjust_price_head()
            self.price_duo_list  = sorted( self.price_duo_list )
            self.price_kong_list = sorted( self.price_kong_list, reverse=True )

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

            d_list = [2,2,2,2,2] + self.size_duo_list
            k_list = [2,2,2,2,2] + self.size_kong_list
            # print(self.vtSymbol)
            # print(d_list)
            # print(k_list)

            for d, item in zip( d_list, sorted(self.price_duo_list,reverse=True) ):
                pnl_hold += (settle - item) * d * size
                # print(d, pnl_hold)

            for k, item in zip( k_list, sorted(self.price_kong_list) ):
                pnl_hold += (item - settle) * k * size
                # print(k, pnl_hold)

            # print(pnl_hold)

            self.unit = len(self.price_duo_list) - len(self.price_kong_list)
            print(self.unit)

            r = [ [self.portfolio.result.date,self.vtSymbol, settle, self.unit, \
                   pnl_trade+pnl_hold-commission-slippage, pnl_trade, pnl_hold, \
                   commission, slippage, str(self.price_duo_list), str(self.price_kong_list), \
                   str(self.size_duo_list), str(self.size_kong_list)] ]

            df = pd.DataFrame(r, columns=['datetime','vtSymbol','price','unit', \
                                          'pnl_net','pnl_trade','pnl_hold', \
                                          'commission','slippage','price_duo_list','price_kong_list', \
                                          'size_duo_list', 'size_kong_list'])
            filename = get_dss() +  'fut/engine/dali/signal_dali_'+self.type+ '_var_' + pz + '.csv'
            if os.path.exists(filename):
                df.to_csv(filename, index=False, mode='a', header=False)
            else:
                df.to_csv(filename, index=False)

            assert self.unit == len(self.size_duo_list) - len(self.size_kong_list)
            assert len(self.size_duo_list) == 0 or len(self.size_kong_list) == 0

        except Exception as e:
            s = traceback.format_exc()
            to_log(s)

    #----------------------------------------------------------------------
    def open(self, price, change):
        pass
        # print('come here open !')

    #----------------------------------------------------------------------
    def close(self, price, change):
        pass
        # print('come here close !')

    #----------------------------------------------------------------------
    def unit_open(self, price, change):
        """开仓"""
        ct = get_contract(self.vtSymbol)
        size = ct.size
        slippage = ct.slippage
        variableCommission = ct.variable_commission
        fixedCommission = ct.fixed_commission

        fixed_size = abs(change)
        commissionCost = fixed_size * fixedCommission + fixed_size * price * size * variableCommission
        slippageCost = fixed_size * size * slippage

        r = [ [self.bar.date+' '+self.bar.time, '多' if change>0 else '空', '开',  \
               fixed_size, price, 0, commissionCost, slippageCost, self.vtSymbol] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl','commission', 'slippage','symbol'])
        pz = str(get_contract(self.vtSymbol).pz)
        filename = get_dss() +  'fut/engine/dali/signal_dali_'+self.type+ '_deal_' + pz + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)


    #----------------------------------------------------------------------
    def unit_close(self, price, fixed_size):
        """平仓"""
        ct = get_contract(self.vtSymbol)
        size = ct.size
        slippage = ct.slippage
        variableCommission = ct.variable_commission
        fixedCommission = ct.fixed_commission

        commissionCost = fixed_size * fixedCommission + fixed_size * price * size * variableCommission
        slippageCost = fixed_size * size * slippage
        pnl = abs(self.pnl) * size

        r = [ [self.bar.date+' '+self.bar.time, '', '平', fixed_size, price, pnl, commissionCost, slippageCost, self.vtSymbol] ]
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
