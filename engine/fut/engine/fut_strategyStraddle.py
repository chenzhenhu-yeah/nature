# encoding: UTF-8

import os
import time
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict
import traceback
import json

from nature import to_log, get_dss, get_contract
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import VtBarData, ArrayManager, Signal, Portfolio, TradeData, SignalResult, DailyResult
from nature import get_file_lock, release_file_lock

########################################################################
class Fut_StraddleSignal(Signal):

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
        filename = get_dss() + 'fut/cfg/signal_pause_var.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = df[df.signal == self.portfolio.name]
            if len(df) > 0:
                symbol_list = str(df.iat[0,1]).split(',')
                if self.vtSymbol in symbol_list:
                    self.paused = True
                    # print(self.vtSymbol + ' right now paused in ' + self.portfolio.name)
                    return
        self.paused = False

        self.am.updateBar(bar)
        if not self.am.inited:
            return

        #print('here')

        self.calculateIndicator()     # 计算指标
        self.generateSignal(bar)      # 触发信号，产生交易指令

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""

        # 告知组合层，已获得最新行情
        if self.portfolio.engine.type == 'backtest':
            if self.bar.close > 0.1 and self.bar.close > 0.1:
                self.portfolio.got_dict[self.vtSymbol] = True
        else:
            if self.bar.AskPrice > 0.1 and self.bar.BidPrice > 0.1 and abs(self.bar.AskPrice - self.bar.BidPrice) < 20:
                self.portfolio.got_dict[self.vtSymbol] = True

        self.can_buy = False
        self.can_short = False
        self.can_sell = False
        self.can_cover = False

        # 记录数据
        r = [[self.bar.date,self.bar.time,self.bar.close,self.bar.AskPrice,self.bar.BidPrice]]
        df = pd.DataFrame(r)

        filename = get_dss() +  'fut/engine/straddle/bar_straddle_'+self.type+ '_' + self.vtSymbol + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

    #----------------------------------------------------------------------
    def generateSignal(self, bar):
        # 开多仓
        if self.can_buy == True:
            self.buy(bar.close, self.fixedSize)

        # 平多仓
        if self.can_sell == True:
            self.sell(bar.close, self.fixedSize)

        # 开空仓
        if self.can_short == True:
            self.short(bar.close, self.fixedSize)

        # 平空仓
        if self.can_cover == True:
            self.cover(bar.close, self.fixedSize)

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
class Fut_StraddlePortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'straddle'
        self.tm = '00:00:00'
        self.got_dict = {}
        for symbol in symbol_list:
            self.got_dict[symbol] = False

        Portfolio.__init__(self, Fut_StraddleSignal, engine, symbol_list, signal_param)
        self.promote = True

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min1'):
        """引擎新推送过来bar，传递给每个signal"""

        # 不处理不相关的品种
        if bar.vtSymbol not in self.vtSymbolList:
            return

        if minx != 'min1':               # 本策略为min1
            return

        # 动态加载新维护的symbol
        config = open(get_dss()+'fut/cfg/config.json')
        setting = json.load(config)
        symbols = setting['symbols_straddle']
        symbols_list = symbols.split(',')

        for vtSymbol in symbols_list:
            if vtSymbol not in self.vtSymbolList:
                self.vtSymbolList.append(vtSymbol)
                self.posDict[vtSymbol] = 0
                self.got_dict[vtSymbol] = False
                signal1 = Fut_StraddleSignal(self, vtSymbol)

                l = self.signalDict[vtSymbol]
                l.append(signal1)

        if self.tm != bar.time:
            self.tm = bar.time
            for symbol in self.vtSymbolList:
                self.got_dict[symbol] = False

        if self.result.date != bar.date + ' ' + bar.time:
            previousResult = self.result
            self.result = DailyResult(bar.date + ' ' + bar.time)
            self.resultList.append(self.result)
            if previousResult:
                self.result.updateClose(previousResult.closeDict)

        # 将bar推送给signal
        for signal in self.signalDict[bar.vtSymbol]:
            signal.onBar(bar, minx)
            # print(bar.vtSymbol, bar.time)

        """
        在此实现P层的业务控制逻辑
        为每一个品种来检查是否触发下单条件
        # 开始处理组合关心的bar , 尤其是品种对价差的加工和处理
        """
        self.control_in_p(bar)

        self.result.updateBar(bar)
        self.result.updatePos(self.posDict)


    #----------------------------------------------------------------------
    def control_in_p(self, bar):
        if (bar.time > '09:31:00' and bar.time < '11:27:00' and bar.vtSymbol[:2] in ['IF','IO']) or \
           (bar.time > '13:01:00' and bar.time < '14:57:00' and bar.vtSymbol[:2] in ['IF','IO']) or \
           (bar.time > '21:01:00' and bar.time < '24:00:00' and bar.vtSymbol[:2] in ['al']) or \
           (bar.time > '00:00:00' and bar.time < '01:00:00' and bar.vtSymbol[:2] in ['al']) or \
           (bar.time > '09:01:00' and bar.time < '11:27:00' and bar.vtSymbol[:2] not in ['IF','IO']) or \
           (bar.time > '13:31:00' and bar.time < '14:57:00' and bar.vtSymbol[:2] not in ['IF','IO']) or \
           (bar.time > '21:01:00' and bar.time < '22:57:00' and bar.vtSymbol[:2] not in ['IF','IO']) :    # 因第一根K线的价格为0

            symbol_got_list = []
            fn = get_dss() +  'fut/engine/straddle/portfolio_straddle_param.csv'
            # while get_file_lock(fn) == False:
            #     time.sleep(0.01)

            df = pd.read_csv(fn)                                                      # 加载最新参数
            for i, row in df.iterrows():
                try:
                    if row.state == 'stop':
                            continue

                    exchangeID = str(get_contract(row.basic_c).exchangeID)
                    if exchangeID in ['CFFEX', 'DCE']:
                        symbol_c = row.basic_c + '-C-' + str(row.strike_c)
                        symbol_p = row.basic_p + '-P-' + str(row.strike_p)
                    else:
                        symbol_c = row.basic_c + 'C' + str(row.strike_c)
                        symbol_p = row.basic_p + 'P' + str(row.strike_p)

                    if symbol_c not in self.got_dict or symbol_p not in self.got_dict:
                        continue

                    if self.got_dict[symbol_c] == False or self.got_dict[symbol_p] == False:
                        continue
                    else:
                        df.at[i, 'tm'] = bar.time

                        symbol_got_list.append(symbol_c)
                        symbol_got_list.append(symbol_p)

                        s_c = self.signalDict[symbol_c][0]
                        s_p = self.signalDict[symbol_p][0]

                        # 开仓
                        if row.hold_c == 0 and row.hold_p == 0:
                            # print('come here ')
                            if row.direction == 'duo':
                                if self.engine.type == 'backtest':
                                    s_c.buy(s_c.bar.close, row.num_c)
                                    s_p.buy(s_p.bar.close, row.num_p)
                                    df.at[i, 'price_c'] = s_c.bar.close
                                    df.at[i, 'price_p'] = s_p.bar.close
                                else:
                                    s_c.buy(s_c.bar.AskPrice, row.num_c)              # 挂卖价
                                    s_p.buy(s_p.bar.AskPrice, row.num_p)              # 挂卖价
                                    df.at[i, 'price_c'] = round( s_c.bar.AskPrice, 2)
                                    df.at[i, 'price_p'] = round( s_p.bar.AskPrice, 2)
                                df.at[i, 'hold_c'] = row.num_c
                                df.at[i, 'hold_p'] = row.num_p

                            if row.direction == 'kong':
                                if self.engine.type == 'backtest':
                                    s_c.short(s_c.bar.close, row.num_c)
                                    s_p.short(s_p.bar.close, row.num_p)
                                    df.at[i, 'price_c'] = s_c.bar.close
                                    df.at[i, 'price_p'] = s_p.bar.close
                                else:
                                    s_c.short(s_c.bar.BidPrice, row.num_c)              # 挂买价
                                    s_p.short(s_p.bar.BidPrice, row.num_p)              # 挂买价
                                    df.at[i, 'price_c'] = round( s_c.bar.BidPrice, 2)
                                    df.at[i, 'price_p'] = round( s_p.bar.BidPrice, 2)
                                df.at[i, 'hold_c'] = -row.num_c
                                df.at[i, 'hold_p'] = -row.num_p

                        # 多单获利平仓
                        if row.hold_c >= 1 and row.hold_p >= 1:
                            if self.engine.type == 'backtest':
                                df.at[i, 'profit_c'] = (s_c.bar.close - row.price_c) * row.hold_c
                                df.at[i, 'profit_p'] = (s_p.bar.close - row.price_p) * row.hold_p
                                df.at[i, 'profit_o'] = df.at[i, 'profit_c'] + df.at[i, 'profit_p']

                                if row.profit_o >= row.profit:
                                    s_c.sell(s_c.bar.close, abs(row.hold_c))
                                    s_p.sell(s_p.bar.close, abs(row.hold_p))
                                    df.at[i, 'hold_c'] = 0
                                    df.at[i, 'hold_p'] = 0
                                    df.at[i, 'state'] = 'stop'
                            else:
                                df.at[i, 'profit_c'] = round( (s_c.bar.BidPrice - row.price_c) * row.hold_c, 2)
                                df.at[i, 'profit_p'] = round( (s_p.bar.BidPrice - row.price_p) * row.hold_p, 2)
                                df.at[i, 'profit_o'] = round( df.at[i, 'profit_c'] + df.at[i, 'profit_p'], 2)

                                if row.profit_o >= row.profit:
                                    s_c.sell(s_c.bar.BidPrice, abs(row.hold_c))
                                    s_p.sell(s_p.bar.BidPrice, abs(row.hold_p))
                                    df.at[i, 'hold_c'] = 0
                                    df.at[i, 'hold_p'] = 0
                                    df.at[i, 'state'] = 'stop'

                        # 空单获利平仓
                        if row.hold_c <= -1 and row.hold_p <= -1:
                            if self.engine.type == 'backtest':
                                df.at[i, 'profit_c'] = (s_c.bar.close - row.price_c) * row.hold_c
                                df.at[i, 'profit_p'] = (s_p.bar.close - row.price_p) * row.hold_p
                                df.at[i, 'profit_o'] = df.at[i, 'profit_c'] + df.at[i, 'profit_p']

                                if row.profit_o >= row.profit:
                                    s_c.cover(s_c.bar.close, abs(row.hold_c))
                                    s_p.cover(s_p.bar.close, abs(row.hold_p))
                                    df.at[i, 'hold_c'] = 0
                                    df.at[i, 'hold_p'] = 0
                                    df.at[i, 'state'] = 'stop'
                            else:
                                df.at[i, 'profit_c'] = round( (s_c.bar.AskPrice - row.price_c) * row.hold_c, 2)
                                df.at[i, 'profit_p'] = round( (s_p.bar.AskPrice - row.price_p) * row.hold_p, 2)
                                df.at[i, 'profit_o'] = round( df.at[i, 'profit_c'] + df.at[i, 'profit_p'], 2)

                                if row.profit_o >= row.profit:
                                    s_c.cover(s_c.bar.AskPrice, abs(row.hold_c))
                                    s_p.cover(s_p.bar.AskPrice, abs(row.hold_p))
                                    df.at[i, 'hold_c'] = 0
                                    df.at[i, 'hold_p'] = 0
                                    df.at[i, 'state'] = 'stop'

                except Exception as e:
                    s = traceback.format_exc()
                    to_log(s)

            df.to_csv(fn, index=False)                                        # 回写文件
            # release_file_lock(fn)
            for symbol in symbol_got_list:
                self.got_dict[symbol] = False

    #----------------------------------------------------------------------
    def daily_open(self):
        Portfolio.daily_open(self)

    #----------------------------------------------------------------------
    def daily_close(self):
        Portfolio.daily_close(self)

        fn = get_dss() +  'fut/engine/straddle/portfolio_straddle_param.csv'
        df = pd.read_csv(fn)
        for i, row in df.iterrows():
            if row.date == self.result.date[:10] and row.hold_c == 0 and row.hold_p == 0:
                df.at[i, 'state'] = 'stop'

        df = df[(df.state == 'run') | (df.date == self.result.date[:10])]
        df.to_csv(fn, index=False)
