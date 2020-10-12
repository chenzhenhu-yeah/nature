# encoding: UTF-8

import os
import pandas as pd
import time
from datetime import datetime, timedelta

from csv import DictReader
from collections import OrderedDict, defaultdict
import traceback

from nature import to_log, get_dss, get_contract
from nature import bsm_call_imp_vol, bsm_put_imp_vol
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import VtBarData, ArrayManager, Signal, Portfolio, TradeData, SignalResult, DailyResult
from nature import get_file_lock, release_file_lock


########################################################################
class Fut_Skew_BiliSignal(Signal):

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
            if self.bar.AskPrice > 0.1 and self.bar.BidPrice > 0.1:
                self.portfolio.got_dict[self.vtSymbol] = True

        self.can_buy = False
        self.can_short = False
        self.can_sell = False
        self.can_cover = False

        # 记录数据
        r = [[self.bar.date,self.bar.time,self.bar.close,self.bar.AskPrice,self.bar.BidPrice]]
        df = pd.DataFrame(r)

        filename = get_dss() +  'fut/engine/skew_bili/bar_skew_bili_'+self.type+ '_' + self.vtSymbol + '.csv'
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
class Fut_Skew_BiliPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'skew_bili'
        self.tm = '00:00:00'
        self.got_dict = {}
        for symbol in symbol_list:
            self.got_dict[symbol] = False

        # self.d_base_dict = {}

        Portfolio.__init__(self, Fut_Skew_BiliSignal, engine, symbol_list, signal_param)

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
        # print('here')
        self.control_in_p(bar)
        # print('here3')

        self.result.updateBar(bar)
        self.result.updatePos(self.posDict)


    #----------------------------------------------------------------------
    def control_in_p(self, bar):
        if (bar.time > '09:31:00' and bar.time < '11:27:00' and bar.vtSymbol[:2] in ['IF','IO']) or \
           (bar.time > '13:01:00' and bar.time < '14:57:00' and bar.vtSymbol[:2] in ['IF','IO']) or \
           (bar.time > '09:01:00' and bar.time < '11:27:00' and bar.vtSymbol[:2] not in ['IF','IO']) or \
           (bar.time > '13:31:00' and bar.time < '14:57:00' and bar.vtSymbol[:2] not in ['IF','IO']) or \
           (bar.time > '21:01:00' and bar.time < '22:57:00' and bar.vtSymbol[:2] not in ['IF','IO']) :    # 因第一根K线的价格为0

            fn = get_dss() +  'fut/engine/skew_bili/portfolio_skew_bili_param.csv'
            cols_ratio = ['symbol_b','symbol_s','num_b','num_s','gap','profit','hold_b','hold_s','state','source','price_b','price_s','profit_b','profit_s','profit_o','tm','delta','theta']
            fn_ratio = get_dss() +  'fut/engine/ratio/portfolio_ratio_param.csv'

            df = pd.read_csv(fn)
            df_ratio = pd.read_csv(fn_ratio)
            symbol_got_list = []

            for i, row in df.iterrows():
                try:
                    r = []
                    if row.state == 'stop':
                        continue

                    basic_b = get_contract(row.symbol_b).basic
                    basic_s = get_contract(row.symbol_s).basic

                    if basic_b[:2] == 'IO':
                        symbol_obj = 'IF' + basic_b[2:]
                    else:
                        symbol_obj = basic_b

                    if symbol_obj not in self.got_dict or row.symbol_b not in self.got_dict or row.symbol_s not in self.got_dict:
                        continue

                    if self.got_dict[symbol_obj] == False or self.got_dict[row.symbol_b] == False or self.got_dict[row.symbol_s] == False:
                        continue
                    else:
                        symbol_got_list.append(symbol_obj)
                        symbol_got_list.append(row.symbol_b)
                        symbol_got_list.append(row.symbol_s)

                        s_obj = self.signalDict[symbol_obj][0]
                        s_b = self.signalDict[row.symbol_b][0]
                        s_s = self.signalDict[row.symbol_s][0]

                        df.at[i, 'tm'] = bar.time

                        # 开仓
                        if row.hold_b == 0 and row.hold_s == 0:
                            opt_flag = get_contract(row.symbol_b).opt_flag
                            strike_b = get_contract(row.symbol_b).strike
                            strike_s = get_contract(row.symbol_s).strike
                            if opt_flag == 'C':
                                iv_b = self.calc_iv(basic_b, 'C', s_obj.bar.close, strike_b, s_b.bar.close)
                                iv_s = self.calc_iv(basic_s, 'C', s_obj.bar.close, strike_s, s_s.bar.close)
                            else:
                                iv_b = self.calc_iv(basic_b, 'P', s_obj.bar.close, strike_b, s_b.bar.close)
                                iv_s = self.calc_iv(basic_s, 'P', s_obj.bar.close, strike_s, s_s.bar.close)
                            skew = round( 100*( iv_s / iv_b - 1 ), 2)
                            print(skew)

                            if skew > row.skew_max:
                                df.at[i, 'skew_max'] = skew
                                df.at[i, 'dida_max'] = 0
                            else:
                                df.at[i, 'dida_max'] = row.dida_max + 1
                                # print(df.at[i, 'dida_max'], row.dida_max)

                            if skew < row.skew_min:
                                df.at[i, 'skew_min'] = skew
                                df.at[i, 'dida_min'] = 0
                            else:
                                df.at[i, 'dida_min'] = row.dida_min + 1

                            # 做多symbol_s，做空symbol_b
                            if df.at[i, 'skew_min'] < row.skew_low_open and skew >= row.skew_low_open and df.at[i, 'dida_min'] > 0:
                                r.append( [row.symbol_s,row.symbol_b,row.num_s,row.num_b,-1000,row.profit,0,0,'run','skew_bili',0.0,0.0,0.0,0.0,0.0,'00:00:00',0.0,0.0] )
                                df.at[i, 'hold_b'] = -row.num_b
                                df.at[i, 'hold_s'] = row.num_s
                                if self.engine.type == 'backtest':
                                    df.at[i, 'price_b'] = s_b.bar.close
                                    df.at[i, 'price_s'] = s_s.bar.close
                                else:
                                    df.at[i, 'price_b'] = s_b.bar.BidPrice
                                    df.at[i, 'price_s'] = s_s.bar.AskPrice

                            # 做多symbol_b，做空symbol_s
                            if df.at[i, 'skew_max'] > row.skew_high_open and skew <= row.skew_high_open and df.at[i, 'dida_max'] > 0:
                                r.append( [row.symbol_b,row.symbol_s,row.num_b,row.num_s,-1000,row.profit,0,0,'run','skew_bili',0.0,0.0,0.0,0.0,0.0,'00:00:00',0.0,0.0] )
                                df.at[i, 'hold_b'] = row.num_b
                                df.at[i, 'hold_s'] = -row.num_s
                                if self.engine.type == 'backtest':
                                    df.at[i, 'price_b'] = s_b.bar.close
                                    df.at[i, 'price_s'] = s_s.bar.close
                                else:
                                    df.at[i, 'price_b'] = s_b.bar.AskPrice
                                    df.at[i, 'price_s'] = s_s.bar.BidPrice

                            df2_ratio = pd.DataFrame(r, columns=cols_ratio)
                            df_ratio = pd.concat([df_ratio, df2_ratio], sort=False)

                        # 获利平仓
                        elif row.hold_b <= -1 and row.hold_s >= 1:
                            if self.engine.type == 'backtest':
                                df.at[i, 'profit_b'] = round( (row.price_b - s_b.bar.close), 2)
                                df.at[i, 'profit_s'] = round( (s_s.bar.close - row.price_s), 2)
                                df.at[i, 'profit_o'] = round( df.at[i, 'profit_b'] + df.at[i, 'profit_s'], 2)
                            else:
                                df.at[i, 'profit_b'] = round( (row.price_b - s_b.bar.AskPrice), 2)
                                df.at[i, 'profit_s'] = round( (s_s.bar.BidPrice - row.price_s), 2)
                                df.at[i, 'profit_o'] = round( df.at[i, 'profit_b'] + df.at[i, 'profit_s'], 2)
                            # print(df.at[i, 'profit_o'])

                            if df.at[i, 'profit_o'] >= row.profit:
                                df.at[i, 'hold_m0'] = 0
                                df.at[i, 'hold_m1'] = 0
                                df.at[i, 'state'] = 'stop'
                                for j, jow in df_ratio.iterrows():
                                    if jow.symbol_b == row.symbol_s and jow.symbol_s == row.symbol_b and jow.num_b == row.num_s and jow.num_s == row.num_b and jow.source == 'skew_bili':
                                        df_ratio.at[j, 'profit'] = -1000

                        # 获利平仓
                        elif row.hold_b >= 1 and row.hold_s <= -1:
                            if self.engine.type == 'backtest':
                                df.at[i, 'profit_b'] = round( (s_b.bar.close - row.price_b), 2)
                                df.at[i, 'profit_s'] = round( (row.price_s - s_s.bar.close), 2)
                                df.at[i, 'profit_o'] = round( df.at[i, 'profit_b'] + df.at[i, 'profit_s'], 2)
                            else:
                                df.at[i, 'profit_b'] = round( (s_b.bar.BidPrice - row.price_b), 2)
                                df.at[i, 'profit_s'] = round( (row.price_s - s_s.bar.AskPrice), 2)
                                df.at[i, 'profit_o'] = round( df.at[i, 'profit_b'] + df.at[i, 'profit_s'], 2)
                            # print(df.at[i, 'profit_o'])

                            if df.at[i, 'profit_o'] >= row.profit:
                                df.at[i, 'hold_b'] = 0
                                df.at[i, 'hold_s'] = 0
                                df.at[i, 'state'] = 'stop'
                                for j, jow in df_ratio.iterrows():
                                    if jow.symbol_b == row.symbol_b and jow.symbol_s == row.symbol_s and jow.num_b == row.num_b and jow.num_s == row.num_s and jow.source == 'skew_bili':
                                        df_ratio.at[j, 'profit'] = -1000

                except Exception as e:
                    s = traceback.format_exc()
                    to_log(s)

            df_ratio.to_csv(fn_ratio, index=False)
            df.to_csv(fn, index=False)

            for symbol in symbol_got_list:
                self.got_dict[symbol] = False


    #----------------------------------------------------------------------
    def calc_iv(self, basic, flag, S0, K, C0):
        r = 0.03
        fn = get_dss() + 'fut/cfg/opt_mature.csv'
        df2 = pd.read_csv(fn)
        df2 = df2[df2.pz == df2.pz]                 # 筛选出不为空的记录
        df2 = df2.set_index('symbol')
        mature_dict = dict(df2.mature)
        date_mature = mature_dict[ basic ]
        date_mature = datetime.strptime(date_mature, '%Y-%m-%d')
        td = datetime.now()
        T = float((date_mature - td).days) / 365                       # 剩余期限

        skew = 0
        if flag == 'C':
            skew = bsm_call_imp_vol(S0, K, T, r, C0)
        if flag == 'P':
            skew = bsm_put_imp_vol(S0, K, T, r, C0)

        return skew

    #----------------------------------------------------------------------
    def daily_open(self):
        Portfolio.daily_open(self)

    #----------------------------------------------------------------------
    def daily_close(self):
        Portfolio.daily_close(self)

        # fn = get_dss() +  'fut/engine/skew_bili/portfolio_skew_bili_param.csv'
        # df = pd.read_csv(fn)
        # for i, row in df.iterrows():
        #     if row.date == self.result.date[:10] and row.source == 'skew_bili' and row.hold_b == 0:
        #         df.at[i, 'state'] = 'stop'
        #
        # df = df[(df.state == 'run') | (df.date == self.result.date[:10])]
        # df.to_csv(fn, index=False)
