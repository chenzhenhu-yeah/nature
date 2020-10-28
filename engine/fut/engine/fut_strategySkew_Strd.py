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
class Fut_Skew_StrdSignal(Signal):

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

        filename = get_dss() +  'fut/engine/skew_strd/bar_skew_strd_'+self.type+ '_' + self.vtSymbol + '.csv'
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
class Fut_Skew_StrdPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'skew_strd'
        self.tm = '00:00:00'
        self.got_dict = {}
        for symbol in symbol_list:
            self.got_dict[symbol] = False

        # self.d_base_dict = {}

        Portfolio.__init__(self, Fut_Skew_StrdSignal, engine, symbol_list, signal_param)

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
           (bar.time > '21:01:00' and bar.time < '24:00:00' and bar.vtSymbol[:2] in ['al']) or \
           (bar.time > '00:00:00' and bar.time < '01:00:00' and bar.vtSymbol[:2] in ['al']) or \
           (bar.time > '09:01:00' and bar.time < '11:27:00' and bar.vtSymbol[:2] not in ['IF','IO']) or \
           (bar.time > '13:31:00' and bar.time < '14:57:00' and bar.vtSymbol[:2] not in ['IF','IO']) or \
           (bar.time > '21:01:00' and bar.time < '22:57:00' and bar.vtSymbol[:2] not in ['IF','IO']) :    # 因第一根K线的价格为0

            # cols = ['tm','basic_m0','basic_m1','strike_m0','strike_m1','fixed_size','profit','state','source','hold_m0','hold_m1','skew_low_open','skew_high_open','price_c_m0','price_p_m0','price_c_m1','price_p_m1','skew_max','dida_max','skew_min','dida_min','profit_m0','profit_m1','profit_o','date']
            fn = get_dss() +  'fut/engine/skew_strd/portfolio_skew_strd_param.csv'

            cols_straddle = ['tm','basic_c','strike_c','basic_p','strike_p','num_c','num_p','direction','hold_c','hold_p','profit','state','source','price_c','price_p','profit_c','profit_p','profit_o','date']
            fn_straddle = get_dss() +  'fut/engine/straddle/portfolio_straddle_param.csv'

            df = pd.read_csv(fn)
            df_straddle = pd.read_csv(fn_straddle)
            symbol_got_list = []

            for i, row in df.iterrows():
                try:
                    r = []
                    if row.state == 'stop':
                        continue

                    if row.basic_m0[:2] == 'IO':
                        symbol_obj = 'IF' + row.basic_m0[2:]
                    else:
                        symbol_obj = row.basic_m0

                    exchangeID = str(get_contract(row.basic_m0).exchangeID)
                    if exchangeID in ['CFFEX', 'DCE']:
                        symbol_c_m0 = row.basic_m0 + '-C-' + str(row.strike_m0)
                        symbol_p_m0 = row.basic_m0 + '-P-' + str(row.strike_m0)
                        symbol_c_m1 = row.basic_m1 + '-C-' + str(row.strike_m1)
                        symbol_p_m1 = row.basic_m1 + '-P-' + str(row.strike_m1)
                    else:
                        symbol_c_m0 = row.basic_m0 + 'C' + str(row.strike_m0)
                        symbol_p_m0 = row.basic_m0 + 'P' + str(row.strike_m0)
                        symbol_c_m1 = row.basic_m1 + 'C' + str(row.strike_m1)
                        symbol_p_m1 = row.basic_m1 + 'P' + str(row.strike_m1)

                    if symbol_obj not in self.got_dict or symbol_c_m0 not in self.got_dict or symbol_p_m0 not in self.got_dict  or symbol_c_m1 not in self.got_dict  or symbol_p_m1 not in self.got_dict:
                        continue

                    if self.got_dict[symbol_obj] == False or self.got_dict[symbol_c_m0] == False or self.got_dict[symbol_p_m0] == False or self.got_dict[symbol_c_m1] == False or self.got_dict[symbol_p_m1] == False:
                        continue
                    else:
                        symbol_got_list.append(symbol_obj)
                        symbol_got_list.append(symbol_c_m0)
                        symbol_got_list.append(symbol_p_m0)
                        symbol_got_list.append(symbol_c_m1)
                        symbol_got_list.append(symbol_p_m1)

                        s_obj = self.signalDict[symbol_obj][0]
                        s_c_m0 = self.signalDict[symbol_c_m0][0]
                        s_p_m0 = self.signalDict[symbol_p_m0][0]
                        s_c_m1 = self.signalDict[symbol_c_m1][0]
                        s_p_m1 = self.signalDict[symbol_p_m1][0]

                        df.at[i, 'tm'] = bar.time

                        # 开仓
                        if row.hold_m0 == 0 and row.hold_m1 == 0:
                            iv_right_c = self.calc_iv(row.basic_m1, 'C', s_obj.bar.close, row.strike_m1, s_c_m1.bar.close)
                            iv_right_p = self.calc_iv(row.basic_m1, 'P', s_obj.bar.close, row.strike_m1, s_p_m1.bar.close)
                            iv_left_c = self.calc_iv(row.basic_m0, 'C', s_obj.bar.close, row.strike_m0, s_c_m0.bar.close)
                            iv_left_p = self.calc_iv(row.basic_m0, 'P', s_obj.bar.close, row.strike_m0, s_p_m0.bar.close)
                            skew = round( 100*( iv_right_c + iv_right_p - iv_left_c - iv_left_p ) / (iv_left_c + iv_left_p), 2)
                            # print(skew)

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

                            # 做空m0，做多m1
                            if df.at[i, 'skew_min'] < row.skew_low_open and skew >= row.skew_low_open and df.at[i, 'dida_min'] > 0:
                                r.append( ['00:00:00',row.basic_m0,row.strike_m0,row.basic_m0,row.strike_m0,row.fixed_size,row.fixed_size,'kong',0,0,1000,'run','skew_strd','','','','','',bar.date] )
                                r.append( ['00:00:00',row.basic_m1,row.strike_m1,row.basic_m1,row.strike_m1,row.fixed_size,row.fixed_size,'duo', 0,0,1000,'run','skew_strd','','','','','',bar.date] )
                                df.at[i, 'hold_m0'] = -1
                                df.at[i, 'hold_m1'] = 1
                                if self.engine.type == 'backtest':
                                    df.at[i, 'price_c_m0'] = s_c_m0.bar.close
                                    df.at[i, 'price_p_m0'] = s_p_m0.bar.close
                                    df.at[i, 'price_c_m1'] = s_c_m1.bar.close
                                    df.at[i, 'price_p_m1'] = s_p_m1.bar.close
                                else:
                                    df.at[i, 'price_c_m0'] = s_c_m0.bar.BidPrice
                                    df.at[i, 'price_p_m0'] = s_p_m0.bar.BidPrice
                                    df.at[i, 'price_c_m1'] = s_c_m1.bar.AskPrice
                                    df.at[i, 'price_p_m1'] = s_p_m1.bar.AskPrice

                            # 做多m0，做空m1
                            if df.at[i, 'skew_max'] > row.skew_high_open and skew <= row.skew_high_open and df.at[i, 'dida_max'] > 0:
                                r.append( ['00:00:00',row.basic_m0,row.strike_m0,row.basic_m0,row.strike_m0,row.fixed_size,row.fixed_size,'duo', 0,0,1000,'run','skew_strd','','','','','',bar.date] )
                                r.append( ['00:00:00',row.basic_m1,row.strike_m1,row.basic_m1,row.strike_m1,row.fixed_size,row.fixed_size,'kong',0,0,1000,'run','skew_strd','','','','','',bar.date] )
                                df.at[i, 'hold_m0'] = 1
                                df.at[i, 'hold_m1'] = -1
                                if self.engine.type == 'backtest':
                                    df.at[i, 'price_c_m0'] = s_c_m0.bar.close
                                    df.at[i, 'price_p_m0'] = s_p_m0.bar.close
                                    df.at[i, 'price_c_m1'] = s_c_m1.bar.close
                                    df.at[i, 'price_p_m1'] = s_p_m1.bar.close
                                else:
                                    df.at[i, 'price_c_m0'] = s_c_m0.bar.AskPrice
                                    df.at[i, 'price_p_m0'] = s_p_m0.bar.AskPrice
                                    df.at[i, 'price_c_m1'] = s_c_m1.bar.BidPrice
                                    df.at[i, 'price_p_m1'] = s_p_m1.bar.BidPrice

                            df2_straddle = pd.DataFrame(r, columns=cols_straddle)
                            df_straddle = pd.concat([df_straddle, df2_straddle], sort=False)

                        # 获利平仓
                        elif row.hold_m0 == -1 and row.hold_m1 == 1:
                            if self.engine.type == 'backtest':
                                df.at[i, 'profit_m0'] = round( (row.price_c_m0 + row.price_p_m0) - (s_c_m0.bar.close + s_p_m0.bar.close), 2)
                                df.at[i, 'profit_m1'] = round( (s_c_m1.bar.close + s_p_m1.bar.close) - (row.price_c_m1 + row.price_p_m1), 2)
                                df.at[i, 'profit_o'] = round( df.at[i, 'profit_m0'] + df.at[i, 'profit_m1'], 2)
                            else:
                                df.at[i, 'profit_m0'] = round( (row.price_c_m0 + row.price_p_m0) - (s_c_m0.bar.AskPrice + s_p_m0.bar.AskPrice), 2)
                                df.at[i, 'profit_m1'] = round( (s_c_m1.bar.BidPrice + s_p_m1.bar.BidPrice) - (row.price_c_m1 + row.price_p_m1), 2)
                                df.at[i, 'profit_o'] = round( df.at[i, 'profit_m0'] + df.at[i, 'profit_m1'], 2)
                            # print(df.at[i, 'profit_o'])

                            if df.at[i, 'profit_o'] >= row.profit:
                                df.at[i, 'hold_m0'] = 0
                                df.at[i, 'hold_m1'] = 0
                                df.at[i, 'state'] = 'stop'
                                for j, jow in df_straddle.iterrows():
                                    if jow.basic == row.basic_m0 and jow.strike == row.strike_m0 and jow.direction == 'kong' and jow.source == 'skew_strd':
                                        df_straddle.at[j, 'profit'] = -1000
                                    if jow.basic == row.basic_m1 and jow.strike == row.strike_m1 and jow.direction == 'duo' and jow.source == 'skew_strd':
                                        df_straddle.at[j, 'profit'] = -1000

                        # 获利平仓
                        elif row.hold_m0 == 1 and row.hold_m1 == -1:
                            if self.engine.type == 'backtest':
                                df.at[i, 'profit_m0'] = round( (s_c_m0.bar.close + s_p_m0.bar.close) - (row.price_c_m0 + row.price_p_m0), 2)
                                df.at[i, 'profit_m1'] = round( (row.price_c_m1 + row.price_p_m1) - (s_c_m1.bar.close + s_p_m1.bar.close), 2)
                                df.at[i, 'profit_o'] = round( df.at[i, 'profit_m0'] + df.at[i, 'profit_m1'], 2)
                            else:
                                df.at[i, 'profit_m0'] = round( (s_c_m0.bar.BidPrice + s_p_m0.bar.BidPrice) - (row.price_c_m0 + row.price_p_m0), 2)
                                df.at[i, 'profit_m1'] = round( (row.price_c_m1 + row.price_p_m1) - (s_c_m1.bar.AskPrice + s_p_m1.bar.AskPrice), 2)
                                df.at[i, 'profit_o'] = round( df.at[i, 'profit_m0'] + df.at[i, 'profit_m1'], 2)
                            # print(df.at[i, 'profit_o'])

                            if df.at[i, 'profit_o'] >= row.profit:
                                df.at[i, 'hold_m0'] = 0
                                df.at[i, 'hold_m1'] = 0
                                df.at[i, 'state'] = 'stop'
                                for j, jow in df_straddle.iterrows():
                                    if jow.basic == row.basic_m0 and jow.strike == row.strike_m0 and jow.direction == 'duo' and jow.source == 'skew_strd':
                                        df_straddle.at[j, 'profit'] = -1000
                                    if jow.basic == row.basic_m1 and jow.strike == row.strike_m1 and jow.direction == 'kong' and jow.source == 'skew_strd':
                                        df_straddle.at[j, 'profit'] = -1000

                except Exception as e:
                    s = traceback.format_exc()
                    to_log(s)


            df_straddle.to_csv(fn_straddle, index=False)
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

        # fn = get_dss() +  'fut/engine/skew_strd/portfolio_skew_strd_param.csv'
        # df = pd.read_csv(fn)
        # for i, row in df.iterrows():
        #     if row.date == self.result.date[:10] and row.source == 'skew_strd' and row.hold_m0 == 0:
        #         df.at[i, 'state'] = 'stop'
        #
        # df = df[(df.state == 'run') | (df.date == self.result.date[:10])]
        # df.to_csv(fn, index=False)
