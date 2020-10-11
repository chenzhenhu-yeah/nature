# encoding: UTF-8

import os
import pandas as pd
import time
from datetime import datetime, timedelta

from csv import DictReader
from collections import OrderedDict, defaultdict
import traceback

from nature import to_log, get_dss, get_contract, send_email
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import VtBarData, ArrayManager, Signal, Portfolio, TradeData, SignalResult, DailyResult
from nature import get_file_lock, release_file_lock


########################################################################
class Fut_SdifferSignal(Signal):

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

        filename = get_dss() +  'fut/engine/sdiffer/bar_sdiffer_'+self.type+ '_' + self.vtSymbol + '.csv'
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
class Fut_SdifferPortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'sdiffer'
        self.tm = '00:00:00'
        self.got_dict = {}
        for symbol in symbol_list:
            self.got_dict[symbol] = False

        self.be_run = 'stop'
        self.d_low_open = -100.0
        self.d_high_open = 100.0
        self.d_low_mail = -100.0
        self.d_high_mail = 100.0
        self.mailed_low = False
        self.mailed_high = False
        self.load_switch()

        self.stop = False
        self.basic_m0 = None
        self.basic_m1 = None
        self.cacl_vars()

        self.symbol_obj= None
        self.d_base_dict = {}

        Portfolio.__init__(self, Fut_SdifferSignal, engine, symbol_list, signal_param)

    #----------------------------------------------------------------------
    def load_switch(self):
        pass
        fn = get_dss() +  'fut/engine/sdiffer/portfolio_sdiffer_switch.csv'
        if os.path.exists(fn):
            df = pd.read_csv(fn)
            rec = df.iloc[0]
            self.be_run = rec.be_run
            self.d_low_open = rec.d_low_open
            self.d_high_open = rec.d_high_open
            self.d_low_mail = rec.d_low_mail
            self.d_high_mail = rec.d_high_mail
            # print(self.be_run, self.d_low_open, self.d_high_open)

    #----------------------------------------------------------------------
    def cacl_vars(self):
        now = datetime.now()
        next_day = now + timedelta(days = 10)
        next_day = next_day.strftime('%Y-%m-%d')

        fn = get_dss() + 'fut/cfg/opt_mature.csv'
        df_opt = pd.read_csv(fn)

        df = df_opt[(df_opt.pz == 'IO') & (df_opt.flag == 'm0')]
        self.basic_m0 = df.iat[0,1]
        mature = df.iat[0,2]

        df = df_opt[(df_opt.pz == 'IO') & (df_opt.flag == 'm1')]
        self.basic_m1 = df.iat[0,1]

        if next_day >= mature:
            self.stop = True

        # fn = get_dss() + 'opt/straddle_differ.csv'
        # df = pd.read_csv(fn)
        # df = df[(df.basic_m0 == self.basic_m0) & (df.basic_m1 == self.basic_m1) & (df.stat == 'y')]
        # if len(df) >= 480:
        #     df = df.iloc[-480:, :]
        #     self.per_10 = df.differ.quantile(0.01)
        #     self.per_50 = df.differ.quantile(0.5)
        #     self.per_90 = df.differ.quantile(0.99)
        # else:
        #     self.per_10 = -100
        #     self.per_50 = 0
        #     self.per_90 = 100

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


            cols = ['date','basic_m0','basic_m1','strike','fixed_size','hold_m0','hold_m1','price_c_m0','price_p_m0','price_c_m1','price_p_m1','d_low_open','d_high_open','d_max','dida_max','d_min','dida_min','profit','state','source','profit_m0','profit_m1','profit_o']
            fn = get_dss() +  'fut/engine/sdiffer/portfolio_sdiffer_param.csv'
            # while get_file_lock(fn) == False:
            #     time.sleep(0.01)

            cols_straddle = ['basic','strike','direction','fixed_size','hold_c','hold_p','profit','state','source','price_c','price_p','profit_c','profit_p','profit_o','tm']
            fn_straddle = get_dss() +  'fut/engine/straddle/portfolio_straddle_param.csv'
            # while get_file_lock(fn_straddle) == False:
            #     time.sleep(0.01)

            df = pd.read_csv(fn)
            df_straddle = pd.read_csv(fn_straddle)
            symbol_got_list = []

            # 先确定当日要交易的合约，生成当日交易记录
            if self.symbol_obj is None:
                try:
                    temp1 = 'IF' + self.basic_m0[2:]
                    # print(temp1)
                    if self.got_dict[temp1]:
                        self.symbol_obj = temp1

                        # 临近到期日，暂停自动交易，否则，发出条件单
                        if self.stop == False:
                            s_obj = self.signalDict[self.symbol_obj][0]
                            obj = s_obj.bar.close
                            gap = 50
                            atm = int(round(round(obj*(100/gap)/1E4,2) * 1E4/(100/gap), 0))     # 获得平值
                            r = [[bar.date, self.basic_m0, self.basic_m1, atm, 1, \
                                  0, 0, '','','','', \
                                  self.d_low_open, self.d_high_open, 0.0,0,0.0,0, \
                                  13,self.be_run,'sdiffer','','',''] ]
                            df2 = pd.DataFrame(r, columns=cols)
                            df = pd.concat([df, df2], sort=False)
                            # print(df)
                except Exception as e:
                    s = traceback.format_exc()
                    to_log(s)
            else:
                for i, row in df.iterrows():
                    try:
                        r = []
                        if row.state == 'stop':
                            continue

                        symbol_c_m0 = row.basic_m0 + '-C-' + str(row.strike)
                        symbol_p_m0 = row.basic_m0 + '-P-' + str(row.strike)
                        symbol_c_m1 = row.basic_m1 + '-C-' + str(row.strike)
                        symbol_p_m1 = row.basic_m1 + '-P-' + str(row.strike)

                        if symbol_c_m0 not in self.got_dict or symbol_p_m0 not in self.got_dict  or symbol_c_m1 not in self.got_dict  or symbol_p_m1 not in self.got_dict:
                            continue

                        if self.got_dict[symbol_c_m0] == False or self.got_dict[symbol_p_m0] == False or self.got_dict[symbol_c_m1] == False or self.got_dict[symbol_p_m1] == False:
                            continue
                        else:
                            symbol_got_list.append(symbol_c_m0)
                            symbol_got_list.append(symbol_p_m0)
                            symbol_got_list.append(symbol_c_m1)
                            symbol_got_list.append(symbol_p_m1)

                            s_c_m0 = self.signalDict[symbol_c_m0][0]
                            s_p_m0 = self.signalDict[symbol_p_m0][0]
                            s_c_m1 = self.signalDict[symbol_c_m1][0]
                            s_p_m1 = self.signalDict[symbol_p_m1][0]

                            d_base_m0 = self.d_base_dict[row.basic_m0 + '_' + str(row.strike)]
                            d_base_m1 = self.d_base_dict[row.basic_m1 + '_' + str(row.strike)]

                            # 开仓
                            if row.hold_m0 == 0 and row.hold_m1 == 0:
                                diff_m0 = 0.5*(s_c_m0.bar.AskPrice+s_c_m0.bar.BidPrice) + 0.5*(s_p_m0.bar.AskPrice+s_p_m0.bar.BidPrice) - d_base_m0
                                diff_m1 = 0.5*(s_c_m1.bar.AskPrice+s_c_m1.bar.BidPrice) + 0.5*(s_p_m1.bar.AskPrice+s_p_m1.bar.BidPrice) - d_base_m1
                                differ  = diff_m1 - diff_m0
                                print('differ: ', differ)

                                if differ <= self.d_low_mail and self.mailed_low == False:
                                    self.mailed_low = True
                                    send_email(get_dss(), 'differ: ' + str(differ), '')

                                if differ >= self.d_high_mail and self.mailed_high == False:
                                    self.mailed_high = True
                                    send_email(get_dss(), 'differ: ' + str(differ), '')

                                if differ >= row.d_max:
                                    df.at[i, 'd_max'] = differ
                                    df.at[i, 'dida_max'] = 0
                                else:
                                    df.at[i, 'dida_max'] = row.dida_max + 1
                                    # print(df.at[i, 'dida_max'], row.dida_max)

                                if differ <= row.d_min:
                                    df.at[i, 'd_min'] = differ
                                    df.at[i, 'dida_min'] = 0
                                else:
                                    df.at[i, 'dida_min'] = row.dida_min + 1
                                    # print(df.at[i, 'dida_min'], row.dida_min)

                                # 做多
                                # if differ < 9:
                                if df.at[i, 'd_min'] <= row.d_low_open and differ >= row.d_low_open and df.at[i, 'dida_min'] > 0:
                                    r.append( [row.basic_m0,row.strike,'kong',1,0,0,1000,'run','sdiffer','','','','','','00:00:00'] )
                                    r.append( [row.basic_m1,row.strike,'duo', 1,0,0,1000,'run','sdiffer','','','','','','00:00:00'] )
                                    df.at[i, 'hold_m0'] = -1
                                    df.at[i, 'hold_m1'] = 1
                                    df.at[i, 'price_c_m0'] = s_c_m0.bar.BidPrice
                                    df.at[i, 'price_p_m0'] = s_p_m0.bar.BidPrice
                                    df.at[i, 'price_c_m1'] = s_c_m1.bar.AskPrice
                                    df.at[i, 'price_p_m1'] = s_p_m1.bar.AskPrice

                                # 做空
                                # if differ > 9:
                                if df.at[i, 'd_max'] >= row.d_high_open and differ <= row.d_high_open and df.at[i, 'dida_max'] > 0:
                                    r.append( [row.basic_m1,row.strike,'kong',1,0,0,1000,'run','sdiffer','','','','','','00:00:00'] )
                                    r.append( [row.basic_m0,row.strike,'duo', 1,0,0,1000,'run','sdiffer','','','','','','00:00:00'] )
                                    df.at[i, 'hold_m0'] = 1
                                    df.at[i, 'hold_m1'] = -1
                                    df.at[i, 'price_c_m0'] = s_c_m0.bar.AskPrice
                                    df.at[i, 'price_p_m0'] = s_p_m0.bar.AskPrice
                                    df.at[i, 'price_c_m1'] = s_c_m1.bar.BidPrice
                                    df.at[i, 'price_p_m1'] = s_p_m1.bar.BidPrice

                                df2_straddle = pd.DataFrame(r, columns=cols_straddle)
                                df_straddle = pd.concat([df_straddle, df2_straddle], sort=False)

                            # 多单获利平仓
                            elif row.hold_m0 == -1 and row.hold_m1 == 1:
                                df.at[i, 'profit_m0'] = round( (row.price_c_m0 + row.price_p_m0) - (s_c_m0.bar.AskPrice + s_p_m0.bar.AskPrice), 2)
                                df.at[i, 'profit_m1'] = round( (s_c_m1.bar.BidPrice + s_p_m1.bar.BidPrice) - (row.price_c_m1 + row.price_p_m1), 2)
                                df.at[i, 'profit_o'] = round( df.at[i, 'profit_m0'] + df.at[i, 'profit_m1'], 2)

                                if df.at[i, 'profit_o'] >= row.profit:
                                    df.at[i, 'hold_m0'] = 0
                                    df.at[i, 'hold_m1'] = 0
                                    df.at[i, 'state'] = 'stop'
                                    for j, jow in df_straddle.iterrows():
                                        if jow.basic == row.basic_m0 and jow.strike == row.strike and jow.direction == 'kong' and jow.source == 'sdiffer':
                                            df_straddle.at[j, 'profit'] = -1000
                                        if jow.basic == row.basic_m1 and jow.strike == row.strike and jow.direction == 'duo' and jow.source == 'sdiffer':
                                            df_straddle.at[j, 'profit'] = -1000

                            # 空单获利平仓
                            elif row.hold_m0 == 1 and row.hold_m1 == -1:
                                df.at[i, 'profit_m0'] = round( (s_c_m0.bar.BidPrice + s_p_m0.bar.BidPrice) - (row.price_c_m0 + row.price_p_m0), 2)
                                df.at[i, 'profit_m1'] = round( (row.price_c_m1 + row.price_p_m1) - (s_c_m1.bar.AskPrice + s_p_m1.bar.AskPrice), 2)
                                df.at[i, 'profit_o'] = round( df.at[i, 'profit_m0'] + df.at[i, 'profit_m1'], 2)

                                if df.at[i, 'profit_o'] >= row.profit:
                                    df.at[i, 'hold_m0'] = 0
                                    df.at[i, 'hold_m1'] = 0
                                    df.at[i, 'state'] = 'stop'
                                    for j, jow in df_straddle.iterrows():
                                        if jow.basic == row.basic_m0 and jow.strike == row.strike and jow.direction == 'duo' and jow.source == 'sdiffer':
                                            df_straddle.at[j, 'profit'] = -1000
                                        if jow.basic == row.basic_m1 and jow.strike == row.strike and jow.direction == 'kong' and jow.source == 'sdiffer':
                                            df_straddle.at[j, 'profit'] = -1000

                    except Exception as e:
                        s = traceback.format_exc()
                        to_log(s)


            df_straddle.to_csv(fn_straddle, index=False)
            # release_file_lock(fn_straddle)

            df.to_csv(fn, index=False)
            # release_file_lock(fn)

            for symbol in symbol_got_list:
                self.got_dict[symbol] = False


    #----------------------------------------------------------------------
    def daily_open(self):
        Portfolio.daily_open(self)

        fn = get_dss() + 'opt/sdiffer_d_base.csv'
        df = pd.read_csv(fn)
        date_list = sorted(list(set(df.date)))
        # print(date_list)
        date = date_list[-1]
        df = df[df.date == date]
        for i, row in df.iterrows():
            self.d_base_dict[row.basic + '_' + str(row.strike)] = row.d_base

    #----------------------------------------------------------------------
    def daily_close(self):
        Portfolio.daily_close(self)

        # fn = get_dss() +  'fut/engine/sdiffer/portfolio_sdiffer_param.csv'
        # df = pd.read_csv(fn)
        # for i, row in df.iterrows():
        #     if row.date == self.result.date[:10] and row.source == 'sdiffer' and row.hold_m0 == 0:
        #         df.at[i, 'state'] = 'stop'
        #
        # df = df[(df.state == 'run') | (df.date == self.result.date[:10])]
        # df.to_csv(fn, index=False)
