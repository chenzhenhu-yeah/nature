# encoding: UTF-8

import os
import pandas as pd
from datetime import datetime, timedelta

from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss, get_contract
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import VtBarData, ArrayManager, Signal, Portfolio, TradeData, SignalResult, DailyResult


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
        if self.bar.AskPrice - self.bar.BidPrice < 3:
            self.portfolio.got_dict[self.vtSymbol] = True

        self.can_buy = False
        self.can_short = False
        self.can_sell = False
        self.can_cover = False

        # 记录数据
        # r = [[self.bar.date,self.bar.time,self.bar.close,self.bar.AskPrice,self.bar.BidPrice,self.portfolio.profit,self.portfolio.profit_c,self.portfolio.profit_p,self.portfolio.profit_c+self.portfolio.profit_p]]
        # df = pd.DataFrame(r)
        #
        # filename = get_dss() +  'fut/engine/sdiffer/bar_sdiffer_'+self.type+ '_' + self.vtSymbol + '.csv'
        # if os.path.exists(filename):
        #     df.to_csv(filename, index=False, mode='a', header=False)
        # else:
        #     df.to_csv(filename, index=False)

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

        self.got_dict = {}
        for symbol in symbol_list:
            self.got_dict[self.symbol] = False

        self.per_10 = 0
        self.per_50 = 0
        self.per_90 = 0
        self.stop = False
        self.basic_m0 = None
        self.basic_m1 = None
        self.cacl_vars()

        self.symbol_obj = None
        self.symbol_c_m0 = None
        self.symbol_p_m0 = None
        self.symbol_c_m1 = None
        self.symbol_p_m1 = None
        self.d_base_m0 = None
        self.d_base_m1 = None

        Portfolio.__init__(self, Fut_SdifferSignal, engine, symbol_list, signal_param)

    #----------------------------------------------------------------------
    def cacl_vars(self):
        now = datetime.strptime(date, '%Y-%m-%d')
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

        fn = get_dss() + 'opt/straddle_diff.csv'
        df = pd.read_csv(fn)
        df = df[(df.basic_m0 == self.basic_m0) & (df.basic_m1 == self.basic_m1) & (df.stat == 'y')]
        df = df.iloc[-480:, :]
        self.per_10 = df.differ.quantile(0.1)
        self.per_50 = df.differ.quantile(0.5)
        self.per_90 = df.differ.quantile(0.9)

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min1'):
        """引擎新推送过来bar，传递给每个signal"""

        if self.stop == True:
            return

        # 不处理不相关的品种
        if bar.vtSymbol not in self.vtSymbolList:
            return

        if minx != 'min1':               # 本策略为min1
            return

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
        if (bar.time > '09:35:00' and bar.time < '11:25:00' and bar.vtSymbol[:2] in ['IF','IO']) or \
           (bar.time > '13:05:00' and bar.time < '14:55:00' and bar.vtSymbol[:2] in ['IF','IO']) or \
           (bar.time > '09:05:00' and bar.time < '11:25:00' and bar.vtSymbol[:2] not in ['IF','IO']) or \
           (bar.time > '13:35:00' and bar.time < '14:55:00' and bar.vtSymbol[:2] not in ['IF','IO']) or \
           (bar.time > '21:05:00' and bar.time < '22:55:00' and bar.vtSymbol[:2] not in ['IF','IO']) :    # 因第一根K线的价格为0

            got_all = True
            for symbol in symbol_list:
               if self.got_dict[self.symbol] == False:
                   got_all = False
                   break

            if got_all == True:
                for symbol in symbol_list:
                    self.got_dict[self.symbol] = False

                # 先确定当日要交易的合约
                if self.symbol_obj is None:
                    self.symbol_obj = 'IF' + self.basic_m0[2:]
                    fn = get_dss() + 'fut/bar/day_' + self.symbol_obj + '.csv'
                    df = pd.read_csv(fn)
                    gap = 50
                    rec = df.iloc[-1,:]
                    obj = rec.close
                    atm = int(round(round(obj*(100/gap)/1E4,2) * 1E4/(100/gap), 0))     # 获得平值
                    self.symbol_c_m0 = self.basic_m0 + '-C-' + str(atm)
                    self.symbol_p_m0 = self.basic_m0 + '-P-' + str(atm)
                    self.symbol_c_m1 = self.basic_m1 + '-C-' + str(atm)
                    self.symbol_p_m1 = self.basic_m1 + '-P-' + str(atm)

                    fn = get_dss() + 'fut/bar/day_' + self.symbol_c_m0 + '.csv'
                    df_m0_c_pre =  pd.read_csv(fn)
                    fn = get_dss() + 'fut/bar/day_' + self.symbol_p_m0 + '.csv'
                    df_m0_p_pre =  pd.read_csv(fn)
                    fn = get_dss() + 'fut/bar/day_' + self.symbol_c_m1 + '.csv'
                    df_m1_c_pre =  pd.read_csv(fn)
                    fn = get_dss() + 'fut/bar/day_' + self.symbol_p_m1 + '.csv'
                    df_m1_p_pre =  pd.read_csv(fn)
                    self.d_base_m0 = df_m0_c_pre.iat[-1,5] + df_m0_p_pre.iat[-1,5]
                    self.d_base_m1 = df_m1_c_pre.iat[-1,5] + df_m1_p_pre.iat[-1,5]
                else:
                    s_c_m0 = self.signalDict[self.symbol_c_m0][0]
                    s_p_m0 = self.signalDict[self.symbol_p_m0][0]
                    s_c_m1 = self.signalDict[self.symbol_c_m1][0]
                    s_p_m1 = self.signalDict[self.symbol_p_m1][0]

                    # 开仓
                    if row.hold_c == 0 and row.hold_p == 0 and row.state == 'run':
                        pass

                    # 多单获利平仓
                    if row.hold_c == 1 and row.hold_p == 1:
                        pass

                    # 空单获利平仓
                    if row.hold_c == -1 and row.hold_p == -1:
                        pass
