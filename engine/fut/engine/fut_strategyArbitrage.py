# encoding: UTF-8

import os
import time
from datetime import datetime
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict
import traceback
import json

from nature import to_log, get_dss, get_contract, send_email
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import VtBarData, ArrayManager, Signal, Portfolio, TradeData, SignalResult, DailyResult
from nature import get_file_lock, release_file_lock

########################################################################
class Fut_ArbitrageSignal(Signal):

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
        # r = [[self.bar.date,self.bar.time,self.bar.close,self.bar.AskPrice,self.bar.BidPrice]]
        # df = pd.DataFrame(r)
        #
        # filename = get_dss() +  'fut/engine/arbitrage/bar_arbitrage_'+self.type+ '_' + self.vtSymbol + '.csv'
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
class Fut_ArbitragePortfolio(Portfolio):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.name = 'arbitrage'
        self.tm = '00:00:00'
        self.got_dict = {}
        for symbol in symbol_list:
            self.got_dict[symbol] = False

        config = open(get_dss()+'fut/cfg/config.json')
        setting = json.load(config)
        symbols = setting['symbols_arbitrage']
        self.pz_list = symbols.split(',')
        self.slice_dict = {}
        self.id = 100

        Portfolio.__init__(self, Fut_ArbitrageSignal, engine, symbol_list, signal_param)

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min1'):
        """引擎新推送过来bar，传递给每个signal"""

        # 不处理不相关的品种
        if get_contract(bar.vtSymbol).pz not in self.pz_list:
            return

        if minx != 'min1':               # 本策略为min1
            return

        # 动态加载symbol对应的signal
        vtSymbol = bar.vtSymbol
        if vtSymbol not in self.vtSymbolList:
            self.vtSymbolList.append(vtSymbol)
            self.got_dict[vtSymbol] = False
            signal1 = Fut_ArbitrageSignal(self, vtSymbol)

            l = self.signalDict[vtSymbol]
            l.append(signal1)

        if self.tm != bar.time:
            self.tm = bar.time

            for pz in self.pz_list:
                self.slice_dict[pz] = []

            for symbol in self.vtSymbolList:
                # if self.got_dict[symbol] == False:
                #     continue

                s = self.signalDict[symbol][0]
                if s.bar is not None:
                    pz = get_contract(symbol).pz
                    if pz in self.slice_dict:
                        self.slice_dict[pz].append([symbol, s.bar.time, s.bar.AskPrice, s.bar.BidPrice, s.bar.close])

                self.got_dict[symbol] = False
            # 输出slice_dict
            pass
            self.pcp()

        # 将bar推送给signal
        for signal in self.signalDict[bar.vtSymbol]:
            signal.onBar(bar, minx)
            # print(bar.vtSymbol, bar.time)

        """
        在此实现P层的业务控制逻辑
        为每一个品种来检查是否触发下单条件
        # 开始处理组合关心的bar , 尤其是品种对价差的加工和处理
        """
        # self.control_in_p(bar)

    #----------------------------------------------------------------------
    def get_rec_from_slice(self, symbol):
        pz = get_contract(symbol).pz
        v = self.slice_dict[pz]
        for row in v:
            if row[0] == symbol:
                return row

        return None

    #----------------------------------------------------------------------
    def pcp(self):
        k_dict = {}
        tm = '00:00:00'
        now = datetime.now()
        today = now.strftime('%Y-%m-%d')
        fn = get_dss() + 'fut/cfg/opt_mature.csv'
        df2 = pd.read_csv(fn)
        df2 = df2[df2.flag == df2.flag]                 # 筛选出不为空的记录
        df2 = df2.set_index('symbol')
        mature_dict = dict(df2.mature)

        for pz in self.slice_dict.keys():
            # print(pz)
            v = self.slice_dict[pz]
            for row in v:
                # print(row)
                symbol = row[0]
                tm = row[1]
                ask_price = row[2]
                bid_price = row[3]
                opt_flag = get_contract(symbol).opt_flag
                basic = get_contract(symbol).basic
                strike = get_contract(symbol).strike
                if basic[:2] == 'IO':
                    symbol_obj = 'IF' + basic[2:]
                else:
                    symbol_obj = basic

                if basic+str(strike) not in k_dict.keys():
                    if opt_flag == 'C':
                        symbol_p = basic + get_contract(symbol).opt_flag_P + str(strike)
                        rec_p = self.get_rec_from_slice(symbol_p)
                        rec_obj = self.get_rec_from_slice(symbol_obj)
                        if rec_p is not None and rec_obj is not None:
                            k_dict[basic+str(strike)] = [basic, strike, ask_price, bid_price, rec_p[2], rec_p[3], rec_obj[2], rec_obj[3], tm]
                    if opt_flag == 'P':
                        symbol_c = basic + get_contract(symbol).opt_flag_C + str(strike)
                        rec_c = self.get_rec_from_slice(symbol_c)
                        rec_obj = self.get_rec_from_slice(symbol_obj)
                        if rec_c is not None and rec_obj is not None:
                            k_dict[basic+str(strike)] = [basic, strike, rec_c[2], rec_c[3], ask_price, bid_price, rec_obj[2], rec_obj[3], tm]

        r = []
        # 逐条记录验证pcp规则
        for k in k_dict.keys():
            rec = k_dict[k]
            term = rec[0]
            x = rec[1]
            tm = rec[8]
            date_mature = mature_dict[ term ]
            date_mature = datetime.strptime(date_mature, '%Y-%m-%d')
            td = datetime.strptime(today, '%Y-%m-%d')
            T = round(float((date_mature - td).days) / 365, 4)                              # 剩余期限
            if T == 0 or T >= 0.2:
                continue

            # 正向套利
            S = rec[6]
            cb = rec[3]
            pa = rec[4]
            if cb > 1E8 or cb == 0 or cb != cb or pa > 1E8 or pa == 0 or pa != pa:
                pass
            else:
                pSc_forward = int( pa + S - cb )
                diff_forward = float(x) - pSc_forward
                rt_forward = round( diff_forward/(S*2*0.15)/T, 2 )
                if rt_forward > 0.12:
                    self.id += 1
                    seq = today[-5:-3] + today[-2:] + str(self.id)
                    r.append( [seq, today, tm, 'pcp', ['forward', term, S, cb, pa, T, x, pSc_forward, diff_forward, rt_forward]] )
                    # to_log( 'pcp: ' + str([today, 'forward', term, x, S, cb, pa, pSc_forward, T, diff_forward, rt_forward]) )

            # 反向套利
            S = rec[7]
            ca = rec[2]
            pb = rec[5]
            if ca > 1E8 or ca == 0 or ca != ca or pb > 1E8 or pb == 0 or pb != pb:
                pass
            else:
                pSc_back = int( pb + S - ca )
                diff_back = pSc_back - float(x)
                rt_back = round( diff_back/(S*2*0.15)/T, 2 )
                if rt_back > 0.12:
                    self.id += 1
                    seq = today[-5:-3] + today[-2:] + str(self.id)
                    r.append( [seq, today, tm, 'pcp', ['back', term, S, ca, pb, T, x, pSc_back, diff_back, rt_back]] )
                    # to_log( 'pcp: ' + str([today, 'back', term, x, S, ca, pb, pSc_back, T, diff_back, rt_back]) )

        df = pd.DataFrame(r, columns=['seq', 'date', 'time', 'type', 'content'])
        fn = get_dss() +  'fut/engine/arbitrage/portfolio_arbitrage_chance.csv'
        if os.path.exists(fn):
            df.to_csv(fn, index=False, mode='a', header=False)
        else:
            df.to_csv(fn, index=False)

        if tm > '09:00:00' and tm < '15:00:00' and r != []:
            send_email(get_dss(), '无风险套利机会'+' '+today+' '+tm, '', [], 'chenzhenhu@yeah.net')

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
            fn = get_dss() +  'fut/engine/arbitrage/portfolio_arbitrage_param.csv'
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

                except Exception as e:
                    s = traceback.format_exc()
                    to_log(s)

            df.to_csv(fn, index=False)                                        # 回写文件
            # release_file_lock(fn)
            for symbol in symbol_got_list:
                self.got_dict[symbol] = False
