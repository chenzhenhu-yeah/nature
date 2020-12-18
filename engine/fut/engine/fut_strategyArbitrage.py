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
            pass

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min1'):
        """新推送过来一个bar，进行处理"""
        self.bar = bar
        if minx == 'min1':
            self.on_bar_minx(bar)

    def on_bar_minx(self, bar):
        self.am.updateBar(bar)
        if not self.am.inited:
            return

        self.calculateIndicator()     # 计算指标

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""

        # 告知组合层，已获得最新行情
        if self.bar.AskPrice > 0.1 and self.bar.BidPrice > 0.1:
            self.portfolio.got_dict[self.vtSymbol] = True

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
        self.pz_list = symbols.split(',')                     # 进行套利监控的品种
        self.slice_dict = {}                                  # 每分种产生一个行情切片
        self.id = 100                                         # 唯一标识套利机会，便于后续手工下单

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

        # 每分种加工切片，输出slice_dict
        if self.tm != bar.time:
            # 清空切片字典
            self.slice_dict = {}

            for symbol in self.vtSymbolList:
                # 本时间段行情没有更新
                if self.got_dict[symbol] == False:
                    continue
                else:
                    self.got_dict[symbol] = False

                s = self.signalDict[symbol][0]
                if s.bar is not None:
                    if s.bar.time == self.tm:
                        basic = get_contract(symbol).basic
                        if basic not in self.slice_dict:
                            self.slice_dict[basic] = []
                        self.slice_dict[basic].append([symbol, s.bar.time, s.bar.AskPrice, s.bar.BidPrice, s.bar.close])

            self.pcp()
            self.die()
            self.tm = bar.time

        # 将bar推送给signal
        for signal in self.signalDict[bar.vtSymbol]:
            signal.onBar(bar, minx)
            # print(bar.vtSymbol, bar.time)

    #----------------------------------------------------------------------
    def get_rec_from_slice(self, symbol):
        """
        从数据结构中获取symbol对应的最新行情
        """
        basic = get_contract(symbol).basic
        if basic in self.slice_dict:
            v = self.slice_dict[basic]
            for row in v:
                if row[0] == symbol:
                    return row

        return None

    #----------------------------------------------------------------------
    def pcp(self):
        """
        将行情切片整理成k_dict，其以basic+strike为键，
        以[basic, strike, c_ask_price, c_bid_price, p_ask_price, p_bid_price, obj_ask_price, obj_bid_price, tm]为值
        """
        k_dict = {}
        tm = '00:00:00'
        now = datetime.now()
        today = now.strftime('%Y-%m-%d')

        # 获取期权的到期日
        fn = get_dss() + 'fut/cfg/opt_mature.csv'
        df2 = pd.read_csv(fn)
        df2 = df2[df2.flag == df2.flag]                 # 筛选出不为空的记录
        df2 = df2.set_index('symbol')
        mature_dict = dict(df2.mature)

        for basic in self.slice_dict.keys():
            # print(pz)
            v = self.slice_dict[basic]
            for row in v:
                # if basic == 'm2105':
                #     print(row)
                symbol = row[0]
                tm = row[1]
                ask_price = row[2]
                bid_price = row[3]
                opt_flag = get_contract(symbol).opt_flag
                strike = get_contract(symbol).strike
                if basic[:2] == 'IO':
                    symbol_obj = 'IF' + basic[2:]
                else:
                    symbol_obj = basic

                # 当键值出现时，一次性将认购和认沽合约都处理了
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
            if T == 0 or T >= 0.3:
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

                if rt_forward >= 0.12 and diff_forward >= 3:
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

                if rt_back >= 0.12 and diff_back >= 3:
                    self.id += 1
                    seq = today[-5:-3] + today[-2:] + str(self.id)
                    r.append( [seq, today, tm, 'pcp', ['back', term, S, ca, pb, T, x, pSc_back, diff_back, rt_back]] )

        df = pd.DataFrame(r, columns=['seq', 'date', 'time', 'type', 'content'])
        fn = get_dss() +  'fut/engine/arbitrage/portfolio_arbitrage_chance.csv'
        if os.path.exists(fn):
            df.to_csv(fn, index=False, mode='a', header=False)
        else:
            df.to_csv(fn, index=False)

        if tm > '09:00:00' and tm < '15:00:00' and r != []:
            pass
            # send_email(get_dss(), '无风险套利机会'+' '+today+' '+tm, '', [], 'chenzhenhu@yeah.net')

    #----------------------------------------------------------------------
    def die(self):
        r = []
        tm = self.tm
        now = datetime.now()
        today = now.strftime('%Y-%m-%d')

        # 获取期权的到期日
        fn = get_dss() + 'fut/cfg/opt_mature.csv'
        df2 = pd.read_csv(fn)
        df2 = df2[df2.flag == df2.flag]                 # 筛选出不为空的记录
        df2 = df2.set_index('symbol')
        mature_dict = dict(df2.mature)

        for basic in self.slice_dict.keys():
            if basic not in mature_dict:
                continue
            date_mature = mature_dict[ basic ]
            date_mature = datetime.strptime(date_mature, '%Y-%m-%d')
            td = datetime.strptime(today, '%Y-%m-%d')
            T = round(float((date_mature - td).days) / 365, 4)                              # 剩余期限
            if T >= 0.2:
                continue

            strike_list = []
            v = self.slice_dict[basic]
            for row in v:
                # if basic == 'm2105':
                #     print(row)
                symbol = row[0]
                if get_contract(symbol).be_opt:
                    strike_list.append( get_contract(symbol).strike )

            strike_list = sorted(set(strike_list))
            n = len(strike_list)
            for gap in [1,2,3]:
                for i in range(n-2*gap):
                    try:
                        s1 = strike_list[i]
                        s2 = strike_list[i+gap]
                        s3 = strike_list[i+2*gap]
                        if s2 -s1 != s3 -s2:
                            continue

                        c1 = self.get_rec_from_slice(basic+get_contract(basic).opt_flag_C+str(s1))[2]  # AskPrice
                        c2 = self.get_rec_from_slice(basic+get_contract(basic).opt_flag_C+str(s2))[3]  # BidPrice
                        c3 = self.get_rec_from_slice(basic+get_contract(basic).opt_flag_C+str(s3))[2]  # AskPrice
                        if c1 == 0 or c2 == 0 or c3 == 0 or c1 != c1 or c2 != c2 or c3 != c3:
                            pass
                        else:
                            cost = round(c1 - 2*c2 + c3, 2)
                            if cost <= 3:
                                self.id += 1
                                seq = today[-5:-3] + today[-2:] + str(self.id)
                                r.append( [seq, today, tm, 'die', ['forward', basic, 'C', s1, c1, s2, c2, s3, c3, cost]] )

                        p1 = self.get_rec_from_slice(basic+get_contract(basic).opt_flag_P+str(s1))[2]  # AskPrice
                        p2 = self.get_rec_from_slice(basic+get_contract(basic).opt_flag_P+str(s2))[3]  # BidPrice
                        p3 = self.get_rec_from_slice(basic+get_contract(basic).opt_flag_P+str(s3))[2]  # AskPrice
                        if p1 == 0 or p2 == 0 or p3 == 0 or p1 != p1 or p2 != p2 or p3 != p3:
                            pass
                        else:
                            cost = round(p1 - 2*p2 + p3, 2)
                            if cost <= 3:
                                self.id += 1
                                seq = today[-5:-3] + today[-2:] + str(self.id)
                                r.append( [seq, today, tm, 'die', ['forward', basic, 'P', s1, p1, s2, p2, s3, p3, cost]] )
                    except:
                        pass
                        # s = traceback.format_exc()
                        # to_log(s)


        df = pd.DataFrame(r, columns=['seq', 'date', 'time', 'type', 'content'])
        fn = get_dss() +  'fut/engine/arbitrage/portfolio_arbitrage_chance.csv'
        if os.path.exists(fn):
            df.to_csv(fn, index=False, mode='a', header=False)
        else:
            df.to_csv(fn, index=False)

        if tm > '09:00:00' and tm < '15:00:00' and r != []:
            df = pd.read_csv(fn)
            df = df[(df.date== today) & (df.type=='die')]
            for result in r:
                rec = result[4]
                zai = False
                for i, row in df.iterrows():
                    c = eval(row['content'])
                    # print(type(c), c)
                    if c[1] == rec[1] and c[2] == rec[2] and c[3] == rec[3] and c[5] == rec[5] and c[7] == rec[7]:
                        zai = True
                if zai == False :
                    # send_email(get_dss(), '无风险套利机会'+' '+today+' '+tm, '', [], 'chenzhenhu@yeah.net')
                    break
