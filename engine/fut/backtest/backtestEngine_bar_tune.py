# encoding: UTF-8
from __future__ import print_function

from datetime import datetime
from collections import OrderedDict, defaultdict
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from nature import Backtest_Result
from nature import get_stk_hfq, to_log, get_dss
from nature import VtBarData, DIRECTION_LONG, DIRECTION_SHORT
from nature import Fut_AtrRsiPortfolio, Fut_RsiBollPortfolio, Fut_AberrationPortfolio
from nature import Fut_DonchianPortfolio, Fut_TurtlePortfolio, Fut_CciBollPortfolio
from nature import Fut_DaLiPortfolio, Fut_DaLictaPortfolio
from nature import Fut_Aberration_RawPortfolio, Fut_Aberration_EnhancePortfolio
from nature import Fut_Cci_RawPortfolio, Fut_Cci_EnhancePortfolio

########################################################################
class BacktestingEngine(object):
    """组合类CTA策略回测引擎"""

    #----------------------------------------------------------------------
    def __init__(self,symbol_list,minx='min5'):
        """Constructor"""
        self.dss = get_dss()
        self.type = 'backtest'

        self.portfolio = None                # 一对一
        self.startDt = None
        self.endDt = None
        self.backtest_dt_list = []
        self.dataDict = OrderedDict()
        self.symbol_list = symbol_list
        self.minx = minx

    #----------------------------------------------------------------------
    def loadPortfolio(self, PortfolioClass, signal_param):
        """每日重新加载投资组合"""
        print('\n')

        p = PortfolioClass(self, self.symbol_list, signal_param)
        self.portfolio = p

    #----------------------------------------------------------------------
    def setPeriod(self, startDt, endDt):
        """设置回测周期"""
        self.startDt = startDt
        self.endDt = endDt

    # 日期字段为 date, time-------------------------------------------------------------------
    def loadData(self):
        """加载数据"""
        for vtSymbol in self.symbol_list:
            #filename = get_dss( )+ 'fut/bar/min5_' + vtSymbol + '.csv'
            #filename = get_dss( ) + 'fut/bar/' + self.minx + '_' + vtSymbol + '.csv'
            #filename = get_dss( ) + 'backtest/bar/' + self.minx + '_' + vtSymbol + '.csv'
            filename = get_dss( ) + 'backtest/fut/' + vtSymbol + '/' +self.minx + '_' + vtSymbol + '.csv'

            df = pd.read_csv(filename)
            for i, d in df.iterrows():
                #print(d)

                bar = VtBarData()
                bar.vtSymbol = vtSymbol
                bar.symbol = vtSymbol
                bar.open = float(d['open'])
                bar.high = float(d['high'])
                bar.low = float(d['low'])
                bar.close = float(d['close'])
                bar.volume = d['volume']

                date = str(d['date'])
                bar.date = date
                bar.time = str(d['time'])
                if '-' in date:
                    bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y-%m-%d %H:%M:%S')
                else:
                    bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M:%S')

                #bar.time = '00:00:00'
                #bar.datetime = bar.date + ' ' + bar.time
                bar.datetime = datetime.strftime(bar.datetime, '%Y%m%d %H:%M:%S')

                barDict = self.dataDict.setdefault(bar.datetime, OrderedDict())
                barDict[bar.vtSymbol] = bar
                # break

            print(vtSymbol + '全部数据加载完成，数据量：' + str(len(df)) )

    # 日期字段为 datetime ----------------------------------------------------------------
    # def loadData(self):
    #     """加载数据"""
    #     for vtSymbol in self.symbol_list:
    #         #filename = get_dss( )+ 'fut/bar/min5_' + vtSymbol + '.csv'
    #         filename = get_dss( ) + 'fut/bar/' + self.minx + '_' + vtSymbol + '.csv'
    #
    #         df = pd.read_csv(filename)
    #         for i, d in df.iterrows():
    #             #print(d)
    #
    #             bar = VtBarData()
    #             bar.vtSymbol = vtSymbol
    #             bar.symbol = vtSymbol
    #             bar.open = float(d['open'])
    #             bar.high = float(d['high'])
    #             bar.low = float(d['low'])
    #             bar.close = float(d['close'])
    #             bar.volume = d['volume']
    #
    #             bar.datetime = d['datetime']
    #             bar.date = bar.datetime[:10]
    #             bar.time = bar.datetime[11:]
    #             bar.date = bar.date.replace('-','')
    #             bar.datetime = bar.date + ' ' + bar.time
    #             #print(bar.datetime); raise False
    #
    #             barDict = self.dataDict.setdefault(bar.datetime, OrderedDict())
    #             barDict[bar.vtSymbol] = bar
    #             # break
    #
    #         print(vtSymbol + '全部数据加载完成，数据量：' + str(len(df)) )

    #----------------------------------------------------------------------
    def _bc_loadInitBar(self, vtSymbol, initBars, minx):
        """读取startDt前n条Bar数据，用于初始化am"""
        assert minx != 'min1'

        dt_list = self.dataDict.keys()
        #print(dt_list)
        dt_list = [x for x in dt_list if x<self.startDt]
        assert len(dt_list) >= initBars

        dt_list = sorted(dt_list)
        init_dt_list = dt_list[-initBars:]
        print( '初始化导入记录数量：', len(init_dt_list) )

        r = []
        for dt in init_dt_list:
            bar_dict = self.dataDict[dt]
            if vtSymbol in bar_dict:
                bar = bar_dict[vtSymbol]
                r.append(bar)

        return r

    #----------------------------------------------------------------------
    def runBacktesting(self):
        print('开始回测')
        for dt, barDict in self.dataDict.items():
            #print(dt)
            if dt < self.startDt or dt >  self.endDt:
                continue

            #print(dt)
            for bar in barDict.values():
                #print(dt, bar.close)
                self.portfolio.onBar(bar, self.minx)

    #----------------------------------------------------------------------
    def _bc_sendOrder(self, vtSymbol, direction, offset, price, volume, pfName):
        """记录交易数据（由portfolio调用）"""
        pass

###########################################################################
def run_once(PortfolioClass,symbol,start_date,end_date,signal_param,minx):
    # 创建回测引擎对象
    e = BacktestingEngine([symbol], minx)
    e.setPeriod(start_date, end_date)
    e.loadData()
    e.loadPortfolio(PortfolioClass, signal_param)
    e.runBacktesting()
    e.portfolio.daily_close()

    bt_r = Backtest_Result(e.portfolio)
    return bt_r.show_result_key()

def test_atrrsi_param(PortfolioClass, minx):
    # vtSymbol = 'ag1912'
    # start_date = '20191015 21:00:00'
    # end_date   = '20191018 15:00:00'

    vtSymbol = 'CF901'
    start_date = '20180119 00:00:00'
    end_date   = '20181231 00:00:00'

    # vtSymbol = 'CF901'
    # start_date = '20180106 00:00:00'
    # end_date   = '20180331 00:00:00'
    #
    # vtSymbol = 'CF901'
    # start_date = '20180401 00:00:00'
    # end_date   = '20180631 00:00:00'
    #
    # vtSymbol = 'CF901'
    # start_date = '20180701 00:00:00'
    # end_date   = '20180931 00:00:00'
    #
    # vtSymbol = 'CF901'
    # start_date = '20181001 00:00:00'
    # end_date   = '20181231 00:00:00'

    r = []
    symbol_list = [vtSymbol]

    for symbol in symbol_list:
        for trailingPercent in [0.5,0.6, 0.7,0.8,0.9]:
            for rsiLength in [5]:
                for victoryPercent in [0.1,0.2,0.3,0.4]:
                    signal_param = {symbol:{'rsiLength':rsiLength, 'trailingPercent':trailingPercent, 'victoryPercent':victoryPercent}}
                    result = run_once(PortfolioClass,symbol,start_date,end_date,signal_param, minx)
                    r.append([ rsiLength,trailingPercent,victoryPercent,result['totalReturn'],result['maxDdPercent'],result['totalTradeCount'],result['sharpeRatio'] ])

    df = pd.DataFrame(r, columns=['rsiLength','trailingPercent','victoryPercent','totalReturn','maxDdPercent','totalTradeCount','sharpeRatio'])
    df.to_csv('q1.csv', index=False)


def test_cciboll_param(PortfolioClass, minx):

    vtSymbol = 'rb1901'
    start_date = '20180119 00:00:00'
    end_date   = '20181231 00:00:00'

    r = []
    symbol_list = [vtSymbol]

    for symbol in symbol_list:
        for bollWindow in [10, 15, 20, 25, 30]:
            for bollDev in [2, 2.5, 3, 3.5, 4, 4.5]:
                for slMultiplier in [3.5, 4, 4.5, 5, 5.5, 6]:
                    signal_param = { symbol:{'bollWindow':bollWindow, 'bollDev':bollDev, 'slMultiplier':slMultiplier} }
                    result = run_once(PortfolioClass,symbol,start_date,end_date,signal_param,minx)
                    r.append([ bollWindow,bollDev,slMultiplier,result['totalReturn'],result['maxDdPercent'],result['totalTradeCount'],result['sharpeRatio'] ])

    df = pd.DataFrame(r, columns=['rsiLength','trailingPercent','victoryPercent','totalReturn','maxDdPercent','totalTradeCount','sharpeRatio'])
    df.to_csv('q1.csv', index=False)

def test_aberration_enhance_param(PortfolioClass, minx):
    vtSymbol = 'ru'
    start_date = '20180209 00:00:00'
    end_date   = '20191231 00:00:00'

    r = []
    symbol_list = [vtSymbol]

    for symbol in symbol_list:
        for bollWindow in [5,10,15]:
            for bollDev in [1.5, 2, 2.5, 3]:
                for slMultiplier in [0.5, 1, 1.5, 2, 2.5]:
                    signal_param = { symbol:{'bollWindow':bollWindow, 'bollDev':bollDev, 'slMultiplier':slMultiplier} }
                    result = run_once(PortfolioClass,symbol,start_date,end_date,signal_param,minx)
                    r.append([ bollWindow,bollDev,slMultiplier,result['totalReturn'],result['maxDdPercent'],result['totalTradeCount'],result['sharpeRatio'] ])

    df = pd.DataFrame(r, columns=['bollWindow','bollDev','slMultiplier','totalReturn','maxDdPercent','totalTradeCount','sharpeRatio'])
    df.to_csv(vtSymbol+'_tune.csv', index=False)

def test_aberration_raw_param(PortfolioClass, minx):
    r = []
    symbol_list = ['m','OI','rb','RM','y']

    start_date = '20180209 00:00:00'
    #start_date = '20190109 00:00:00'
    end_date   = '20191231 00:00:00'

    for symbol in symbol_list:
        for bollWindow in [10]:
            for bollDev in [2, 2.2, 2.4, 2.6, 2.8, 3]:
                signal_param = { symbol:{'bollWindow':bollWindow, 'bollDev':bollDev} }
                result = run_once(PortfolioClass,symbol,start_date,end_date,signal_param,minx)
                r.append([ bollWindow,bollDev,symbol,result['totalReturn'],result['maxDdPercent'],result['totalTradeCount'],result['sharpeRatio'] ])

    df = pd.DataFrame(r, columns=['bollWindow','bollDev','symbol','totalReturn','maxDdPercent','totalTradeCount','sharpeRatio'])
    df.to_csv('_tune.csv', index=False)

def test_cci_raw_param(PortfolioClass, minx):
    r = []
    #symbol_list = ['m','OI','rb','RM','y']
    symbol_list = ['ru']

    start_date = '20180609 00:00:00'
    #start_date = '20190109 00:00:00'
    end_date   = '20191231 00:00:00'

    for symbol in symbol_list:
        for slMultiplier in [1.5]:
            for cciWindow in [60]:
                for hard_stop_ratio in [0.005]:
                    signal_param = { symbol:{'cciWindow':cciWindow, 'slMultiplier':slMultiplier, 'hard_stop_ratio':hard_stop_ratio} }
                    result = run_once(PortfolioClass,symbol,start_date,end_date,signal_param,minx)
                    r.append([ symbol,cciWindow,slMultiplier,hard_stop_ratio,result['totalReturn'],result['maxDdPercent'],result['totalTradeCount'],result['sharpeRatio'] ])

    df = pd.DataFrame(r, columns=['symbol','cciWindow', 'slMultiplier', 'hard_stop_ratio', 'totalReturn','maxDdPercent','totalTradeCount','sharpeRatio'])
    df.to_csv('_tune.csv', index=False)


if __name__ == '__main__':
    #PortfolioClass = Fut_AtrRsiPortfolio
    #PortfolioClass = Fut_TurtlePortfolio
    # PortfolioClass = Fut_AberrationPortfolio
    # PortfolioClass = Fut_RsiBollPortfolio
    # PortfolioClass = Fut_DonchianPortfolio
    # PortfolioClass = Fut_CciBollPortfolio
    #PortfolioClass = Fut_DaLiPortfolio
    #PortfolioClass = Fut_DaLictaPortfolio
    #PortfolioClass = Fut_Aberration_RawPortfolio
    PortfolioClass = Fut_Aberration_EnhancePortfolio
    #PortfolioClass = Fut_Cci_RawPortfolio


    minx = 'day'
    #minx = 'min15'
    #minx = 'min5'

    #test_one(PortfolioClass, minx)
    #test_atrrsi_param(PortfolioClass, minx)
    #test_cciboll_param(PortfolioClass, minx)
    test_aberration_enhance_param(PortfolioClass, minx)
    #test_aberration_raw_param(PortfolioClass, minx)
    #test_cci_raw_param(PortfolioClass, minx)
