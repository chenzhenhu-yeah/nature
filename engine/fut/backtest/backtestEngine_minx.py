# encoding: UTF-8
from __future__ import print_function

from datetime import datetime
from collections import OrderedDict, defaultdict
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from nature import get_stk_hfq, to_log, get_dss
from nature import VtBarData, DIRECTION_LONG, DIRECTION_SHORT, BarGenerator

from nature import Backtest_Result
from nature import Fut_DualBandPortfolio

########################################################################
class BacktestingEngine(object):
    """组合类CTA策略回测引擎"""

    #----------------------------------------------------------------------
    def __init__(self, symbol_list, minx_s, minx_l):
        """Constructor"""
        self.dss = get_dss()
        self.type = 'backtest'

        self.portfolio = None                # 一对一
        self.startDt = None
        self.endDt = None
        self.symbol_list = symbol_list

        self.backtest_dt_list = []

        self.minx_s = minx_s
        self.dataDict_s = OrderedDict()

        self.minx_l = minx_l
        self.dataDict_l = OrderedDict()


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

    #----------------------------------------------------------------------
    def loadData(self):
        """加载数据"""
        self.load_data_s()
        self.load_data_l()

    #----------------------------------------------------------------------
    def load_data_s(self):

        for vtSymbol in self.symbol_list:
            filename = get_dss( ) + 'backtest/fut/' + vtSymbol + '/' +self.minx_s + '_' + vtSymbol + '.csv'

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

                barDict = self.dataDict_s.setdefault(bar.datetime, OrderedDict())
                barDict[bar.vtSymbol] = bar
                # break

            print(self.minx_s + ' 加载数据量：' + str(len(df)) )


    #----------------------------------------------------------------------
    def load_data_l(self):

        for vtSymbol in self.symbol_list:
            filename = get_dss( ) + 'backtest/fut/' + vtSymbol + '/' +self.minx_l + '_' + vtSymbol + '.csv'

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

                barDict = self.dataDict_l.setdefault(bar.datetime, OrderedDict())
                barDict[bar.vtSymbol] = bar
                # break

            print(self.minx_l + ' 加载数据量：' + str(len(df)) )


    #----------------------------------------------------------------------
    def _bc_loadInitBar(self, vtSymbol, initBars, minx):
        """读取startDt前n条Bar数据，用于初始化am"""
        r = []

        if minx == self.minx_s:
            dt_list = self.dataDict_s.keys()
            #print(dt_list)
            dt_list = [x for x in dt_list if x<self.startDt]
            assert len(dt_list) >= initBars

            dt_list = sorted(dt_list)
            init_dt_list = dt_list[-initBars:]
            print( self.minx_s + ' 初始化导入记录数量：', len(init_dt_list) )


            for dt in init_dt_list:
                bar_dict = self.dataDict_s[dt]
                if vtSymbol in bar_dict:
                    bar = bar_dict[vtSymbol]
                    r.append(bar)

        if minx == self.minx_l:
            dt_list = self.dataDict_l.keys()
            #print(dt_list)
            dt_list = [x for x in dt_list if x<self.startDt]
            assert len(dt_list) >= initBars

            dt_list = sorted(dt_list)
            init_dt_list = dt_list[-initBars:]
            print( self.minx_l + ' 初始化导入记录数量：', len(init_dt_list) )


            for dt in init_dt_list:
                bar_dict = self.dataDict_l[dt]
                if vtSymbol in bar_dict:
                    bar = bar_dict[vtSymbol]
                    r.append(bar)

        return r

    #----------------------------------------------------------------------
    def runBacktesting(self):
        """运行回测"""
        # 提取回测起始日期之后的dt_s_list
        dt_s_list = self.dataDict_s.keys()
        dt_s_list = [dt for dt in dt_s_list if dt >=self.startDt]
        dt_s_list = sorted(dt_s_list)

        # 大循环为dt_l
        for dt_l, barDict_l in self.dataDict_l.items():
            if dt_l < self.startDt or dt_l >  self.endDt:
                #print(dt_l)
                continue

            # 通过循环，获得当前长周期时点前的dt_s，并推送
            loop = True
            while loop:
                if len(dt_s_list) > 0:
                    dt_s = dt_s_list[0]
                    if  dt_s <= dt_l:
                        barDict_s = self.dataDict_s[dt_s]
                        for bar in barDict_s.values():
                            self.portfolio.onBar(bar, self.minx_s)
                        # pop已推送的数据
                        dt_s_list.pop(0)
                        # print('s: ', dt_s)
                    else:
                        loop = False
                else:
                    loop = False

            # print('l: ', dt_l)
            # 推送长周期时点数据
            for bar in barDict_l.values():
                self.portfolio.onBar(bar, self.minx_l)


    #----------------------------------------------------------------------
    def _bc_sendOrder(self, vtSymbol, direction, offset, price, volume, pfName):
        """记录交易数据（由portfolio调用）"""
        pass

########################################################################################
def run_once(PortfolioClass,symbol,start_date,end_date,signal_param, minx_s, minx_l):
    # 创建回测引擎对象
    e = BacktestingEngine([symbol], minx_s, minx_l)
    e.setPeriod(start_date, end_date)
    e.loadData()
    e.loadPortfolio(PortfolioClass, signal_param)
    e.runBacktesting()

    bt_r = Backtest_Result(e.portfolio)
    return bt_r.show_result_key()

def test_one(PortfolioClass, minx_s, minx_l):
    vtSymbol = 'CF'

    start_date = '20190109 00:00:00'
    #start_date = '20180609 00:00:00'
    end_date   = '20191231 00:00:00'

    #signal_param = {vtSymbol:{'trailingPercent':0.6, 'victoryPercent':0.3}}
    signal_param = {}
    run_once(PortfolioClass,vtSymbol,start_date,end_date,signal_param,minx_s, minx_l)

if __name__ == '__main__':
    PortfolioClass = Fut_DualBandPortfolio

    minx_s = 'min30'
    minx_l = 'day'

    test_one(PortfolioClass, minx_s, minx_l)
