# encoding: UTF-8
from __future__ import print_function

from csv import DictReader
from datetime import datetime
from collections import OrderedDict, defaultdict

import os
import schedule
import time
from datetime import datetime
import numpy as np
import pandas as pd
import tushare as ts
import json
import threading
from multiprocessing.connection import Listener
from multiprocessing.connection import Client
import traceback

from nature import SOCKET_BAR
from nature import to_log, is_trade_day, send_email, get_dss
from nature import VtBarData, DIRECTION_LONG, DIRECTION_SHORT, BarGenerator
from nature import Book, a_file

from nature import Fut_AtrRsiPortfolio, Fut_RsiBollPortfolio, Fut_CciBollPortfolio
from nature import Fut_DaLiPortfolio, Fut_DaLictaPortfolio, Fut_TurtlePortfolio

#from ipdb import set_trace



########################################################################
class FutEngine(object):
    """
    交易引擎不间断运行。开市前，重新初始化引擎，并加载数据；闭市后，保存数据到文件。
    收到交易指令后，传给交易路由，完成实际下单交易。
    """

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""

        self.dss = get_dss()
        self.gateway = None                # 路由
        self.portfolio_list = []           # 组合
        self.vtSymbol_list = []            # 品种

        self.dataDict = OrderedDict()
        self.startDt = None
        self.endDt = None

        # 加载品种
        config = open(get_dss()+'fut/cfg/config.json')
        setting = json.load(config)
        symbols = setting['symbols_trade']
        self.vtSymbol_list = symbols.split(',')

    #----------------------------------------------------------------------
    def setPeriod(self, startDt, endDt):
        """设置回测周期"""
        self.startDt = startDt
        self.endDt = endDt

    #----------------------------------------------------------------------
    def init_daily(self):
        """每日初始化交易引擎"""

        # 初始化组合
        self.portfolio_list = []

        config = open(get_dss()+'fut/cfg/config.json')
        setting = json.load(config)

        if 'symbols_rsiboll' in setting:
            symbols = setting['symbols_rsiboll']
            rsiboll_symbol_list = symbols.split(',')
            self.loadPortfolio(Fut_RsiBollPortfolio, rsiboll_symbol_list)

        if 'symbols_cciboll' in setting:
            symbols = setting['symbols_cciboll']
            cciboll_symbol_list = symbols.split(',')
            self.loadPortfolio(Fut_CciBollPortfolio, cciboll_symbol_list)

        if 'symbols_dali' in setting:
            symbols = setting['symbols_dali']
            dali_symbol_list = symbols.split(',')
            self.loadPortfolio(Fut_DaLiPortfolio, dali_symbol_list)

        if 'symbols_dalicta' in setting:
            symbols = setting['symbols_dalicta']
            dalicta_symbol_list = symbols.split(',')
            self.loadPortfolio(Fut_DaLictaPortfolio, dalicta_symbol_list)

        if 'symbols_atrrsi' in setting:
            symbols = setting['symbols_atrrsi']
            atrrsi_symbol_list = symbols.split(',')
            self.loadPortfolio(Fut_AtrRsiPortfolio, atrrsi_symbol_list)

        if 'symbols_turtle' in setting:
            symbols = setting['symbols_turtle']
            turtle_symbol_list = symbols.split(',')
            self.loadPortfolio(Fut_TurtlePortfolio, turtle_symbol_list)

    #----------------------------------------------------------------------
    def loadPortfolio(self, PortfolioClass, symbol_list):
        """加载投资组合"""

        p = PortfolioClass(self, symbol_list, {})
        p.daily_open()
        self.portfolio_list.append(p)

    #----------------------------------------------------------------------
    def loadData(self):
        """加载数据"""
        for vtSymbol in self.vtSymbol_list:
            filename = get_dss( )+ 'fut/bar/min1_' + vtSymbol + '.csv'

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

                bar.datetime = datetime.strftime(bar.datetime, '%Y-%m-%d %H:%M:%S')

                barDict = self.dataDict.setdefault(bar.datetime, OrderedDict())
                barDict[bar.vtSymbol] = bar

                # break

    # -----------------------------------------------------------
    def run_playback(self):
        g5 = BarGenerator('min5')
        g15 = BarGenerator('min15')
        g30 = BarGenerator('min30')
        gday = BarGenerator('day')

        for dt, barDict in self.dataDict.items():
            #print(dt)
            if dt < self.startDt or dt > self.endDt:
                continue
            #print(dt)
            try:
                for bar in barDict.values():

                    bar_day = gday.update_bar(bar)
                    if bar_day is not None:
                        #gday.save_bar(bar_day)
                        for p in self.portfolio_list:
                            p.onBar(bar_day, 'day')


                    bar_min30 = g30.update_bar(bar)
                    if bar_min30 is not None:
                        #g30.save_bar(bar_min30)
                        for p in self.portfolio_list:
                            p.onBar(bar_min30, 'min30')

                    bar_min15 = g15.update_bar(bar)
                    if bar_min15 is not None:
                        #g15.save_bar(bar_min5)
                        for p in self.portfolio_list:
                            p.onBar(bar_min15, 'min15')

                    bar_min5 = g5.update_bar(bar)
                    if bar_min5 is not None:
                        #g5.save_bar(bar_min5)
                        for p in self.portfolio_list:
                            p.onBar(bar_min5, 'min5')

                    for p in self.portfolio_list:
                        p.onBar(bar, 'min1')

            except Exception as e:
                print('-'*30)
                #traceback.print_exc()
                s = traceback.format_exc()
                print(s)

                # 对文件并发访问，存着读空文件的可能！！！
                print('file error ')


        print('回放结束。')

    #----------------------------------------------------------------------
    def _bc_loadInitBar(self, vtSymbol, initBars, minx):
        """反调函数，因引擎知道数据在哪，初始化Bar数据，"""

        assert minx != 'min1'

        r = []

        # 直接读取signal对应minx相关的文件。
        fname = self.dss + 'fut/bar/' + minx + '_' + vtSymbol + '.csv'
        #print(fname)
        df = pd.read_csv(fname)
        df['datetime'] = df['date'] + ' ' + df['time']
        df = df[df.datetime < self.startDt]
        print(vtSymbol, len(df), minx)
        assert len(df) >= initBars

        df = df.sort_values(by=['date','time'])
        df = df.iloc[-initBars:]
        # print(df)

        for i, row in df.iterrows():
            d = dict(row)
            #print(d)
            # print(type(d))
            bar = VtBarData()
            bar.__dict__ = d
            #print(bar.__dict__)
            r.append(bar)

        #print(r)
        return r

    #----------------------------------------------------------------------
    def _bc_sendOrder(self, vtSymbol, direction, offset, price, volume, pfName):
        """记录交易数据（由portfolio调用）"""

        # 记录成交数据
        dt = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        time.sleep(0.1)
        order_id = str(int(time.time()))

        r = [[dt,pfName,order_id,'minx',vtSymbol, direction, offset, price, volume]]
        print('send order: ', r)
        fn = 'fut/engine/engine_deal.csv'
        a_file(fn, str(r)[2:-2])

        if self.gateway is not None:
            self.gateway._bc_sendOrder(vtSymbol, direction, offset, price, volume, pfName)

    #----------------------------------------------------------------------
    def worker_open(self):
        """盘前加载配置及数据"""
        self.init_daily()

    #----------------------------------------------------------------------
    def worker_close(self):
        """盘后保存及展示数据"""


        self.gateway = None                # 路由

        self.vtSymbol_list = []

        # 保存信号参数
        for p in self.portfolio_list:
            p.daily_close()
        self.portfolio_list = []           # 组合

    #----------------------------------------------------------------------
def start():
    print(u'期货交易引擎开始回放')

    start_date = '2020-01-17 09:00:00'
    end_date   = '2020-01-17 15:00:00'

    e = FutEngine()
    e.setPeriod(start_date, end_date)
    e.loadData()

    e.worker_open()
    e.run_playback()
    e.worker_close()

if __name__ == '__main__':
    start()
