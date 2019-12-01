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

from nature import Fut_AtrRsiPortfolio, Fut_RsiBollPortfolio, Fut_CciBollPortfolio, Fut_DaLiPortfolio
from nature import Gateway_Ht_CTP
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

        # 开启bar监听服务
        #threading.Thread( target=self.bar_service, args=() ).start()
        threading.Thread( target=self.put_service, args=() ).start()

    #----------------------------------------------------------------------
    def init_daily(self):
        """每日初始化交易引擎"""

        # 加载品种
        config = open(get_dss()+'fut/cfg/config.json')
        setting = json.load(config)
        symbols = setting['symbols_trade']
        self.vtSymbol_list = symbols.split(',')

        # 初始化组合
        self.portfolio_list = []

        symbols = setting['symbols_atrrsi']
        atrrsi_symbol_list = symbols.split(',')
        self.loadPortfolio(Fut_AtrRsiPortfolio, atrrsi_symbol_list)

        symbols = setting['symbols_rsiboll']
        rsiboll_symbol_list = symbols.split(',')
        self.loadPortfolio(Fut_RsiBollPortfolio, rsiboll_symbol_list)

        symbols = setting['symbols_cciboll']
        cciboll_symbol_list = symbols.split(',')
        self.loadPortfolio(Fut_CciBollPortfolio, cciboll_symbol_list)

        symbols = setting['symbols_dali']
        dali_symbol_list = symbols.split(',')
        self.loadPortfolio(Fut_DaLiPortfolio, dali_symbol_list)

        # 初始化路由
        self.gateway = Gateway_Ht_CTP()
        self.gateway.run()

    #----------------------------------------------------------------------
    def loadPortfolio(self, PortfolioClass, symbol_list):
        """加载投资组合"""

        p = PortfolioClass(self, symbol_list, {})
        p.daily_open()
        self.portfolio_list.append(p)

    # 文件通信接口  -----------------------------------------------------------
    def put_service(self):
        print('in put_svervice')
        vtSymbol_dict = {}         # 缓存中间bar
        g5 = BarGenerator('min5')
        g15 = BarGenerator('min15')

        while True:
            time.sleep(1)
            for id in self.vtSymbol_list:
                try:
                    fname = self.dss + 'fut/put/min1_' + id + '.csv'
                    #print(fname)
                    df = pd.read_csv(fname)
                    d = dict(df.loc[0,:])
                    #print(d)
                    #print(type(d))
                    bar = VtBarData()
                    bar.__dict__ = d
                    bar.vtSymbol = id
                    bar.symbol = id

                    if id not in vtSymbol_dict:
                        vtSymbol_dict[id] = bar
                    elif vtSymbol_dict[id].time != bar.time:
                        vtSymbol_dict[id] = bar

                        bar_min15 = g15.update_bar(bar)
                        if bar_min15 is not None:
                            g15.save_bar(bar_min15)
                            for p in self.portfolio_list:
                                p.onBar(bar_min15, 'min15')

                        bar_min5 = g5.update_bar(bar)
                        if bar_min5 is not None:
                            g5.save_bar(bar_min5)
                            for p in self.portfolio_list:
                                p.onBar(bar_min5, 'min5')

                        for p in self.portfolio_list:
                            p.onBar(bar, 'min1')

                except Exception as e:
                    # 对文件并发访问，存着读空文件的可能！！！
                    #print('-'*30)
                    #traceback.print_exc()
                    s = traceback.format_exc()
                    to_log(s)

    #----------------------------------------------------------------------
    def _bc_loadInitBar(self, vtSymbol, initBars, minx):
        """反调函数，因引擎知道数据在哪，初始化Bar数据，"""
        r = []

        today = time.strftime('%Y%m%d',time.localtime())
        # 直接读取signal对应minx相关的文件。
        fname = self.dss + 'fut/bar/' + minx + '_' + vtSymbol + '.csv'
        #print(fname)
        df = pd.read_csv(fname)
        assert len(df) >= initBars

        df = df.sort_values(by=['date','time'])
        df = df.iloc[-initBars:]
        #print(df)

        for i, row in df.iterrows():
            d = dict(row)
            # print(d)
            # print(type(d))
            bar = VtBarData()
            bar.__dict__ = d
            r.append(bar)

        return r

    #----------------------------------------------------------------------
    def _bc_sendOrder(self, vtSymbol, direction, offset, price, volume, pfName):
        """记录交易数据（由portfolio调用）"""

        # 记录成交数据
        dt = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        time.sleep(0.1)
        order_id = str(int(time.time()))

        r = [[dt,pfName,order_id,'minx',vtSymbol, direction, offset, price, volume]]
        #print('send order: ', r)
        to_log( str(r)[3:-2] )
        fn = 'fut/deal/engine_deal.csv'
        a_file(fn, str(r)[2:-2])

        if self.gateway is not None:
            self.gateway._bc_sendOrder(vtSymbol, direction, offset, price, volume, pfName)

    #----------------------------------------------------------------------
    def worker_open(self):
        """盘前加载配置及数据"""
        try:
            self.init_daily()

        except Exception as e:
            s = traceback.format_exc()
            to_log(s)

    #----------------------------------------------------------------------
    def worker_close(self):
        """盘后保存及展示数据"""
        try:
            self.gateway.release()
            self.gateway = None                # 路由
            self.vtSymbol_list = []

            # 保存信号参数
            for p in self.portfolio_list:
                p.daily_close()
            self.portfolio_list = []           # 组合

        except Exception as e:
            s = traceback.format_exc()
            to_log(s)

#----------------------------------------------------------------------
def start():

    e = FutEngine()
    # schedule.every().day.at("08:56").do(e.worker_open)
    # schedule.every().day.at("15:03").do(e.worker_close)
    # schedule.every().day.at("20:56").do(e.worker_open)
    # schedule.every().day.at("02:33").do(e.worker_close)

    schedule.every().monday.at("08:56").do(e.worker_open)
    schedule.every().monday.at("15:03").do(e.worker_close)
    schedule.every().monday.at("20:56").do(e.worker_open)
    schedule.every().tuesday.at("02:33").do(e.worker_close)

    schedule.every().tuesday.at("08:56").do(e.worker_open)
    schedule.every().tuesday.at("15:03").do(e.worker_close)
    schedule.every().tuesday.at("20:56").do(e.worker_open)
    schedule.every().wednesday.at("02:33").do(e.worker_close)

    schedule.every().wednesday.at("08:56").do(e.worker_open)
    schedule.every().wednesday.at("15:03").do(e.worker_close)
    schedule.every().wednesday.at("20:56").do(e.worker_open)
    schedule.every().thursday.at("02:33").do(e.worker_close)

    schedule.every().thursday.at("08:56").do(e.worker_open)
    schedule.every().thursday.at("15:03").do(e.worker_close)
    schedule.every().thursday.at("20:56").do(e.worker_open)
    schedule.every().friday.at("02:33").do(e.worker_close)

    schedule.every().friday.at("08:56").do(e.worker_open)
    schedule.every().friday.at("15:03").do(e.worker_close)
    schedule.every().friday.at("20:56").do(e.worker_open)
    schedule.every().saturday.at("02:33").do(e.worker_close)

    print(u'期货交易引擎开始运行')
    while True:
        schedule.run_pending()
        time.sleep(10)

if __name__ == '__main__':
    start()

    # engine5 = FutEngine()
    # engine5.worker_open()
