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
from nature import VtBarData, DIRECTION_LONG, DIRECTION_SHORT
from nature import Book, a_file

from nature import Fut_AtrRsiPortfolio
from nature import Gateway_Ht_CTP
#from ipdb import set_trace



########################################################################
class BarGenerator(object):

    #----------------------------------------------------------------------
    def __init__(self, minx):
        """Constructor"""
        self.minx = minx
        self.bar_minx_dict = {}

    #----------------------------------------------------------------------
    def update_bar(self, new_bar):

        id = new_bar.vtSymbol
        if id in self.bar_minx_dict:
            bar = self.bar_minx_dict[id]
        else:
            bar = new_bar
            self.bar_minx_dict[id] = bar
            return None

        # 更新数据
        if bar.high < new_bar.high:
            bar.high = new_bar.high
        if bar.low > new_bar.low:
            bar.low =  new_bar.low
        bar.close = new_bar.close

        if self.minx == 'min5' and new_bar.time[3:5] in ['05','10','15','20','25','30','35','40','45','50','55','00']:
            # 将 bar的分钟改为整点，推送并保存bar
            bar.time = new_bar.time[:-2] + '00'
            return self.bar_minx_dict.pop(id)
        elif self.minx == 'min15' and new_bar.time[3:5] in ['15','30','45','00']:
            # 将 bar的分钟改为整点，推送并保存bar
            bar.time = new_bar.time[:-2] + '00'
            return self.bar_minx_dict.pop(id)
        else:
            self.bar_minx_dict[id] = bar

        return None

    #----------------------------------------------------------------------
    def save_bar(self, bar):
        df = pd.DataFrame([bar.__dict__])
        cols = ['date','time','open','high','low','close','volume']
        df = df[cols]

        fname = get_dss() + 'fut/put/rec/' + self.minx + '_' + bar.vtSymbol + '.csv'
        if os.path.exists(fname):
            df.to_csv(fname, index=False, mode='a', header=False)
        else:
            df.to_csv(fname, index=False, mode='a')

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

        # 初始化路由
        self.gateway = Gateway_Ht_CTP()
        self.gateway.run()

    #----------------------------------------------------------------------
    def loadPortfolio(self, PortfolioClass, symbol_list):
        """加载投资组合"""
        to_log('in FutEngine.loadPortfolio')

        p = PortfolioClass(self, symbol_list, {})
        p.init()
        p.daily_open()
        self.portfolio_list.append(p)

    # 文件通信接口  -----------------------------------------------------------
    def put_service(self):
        print('in put_svervice')
        vtSymbol_dict = {}         # 缓存中间bar
        g5 = BarGenerator('min5')

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

                        bar_min5 = g5.update_bar(bar)
                        if bar_min5 is not None:
                            g5.save_bar(bar_min5)
                            for p in self.portfolio_list:
                                p.onBar(bar_min5, 'min5')

                        for p in self.portfolio_list:
                            p.onBar(bar, 'min1')

                except Exception as e:
                    # print('-'*30)
                    # #traceback.print_exc()
                    # s = traceback.format_exc()
                    # print(s)

                    # 对文件并发访问，存着读空文件的可能！！！
                    print('file error ')

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
        print('send order: ', r)
        fn = 'fut/deal/engine_deal.csv'
        a_file(fn, str(r)[2:-2])

        if self.gateway is not None:
            self.gateway._bc_sendOrder(vtSymbol, direction, offset, price, volume, pfName)

    #----------------------------------------------------------------------
    def worker_open(self):
        """盘前加载配置及数据"""
        to_log('in FutEngine.worker_open')
        print('in worker_open')

        self.init_daily()

    #----------------------------------------------------------------------
    def worker_close(self):
        """盘后保存及展示数据"""
        print('begin worker_close')

        self.gateway.release()
        self.gateway = None                # 路由

        self.vtSymbol_list = []

        # 保存信号参数
        for p in self.portfolio_list:
            p.daily_close()
        self.portfolio_list = []           # 组合

        to_log('in FutEngine.worker_close')

#----------------------------------------------------------------------
def start():

    engine5 = FutEngine()
    schedule.every().day.at("09:16").do(engine5.worker_open)
    schedule.every().day.at("15:03").do(engine5.worker_close)
    schedule.every().day.at("20:56").do(engine5.worker_open)
    schedule.every().day.at("02:33").do(engine5.worker_close)

    print(u'期货交易引擎开始运行')
    while True:
        schedule.run_pending()
        time.sleep(10)

if __name__ == '__main__':
    start()

    # engine5 = FutEngine()
    # engine5.worker_open()
