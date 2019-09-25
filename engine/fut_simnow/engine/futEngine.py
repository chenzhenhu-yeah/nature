# encoding: UTF-8
from __future__ import print_function

from csv import DictReader
from datetime import datetime
from collections import OrderedDict, defaultdict

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

from nature import SOCKET_BAR
from nature import to_log, is_trade_day, send_email
from nature import VtBarData, DIRECTION_LONG, DIRECTION_SHORT
from nature import Book, a_file
from nature import Fut_AtrRsiPortfolio, get_dss, Gateway_Simnow_CTP
#from ipdb import set_trace

########################################################################
class FutEngine(object):
    """
    交易引擎不间断运行。开市前，重新初始化引擎，并加载数据；闭市后，保存数据到文件。
    收到交易指令后，传给交易路由，完成实际下单交易。
    """

    #----------------------------------------------------------------------
    def __init__(self,dss,minx,gateway):
        """Constructor"""

        self.dss = dss
        self.minx = minx
        self.gateway = gateway

        self.portfolio_list = []
        self.vtSymbol_dict = {}

        # 加载配置
        config = open(get_dss()+'fut/cfg/config.json')
        setting = json.load(config)
        symbols = setting['symbols']
        self.vtSymbol_list = symbols.split(',')

        # 开启bar监听服务
        #threading.Thread( target=self.bar_service, args=() ).start()
        threading.Thread( target=self.put_service, args=() ).start()

    #----------------------------------------------------------------------
    def init_daily(self):
        """每日初始化交易引擎"""

        self.portfolio_list = []
        self.loadPortfolio(Fut_AtrRsiPortfolio, 'AtrRsi')

    #----------------------------------------------------------------------
    def loadPortfolio(self, PortfolioClass, name):
        """加载投资组合"""
        to_log('in FutEngine.loadPortfolio')

        p = PortfolioClass(self, name)
        p.init()
        p.loadParam()
        self.portfolio_list.append(p)

    # 进程间通信接口，暂未启用-------------------------------------------------
    def bar_service(self):
        print('in bar_svervice')

        r, dt = is_trade_day()
        if r == False:
            return

        address = ('localhost', SOCKET_BAR)
        while True:
            with Listener(address, authkey=b'secret password') as listener:
                with listener.accept() as conn:
                    #print('connection accepted from', listener.last_accepted)
                    s = conn.recv()
                    d = eval(s)
                    bar = VtBarData()
                    bar.__dict__ = d
                    for p in self.portfolio_list:
                        p.onBar(bar)
                        #threading.Thread( target=p.onBar, args=(bar,) ).start()

                        #threading.Thread( target=self.onBar, args=(bar,) ).start()
                        #self.onBar(bar)


    # 文件通信接口  -----------------------------------------------------------
    def put_service(self):
        print('in put_svervice')
        while True:
            time.sleep(1)
            for id in self.vtSymbol_list:
                try:
                    fname = self.dss + 'fut/put/'+ self.minx + '_' + id + '.csv'
                    #print(fname)
                    df = pd.read_csv(fname)
                    d = dict(df.loc[0,:])
                    #print(d)
                    #print(type(d))
                    bar = VtBarData()
                    bar.__dict__ = d
                    bar.vtSymbol = id
                    bar.symbol = id
                    if id not in self.vtSymbol_dict:
                        self.vtSymbol_dict[id] = bar
                    elif self.vtSymbol_dict[id].time != bar.time:
                        self.vtSymbol_dict[id] = bar
                        #self.onBar(bar)
                        for p in self.portfolio_list:
                            p.onBar(bar)
                except Exception as e:
                    print('error，读取文件错误')
                    print(e)


    #----------------------------------------------------------------------
    def onBar(self, bar):
        print('in On_Bar')
        print(bar.__dict__)
        print(type(bar))

    #----------------------------------------------------------------------
    def _bc_loadInitBar(self, vtSymbol, initBars):
        """反调函数，因引擎知道数据在哪，初始化Bar数据，"""
        r = []
        try:
            today = time.strftime('%Y%m%d',time.localtime())
            fname = self.dss + 'fut/bar/' + self.minx + '_' + vtSymbol + '.csv'
            #print(fname)
            df = pd.read_csv(fname)
            df = df.sort_values(by=['date','time'])
            df = df.iloc[-initBars:]
            #print(df)

            for i, row in df.iterrows():
                d = dict(row)
                #print(d)
                #print(type(d))
                bar = VtBarData()
                bar.__dict__ = d
                r.append(bar)
        except Exception as e:
            print('error ')
            print(e)

        return r

    #----------------------------------------------------------------------
    def _bc_sendOrder(self, vtSymbol, direction, offset, price, volume, pfName):
        """记录交易数据（由portfolio调用）"""

        # 记录成交数据
        dt = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        time.sleep(0.1)
        order_id = str(int(time.time()))

        #发单到真实交易路由
        self.gateway._bc_sendOrder(vtSymbol, direction, offset, price, volume, pfName)

        r = [[dt,pfName,order_id,self.minx,vtSymbol, direction, offset, price, volume]]
        print('send order: ', r)
        fn = 'fut/deal.csv'
        a_file(fn, str(r)[2:-2])

    #----------------------------------------------------------------------
    def worker_open(self):
        """盘前加载配置及数据"""
        to_log('in FutEngine.worker_open')
        print('in worker_open')

        self.init_daily()

    #----------------------------------------------------------------------
    def worker_close(self):
        """盘后保存及展示数据"""
        to_log('in FutEngine.worker_close')

        print('begin worker_close')

        # 保存信号参数
        for p in self.portfolio_list:
            p.saveParam()

#----------------------------------------------------------------------
def start():
    dss = get_dss()
    engine1 = FutEngine(dss,'min1')
    schedule.every().day.at("20:45").do(engine1.worker_open)
    schedule.every().day.at("15:11").do(engine1.worker_close)

    engine5 = FutEngine(dss,'min5')
    schedule.every().day.at("20:46").do(engine5.worker_open)
    schedule.every().day.at("15:12").do(engine5.worker_close)

    engine15 = FutEngine(dss,'min15')
    schedule.every().day.at("20:47").do(engine15.worker_open)
    schedule.every().day.at("15:13").do(engine15.worker_close)


    print(u'期货交易引擎开始运行')
    while True:
        schedule.run_pending()
        time.sleep(10)


if __name__ == '__main__':
    # start()
    gateway = Gateway_Simnow_CTP()

    dss = get_dss()
    engine15 = FutEngine(dss,'min15',gateway)
    engine15.worker_open()

    dss = get_dss()
    engine5 = FutEngine(dss,'min5',gateway)
    engine5.worker_open()
