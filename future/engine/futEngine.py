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
from nature import Book
from nature import NearBollPortfolio, GatewayPingan, TradeEngine
#from ipdb import set_trace

from nature import get_stk_hfq, get_trading_dates, get_adj_factor


########################################################################
class FutEngine(object):
    """
    交易引擎不间断运行。开市前，重新初始化引擎，并加载数据；闭市后，保存数据到文件。
    收到交易指令后，传给交易路由，完成实际下单交易。
    """

    #----------------------------------------------------------------------
    def __init__(self,dss):
        """Constructor"""
        to_log('in FutEngine.__init__')
        # TradeEngine.__init__(self, dss, gateway)
        self.portfolio_list = []

        # 开启bar监听服务
        threading.Thread( target=self.bar_service, args=() ).start()

    #----------------------------------------------------------------------
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

    #----------------------------------------------------------------------
    def onBar(self, bar):
        print('in On_Bar')
        print(bar.__dict__)
        print(type(bar))

    #----------------------------------------------------------------------
    def loadPortfolio(self, PortfolioClass, name):
        """每日重新加载投资组合"""
        to_log('in FutEngine.loadPortfolio')

        p = PortfolioClass(self, name)
        p.init()
        self.portfolio_list.append(p)


    #----------------------------------------------------------------------
    def worker_open(self):
        """盘前加载配置及数据"""
        to_log('in FutEngine.worker_open')
        print('in worker_open')

        self.loadPortfolio(AtrRsiPortfolio, 'AtrRsi')

    #----------------------------------------------------------------------
    def worker_close(self):
        """盘后保存及展示数据"""
        to_log('in FutEngine.worker_close')

        print('begin worker_close')
        # 打印当日成交记录
        tradeList = self.getTradeData()
        to_log( '当日成交记录：' + str(tradeList) )

        # 保存信号参数，此项工作应该放到portfolio中去做更好


    #----------------------------------------------------------------------
    def run(self):
        """运行"""
        schedule.every().day.at("20:08").do(self.worker_open)
        schedule.every().day.at("15:50").do(self.worker_close)

        print(u'期货交易引擎开始运行')
        while True:
            schedule.run_pending()
            time.sleep(10)

#----------------------------------------------------------------------
def start():
    dss = '../../../data/'
    engine = FutEngine(dss)
    print('here')
    engine.run()

if __name__ == '__main__':
    start()
    # dss = '../../../data/'
    # engine = BollEngine(dss, GatewayPingan())
    # engine.worker_1430()
    # engine.worker_1450()
    # engine.worker_1500()

    # df = ts.get_realtime_quotes('300408')
    # d = df.loc[0,:]
    # print(type(d))
    # print(d)
