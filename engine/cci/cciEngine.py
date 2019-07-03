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

from nature import to_log, is_trade_day,send_email
from nature import VtBarData, DIRECTION_LONG, DIRECTION_SHORT
from nature import Book
from nature import CciPortfolio, GatewayPingan, TradeEngine
#from ipdb import set_trace

from nature import get_stk_hfq, get_trading_dates, get_adj_factor

SIZE_DICT = {}
PRICETICK_DICT = {}
VARIABLE_COMMISSION_DICT = {}
FIXED_COMMISSION_DICT = {}
SLIPPAGE_DICT = {}

########################################################################
class CciEngine(TradeEngine):
    """
    交易引擎不间断运行。开市前，重新初始化引擎，并加载数据；闭市后，保存数据到文件。
    收到交易指令后，传给交易路由，完成实际下单交易。
    """

    #----------------------------------------------------------------------
    def __init__(self,dss,gateway):
        """Constructor"""
        to_log('in CciEngine.__init__')
        TradeEngine.__init__(self, dss, gateway)

    #----------------------------------------------------------------------
    def worker_1430(self):
        """盘前加载配置及数据"""
        to_log('in TradeEngine.worker_1430')

        print('begin worker_0300')
        r, dt = is_trade_day()
        if r == False:
            return

        self.init_daily()
        self.currentDt = dt
        self.loadPortfolio(CciPortfolio, 'cci')
        self.loadHold()
        self.loadData()
        self.load_signal_param()


    #----------------------------------------------------------------------
    def worker_1450(self):
        """盘中推送bar"""
        to_log('in TradeEngine.worker_1450')

        print('begin worker_1450')
        r, dt = self.is_trade_day()
        if r == False:
            return

        self.currentDt = dt
        for vtSymbol in self.vtSymbolList:
            #print(vtSymbol)
            df = None
            i = 0
            while df is None and i<3:
                try:
                    i += 1
                    df = ts.get_realtime_quotes(vtSymbol)
                except Exception as e:
                    print('error get_realtime_quotes')
                    print(e)
                    time.sleep(1)

            if df is None:
                continue

            code = vtSymbol
            df1 = None
            i = 0
            while df1 is None and i<3:
                try:
                    i += 1
                    df1 = get_adj_factor(self.dss, code)
                except Exception as e:
                    print('error adj_factor')
                    print(e)
                    time.sleep(1)
            if df1 is None:
                continue

            factor = float(df1.at[0,'adj_factor'])
            d = df.loc[0,:]
            bar = VtBarData()
            bar.vtSymbol = vtSymbol
            bar.symbol = vtSymbol
            bar.open = float(d['open'])*factor
            bar.high = float(d['high'])*factor
            bar.low = float(d['low'])*factor
            bar.close = float(d['price'])*factor
            bar.close_bfq = float(d['price'])
            date = d['date'].split('-')             #去掉字符串中间的'-'
            date = ''.join(date)
            bar.date = date
            bar.time = '00:00:00'
            bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M:%S')
            bar.volume = float(d['volume'])

            barDict = self.dataDict.setdefault(bar.datetime, OrderedDict())
            barDict[bar.vtSymbol] = bar

            self.portfolio.onBar(bar)

        send_email(self.dss, 'worker_1450处理完毕', '')


    #----------------------------------------------------------------------
    def worker_1500(self):
        """盘后保存及展示数据"""
        to_log('in TradeEngine.worker_1500')

        print('begin worker_1700')
        # 打印当日成交记录
        tradeList = self.getTradeData()
        to_log( '当日成交记录：' + str(tradeList) )

        r = []
        for code in self.portfolio.posDict.keys():
            if self.portfolio.posDict[code] > 0:
                for signal in self.portfolio.signalDict[code]:
                    r.append([code,signal.buyPrice,signal.intraTradeLow,signal.longStop])

        # 参数不一样！！！！！！
        df = pd.DataFrame(r, columns=['code','buyPrice','intraTradeLow','longStop'])
        df.to_csv('signal_param.csv', index=False)


    #----------------------------------------------------------------------
    def run(self):
        """运行"""
        schedule.every().day.at("14:30").do(self.worker_1430)
        schedule.every().day.at("14:50").do(self.worker_1450)
        schedule.every().day.at("15:00").do(self.worker_1500)

        self.output(u'交易引擎开始运行')
        while True:
            schedule.run_pending()
            time.sleep(10)

#----------------------------------------------------------------------
def start():
    dss = '../../../data/'
    engine = CciEngine(dss, GatewayPingan())
    engine.run()

if __name__ == '__main__':
    start()
    # engine = TradeEngine()
    # engine.worker_1430()
    # engine.worker_1450()
    # engine.worker_1500()

    # df = ts.get_realtime_quotes('300408')
    # d = df.loc[0,:]
    # print(type(d))
    # print(d)
