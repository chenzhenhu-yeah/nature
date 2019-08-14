# encoding: UTF-8
from __future__ import print_function

from csv import DictReader
from datetime import datetime
from collections import OrderedDict, defaultdict
import schedule
import time
import numpy as np
import pandas as pd
import tushare as ts
import json

from nature import to_log, is_trade_day, send_email
from nature import VtBarData, DIRECTION_LONG, DIRECTION_SHORT
from nature import get_stk_hfq, get_trading_dates, get_adj_factor, get_hfq_factor
from nature import Book
from nature import stk_NearBollPortfolio, GatewayPingan

########################################################################
class StkEngine(object):
    """
    交易引擎不间断运行。开市前，重新初始化引擎，并加载数据；闭市后，保存数据到文件。
    收到交易指令后，传给交易路由，完成实际下单交易。
    """

    #----------------------------------------------------------------------
    def __init__(self,dss,gateway):
        """Constructor"""
        self.dss = dss
        self.gateway = gateway
        self.portfolio_list = []

    #----------------------------------------------------------------------
    def init_daily(self):
        """每日初始化交易引擎"""
        self.portfolio_list = []
        self.loadPortfolio(stk_NearBollPortfolio, 'boll')

        r, dt = is_trade_day()
        self.currentDt = dt

        self.tradeDict = OrderedDict()

    #----------------------------------------------------------------------
    def loadPortfolio(self, PortfolioClass, name):
        """每日重新加载投资组合"""

        p = PortfolioClass(self, name)
        p.init()
        self.portfolio_list.append(p)

    #----------------------------------------------------------------------
    def _bc_loadInitBar(self, vtSymbol, initBars):
        """读取Bar数据，"""
        r = []
        #df = get_stk_hfq(self.dss, vtSymbol, begin_date=None, end_date='2019-07-18')
        df = get_stk_hfq(self.dss, vtSymbol, begin_date=None, end_date=None)
        if df is not None:
            df = df.iloc[:initBars]
            df = df.sort_values('date')
            for i, row in df.iterrows():
                d = dict(row)
                #print(d)
                bar = VtBarData()
                bar.vtSymbol = vtSymbol
                bar.symbol = vtSymbol
                bar.open = float(d['open'])
                bar.high = float(d['high'])
                bar.low = float(d['low'])
                bar.close = float(d['close'])
                date = d['date'].split('-')             #去掉字符串中间的'-'
                date = ''.join(date)
                bar.date = date
                bar.time = '00:00:00'
                bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M:%S')
                bar.volume = float(d['volume'])

                r.append(bar)

        return r

    #----------------------------------------------------------------------
    def _bc_sendOrder(self, vtSymbol, direction, offset, price, volume, pfName):
        """记录交易数据（由portfolio调用）"""

        # 记录成交数据
        trade = TradeData(vtSymbol, direction, offset, price, volume)
        l = self.tradeDict.setdefault(self.currentDt, [])
        l.append(trade)

        print('send order: ', vtSymbol, direction, offset, price, volume, pfName )# 此处还应判断cash
        self.gateway._bc_sendOrder(vtSymbol, direction, offset, price, volume, pfName) #发单到真实交易路由

    #----------------------------------------------------------------------
    def output(self, content):
        """输出信息"""
        print(content)

    #----------------------------------------------------------------------
    def getTradeData(self, vtSymbol=''):
        """获取交易数据"""
        tradeList = []

        for l in self.tradeDict.values():
            for trade in l:
                if not vtSymbol:
                    tradeList.append(trade)
                elif trade.vtSymbol == vtSymbol:
                    tradeList.append(trade)

        return tradeList


    #----------------------------------------------------------------------
    def worker_1430(self):
        """盘前加载配置及数据"""
        to_log('in StkEngine.worker_1430')
        self.init_daily()

    #----------------------------------------------------------------------
    def worker_1450(self):
        """盘中推送bar"""
        to_log('in TradeEngine.worker_1450')

        for p in self.portfolio_list:
            for vtSymbol in p.vtSymbolList:
                #print(vtSymbol)
                df = None
                i = 0
                while df is None and i<2:
                    try:
                        i += 1
                        df = ts.get_realtime_quotes(vtSymbol)
                    except Exception as e:
                        print('error get_realtime_quotes')
                        print(e)
                        time.sleep(0.1)

                if df is None:
                    to_log('ignore '+ vtSymbol)
                    continue

                adj_factor = None
                df1 = None
                i = 0
                while df1 is None and i<2:
                    try:
                        i += 1
                        df1 = get_adj_factor(self.dss, vtSymbol)
                        adj_factor = float(df1.at[0,'adj_factor'])
                    except Exception as e:
                        print('error adj_factor ' + vtSymbol)
                        print(e)
                        time.sleep(0.1)
                if df1 is None:
                    to_log('ignore '+ vtSymbol)
                    continue

                hfq_factor = get_hfq_factor(self.dss, vtSymbol)
                factor = hfq_factor
                if adj_factor is not None:
                    if abs((adj_factor-hfq_factor)/adj_factor)> 0.01:    # 差异大，今天有除权
                        factor = adj_factor

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

                #to_log(vtSymbol+' '+d['price']+' * '+str(factor)+' = '+str(bar.close))

                p.onBar(bar)

        send_email(self.dss, 'worker_1450处理完毕', '')


    #----------------------------------------------------------------------
    def worker_1500(self):
        """盘后保存及展示数据"""
        to_log('in TradeEngine.worker_1500')

        print('begin worker_1700')
        # 打印当日成交记录
        tradeList = self.getTradeData()
        to_log( '当日成交记录：' + str(tradeList) )

        # 保存信号参数
        for p in self.portfolio_list:
            p.saveParam()

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

########################################################################
class TradeData(object):
    """实际成交信息"""

    #----------------------------------------------------------------------
    def __init__(self, vtSymbol, direction, offset, price, volume):
        """Constructor"""
        self.vtSymbol = vtSymbol
        self.direction = direction
        self.offset = offset
        self.price = price
        self.volume = volume

    def print_tradedata(self):
        print(self.vtSymbol, self.direction, self.offset,self.price,self.volume)

#----------------------------------------------------------------------
def start():
    dss = '../../../data/'
    engine = StkEngine(dss, GatewayPingan())
    engine.run()

if __name__ == '__main__':
    #try:
        start()

        # dss = '../../../data/'
        # engine = StkEngine(dss, GatewayPingan())
        # engine.worker_1430()
        # engine.worker_1450()
        # engine.worker_1500()
        # print('come here ')
        
    # except Exception as e:
    #     print('error')
    #     print(e)
    #     while True:
    #         time.sleep(300)
