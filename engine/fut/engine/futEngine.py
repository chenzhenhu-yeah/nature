# encoding: UTF-8
from __future__ import print_function

from csv import DictReader
from collections import OrderedDict, defaultdict

import socket
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
import sys

from nature import SOCKET_BAR
from nature import to_log, is_trade_day, send_email, get_dss, get_contract, is_market_date, get_symbols_trade
from nature import VtBarData, DIRECTION_LONG, DIRECTION_SHORT, BarGenerator
from nature import Book, a_file

from nature import Gateway_Ht_CTP, pandian_run
from nature import Fut_AtrRsiPortfolio, Fut_RsiBollPortfolio, Fut_CciBollPortfolio
from nature import Fut_DaLiPortfolio, Fut_DaLictaPortfolio, Fut_TurtlePortfolio
from nature import Fut_OwlPortfolio
from nature import Fut_Aberration_EnhancePortfolio, Fut_Cci_RawPortfolio
from nature import Fut_IcPortfolio, Fut_YuePortfolio
from nature import Fut_AvengerPortfolio, Fut_FollowPortfolio, Fut_RatioPortfolio

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
        self.type = 'trade'
        self.gateway = None                # 路由
        self.portfolio_list = []           # 组合
        self.vtSymbol_list = []            # 品种
        self.working = False
        self.seq_tm = ''
        self.bar_list = []
        self.lock = threading.Lock()

        # 开启bar监听服务
        # threading.Thread( target=self.put_service, args=() ).start()
        threading.Thread( target=self.traded_service, args=() ).start()
        threading.Thread( target=self.bar_listen_service, args=() ).start()
        threading.Thread( target=self.on_bar_service, args=() ).start()

    #----------------------------------------------------------------------
    def init_daily(self):
        """每日初始化交易引擎"""

        # 加载品种
        config = open(get_dss()+'fut/cfg/config.json')
        setting = json.load(config)
        # symbols = setting['symbols_trade']
        # self.vtSymbol_list = symbols.split(',')
        self.vtSymbol_list = get_symbols_trade()
        print(self.vtSymbol_list)

        # 初始化组合
        self.portfolio_list = []

        # if 'symbols_rsiboll' in setting:
        #     symbols = setting['symbols_rsiboll']
        #     if len(symbols) > 0:
        #         rsiboll_symbol_list = symbols.split(',')
        #         self.loadPortfolio(Fut_RsiBollPortfolio, rsiboll_symbol_list)

        # if 'symbols_cciboll' in setting:
        #     symbols = setting['symbols_cciboll']
        #     if len(symbols) > 0:
        #         cciboll_symbol_list = symbols.split(',')
        #         self.loadPortfolio(Fut_CciBollPortfolio, cciboll_symbol_list)

        if 'symbols_dali' in setting:
            symbols = setting['symbols_dali']
            if len(symbols) > 0:
                dali_symbol_list = symbols.split(',')
                self.loadPortfolio(Fut_DaLiPortfolio, dali_symbol_list)

        # if 'symbols_dalicta' in setting:
        #     symbols = setting['symbols_dalicta']
        #     if len(symbols) > 0:
        #         dalicta_symbol_list = symbols.split(',')
        #         for symbol in dalicta_symbol_list:
        #             self.loadPortfolio(Fut_DaLictaPortfolio, [symbol])

        # if 'symbols_atrrsi' in setting:
        #     symbols = setting['symbols_atrrsi']
        #     if len(symbols) > 0:
        #         atrrsi_symbol_list = symbols.split(',')
        #         self.loadPortfolio(Fut_AtrRsiPortfolio, atrrsi_symbol_list)

        # if 'symbols_turtle' in setting:
        #     symbols = setting['symbols_turtle']
        #     if len(symbols) > 0:
        #         turtle_symbol_list = symbols.split(',')
        #         self.loadPortfolio(Fut_TurtlePortfolio, turtle_symbol_list)

        if 'symbols_owl' in setting:
            symbols = setting['symbols_owl']
            if len(symbols) > 0:
                owl_symbol_list = symbols.split(',')
                self.loadPortfolio(Fut_OwlPortfolio, owl_symbol_list)

        # if 'symbols_aberration_enhance' in setting:
        #     symbols = setting['symbols_aberration_enhance']
        #     if len(symbols) > 0:
        #         aberration_enhance_symbol_list = symbols.split(',')
        #         self.loadPortfolio(Fut_Aberration_EnhancePortfolio, aberration_enhance_symbol_list)
        #
        # if 'symbols_cci_raw' in setting:
        #     symbols = setting['symbols_cci_raw']
        #     if len(symbols) > 0:
        #         cci_raw_symbol_list = symbols.split(',')
        #         self.loadPortfolio(Fut_Cci_RawPortfolio, cci_raw_symbol_list)

        if 'symbols_ic' in setting:
            symbols = setting['symbols_ic']
            if len(symbols) > 0:
                ic_symbol_list = symbols.split(',')
            else:
                ic_symbol_list = []
            fn = get_dss() +  'fut/engine/ic/portfolio_ic_param.csv'
            if os.path.exists(fn):
                df = pd.read_csv(fn)
                for i, row in df.iterrows():
                    if row.symbol_g in ic_symbol_list and row.symbol_d in ic_symbol_list:
                        self.loadPortfolio(Fut_IcPortfolio, [row.symbol_g, row.symbol_d])

        if 'symbols_yue' in setting:
            symbols = setting['symbols_yue']
            if len(symbols) > 0:
                yue_symbol_list = symbols.split(',')
            else:
                yue_symbol_list = []
            fn = get_dss() +  'fut/engine/yue/portfolio_yue_param.csv'
            if os.path.exists(fn):
                df = pd.read_csv(fn)
                for i, row in df.iterrows():
                    if row.symbol_a in yue_symbol_list and row.symbol_b in yue_symbol_list:
                        self.loadPortfolio(Fut_YuePortfolio, [row.symbol_a, row.symbol_b])

        if self.seq_tm == 'morning':
            if 'symbols_avenger' in setting:
                symbols = setting['symbols_avenger']
                if len(symbols) > 0:
                    avenger_symbol_list = symbols.split(',')
                else:
                    avenger_symbol_list = []
                fn = get_dss() +  'fut/engine/avenger/portfolio_avenger_param.csv'
                if os.path.exists(fn):
                    df = pd.read_csv(fn)
                    for i, row in df.iterrows():
                        if row.symbol_o in avenger_symbol_list and row.symbol_c in avenger_symbol_list and row.symbol_p in avenger_symbol_list:
                            self.loadPortfolio(Fut_AvengerPortfolio, [row.symbol_o, row.symbol_c, row.symbol_p])

        if 'symbols_follow' in setting:
            symbols = setting['symbols_follow']
            if len(symbols) > 0:
                follow_symbol_list = symbols.split(',')
            else:
                follow_symbol_list = []
            fn = get_dss() +  'fut/engine/follow/portfolio_follow_param.csv'
            if os.path.exists(fn):
                df = pd.read_csv(fn)
                for i, row in df.iterrows():
                    if row.symbol_o in follow_symbol_list and row.symbol_c in follow_symbol_list and row.symbol_p in follow_symbol_list:
                        self.loadPortfolio(Fut_FollowPortfolio, [row.symbol_o, row.symbol_c, row.symbol_p])

        if 'symbols_ratio' in setting:
            symbols = setting['symbols_ratio']
            if len(symbols) > 0:
                ratio_symbol_list = symbols.split(',')
            else:
                ratio_symbol_list = []
            fn = get_dss() +  'fut/engine/ratio/portfolio_ratio_param.csv'
            if os.path.exists(fn):
                df = pd.read_csv(fn)
                for i, row in df.iterrows():
                    if row.symbol_c in ratio_symbol_list and row.symbol_p in ratio_symbol_list:
                        self.loadPortfolio(Fut_RatioPortfolio, [row.symbol_c, row.symbol_p])

        # 初始化路由
        self.gateway = Gateway_Ht_CTP()
        self.gateway.open()

    #----------------------------------------------------------------------
    def loadPortfolio(self, PortfolioClass, symbol_list):
        """加载投资组合"""
        try:
            p = PortfolioClass(self, symbol_list, {})
            p.daily_open()
            self.portfolio_list.append(p)

        except Exception as e:
            s = traceback.format_exc()
            to_log(s)
            to_log('加载投资组合出现异常')

    #----------------------------------------------------------------------
    def traded_service(self):
        print('成交回报线程开始工作')

        fn_trade = self.dss + 'fut/engine/gateway_trade.csv'
        df_trade = pd.read_csv(fn_trade)
        n = len(df_trade)
        tradeid_list = []

        while True:
            time.sleep(23)
            if self.working == True:
                df_trade = pd.read_csv(fn_trade, skiprows=n)
                df_trade.columns = ['Direction','ExchangeID','InstrumentID','Offset','OrderID','Price','SysID','TradeID','TradeTime','TradingDay','Volume']
                # print(df_trade)
                for i, row in df_trade.iterrows():
                    n += 1
                    if row.TradeID in tradeid_list:
                        continue
                    else:
                        print( '分发成交回报，TradeID: '+str(row.TradeID)+'， symbol: '+str(row.InstrumentID) )
                        tradeid_list.append(row.TradeID)
                        for p in self.portfolio_list:
                            p.on_trade( {'symbol':row.InstrumentID,'direction':row.Direction,'offset':row.Offset,'price':row.Price,'volume':row.Volume} )
            else:
                tradeid_list = []


    # 进程间通信接口------------------------------------------------------------
    # def bar_listen_service(self): 进程通信接口总出现抛异常的情况
    #     print('bar_listen_svervice 线程开始工作')
    #
    #     address = ('localhost', SOCKET_BAR)
    #     while True:
    #         try :
    #             with Listener(address, authkey=b'secret password') as listener:
    #                 with listener.accept() as conn:
    #                     b = conn.recv_bytes(81920)
    #                     s = str(b, encoding = "utf8")
    #                     # s = conn.recv()
    #                     d = eval(s)
    #                     bar = VtBarData()
    #                     bar.__dict__ = d
    #                     self.lock.acquire()
    #                     self.bar_list.append(bar)
    #                     self.lock.release()
    #         except Exception as e:
    #             r = traceback.format_exc()
    #             to_log(r)

    def bar_listen_service(self):
        print('bar_listen_svervice 线程开始工作')

        server = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        server.bind(('localhost', SOCKET_BAR))
        server.listen(5)

        while True:
            try:
                conn,addr = server.accept()
                b = conn.recv(10240)
                s = str(b, encoding='utf-8')
                # print( 'recive:', len(s) )
                conn.close()

                d = eval(s)
                if d['vtSymbol'] in self.vtSymbol_list:
                    bar = VtBarData()
                    bar.__dict__ = d
                    self.lock.acquire()
                    self.bar_list.append(bar)
                    self.lock.release()
                else:
                    pass

            except Exception as e:
                print('error ')
                r = traceback.format_exc()
                to_log(r)

    # ------------------------------------------------------------------------
    def on_bar_service(self):
        print('on_bar_service 线程开始工作')

        vtSymbol_dict = {}         # 缓存中间bar
        g5 = BarGenerator('min5')
        g15 = BarGenerator('min15')
        g30 = BarGenerator('min30')
        gday = BarGenerator('day')

        while True:
            try:
                self.lock.acquire()
                if len(self.bar_list) > 0:
                    bar = self.bar_list.pop(0)
                    id = bar.vtSymbol
                    self.lock.release()

                    if id not in vtSymbol_dict:
                        vtSymbol_dict[id] = bar
                    elif vtSymbol_dict[id].time != bar.time:
                        vtSymbol_dict[id] = bar

                        bar_day = gday.update_bar(bar)
                        if bar_day is not None:
                            gday.save_bar(bar_day)
                            for p in self.portfolio_list:
                                p.onBar(bar_day, 'day')

                        bar_min30 = g30.update_bar(bar)
                        if bar_min30 is not None:
                            g30.save_bar(bar_min30)
                            for p in self.portfolio_list:
                                p.onBar(bar_min30, 'min30')

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
                else:
                    self.lock.release()
                    time.sleep(1)

            except Exception as e:
                # 对文件并发访问，存着读空文件的可能！！！
                #print('-'*30)
                #traceback.print_exc()
                s = traceback.format_exc()
                to_log(s)

    # 文件通信接口  -----------------------------------------------------------
    def put_service(self):
        print('put_service 线程开始工作')

        vtSymbol_dict = {}         # 缓存中间bar
        g5 = BarGenerator('min5')
        g15 = BarGenerator('min15')
        g30 = BarGenerator('min30')
        gday = BarGenerator('day')

        while True:
            time.sleep(1)
            for id in self.vtSymbol_list:
                try:
                    fname = self.dss + 'fut/put/min1_' + id + '.csv'
                    if os.path.exists(fname):
                        # print(fname)
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

                            bar_day = gday.update_bar(bar)
                            if bar_day is not None:
                                gday.save_bar(bar_day)
                                for p in self.portfolio_list:
                                    p.onBar(bar_day, 'day')

                            bar_min30 = g30.update_bar(bar)
                            if bar_min30 is not None:
                                g30.save_bar(bar_min30)
                                for p in self.portfolio_list:
                                    p.onBar(bar_min30, 'min30')

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
        #fname = self.dss + 'fut/bar/' + minx + '_' + vtSymbol + '.csv'
        fname = self.dss + 'fut/put/rec/' + minx + '_' + vtSymbol + '.csv'
        #print(fname)

        if os.path.exists(fname):
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
        time.sleep(0.01)
        order_id = str(int(time.time()))

        r = [[dt,pfName,order_id,'minx',vtSymbol, direction, offset, price, volume]]
        #print('send order: ', r)
        to_log( str(r)[3:-2] )
        fn = 'fut/engine/engine_deal.csv'
        a_file(fn, str(r)[2:-2])

        priceTick = get_contract(vtSymbol).price_tick
        price = int(round(price/priceTick, 0)) * priceTick

        if self.gateway is not None:
            # self.gateway._bc_sendOrder(dt, vtSymbol, direction, offset, price_deal, volume, pfName)
            threading.Thread( target=self.gateway._bc_sendOrder, args=(dt, vtSymbol, direction, offset, price, volume, pfName) ).start()


    #----------------------------------------------------------------------
    def worker_open(self):
        """盘前加载配置及数据"""
        try:
            if is_market_date() == False:
                self.working = False
                return

            now = datetime.now()
            tm = now.strftime('%H:%M:%S')
            print('-'*60)
            print( 'in worker open, now time is: ', now )
            print('\n')
            if tm > '08:30:00' and tm < '14:30:00':
                self.seq_tm = 'morning'
            if tm > '20:30:00' and tm < '22:30:00':
                self.seq_tm = 'night'

            self.init_daily()
            self.working = True
            time.sleep(600)
            if self.gateway is not None:
                self.gateway.check_risk()

        except Exception as e:
            s = traceback.format_exc()
            to_log(s)

    #----------------------------------------------------------------------
    def worker_close(self):
        """盘后保存及展示数据"""
        try:
            if self.working == False:
                return

            # 保存信号参数
            for p in self.portfolio_list:
                p.daily_close()
            self.portfolio_list = []           # 组合
            self.working = False

            # print('seq_tm: ', self.seq_tm)
            # if self.seq_tm == 'morning':
            #     print('begin pandian_run')
            #     pandian_run()
            #     print('end pandian_run')

            print('begin gateway release ')
            if self.gateway is not None:
                self.gateway.release()
            self.gateway = None                # 路由
            self.vtSymbol_list = []

            now = datetime.now()
            print( 'in worker close, now time is: ', now )

        except Exception as e:
            s = traceback.format_exc()
            to_log(s)

#----------------------------------------------------------------------
def anytime():
    print('期货交易引擎开始运行_anytime')

    e = FutEngine()
    time.sleep(10)
    e.worker_open()
    schedule.every().day.at("15:03").do(e.worker_close)
    schedule.every().day.at("02:31").do(e.worker_close)

    while True:
        schedule.run_pending()
        time.sleep(10)

#----------------------------------------------------------------------
def start():
    print('期货交易引擎开始运行')

    e = FutEngine()

    schedule.every().monday.at("08:56").do(e.worker_open)
    schedule.every().monday.at("15:03").do(e.worker_close)
    schedule.every().monday.at("20:56").do(e.worker_open)
    schedule.every().tuesday.at("02:31").do(e.worker_close)

    schedule.every().tuesday.at("08:56").do(e.worker_open)
    schedule.every().tuesday.at("15:03").do(e.worker_close)
    schedule.every().tuesday.at("20:56").do(e.worker_open)
    schedule.every().wednesday.at("02:31").do(e.worker_close)

    schedule.every().wednesday.at("08:56").do(e.worker_open)
    schedule.every().wednesday.at("15:03").do(e.worker_close)
    schedule.every().wednesday.at("20:56").do(e.worker_open)
    schedule.every().thursday.at("02:31").do(e.worker_close)

    schedule.every().thursday.at("08:56").do(e.worker_open)
    schedule.every().thursday.at("15:03").do(e.worker_close)
    schedule.every().thursday.at("20:56").do(e.worker_open)
    schedule.every().friday.at("02:31").do(e.worker_close)

    schedule.every().friday.at("08:56").do(e.worker_open)
    schedule.every().friday.at("15:03").do(e.worker_close)
    schedule.every().friday.at("20:56").do(e.worker_open)
    schedule.every().saturday.at("02:31").do(e.worker_close)

    while True:
        schedule.run_pending()
        time.sleep(10)

if __name__ == '__main__':

    if len(sys.argv) == 1:
        start()

    if len(sys.argv) == 2:
        if sys.argv[1] == '-anytime':
            anytime()
