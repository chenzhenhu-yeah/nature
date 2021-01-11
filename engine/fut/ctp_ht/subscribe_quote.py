import socket
import os
import time
from datetime import datetime
import json
import numpy as np
import pandas as pd
import schedule
import threading
from multiprocessing.connection import Listener
from multiprocessing.connection import Client
import traceback
from csv import DictReader
import sys


from nature import CtpTrade
from nature import CtpQuote
from nature import Tick

from nature import VtBarData
from nature import SOCKET_BAR, get_dss, to_log, get_contract, is_market_date
from nature import get_symbols_quote

from nature import SOCKET_GET_TICK
address = ('localhost', SOCKET_GET_TICK)


def get_tick(symbol):
    try :
        with Client(address, authkey=b'secret password') as conn:
            conn.send(symbol)
            r = conn.recv()
            if r != '':
                r = eval(r)
                return r
    except:
        pass

    raise ValueError


########################################################################
class Spread(object):

    #----------------------------------------------------------------------
    def __init__(self):
        self.tm = '00:00:00'
        self.bar_dict = {}
        self.got_dict = {}
        self.symbol_list = []

        self.spread_dict = {}
        self.process_dict = {}
        fn = get_dss() +  'fut/cfg/spread_param.csv'
        df = pd.read_csv(fn)
        for i, row in df.iterrows():
            self.symbol_list.append(row.s0)
            self.symbol_list.append(row.s1)
            self.spread_dict[row.nm] = [row.s0, row.s1]
            self.process_dict[row.nm] = False

        self.symbol_list = list(set(self.symbol_list))
        for symbol in self.symbol_list:
            self.got_dict[symbol] = False

    #----------------------------------------------------------------------
    def proc_spread_bar(self, bar, minx='min1'):
        # 不处理不相关的品种
        if bar.vtSymbol not in self.symbol_list:
            return

        if self.tm != bar.time:
            self.tm = bar.time
            for symbol in self.symbol_list:
                self.got_dict[symbol] = False
            for k in self.process_dict.keys():
                self.process_dict[k] = False

        self.bar_dict[bar.vtSymbol] = bar
        self.got_dict[bar.vtSymbol] = True

        return self.control_in_p(bar)

    #----------------------------------------------------------------------
    def control_in_p(self, bar):
        try:
            for k in self.spread_dict.keys():
                s0 = self.spread_dict[k][0]
                s1 = self.spread_dict[k][1]
                if s0 not in self.got_dict or s1 not in self.got_dict:
                    continue
                if self.got_dict[s0] and self.got_dict[s1] and self.process_dict[k] == False:
                    self.process_dict[k] = True
                    s0 = self.bar_dict[s0]
                    s1 = self.bar_dict[s1]

                    bar_s = VtBarData()
                    bar_s.vtSymbol = k
                    bar_s.symbol = k
                    bar_s.exchange = s0.exchange
                    bar_s.date = s0.date
                    bar_s.time = s0.time

                    bar_s.close = s0.close - s1.close
                    bar_s.AskPrice = s0.AskPrice - s1.BidPrice
                    bar_s.BidPrice = s0.BidPrice - s1.AskPrice

                    # print(bar_s.time, bar_s.vtSymbol, bar_s.close, bar_s.AskPrice, bar_s.BidPrice)
                    df = pd.DataFrame([bar_s.__dict__])
                    cols = ['date','time','open','high','low','close','volume']
                    df = df[cols]
                    fname = get_dss() + 'fut/bar/min1_' + k + '.csv'
                    if os.path.exists(fname):
                        df.to_csv(fname, index=False, mode='a', header=False)
                    else:
                        df.to_csv(fname, index=False, mode='a')

                    return bar_s
        except Exception as e:
            s = traceback.format_exc()
            to_log(s)

        return None


class HuQuote(CtpQuote):
    #----------------------------------------------------------------------

    def __init__(self):
        """Constructor"""
        CtpQuote.__init__(self)

        # 加载配置
        # config = open(get_dss()+'fut/cfg/config.json')
        # setting = json.load(config)
        # symbols = setting['symbols_quote']
        # self.id_list = symbols.split(',')
        self.id_list = get_symbols_quote()

        self.dss = get_dss()
        self.tradeDay = ''
        self.night_day = ''
        self.temp_tradeDay = ''
        self.bar_min1_dict = {}

        self.ticks_dict = {}
        self.tm100 = 0
        self.cc = 0

        self.spread = Spread()

    #----------------------------------------------------------------------
    def _OnRtnDepthMarketData(self, pDepthMarketData):
        """"""
        #print('in _OnRtnDepthMarketData: ', pDepthMarketData.getInstrumentID())
        tick = Tick()

        tick.AskPrice = pDepthMarketData.getAskPrice1()
        tick.AskVolume = pDepthMarketData.getAskVolume1()
        tick.AveragePrice = pDepthMarketData.getAveragePrice()
        tick.BidPrice = pDepthMarketData.getBidPrice1()
        tick.BidVolume = pDepthMarketData.getBidVolume1()
        tick.Instrument = pDepthMarketData.getInstrumentID()
        tick.LastPrice = pDepthMarketData.getLastPrice()
        tick.OpenInterest = pDepthMarketData.getOpenInterest()
        tick.Volume = pDepthMarketData.getVolume()
        # tick.SettlementPrice = pDepthMarketData.getSettlementPrice()

        # 只做为临时变量，因郑交所与其他不一样!!!
        self.temp_tradeDay = pDepthMarketData.getTradingDay()

        tick.UpdateTime = pDepthMarketData.getUpdateTime()
        tick.UpdateMillisec = pDepthMarketData.getUpdateMillisec()
        tick.UpperLimitPrice = pDepthMarketData.getUpperLimitPrice()
        tick.LowerLimitPrice = pDepthMarketData.getLowerLimitPrice()
        tick.PreOpenInterest = pDepthMarketData.getPreOpenInterest()

        tick.PreSettlementPrice = pDepthMarketData.getPreSettlementPrice()
        tick.PreClosePrice = pDepthMarketData.getPreClosePrice()
        tick.OpenPrice = pDepthMarketData.getOpenPrice()
        tick.PreDelta = pDepthMarketData.getPreDelta()
        tick.CurrDelta = pDepthMarketData.getCurrDelta()

        # 非交易时段也会产生少量数据，增加时段管控避免各种意外情况
        if (tick.UpdateTime>='08:59:59' and tick.UpdateTime <= '15:00:01') or \
        (tick.UpdateTime>='20:59:59' and tick.UpdateTime <= '23:59:59') or \
        (tick.UpdateTime>='00:00:00' and tick.UpdateTime <= '02:30:01') :
            #threading.Thread( target=self.OnTick, args=(tick,) ).start()
            #多线程容易出错。

            self.OnTick(tick)

    # 保存tick到文件 -----------------------------------------------------------
    def save_tick_file(self, symbol):

        fname = self.dss + 'fut/tick/tick_' + self.tradeDay + '_' + symbol + '.csv'
        cols = ['Localtime','Instrument','LastPrice','Volume','OpenInterest','AveragePrice', 'UpperLimitPrice', 'LowerLimitPrice', 'PreSettlementPrice', 'AskPrice','AskVolume','BidPrice','BidVolume','UpdateDate','UpdateTime']
        df = pd.DataFrame(self.ticks_dict[symbol], columns=cols)
        if os.path.exists(fname):
            df.to_csv(fname, index=False, mode='a', header=False)
        else:
            df.to_csv(fname, index=False)

        # 清空tick队列
        self.ticks_dict[symbol] = []


    # 缓存tick -----------------------------------------------------------
    def cache_tick(self, f, UpdateDate):
        Localtime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        rec =  [Localtime, f.Instrument, f.LastPrice, f.Volume, f.OpenInterest, f.AveragePrice, f.UpperLimitPrice, f.LowerLimitPrice, f.PreSettlementPrice, f.AskPrice, f.AskVolume, f.BidPrice, f.BidVolume, UpdateDate, f.UpdateTime]

        if f.Instrument in self.ticks_dict:
            self.ticks_dict[f.Instrument].append( rec )
            # print('here2')
        else:
            # 首笔, 建字典键值
            self.ticks_dict[f.Instrument] = [rec]
            # print('here3')

        # 中午及夜盘中段保存tick到文件，减少内存占用
        if (f.UpdateTime >= '11:29:50' and f.UpdateTime <= '11:32:59') or (f.UpdateTime >= '22:59:50' and f.UpdateTime <= '23:02:59') :
            # print('here! ', f.Instrument)
            self.save_tick_file(f.Instrument)


    # 处理收到的tick-----------------------------------------------------------
    def OnTick(self, f: Tick):
        """"""
        # 以白银作为首笔，确定当前的self.tradeDay
        if len(self.tradeDay) != 8:
            if f.Instrument[:2] == 'ag':
                # 赋值后，在此交易时段内保持不变。
                self.tradeDay = self.temp_tradeDay
                print('got ', f.Instrument)
                print('self.tradeDay ', self.tradeDay)

                # 防止意料之外的情况，至今没有找到原因
                if len(self.tradeDay) != 8:
                    print('出现了点意外：self.tradeDay ', self.tradeDay)
                    return
            else:
                # 等待首笔Tick品种为白银
                print('here ', f.Instrument)
                return

        # 夜盘时段，零点前，UpdateDate为当日日期。
        # 夜盘时段，零点后，UpdateDate与tradeDay一致。
        UpdateDate = self.tradeDay[:4] + '-' + self.tradeDay[4:6] + '-' + self.tradeDay[6:8]
        if f.UpdateTime >= '20:59:59':
            if self.night_day == '':
                self.night_day = time.strftime('%Y-%m-%d',time.localtime())
            UpdateDate = self.night_day

        # 缓存Tick
        self.cache_tick(f, UpdateDate)

        # 处理Bar
        self._Generate_Bar_MinOne(f, UpdateDate)

    # 使用socket监听接口实现通信-------------------------------------------------------------
    # def send_bar(self, bar, minx): 进程通信接口总出现抛异常的情况
    #     address = ('localhost', SOCKET_BAR)
    #     try :
    #         with Client(address, authkey=b'secret password') as conn2:
    #             s = str(bar.__dict__)
    #             # print('s: ', len(s))
    #             b = bytes(s, encoding = "utf8")
    #             # print('b: ', len(b))
    #             # conn2.send(s)
    #             conn2.send_bytes(b)
    #
    #     except Exception as e:
    #         print('error，发送太密集')
    #         r = traceback.format_exc()
    #         to_log(r)

    def send_bar(self, bar, minx):
        i = 0
        while i < 2:
            try :
                client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                client.connect(('localhost', SOCKET_BAR))

                s = str(bar.__dict__)
                b = bytes(s, encoding='utf-8')
                client.send(b)
                client.close()
                i += 2
            except Exception as e:
                i += 1
                print('error，发送太密集')
                r = traceback.format_exc()
                to_log(r)

    # 保存新生成的bar到put目录下的相应文件，通过该文件来进行多进程数据的同步-----------------------
    def put_bar(self, bar, minx):
        df = pd.DataFrame([bar.__dict__])
        cols = ['date','time','open','high','low','close','volume','AskPrice','AskVolume','BidPrice','BidVolume','AveragePrice','UpperLimitPrice','LowerLimitPrice','PreSettlementPrice','PreClosePrice','OpenPrice','PreDelta','CurrDelta','PreOpenInterest','OpenInterest']
        df = df[cols]

        fname = self.dss + 'fut/put/' + minx + '_' + bar.vtSymbol + '.csv'
        df.to_csv(fname, index=False)

    # 新接收tick后，加工生成Bar--------------------------------------------------
    def _Generate_Bar_MinOne(self, tick, UpdateDate):
        """生成、推送、保存Bar"""
        new_bar = VtBarData()
        new_bar.date = UpdateDate
        new_bar.time = tick.UpdateTime
        new_bar.vtSymbol = tick.Instrument
        new_bar.open = tick.LastPrice
        new_bar.high = tick.LastPrice
        new_bar.low =  tick.LastPrice
        new_bar.close = tick.LastPrice

        # 上一个bar存储在变量bar中，最新bar存储在变量new_bar中，
        id = new_bar.vtSymbol
        if id in self.bar_min1_dict:
            bar = self.bar_min1_dict[id]
        else:
            bar = new_bar
            self.bar_min1_dict[id] = bar
            return

        if bar.time[3:5] != new_bar.time[3:5] :
            # 将 bar的分钟改为整点，推送并保存bar
            bar.date = new_bar.date
            bar.time = new_bar.time[:-2] + '00'

            self.send_bar(bar, 'min1')
            bar_spread = self.spread.proc_spread_bar(bar, 'min1')
            if bar_spread is not None:
                self.send_bar(bar_spread, 'min1')

            if bar.vtSymbol[:2] == 'ag':
                self.put_bar(bar, 'min1')

            bar.open = new_bar.open
            bar.high = new_bar.high
            bar.low = new_bar.low
            bar.close = new_bar.close
        else:
            # 分钟内，仅更新数据即可
            if bar.high < new_bar.high:
                bar.high = new_bar.high
            if bar.low > new_bar.low:
                bar.low =  new_bar.low
            bar.close = new_bar.close

            bar.volume = tick.Volume
            bar.OpenInterest = tick.OpenInterest

            bar.AskPrice = tick.AskPrice
            bar.BidPrice = tick.BidPrice
            bar.AskVolume = tick.AskVolume
            bar.BidVolume = tick.BidVolume

            bar.AveragePrice = tick.AveragePrice
            bar.UpperLimitPrice = tick.UpperLimitPrice
            bar.LowerLimitPrice = tick.LowerLimitPrice
            bar.PreOpenInterest = tick.PreOpenInterest

            bar.PreSettlementPrice = tick.PreSettlementPrice
            bar.PreClosePrice = tick.PreClosePrice
            bar.OpenPrice = tick.OpenPrice
            bar.PreDelta = tick.PreDelta
            bar.CurrDelta = tick.CurrDelta

        self.bar_min1_dict[id] = bar


class TestQuote(object):
    """TestQuote"""

    def __init__(self, addr: str, broker: str, investor: str, pwd: str):
        """"""
        self.front = addr
        self.broker = broker
        self.investor = investor
        self.pwd = pwd
        self.q = None
        self.working = False
        self.dss = get_dss()

        threading.Thread( target=self.get_tick_service, args=() ).start()

    #----------------------------------------------------------------------
    def get_tick_service(self):
        print('in get_tick_service ')

        while True:
            try:
                with Listener(address, authkey=b'secret password') as listener:
                    with listener.accept() as conn:
                        s = conn.recv()
                        tick = ''
                        if self.q is not None:
                            if s in self.q.ticks_dict:
                                tick = self.q.ticks_dict[s][-1]
                                cols = ['Localtime','Instrument','LastPrice','Volume','OpenInterest','AveragePrice', 'UpperLimitPrice', 'LowerLimitPrice', 'PreSettlementPrice', 'AskPrice','AskVolume','BidPrice','BidVolume','UpdateDate','UpdateTime']
                                tick = dict(zip(cols, tick))
                        else:
                            now = datetime.now()
                            today = now.strftime('%Y%m%d')
                            fn = self.dss + 'fut/tick/tick_' + today + '_' + s + '.csv'
                            if os.path.exists(fn):
                                df = pd.read_csv(fn)
                                rec = df.iloc[-1,:]
                                tick = dict(rec)

                        conn.send( str(tick) )
            except Exception as e:
                # print(e)
                s = traceback.format_exc()
                to_log(s)

    #----------------------------------------------------------------------
    def run(self):
        if is_market_date() == False:
            self.working = False
            return

        now = datetime.now()
        print('-'*60)
        print( 'in run, now time is: ', now )
        print('\n')

        time.sleep(3)
        del self.q
        time.sleep(3)

        self.q = HuQuote()
        self.q.OnConnected = lambda x: self.q.ReqUserLogin(self.investor, self.pwd, self.broker)
        self.q.OnUserLogin = lambda o, i: self.subscribe_ids(self.q.id_list)

        self.q.ReqConnect(self.front)
        self.working = True

    def subscribe_ids(self, ids):
        for id in ids:
            self.q.ReqSubscribeMarketData(id)

    def release(self):
        if self.working == False:
            return

        # 对quote的 ReqUserLogout方法做了修改
        self.q.ReqUserLogout()
        self.working = False

        for symbol in self.q.ticks_dict.keys():
            self.q.save_tick_file(symbol)

        now = datetime.now()
        print( 'in release, now time is: ', now )
        self.q = None


    #----------------------------------------------------------------------
    def anytime(self):
        print('行情接收器开始运行_anytime')

        self.run()
        schedule.every().day.at("15:02").do(self.release)
        schedule.every().day.at("02:32").do(self.release)

        while True:
            schedule.run_pending()
            time.sleep(10)

    #----------------------------------------------------------------------
    def daily_worker(self):
        print('行情接收器开始运行_daily_worker')

        schedule.every().monday.at("08:48").do(self.run)
        schedule.every().monday.at("15:02").do(self.release)
        schedule.every().monday.at("20:48").do(self.run)
        schedule.every().tuesday.at("02:32").do(self.release)

        schedule.every().tuesday.at("08:48").do(self.run)
        schedule.every().tuesday.at("15:02").do(self.release)
        schedule.every().tuesday.at("20:48").do(self.run)
        schedule.every().wednesday.at("02:32").do(self.release)

        schedule.every().wednesday.at("08:48").do(self.run)
        schedule.every().wednesday.at("15:02").do(self.release)
        schedule.every().wednesday.at("20:48").do(self.run)
        schedule.every().thursday.at("02:32").do(self.release)

        schedule.every().thursday.at("08:48").do(self.run)
        schedule.every().thursday.at("15:02").do(self.release)
        schedule.every().thursday.at("20:48").do(self.run)
        schedule.every().friday.at("02:32").do(self.release)

        schedule.every().friday.at("08:48").do(self.run)
        schedule.every().friday.at("15:02").do(self.release)
        schedule.every().friday.at("20:48").do(self.run)
        schedule.every().saturday.at("02:32").do(self.release)

        while True:
            schedule.run_pending()
            time.sleep(10)

if __name__ == "__main__":
    try:
        # 加载配置
        config = open(get_dss()+'fut/cfg/config.json')
        setting = json.load(config)
        front_trade = setting['front_trade']
        front_quote = setting['front_quote']
        broker = setting['broker']
        investor = ''
        pwd = ''

        qq = TestQuote(front_quote, broker, investor, pwd)

        # print( '参数个数为:', len(sys.argv), '个参数。' )
        # print( '参数列表:', str(sys.argv) )

        if len(sys.argv) == 1:
            qq.daily_worker()

        if len(sys.argv) == 2:
            if sys.argv[1] == '-anytime':
                qq.anytime()

    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

        qq.run()
        input()
