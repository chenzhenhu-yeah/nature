
import os
import time
from datetime import datetime
import json
import pandas as pd
import schedule
import threading
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

# fn = get_dss() + 'fut/cfg/trade_time.csv'
# df_tt = pd.read_csv(fn, dtype='str')
# df_tt = df_tt[df_tt.seq=='1']
# df_tt_2300 = df_tt[df_tt.end=='23:00:59']
# df_tt_0230 = df_tt[df_tt.end=='02:30:59']
#
# pz_2300_list = list(df_tt_2300.symbol)
# pz_0230_list = list(df_tt_0230.symbol)
# print('夜盘结束时间为23:00的品种： ', pz_2300_list)
# print('夜盘结束时间为02:30的品种： ', pz_0230_list)

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

            # t0 = time.time()
            self.OnTick(tick)
            # t1 = time.time()

            # if self.cc < 1000:
            #     self.tm100 += t1-t0
            #     self.cc += 1
            # else:
            #     print(self.tm100)
            #     self.tm100 = 0
            #     self.cc = 0

    # 保存tick到文件 -----------------------------------------------------------
    def save_tick_file_origin(self, f, UpdateDate):
        df = pd.DataFrame([f.__dict__])
        df['Localtime'] = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        df['UpdateDate'] = UpdateDate
        cols = ['Localtime','LastPrice','Instrument','AskPrice','AskVolume','BidPrice','BidVolume','AveragePrice','UpperLimitPrice','LowerLimitPrice','PreSettlementPrice','PreClosePrice','OpenPrice','PreDelta','CurrDelta','PreOpenInterest','OpenInterest','UpdateMillisec','Volume','UpdateDate','UpdateTime']
        df = df[cols]

        fname = self.dss + 'fut/tick/tick_' + self.tradeDay + '_' + f.Instrument + '.csv'
        if os.path.exists(fname):
            df.to_csv(fname, index=False, mode='a', header=False)
        else:
            df.to_csv(fname, index=False, mode='a')

    # 保存tick到文件 -----------------------------------------------------------
    def save_tick_file(self, f, UpdateDate):
        fname = self.dss + 'fut/tick/tick_' + self.tradeDay + '_' + f.Instrument + '.csv'

        df = pd.DataFrame([f.__dict__])
        df['Localtime'] = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        df['UpdateDate'] = UpdateDate
        cols = ['Localtime','LastPrice','Instrument','AskPrice','AskVolume','BidPrice','BidVolume','AveragePrice','UpperLimitPrice','LowerLimitPrice','PreSettlementPrice','PreClosePrice','OpenPrice','PreDelta','CurrDelta','PreOpenInterest','OpenInterest','UpdateMillisec','Volume','UpdateDate','UpdateTime']
        df = df[cols]

        if f.Instrument in self.ticks_dict:
            df1 = self.ticks_dict[f.Instrument]
            df = pd.concat([df1, df])
            self.ticks_dict[f.Instrument] = df
        else:
            # 首次，建文件
            # df.to_csv(fname, index=False, mode='a')
            df.to_csv(fname, index=False)

            # 清空df, 建字典键值
            df = df.drop(index=df.index)
            self.ticks_dict[f.Instrument] = df

        if len(df) >= 360:
            df.to_csv(fname, index=False, mode='a', header=False)

            # 清空df
            df = df.drop(index=df.index)
            self.ticks_dict[f.Instrument] = df

    # 处理收到的tick-----------------------------------------------------------
    def OnTick(self, f: Tick):
        """"""
        # 以白银作为首笔，确定当前的self.tradeDay
        if len(self.tradeDay) != 8:
            if f.Instrument[:2] == 'ag':
                # 赋值后，在此交易时段内保持不变。
                self.tradeDay = self.temp_tradeDay

                # 防止意料之外的情况，至今没有找到原因
                if len(self.tradeDay) != 8:
                    print('self.tradeDay ', self.tradeDay)
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

        # 保存Tick到文件
        self.save_tick_file(f, UpdateDate)
        # self.save_tick_file_origin(f, UpdateDate)

        # 处理Bar
        self._Generate_Bar_MinOne(f, UpdateDate)

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

        # 收盘前，确保委托单能够成功发出！
        # if tick.UpdateTime in ['14:58:57','14:58:58','14:58:59']:
        #     bar.time = '15:00:00'
        #     self.put_bar(bar, 'min1')
        #
        # c = get_contract(bar.vtSymbol)
        # if c.pz in pz_0230_list and tick.UpdateTime in ['02:28:57','02:28:58','02:28:59']:
        #     bar.time = '02:30:00'
        #     self.put_bar(bar, 'min1')
        # elif c.pz in pz_2300_list and tick.UpdateTime in ['22:58:57','22:58:58','22:58:59']:
        #     bar.time = '23:00:00'
        #     self.put_bar(bar, 'min1')

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
            df = self.q.ticks_dict[symbol]
            fname = self.dss + 'fut/tick/tick_' + self.q.tradeDay + '_' + symbol + '.csv'
            df.to_csv(fname, index=False, mode='a', header=False)

        now = datetime.now()
        print( 'in release, now time is: ', now )


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
