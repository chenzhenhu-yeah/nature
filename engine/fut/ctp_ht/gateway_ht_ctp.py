
import numpy as np
import pandas as pd
import smtplib
from email.mime.text import MIMEText
import schedule
import time
from datetime import datetime
import threading
import traceback
import json
import os
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import CtpTrade
from nature import CtpQuote
from nature import DirectType, OffsetType
from nature.strategy import DIRECTION_LONG, DIRECTION_SHORT, OFFSET_OPEN, OFFSET_CLOSE
from nature import get_dss, send_email, to_log, get_contract

def get_exchangeID(symbol):
    c = get_contract(symbol)
    return c.exchangeID

class Gateway_Ht_CTP(object):
    def __init__(self):
        self.state = 'CLOSE'
        self.lock = threading.Lock()
        self.file_order_length = 0
        self.file_trade_length = 0

        # 加载配置
        config = open(get_dss()+'fut/cfg/config.json')
        setting = json.load(config)

        self.front = setting['front_trade']
        self.broker = setting['broker']
        self.investor = setting['investor']
        self.pwd = setting['pwd']
        self.appid = setting['appid']
        self.authcode = setting['auth_code']
        self.proc = ''

        self.t = CtpTrade()
        self.t._OnRspQryAccount = self.on_qry_account
        self.t.OnConnected = self.on_connect
        self.t.OnUserLogin = lambda o, x: print('Trade logon:', x)
        self.t.OnDisConnected = lambda o, x: print(x)
        self.t.OnRtnNotice = lambda obj, time, msg: print(f'OnNotice: {time}:{msg}')
        self.t.OnErrRtnQuote = lambda obj, quote, info: None
        self.t.OnErrRtnQuoteInsert = lambda obj, o: None
        self.t.OnOrder = lambda obj, o: None
        self.t.OnErrOrder = lambda obj, f, info: None
        #self.t.OnTrade = lambda obj, o: None
        self.t.OnTrade = self.on_trade
        self.t.OnInstrumentStatus = lambda obj, inst, stat: None

        pf = setting['gateway_pf']
        self.pf_dict = eval(pf)

        self.state = 'INITED'


    #----------------------------------------------------------------------
    def on_trade(self, obj, f):
        #print('in on_trade \n{0}'.format(f.__dict__))
        r = [f.__dict__]
        df = pd.DataFrame(r)
        filename = get_dss() +  'fut/engine/gateway_trade.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)


    #----------------------------------------------------------------------
    def on_qry_account(self, pTradingAccount, pRspInfo, nRequestID, bIsLast):
        self.t.OnAccount(pTradingAccount, pRspInfo, nRequestID, bIsLast)
        risk = round( float( self.t.account.Risk ), 2 )
        if self.state == 'OPEN':
            send_email(get_dss(), 'Risk: '+str(risk), ' ')

    #----------------------------------------------------------------------
    def check_risk(self):
        self.t.t.ReqQryTradingAccount(self.broker, self.investor)

    #----------------------------------------------------------------------
    def check_trade(self):
        fn = get_dss() +  'fut/engine/gateway_order.csv'
        df1 = pd.read_csv(fn)
        df1 = df1[self.file_order_length:]

        # 读取发单文件，获得最新的发单记录，按合约、多空、开平字段进行汇总
        if len(df1) > 0:
            g1 = df1.groupby(by=['symbol','direction','offset'])
            r1 = g1.agg({'volume':np.sum})
            r1 = r1.reset_index()
            #print(r1)

        fn = get_dss() +  'fut/engine/gateway_trade.csv'
        df2 = pd.read_csv(fn)
        df2 = df2[self.file_trade_length:]

        # 读取成交文件，获得最新的成交记录，按合约、多空、开平字段进行汇总
        if len(df2) > 0:
            # 处理路由记录存在重复的情况
            df2 = df2.drop_duplicates()
            g2 = df2.groupby(by=['InstrumentID','Direction','Offset'])
            r2 = g2.agg({'Volume':np.sum})
            r2 = r2.reset_index()
            r2.columns = ['symbol','direction','offset','volume']
            #print(r2)

        if len(df1) > 0:
            if len(df2) > 0:
                result = pd.merge(r1, r2, how='left', on=['symbol','direction','offset','volume'], indicator=True)
                #print(result)
                result = result[result._merge == 'left_only']
                #print(result)
                if len(result) > 0:
                    # symbol_name = str(set(result.symbol))
                    # send_email(get_dss(), 'Alert: 发单未成交：' + symbol_name, '')
                    send_email(get_dss(), 'Alert: 发单未成交', '')
                else:
                    to_log('all order dealed')
            else:
                send_email(get_dss(), 'Alert: 发单全部未成交', '')

    #----------------------------------------------------------------------
    def on_connect(self, obj):
        self.t.ReqUserLogin(self.investor, self.pwd, self.broker, self.proc, self.appid, self.authcode)

        i = 0
        # 若出现非正常情况，夜盘最多推迟两小时
        while self.t.account is None and i < 7200:
            time.sleep(1)
            i += 1
        time.sleep(2)

        if i < 3600:
            self.state = 'OPEN'
            print('gateway connected.')
        else:
            print('gateway not connected.')

    def open(self):
        print(self.state)
        if self.state == 'INITED':
            self.t.ReqConnect(self.front)

        time.sleep(9)
        fn = get_dss() +  'fut/engine/gateway_order.csv'
        if os.path.exists(fn):
            df = pd.read_csv(fn)
            self.file_order_length = len(df)

        fn = get_dss() +  'fut/engine/gateway_trade.csv'
        if os.path.exists(fn):
            df = pd.read_csv(fn)
            self.file_trade_length = len(df)

    def release(self):
        if self.state == 'OPEN':
            self.check_trade()
            self.t.ReqUserLogout()

    #----------------------------------------------------------------------
    #停止单需要盯min1,肯定要单启一个线程，线程中循环遍历队列（内部变量），无需同步，用List的pop(0)和append来实现。
    #----------------------------------------------------------------------
    def _bc_sendOrder(self, dt, code, direction, offset, price, volume, portfolio):
        filename = get_dss() +  'gateway_closed.csv'
        if os.path.exists(filename):
            return

        if self.state == 'CLOSE':
            print('gateway closed')
            return

        pz = get_contract(code).pz
        if portfolio in self.pf_dict:
            pz_list = self.pf_dict[portfolio].split(',')
            if (pz in pz_list) or ('*' in pz_list):
                print(portfolio, ' ', pz, ' send order here!')
            else:
                print(portfolio, ' ', pz, ' just test order here!')
                return
        else:
            print(portfolio, ' ', pz, ' just test order here!')
            return

        exchangeID = get_exchangeID(code)
        if exchangeID == '':
            return

        try:
            now = datetime.now()
            now = now.strftime('%H:%M')
            if now in ['10:14', '10:15', '10:16'] and code[:2] not in ['IF', 'IO']:
                time.sleep(960)

            self.lock.acquire()

            # 对价格四舍五入
            priceTick = get_contract(code).price_tick
            price = int(round(price/priceTick, 0)) * priceTick

            r = [[dt, code, direction, offset, price, volume]]
            print( str(r) )
            c = ['datetime', 'symbol', 'direction', 'offset', 'price', 'volume']
            df = pd.DataFrame(r, columns=c)
            filename = get_dss() +  'fut/engine/gateway_order.csv'
            if os.path.exists(filename):
                df.to_csv(filename, index=False, mode='a', header=False)
            else:
                df.to_csv(filename, index=False)

            if direction == DIRECTION_LONG and offset == 'Open':
                self.t.ReqOrderInsert(code, DirectType.Buy, OffsetType.Open, price, volume, exchangeID)
            if direction == DIRECTION_SHORT and offset == 'Open':
                self.t.ReqOrderInsert(code, DirectType.Sell, OffsetType.Open, price, volume, exchangeID)
            if direction == DIRECTION_LONG and offset == 'Close':
                self.t.ReqOrderInsert(code, DirectType.Buy, OffsetType.Close, price, volume, exchangeID)
            if direction == DIRECTION_SHORT and offset == 'Close':
                self.t.ReqOrderInsert(code, DirectType.Sell, OffsetType.Close, price, volume, exchangeID)

        except Exception as e:
            s = traceback.format_exc()
            to_log(s)
            send_email(get_dss(), '交易路由出错', s)

        # 流控
        time.sleep(0.5)
        print('流控')
        self.lock.release()

if __name__ == "__main__":
    # g = Gateway_Ht_CTP()
    # g.run()
    # time.sleep(10)
    # g.check_risk()
    # input()
    pass
