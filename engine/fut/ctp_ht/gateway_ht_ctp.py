

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
from datetime import datetime
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

        pz= setting['gateway_pz']
        self.pz_list = pz.split(',')
        pf = setting['gateway_pf']
        self.pf_list = pf.split(',')

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
        send_email(get_dss(), 'Risk: '+str(risk), ' ')

    #----------------------------------------------------------------------
    def check_risk(self):
        self.t.t.ReqQryTradingAccount(self.broker, self.investor)

    #----------------------------------------------------------------------
    def on_connect(self, obj):
        self.t.ReqUserLogin(self.investor, self.pwd, self.broker, self.proc, self.appid, self.authcode)

        i = 0
        while self.t.account is None and i < 3600:
            time.sleep(1)
            i += 1
        time.sleep(2)

        if i < 3600:
            self.state = 'OPEN'
            print('gateway connected.')
        else:
            print('gateway not connected.')

    def run(self):
        print(self.state)
        if self.state == 'INITED':
            self.t.ReqConnect(self.front)

    def release(self):
        if self.state == 'OPEN':
            self.t.ReqUserLogout()

    #----------------------------------------------------------------------
    #停止单需要盯min1,肯定要单启一个线程，线程中循环遍历队列（内部变量），无需同步，用List的pop(0)和append来实现。
    #----------------------------------------------------------------------
    def _bc_sendOrder(self, dt, code, direction, offset, price, volume, portfolio):
        self.lock.acquire()
        try:
            filename = get_dss() +  'gateway_closed.csv'
            if os.path.exists(filename):
                return

            if self.state == 'CLOSE':
                print('gateway closed')
                return

            pz = get_contract(code).pz
            if pz in self.pz_list and portfolio in self.pf_list:
                print(pz, portfolio, ' send order here!')
            else:
                print(pz, ' just test order here!')
                return

            exchangeID = get_exchangeID(code)
            if exchangeID == '':
                return 'error'

            # 对价格四舍五入
            priceTick = get_contract(code).price_tick
            price = int(round(price/priceTick, 0)) * priceTick

            r = [[dt, code, direction, offset, price, volume]]
            print( str(r) )
            c = ['datetime', 'symbol', 'direction', 'offset', 'price', 'volume']
            df = pd.DataFrame(r, columns=c)
            filename = get_dss() +  'fut/engine/gateway_deal.csv'
            if os.path.exists(filename):
                df.to_csv(filename, index=False, mode='a', header=False)
            else:
                df.to_csv(filename, index=False)

            if direction == DIRECTION_LONG and offset == '开仓':
                self.t.ReqOrderInsert(code, DirectType.Buy, OffsetType.Open, price, volume, exchangeID)
            if direction == DIRECTION_SHORT and offset == '开仓':
                self.t.ReqOrderInsert(code, DirectType.Sell, OffsetType.Open, price, volume, exchangeID)
            if direction == DIRECTION_LONG and offset == '平仓':
                self.t.ReqOrderInsert(code, DirectType.Buy, OffsetType.Close, price, volume, exchangeID)
            if direction == DIRECTION_SHORT and offset == '平仓':
                self.t.ReqOrderInsert(code, DirectType.Sell, OffsetType.Close, price, volume, exchangeID)

        except Exception as e:
            s = traceback.format_exc()
            to_log(s)
            send_email(get_dss(), '交易路由出错', s)

        # 流控
        time.sleep(1)
        print('流控')
        self.lock.release()

if __name__ == "__main__":
    # g = Gateway_Ht_CTP()
    # g.run()
    # time.sleep(10)
    # g.check_risk()
    # input()
    pass
