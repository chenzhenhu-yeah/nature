

import pandas as pd
import smtplib
from email.mime.text import MIMEText
import schedule
import time
from datetime import datetime
import threading
import traceback
import json
from csv import DictReader
from datetime import datetime
from collections import OrderedDict, defaultdict

from nature import CtpTrade
from nature import CtpQuote
from nature import DirectType, OffsetType
from nature.strategy import DIRECTION_LONG, DIRECTION_SHORT, OFFSET_OPEN, OFFSET_CLOSE
from nature import get_dss, send_email


class Contract(object):
    def __init__(self,pz,size,price_tick,variable_commission,fixed_commission,slippage,exchangeID):
        """Constructor"""
        self.pz = pz
        self.size = size
        self.price_tick = price_tick
        self.variable_commission = variable_commission
        self.fixed_commission = fixed_commission
        self.slippage = slippage
        self.exchangeID = exchangeID

contract_dict = {}
filename_setting_fut = get_dss() + 'fut/cfg/setting_fut_AtrRsi.csv'
with open(filename_setting_fut,encoding='utf-8') as f:
    r = DictReader(f)
    for d in r:
        contract_dict[ d['pz'] ] = Contract(d['pz'],int(d['size']),float(d['priceTick']),float(d['variableCommission']),float(d['fixedCommission']),float(d['slippage']),d['exchangeID'])

def get_contract(symbol):
    pz = symbol[:2]
    if pz.isalpha():
        pass
    else:
        pz = symbol[:1]

    if pz in contract_dict:
        return contract_dict[pz]
    else:
        #return None
        assert False

def get_exchangeID(symbol):
    c = get_contract(symbol)
    return c.exchangeID

class Gateway_Ht_CTP(object):
    def __init__(self):
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
        self.t.OnConnected = self.on_connect
        self.t.OnUserLogin = lambda o, x: print('Trade logon:', x)
        self.t.OnDisConnected = lambda o, x: print(x)
        self.t.OnRtnNotice = lambda obj, time, msg: print(f'OnNotice: {time}:{msg}')
        self.t.OnErrRtnQuote = lambda obj, quote, info: None
        self.t.OnErrRtnQuoteInsert = lambda obj, o: None
        self.t.OnOrder = lambda obj, o: None
        self.t.OnErrOrder = lambda obj, f, info: None
        self.t.OnTrade = lambda obj, o: None
        self.t.OnInstrumentStatus = lambda obj, inst, stat: None

        # threading.Thread(target=self.start, args=()).start()

    def on_connect(self, obj):
        self.t.ReqUserLogin(self.investor, self.pwd, self.broker, self.proc, self.appid, self.authcode)

    def run(self):
        self.t.ReqConnect(self.front)

    def release(self):
        self.t.ReqUserLogout()

    #----------------------------------------------------------------------
    #停止单需要盯min1,肯定要单启一个线程，线程中循环遍历队列（内部变量），无需同步，用List的pop(0)和append来实现。
    #----------------------------------------------------------------------
    def _bc_sendOrder(self, code, direction, offset, price, volume, portfolio):
        try:
            if self.t.logined == False:
                print('ctp trade not login')
                return ''

            exchangeID = get_exchangeID(code)
            if exchangeID == '':
                return 'error'

            # 当前还处于测试阶段，只开1仓，降低价格不成交
            volume = 1

            if direction == DIRECTION_LONG and offset == '开仓':
                price = price*0.97
                self.t.ReqOrderInsert(code, DirectType.Buy, OffsetType.Open, price, volume, exchangeID)
            if direction == DIRECTION_SHORT and offset == '开仓':
                price = price*1.03
                self.t.ReqOrderInsert(code, DirectType.Sell, OffsetType.Open, price, volume, exchangeID)

            # if direction == DIRECTION_LONG and offset == '平仓':
            #     self.t.ReqOrderInsert(code, DirectType.Buy, OffsetType.Close, price, volume, exchangeID)
            # if direction == DIRECTION_SHORT and offset == '平仓':
            #     self.t.ReqOrderInsert(code, DirectType.Sell, OffsetType.Close, price, volume, exchangeID)

            send_email(get_dss(), '开仓', '')

        except Exception as e:
            now = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
            print(now)
            print('-'*60)
            traceback.print_exc()

            # s = traceback.format_exc()
            # to_log(s)

    # #----------------------------------------------------------------------
    # def start(self):
    #     schedule.every().day.at("20:56").do(self.run)
    #     schedule.every().day.at("15:06").do(self.release)
    #
    #     print(u'gateway_ht_ctp 路由期货交易接口开始运行')
    #     while True:
    #         schedule.run_pending()
    #         time.sleep(10)


if __name__ == "__main__":
    pass
