
import time
import schedule
import threading
import traceback

from nature import CtpTrade
from nature import CtpQuote
from nature import DirectType, OffsetType
from nature.strategy import DIRECTION_LONG, DIRECTION_SHORT, OFFSET_OPEN, OFFSET_CLOSE

def get_exchangeID(symbol):
    # 上期-SHFE, 中金-CFFEX, 大商-DCE, 能源所-INE、郑商所-CZCE
    pz = symbol[:2]
    if pz.isalpha():
        pass
    else:
        pz = symbol[:1]

    r = ''
    if pz in ['c']:
        r = 'DCE'
    if pz in ['CF','SR']:
        r = 'CZCE'
    if pz in ['ag','rb']:
        r = 'SHFE'
    return r

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

        threading.Thread(target=self.start, args=()).start()

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

            if direction == DIRECTION_LONG and offset == '开仓':
                self.t.ReqOrderInsert(code, DirectType.Buy, OffsetType.Open, price, volume, exchangeID)
            if direction == DIRECTION_SHORT and offset == '开仓':
                self.t.ReqOrderInsert(code, DirectType.Sell, OffsetType.Open, price, volume, exchangeID)
            if direction == DIRECTION_LONG and offset == '平仓':
                self.t.ReqOrderInsert(code, DirectType.Buy, OffsetType.Close, price, volume, exchangeID)
            if direction == DIRECTION_SHORT and offset == '平仓':
                self.t.ReqOrderInsert(code, DirectType.Sell, OffsetType.Close, price, volume, exchangeID)
        except Exception as e:
            now = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
            print(now)
            print('-'*60)
            traceback.print_exc()

            # s = traceback.format_exc()
            # to_log(s)


    #----------------------------------------------------------------------
    def start(self):
        schedule.every().day.at("20:56").do(self.run)
        schedule.every().day.at("15:06").do(self.release)

        print(u'gateway_ht_ctp 路由期货交易接口开始运行')
        while True:
            schedule.run_pending()
            time.sleep(10)


if __name__ == "__main__":
    pass