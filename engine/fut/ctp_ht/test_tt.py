
import time

from nature import CtpTrade
from nature import CtpQuote
from nature import DirectType, OffsetType


class TestTrade(object):
    def __init__(self, addr: str, broker: str, investor: str, pwd: str, appid: str, auth_code: str, proc: str):
        self.front = addr
        self.broker = broker
        self.investor = investor
        self.pwd = pwd
        self.appid = appid
        self.authcode = auth_code
        self.proc = proc

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

    def on_connect(self, obj):
        self.t.ReqUserLogin(self.investor, self.pwd, self.broker, self.proc, self.appid, self.authcode)

    def run(self):
        self.t.ReqConnect(self.front)
        # self.t.ReqConnect('tcp://192.168.52.4:41205')

    def release(self):
        self.t.ReqUserLogout()




if __name__ == "__main__":

    # 加载配置
    config = open(get_dss()+'fut/cfg/config.json')
    setting = json.load(config)
    front_trade = setting['front_trade']
    broker = setting['broker']
    investor = setting['investor']
    pwd = setting['pwd']
    appid = setting['appid']
    auth_code = setting['auth_code']
    proc = ''

    tt = TestTrade(front_trade, broker, investor, pwd, appid, auth_code, proc)
    tt.run()
    time.sleep(5)

    # simnow 发单测试成功(2019-09-19), 上期-SHFE, 中金-CFFEX, 大商-DCE, 能源所-INE、郑商所-CZCE
    # 修改了 trade.py 中的 ReqOrderInsert，添加 ExchangeID 字段
    tt.t.ReqOrderInsert('c2001', DirectType.Buy, OffsetType.Open, 1811, 1, 'DCE')


    input()
    tt.release()
    input()
