# encoding: UTF-8
from __future__ import print_function

from csv import DictReader
from datetime import datetime
from collections import OrderedDict, defaultdict


import schedule
import time
from datetime import datetime
import numpy as np
import tushare as ts
import json

from nature import to_log
from nature import VtBarData, DIRECTION_LONG, DIRECTION_SHORT
from nature import Book
from nature import NearBollPortfolio
#from ipdb import set_trace

from nature import get_stk_hfq, get_trading_dates

SIZE_DICT = {}
PRICETICK_DICT = {}
VARIABLE_COMMISSION_DICT = {}
FIXED_COMMISSION_DICT = {}
SLIPPAGE_DICT = {}

dss = '../../../data/'
# 加载配置
config = open(dss+'csv/config.json')
setting = json.load(config)
pro_id = setting['pro_id']              # 设置服务器
pro = ts.pro_api(pro_id)

########################################################################
class Gateway(object):
    def __init__(self):
        pass

class GatewayPingan(Gateway):
    def __init__(self):
        pass

    def send_instruction(ins_dict):
        address = ('localhost', 9002)
        again = True
        while again:
            time.sleep(1)
            try :
                with Client(address, authkey=b'secret password') as conn:
                    # to_log('stare send ins: '+str(ins_dict))
                    conn.send(ins_dict)
                    again = False
            except:
                pass

    def sendOrder(self, code, direction, offset, price, volume):
        cost = int(price*volume)
        df = ts.get_realtime_quotes(code)
        name = df.at[0,'name']
        ins_dict = {}

        if direction == DIRECTION_LONG:
            ins_dict = {'ins':'buy_order','portfolio':'boll','code':code,'num':volume,'price':price,'cost':cost,'agent':'pingan','name':name}
        if direction == DIRECTION_SHORT:
            ins_dict = {'ins':'sell_order','portfolio':'boll','code':code,'num':volume,'price':price,'cost':cost,'agent':'pingan','name':name}
        if ins_dict != {}:
            self.send_instruction(ins_dict)


########################################################################
class TradeEngine(object):
    """组合类CTA策略回测引擎"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        to_log('in TradeEngine.__init__')

        self.portfolio = None

        # 合约配置信息
        self.vtSymbolList = []
        self.sizeDict = {}                  # 合约大小字典
        self.priceTickDict = {}             # 最小价格变动字典
        self.variableCommissionDict = {}    # 变动手续费字典
        self.fixedCommissionDict = {}       # 固定手续费字典
        self.slippageDict = {}              # 滑点成本字典

        self.cash = 0
        self.portfolioValue = 100E4
        self.currentDt = None

        self.dataDict = OrderedDict()
        self.tradeDict = OrderedDict()

        self.gateway = GatewayPingan()

    #----------------------------------------------------------------------
    def print_engine(self):
        print('self.portfolio:')
        self.portfolio.print_portfolio()

        # 合约配置信息
        print(self.vtSymbolList)
        print(self.sizeDict)            # 合约大小字典
        print(self.priceTickDict)             # 最小价格变动字典
        print(self.variableCommissionDict)    # 变动手续费字典
        print(self.fixedCommissionDict)       # 固定手续费字典
        print(self.slippageDict)              # 滑点成本字典

        print(self.portfolioValue)
        print(self.currentDt)

        print('self.dataDict: ')
        print(len(self.dataDict))
        for k,v in self.dataDict.items():
            print(k)
            print(v)
            break

        print('self.tradeDict: ')
        print(len(self.tradeDict))
        for k,v in self.tradeDict.items():
            print(k)
            print(v)
            break



    #----------------------------------------------------------------------
    def loadHold(self):
        """每日重新加载持仓"""
        to_log('in TradeEngine.loadHold')

        dates = get_trading_dates(dss)
        preday = dates[-2]
        today = dates[-1]
        pfFile = dss + 'csv/hold.csv'
        p1 = Book(dss, pfFile,preday, today)
        self.cash = p1.cash

        for tactic in p1.hold_TacticList:
            if tactic.tacticName == 'boll':
                for hold in tactic.hold_Array:
                    code = hold[0]
                    num = hold[2]
                    self.portfolio.posDict[code] = num

    #----------------------------------------------------------------------
    def loadPortfolio(self):
        """每日更新投资组合"""
        to_log('in TradeEngine.loadPortfolio')

        self.portfolio = NearBollPortfolio(self)
        filename = dss + 'csv/setting.csv'

        with open(filename,encoding='utf-8') as f:
            r = DictReader(f)
            for d in r:
                self.vtSymbolList.append(d['vtSymbol'])

                SIZE_DICT[d['vtSymbol']] = int(d['size'])
                PRICETICK_DICT[d['vtSymbol']] = float(d['priceTick'])
                VARIABLE_COMMISSION_DICT[d['vtSymbol']] = float(d['variableCommission'])
                FIXED_COMMISSION_DICT[d['vtSymbol']] = float(d['fixedCommission'])
                SLIPPAGE_DICT[d['vtSymbol']] = float(d['slippage'])

        self.portfolio.init(self.portfolioValue, self.vtSymbolList, SIZE_DICT)

        self.output(u'投资组合的合约代码%s' %(self.vtSymbolList))

    #----------------------------------------------------------------------
    def loadData(self):
        """每日重新加载数据"""
        to_log('in TradeEngine.loadData')

        dataDict = OrderedDict()

        for vtSymbol in self.vtSymbolList:
            df = get_stk_hfq(dss, vtSymbol)
            df = df.sort_values(['date'])
            for i, d in df.iterrows():
                #print(d)
                #set_trace()

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
                #print(bar.datetime)
                #return

                barDict = self.dataDict.setdefault(bar.datetime, OrderedDict())
                barDict[bar.vtSymbol] = bar

        self.output(u'全部数据加载完成')


    #----------------------------------------------------------------------
    def is_trade_day(self):
        #now = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0)
        now = datetime.now()
        today = now.strftime('%Y%m%d')
        today = datetime.strptime(today + ' ' + '00:00:00', '%Y%m%d %H:%M:%S')
        weekday = int(now.strftime('%w'))
        #print(weekday)

        if 0 <= weekday <= 6:
            return True, today
        else:
            return False, today

    #----------------------------------------------------------------------
    def worker_0300(self):
        to_log('in TradeEngine.worker_0300')

        print('begin worker_0300')
        r, dt = self.is_trade_day()
        if r == False:
            return

        self.currentDt = dt
        self.loadPortfolio()
        self.loadHold()
        self.loadData()


    #----------------------------------------------------------------------
    def worker_1450(self):
        to_log('in TradeEngine.worker_1450')

        print('begin worker_1450')
        r, dt = self.is_trade_day()
        if r == False:
            return

        self.currentDt = dt


        #print(type(dt))
        #print(dt)
        #barDict = self.dataDict[dt]
        for vtSymbol in self.vtSymbolList:
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
            if code[0] == '6':
                code += '.SH'
            else:
                code += '.SZ'

            df1 = None
            i = 0
            while df1 is None and i<3:
                try:
                    i += 1
                    df1 = pro.adj_factor(ts_code=code, trade_date='')
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

            #bar.print_bar()
            #set_trace()

    #----------------------------------------------------------------------
    def worker_1700(self):
        to_log('in TradeEngine.worker_1700')

        print('begin worker_1700')
        tradeList = self.getTradeData()
        print(tradeList)

    #----------------------------------------------------------------------
    def run(self):
        """运行"""
        schedule.every().day.at("14:30").do(self.worker_0300)
        schedule.every().day.at("14:50").do(self.worker_1450)
        schedule.every().day.at("15:00").do(self.worker_1700)

        self.output(u'交易引擎开始运行')
        while True:
            schedule.run_pending()
            time.sleep(10)

    #----------------------------------------------------------------------
    def sendOrder(self, vtSymbol, direction, offset, price, volume):
        """记录交易数据（由portfolio调用）"""
        # 对价格四舍五入
        priceTick = PRICETICK_DICT[vtSymbol]
        price = int(round(price/priceTick, 0)) * priceTick

        # 记录成交数据
        trade = TradeData(vtSymbol, direction, offset, price, volume)
        l = self.tradeDict.setdefault(self.currentDt, [])
        l.append(trade)

        print('send order: ', vtSymbol, direction, offset, price, volume )# 此处还应判断cash
        self.gateway(vtSymbol, direction, offset, price, volume) #发单到真实交易路由


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


########################################################################
class TradeData(object):
    """"""

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
def formatNumber(n):
    """格式化数字到字符串"""
    rn = round(n, 2)        # 保留两位小数
    return format(rn, ',')  # 加上千分符

def start():
    engine = TradeEngine()
    engine.run()

if __name__ == '__main__':
    start()
    # engine = TradeEngine()
    # engine.worker_0300()
    # engine.worker_1450()
    # engine.worker_1700()

    # df = ts.get_realtime_quotes('300408')
    # d = df.loc[0,:]
    # print(type(d))
    # print(d)
