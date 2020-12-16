
import os
import pandas as pd
import numpy as np
import talib
import tushare as ts
from csv import DictReader
from collections import OrderedDict, defaultdict
import threading
from datetime import datetime
import traceback

from nature import send_instruction, get_dss, to_log, get_contract

DIRECTION_LONG = 'Buy'
DIRECTION_SHORT = 'Sell'

OFFSET_OPEN = 'Open'
OFFSET_CLOSE = 'Close'
OFFSET_CLOSETODAY = 'Close'
OFFSET_CLOSEYESTERDAY = 'Close'

# 默认空值
EMPTY_STRING = ''
EMPTY_UNICODE = u''
EMPTY_INT = 0
EMPTY_FLOAT = 0.0


########################################################################
class SignalResult(object):
    """一次完整的开平交易"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.unit = 0
        self.entry = 0                  # 开仓均价
        self.exit = 0                   # 平仓均价
        self.pnl = 0                    # 盈亏

    #----------------------------------------------------------------------
    def open(self, price, change):
        """开仓或者加仓"""
        cost = self.unit * self.entry    # 计算之前的开仓成本
        cost += change * price          # 加上新仓位的成本
        self.unit += change              # 加上新仓位的数量
        self.entry = cost / self.unit    # 计算新的平均开仓成本

    #----------------------------------------------------------------------
    def close(self, price):
        """平仓"""
        self.exit = price
        self.pnl = self.unit * (self.exit - self.entry)


########################################################################
class Signal(object):
    """
    策略信号，实现策略逻辑，实例对象对应一支具体证券，属于且仅属于某个组合。
    包含K线容器，此容器用于计算指标。
    """
    def __init__(self, portfolio, vtSymbol, size_am=100):
        """Constructor"""
        self.portfolio = portfolio      # 投资组合
        self.vtSymbol = vtSymbol        # 合约代码
        self.am = ArrayManager(self.initBars, size_am)        # K线容器
        self.bar = None                 # 最新K线
        self.result = None              # 当前的交易
        self.unit = 0
        self.paused = False
        self.order_list = []
        self.lock = threading.Lock()

        # 读取配置文件，看是否已暂停
        filename = get_dss() + 'fut/cfg/signal_pause_var.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = df[df.signal == self.portfolio.name]
            if len(df) > 0:
                symbol_list = str(df.iat[0,1]).split(',')
                if self.vtSymbol in symbol_list:
                    self.paused = True
                    print(self.vtSymbol + ' paused in ' + self.portfolio.name)

        # 载入历史数据，并采用回放计算的方式初始化策略数值
        initData = self.portfolio.engine._bc_loadInitBar(self.vtSymbol, self.initBars, self.minx)
        for bar in initData:
            self.bar = bar
            self.am.updateBar(bar)

        print('完成加载组合 ' + portfolio.name +  ' 品种 ' + vtSymbol + ' 类型 ' + self.type)

    #----------------------------------------------------------------------
    def onBar(self, bar):
        assert False, '子类必须实现此函数'

    #----------------------------------------------------------------------
    def on_trade(self, t):
        pass

    #----------------------------------------------------------------------
    def newSignal(self, direction, offset, price, volume):
        """调用组合中的接口，传递下单指令"""
        self.portfolio._bc_newSignal(self, direction, offset, price, volume)

    #----------------------------------------------------------------------
    def buy(self, price, volume):
        """买入开仓"""
        self.order_list.append( {'direction': 'Buy', 'offset':'Open', 'price':price, 'volume':abs(volume), 'traded':0} )
        self.open(price, volume)
        self.newSignal(DIRECTION_LONG, OFFSET_OPEN, price, volume)

    #----------------------------------------------------------------------
    def sell(self, price, volume):
        """卖出平仓"""
        self.order_list.append( {'direction': 'Sell', 'offset':'Close', 'price':price, 'volume':abs(volume), 'traded':0} )
        self.close(price, -volume)
        self.newSignal(DIRECTION_SHORT, OFFSET_CLOSE, price, volume)

    #----------------------------------------------------------------------
    def short(self, price, volume):
        """卖出开仓"""
        self.order_list.append( {'direction': 'Sell', 'offset':'Open', 'price':price, 'volume':abs(volume), 'traded':0} )
        self.open(price, -volume)
        self.newSignal(DIRECTION_SHORT, OFFSET_OPEN, price, volume)

    #----------------------------------------------------------------------
    def cover(self, price, volume):
        """买入平仓"""
        self.order_list.append( {'direction': 'Buy', 'offset':'Close', 'price':price, 'volume':abs(volume), 'traded':0} )
        self.close(price, volume)
        self.newSignal(DIRECTION_LONG, OFFSET_CLOSE, price, volume)

    #----------------------------------------------------------------------
    def open(self, price, change):
        self.unit += change

        if not self.result:
            self.result = SignalResult()
        self.result.open(price, change)

        r = [ [self.bar.date+' '+self.bar.time, '多' if change>0 else '空', '开',  \
               abs(change), price, 0, self.vtSymbol ] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl', 'symbol'])
        pz = str(get_contract(self.vtSymbol).pz)
        filename = get_dss() +  'fut/engine/'+self.portfolio.name+'/signal_'+self.portfolio.name+'_'+self.type+ '_deal_' + pz + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

    #----------------------------------------------------------------------
    def close(self, price, change):
        self.unit += change
        self.result.close(price)

        r = [ [self.bar.date+' '+self.bar.time, '多' if change>0 else '空', '平',  \
               abs(change), price, self.result.pnl, self.vtSymbol] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl','symbol'])
        pz = str(get_contract(self.vtSymbol).pz)
        filename = get_dss() +  'fut/engine/'+self.portfolio.name+'/signal_'+self.portfolio.name+'_'+self.type+ '_deal_' + pz + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename, index=False, mode='a', header=False)
        else:
            df.to_csv(filename, index=False)

        self.result = None


########################################################################
class Portfolio(object):
    """
    证券组合，包含多个证券标的，每个证券对应一个signal。
    负责在engine与signal之间进行上传下达，并做组合层面的风控。
    。
    """

    #----------------------------------------------------------------------
    def __init__(self, SignalClass1, engine, symbol_list, signal1_param={}, SignalClass2=None, signal2_param={}):
        """Constructor"""
        self.engine = engine                 # 所属引擎
        self.portfolioValue = 100E4          # 组合市值
        self.signalDict = defaultdict(list)  # 信号字典，code为键, signal列表为值
        self.posDict = {}
        self.name_second = self.name
        self.promote = False

        self.result = DailyResult('00-00-00 00:00:00')
        self.resultList = []

        self.vtSymbolList = symbol_list

        # 初始化信号字典、持仓字典
        for vtSymbol in self.vtSymbolList:
            self.posDict[vtSymbol] = 0
            # 每个portfolio可以管理多种类型signal,暂只管理同两种类型的signal
            signal1 = SignalClass1(self, vtSymbol)
            signal1.load_param()

            if vtSymbol in signal1_param:
                param_dict = signal1_param[vtSymbol]
                signal1.set_param(param_dict)

            l = self.signalDict[vtSymbol]
            l.append(signal1)

            if SignalClass2 is not None:
                signal2 = SignalClass2(self, vtSymbol)
                signal2.load_param()

                if vtSymbol in signal2_param:
                    param_dict = signal2_param[vtSymbol]
                    signal2.set_param(param_dict)
                    #print('here')

                l.append(signal2)


        #print('投资组合的合约代码%s' %(self.vtSymbolList))

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min1'):
        """引擎新推送过来bar，传递给每个signal"""

        # 不处理不相关的品种
        if bar.vtSymbol not in self.vtSymbolList:
            return

        if minx != 'min1':
            if self.result.date != bar.date + ' ' + bar.time:
                previousResult = self.result
                self.result = DailyResult(bar.date + ' ' + bar.time)
                self.resultList.append(self.result)
                if previousResult:
                    self.result.updateClose(previousResult.closeDict)

        # 将bar推送给signal
        for signal in self.signalDict[bar.vtSymbol]:
            signal.onBar(bar, minx)

        if minx != 'min1':
            self.result.updateBar(bar)
            self.result.updatePos(self.posDict)

    #----------------------------------------------------------------------
    def check_order_risk(self, signal, price, volume):
        r = None
        if volume > 30:
            r = ' : volume > 30'

        if abs(signal.bar.close - price) > 30:
            r = ' : price gap > 30 '

        if r is not None:
            to_log('order risk check error: ' + signal.vtSymbol + r )
            # raise ValueError

        return None

    #----------------------------------------------------------------------
    def _bc_newSignal(self, signal, direction, offset, price, volume):
        """
        对交易信号进行过滤，符合条件的才发单执行。
        计算真实交易价格和数量。
        """
        # 下单风控
        if self.check_order_risk(signal, price, volume) is not None:
            return

        multiplier = self.portfolioValue * 0.01 / get_contract(signal.vtSymbol).size
        multiplier = int(round(multiplier, 0))
        #print(multiplier)
        multiplier = 1

        #print(self.posDict)
        # 计算合约持仓
        if direction == DIRECTION_LONG:
            self.posDict[signal.vtSymbol] += volume*multiplier
        else:
            self.posDict[signal.vtSymbol] -= volume*multiplier

        #print(self.posDict)

        # 对价格四舍五入
        priceTick = get_contract(signal.vtSymbol).price_tick
        price = int(round(price/priceTick, 0)) * priceTick

        now = datetime.now()
        tm = now.strftime('%H:%M:%S')
        if (tm > '14:45:00' and tm < '15:00:00') or (tm > '22:45:00' and tm < '23:00:00') or self.promote:
            if direction == DIRECTION_LONG:
                price += priceTick
            if direction == DIRECTION_SHORT:
                price -= priceTick

        self.engine._bc_sendOrder(signal.vtSymbol, direction, offset, price, volume*multiplier, self.name)

        # 记录成交数据
        trade = TradeData(self.result.date, signal.vtSymbol, direction, offset, price, volume*multiplier)
        # l = self.tradeDict.setdefault(self.result.date, [])
        # l.append(trade)

        self.result.updateTrade(trade)
        #print('here')

    #----------------------------------------------------------------------
    def on_trade(self, t):
        """引擎新推送过来成交回报，传递给每个signal"""
        # print( self.name, '收到成交回报 ')

        # 不处理不相关的品种
        if t['symbol'] not in self.vtSymbolList:
            return

        # 将bar推送给signal
        for signal in self.signalDict[t['symbol']]:
            signal.on_trade(t)


    #----------------------------------------------------------------------
    def daily_open(self):
        # 从文件中读取posDict、portfolioValue
        filename = self.engine.dss + 'fut/engine/' + self.name +'/portfolio_' + self.name_second + '_var.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = df.sort_values(by='datetime')
            df = df.reset_index()
            if len(df) > 0:
                rec = df.iloc[-1,:]
                self.portfolioValue = rec.portfolioValue
                d = eval(rec.posDict)
                c = eval(rec.closeDict)
                cnow = {}

                #self.posDict.update(d)
                for vtSymbol in self.vtSymbolList:
                    if vtSymbol in d:
                        self.posDict[vtSymbol] = d[vtSymbol]
                    if vtSymbol in c:
                        cnow[vtSymbol] = c[vtSymbol]
                    else:
                        cnow[vtSymbol] = 0

                # 初始化DailyResult
                self.result.updatePos(self.posDict)
                self.result.updateClose(cnow)

        # 所有Signal读取保存到文件的变量
        for code in self.vtSymbolList:
            for signal in self.signalDict[code]:
                signal.load_var()

    #----------------------------------------------------------------------
    def daily_close(self):
        # 保存Signal变量到文件
        for code in self.vtSymbolList:
            for signal in self.signalDict[code]:
                signal.save_var()

        # 计算result
        tr = []
        totalNetPnl = 0
        totalTradingPnl = 0
        totalholdingPnl = 0
        totalCommission = 0
        totalSlippage = 0
        n = len(self.resultList)
        #print(n)
        for i in range(n):
            result = self.resultList[i]
            result.calculatePnl()
            totalNetPnl += result.netPnl
            totalTradingPnl += result.tradingPnl
            totalholdingPnl += result.holdingPnl
            totalCommission += result.commission
            totalSlippage += result.slippage
            #print(result.date, result.tradeCount)

            for vtSymbol, l in result.tradeDict.items():
                for trade in l:
                    tr.append( [trade.vtSymbol,trade.dt,trade.direction,trade.offset,trade.price,trade.volume] )

        # 保存组合交易记录
        df = pd.DataFrame(tr, columns=['vtSymbol','datetime','direction','offset','price','volume'])
        filename = self.engine.dss + 'fut/engine/' + self.name +'/portfolio_' + self.name_second + '_deal.csv'
        if os.path.exists(filename):
            df.to_csv(filename,index=False,mode='a',header=False)
        else:
            df.to_csv(filename,index=False)

        # 保存portfolioValue,posDict,closeDict到文件
        dt = self.result.date
        r = [ [dt, int(self.portfolioValue + totalNetPnl), int(totalNetPnl), int(totalTradingPnl), int(totalholdingPnl), round(totalCommission,2), int(totalSlippage), str(self.posDict), str(self.result.closeDict)] ]
        df = pd.DataFrame(r, columns=['datetime','portfolioValue','netPnl','totalTradingPnl','totalholdingPnl','totalCommission','totalSlippage','posDict','closeDict'])
        filename = self.engine.dss + 'fut/engine/' + self.name +'/portfolio_' + self.name_second + '_var.csv'
        if os.path.exists(filename):
            df.to_csv(filename,index=False,mode='a',header=False)
        else:
            df.to_csv(filename,index=False)


########################################################################
class TradeData(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, dt, vtSymbol, direction, offset, price, volume):
        """Constructor"""
        self.dt = dt
        self.vtSymbol = vtSymbol
        self.direction = direction
        self.offset = offset
        self.price = price
        self.volume = volume


########################################################################
class DailyResult(object):
    """每日的成交记录"""

    #----------------------------------------------------------------------
    def __init__(self, date):
        """Constructor"""
        self.date = date

        self.closeDict = {}                     # 收盘价字典
        self.previousCloseDict = {}             # 昨收盘字典

        self.tradeDict = defaultdict(list)      # 成交字典
        self.posDict = {}                       # 持仓字典（开盘时）

        self.tradingPnl = 0                     # 交易盈亏
        self.holdingPnl = 0                     # 持仓盈亏
        self.totalPnl = 0                       # 总盈亏
        self.commission = 0                     # 佣金
        self.slippage = 0                       # 滑点
        self.netPnl = 0                         # 净盈亏
        self.tradeCount = 0                     # 成交笔数

    #----------------------------------------------------------------------
    def updateTrade(self, trade):
        """更新交易"""
        l = self.tradeDict[trade.vtSymbol]
        l.append(trade)
        self.tradeCount += 1

        #print(self.date, trade.vtSymbol, self.tradeCount)

    #----------------------------------------------------------------------
    def updatePos(self, d):
        """更新昨持仓"""
        self.posDict.update(d)

    #----------------------------------------------------------------------
    def updateBar(self, bar):
        """更新K线"""
        self.closeDict[bar.vtSymbol] = bar.close

    #----------------------------------------------------------------------
    def updateClose(self, d):
        """更新昨收盘"""
        self.previousCloseDict.update(d)
        self.closeDict.update(d)

    #----------------------------------------------------------------------
    def calculateTradingPnl(self):
        """计算当日交易盈亏"""
        for vtSymbol, l in self.tradeDict.items():
            #close = self.closeDict[vtSymbol]
            previousClose = self.previousCloseDict.get(vtSymbol, 0)
            ct = get_contract(vtSymbol)
            size = ct.size
            slippage = ct.slippage
            variableCommission = ct.variable_commission
            fixedCommission = ct.fixed_commission

            for trade in l:
                if trade.direction == DIRECTION_LONG:
                    side = 1
                else:
                    side = -1

                commissionCost = (trade.volume * fixedCommission +
                                  trade.volume * trade.price * size * variableCommission)
                slippageCost = trade.volume * size * slippage
                if previousClose == 0:
                    pnl = 0
                else:
                    pnl = side * (previousClose - trade.price) * trade.volume * size

                self.commission += commissionCost
                self.slippage += slippageCost
                self.tradingPnl += pnl

    #----------------------------------------------------------------------
    def calculateHoldingPnl(self):
        """计算当日持仓盈亏"""
        try:
            for vtSymbol, pos in self.posDict.items():
                previousClose = self.previousCloseDict.get(vtSymbol, 0)
                #close = self.closeDict[vtSymbol]
                #防止出错
                close = self.closeDict.get(vtSymbol, 0)

                if close != 0 and previousClose != 0:
                    ct = get_contract(vtSymbol)
                    size = ct.size
                    pnl = (close - previousClose) * pos * size
                    self.holdingPnl += pnl
                else:
                    # to_log(vtSymbol + ' close value not in result')
                    pass

        except Exception as e:
            s = traceback.format_exc()
            to_log(s)

    #----------------------------------------------------------------------
    def calculatePnl(self):
        """计算总盈亏"""
        self.calculateHoldingPnl()
        self.calculateTradingPnl()
        self.totalPnl = self.holdingPnl + self.tradingPnl
        self.netPnl = self.totalPnl - self.commission - self.slippage

        #print(self.date, self.netPnl, self.holdingPnl, self.tradingPnl, self.commission, self.slippage)

        return self.netPnl

########################################################################
class Gateway(object):
    def __init__(self):
        pass

class GatewayPingan(Gateway):
    def __init__(self):
        pass

    def _bc_sendOrder(self, code, direction, offset, price, volume, portfolio):
        cost = int(price*volume)
        df = ts.get_realtime_quotes(code)
        name = df.at[0,'name']
        ins_dict = {}

        if direction == DIRECTION_LONG:
            ins_dict = {'ins':'buy_order','portfolio':portfolio,'code':code,'num':volume,'price':price,'cost':cost,'agent':'pingan','name':name}
        if direction == DIRECTION_SHORT:
            ins_dict = {'ins':'sell_order','portfolio':portfolio,'code':code,'num':volume,'price':price,'cost':cost,'agent':'pingan','name':name}
        if ins_dict != {}:
            send_instruction(ins_dict)


########################################################################
class Tick:
    """分笔数据"""

    def __init__(self):
        self.Instrument = ''           # 合约代码
        self.LastPrice = 0.0           # 最新价
        self.AskPrice = 0.0            # 挂卖价
        self.BidPrice = 0.0            # 挂买价
        self.AskVolume = 1             # 挂卖量
        self.BidVolume = 1             # 挂买量
        self.UpdateTime = ''           # 时间
        self.UpdateMillisec = 0        # 毫秒
        self.Volume = 1                # 成交量
        self.OpenInterest = 1.0        # 持仓量
        self.AveragePrice = 0.0        # 均价
        self.UpperLimitPrice = 0.0     # 涨板价
        self.LowerLimitPrice = 0.0     # 跌板价
        self.PreOpenInterest = 0.0     # 昨持仓


        self.PreSettlementPrice = 0.0  # 前结算价
        self.PreClosePrice = 0.0       # 前收盘价
        self.OpenPrice = 0.0           # 开盘价
        self.PreDelta = 0.0
        self.CurrDelta = 0.0

########################################################################
class VtBaseData(object):
    """回调函数推送数据的基础类，其他数据类继承于此"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.gatewayName = EMPTY_STRING         # Gateway名称
        self.rawData = None                     # 原始数据

########################################################################
class VtBarData(VtBaseData):
    """K线数据"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtBarData, self).__init__()

        self.vtSymbol = EMPTY_STRING        # vt系统代码
        self.symbol = EMPTY_STRING          # 代码
        self.exchange = EMPTY_STRING        # 交易所

        self.open = EMPTY_FLOAT             # OHLC
        self.high = EMPTY_FLOAT
        self.low = EMPTY_FLOAT
        self.close = EMPTY_FLOAT
        self.close_bfq = EMPTY_FLOAT

        self.date = EMPTY_STRING            # bar开始的时间，日期
        self.time = EMPTY_STRING            # 时间
        self.datetime = None                # python的datetime时间对象

        self.interval = EMPTY_UNICODE       # K线周期

        self.volume = EMPTY_INT             # 成交量
        self.OpenInterest = EMPTY_INT       # 持仓量

        self.AskPrice = 0.0            # 挂卖价
        self.BidPrice = 0.0            # 挂买价
        self.AskVolume = 1             # 挂卖量
        self.BidVolume = 1             # 挂买量

        self.AveragePrice = 0.0        # 均价
        self.UpperLimitPrice = 0.0     # 涨板价
        self.LowerLimitPrice = 0.0     # 跌板价
        self.PreOpenInterest = 0.0     # 昨持仓

        self.PreSettlementPrice = 0.0  # 前结算价
        self.PreClosePrice = 0.0       # 前收盘价
        self.OpenPrice = 0.0           # 开盘价
        self.PreDelta = 0.0
        self.CurrDelta = 0.0

    def print_bar(self):
        print(self.vtSymbol)
        print(self.date)
        print(self.open)
        print(self.high)
        print(self.low)
        print(self.close)




########################################################################
class BarGenerator(object):

    #----------------------------------------------------------------------
    def __init__(self, minx):
        """Constructor"""
        self.minx = minx
        self.bar_dict = {}
        self.r_dict = {}

    #----------------------------------------------------------------------
    def update_bar(self, new_bar):
        symbol = new_bar.vtSymbol

        if symbol in self.r_dict:
            r = self.r_dict[symbol]
        else:
            r = []
            self.r_dict[symbol] = r

        if symbol in self.bar_dict:
            bar = self.bar_dict[symbol]
        else:
            bar = new_bar
            self.bar_dict[symbol] = bar
            return None

        # 更新数据
        if bar.high < new_bar.high:
            bar.high = new_bar.high
        if bar.low > new_bar.low:
            bar.low =  new_bar.low
        bar.close = new_bar.close

        if self.minx == 'min5':
            if new_bar.time[3:5] in ['04','09','14','19','24','29','34','39','44','49','54','59']:
                # 将 bar的秒钟改为整点，推送并保存bar
                bar.date = new_bar.date
                bar.time = new_bar.time[:-2] + '00'
                self.bar_dict.pop(symbol)
                r.append( [bar.date, bar.time, bar.open, bar.high, bar.low, bar.close, 0] )
                return bar

        elif self.minx == 'min15':
            if new_bar.time[3:5] in ['14','29','44','59']:
                bar.date = new_bar.date
                bar.time = new_bar.time[:-2] + '00'
                self.bar_dict.pop(symbol)
                r.append( [bar.date, bar.time, bar.open, bar.high, bar.low, bar.close, 0] )
                return bar

        elif self.minx == 'min30':
            min30_list = ['09:29','09:59','10:44','11:14',
                          '13:44','14:14','14:44','14:59', \
                          '21:29','21:59','22:29','22:59', \
                          '23:29','23:59','00:29','00:59', \
                          '01:29','01:59','02:29']
            if new_bar.time[:5] in min30_list:
                bar.date = new_bar.date
                bar.time = new_bar.time[:-2] + '00'
                self.bar_dict.pop(symbol)
                r.append( [bar.date, bar.time, bar.open, bar.high, bar.low, bar.close, 0] )
                return bar

        elif self.minx == 'day':
            if new_bar.time[:5] in ['14:58']:
                bar.date = new_bar.date
                bar.time = new_bar.time[:-2] + '00'
                self.bar_dict.pop(symbol)
                r.append( [new_bar.date, bar.time, bar.open, bar.high, bar.low, bar.close, 0] )
                return bar

        self.bar_dict[symbol] = bar
        return None

    #----------------------------------------------------------------------
    def save_bar(self, bar):
        df = pd.DataFrame([bar.__dict__])
        cols = ['date','time','open','high','low','close','volume']
        df = df[cols]

        fname = get_dss() + 'fut/put/rec/' + self.minx + '_' + bar.vtSymbol + '.csv'
        if os.path.exists(fname):
            df.to_csv(fname, index=False, mode='a', header=False)
        else:
            df.to_csv(fname, index=False)


########################################################################
class ArrayManager(object):
    """
    K线序列管理工具，负责：
    1. K线时间序列的维护
    2. 常用技术指标的计算
    """

    #----------------------------------------------------------------------
    def __init__(self, initDays=90, size=100):
        """Constructor"""
        self.count = 0                      # 缓存计数
        self.size = size                    # 缓存大小
        self.initDays = initDays
        self.inited = False                 # True if count>=size

        self.openArray = np.zeros(size)     # OHLC
        self.highArray = np.zeros(size)
        self.lowArray = np.zeros(size)
        self.closeArray = np.zeros(size)
        self.volumeArray = np.zeros(size)

    #----------------------------------------------------------------------
    def updateBar(self, bar):
        """更新K线"""
        self.openArray[:-1] = self.openArray[1:]
        self.highArray[:-1] = self.highArray[1:]
        self.lowArray[:-1] = self.lowArray[1:]
        self.closeArray[:-1] = self.closeArray[1:]
        self.volumeArray[:-1] = self.volumeArray[1:]

        self.openArray[-1] = bar.open
        self.highArray[-1] = bar.high
        self.lowArray[-1] = bar.low
        self.closeArray[-1] = bar.close
        self.volumeArray[-1] = bar.volume

        self.count += 1
        if not self.inited and self.count >= self.initDays:
            self.inited = True

    #----------------------------------------------------------------------
    @property
    def open(self):
        """获取开盘价序列"""
        return self.openArray

    #----------------------------------------------------------------------
    @property
    def high(self):
        """获取最高价序列"""
        return self.highArray

    #----------------------------------------------------------------------
    @property
    def low(self):
        """获取最低价序列"""
        return self.lowArray

    #----------------------------------------------------------------------
    @property
    def close(self):
        """获取收盘价序列"""
        return self.closeArray

    #----------------------------------------------------------------------
    @property
    def volume(self):
        """获取成交量序列"""
        return self.volumeArray

    #----------------------------------------------------------------------
    def sma(self, n, array=False):
        """简单均线"""
        result = talib.SMA(self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def ema(self, n, array=False):
        """指数平均数指标"""
        result = talib.EMA(self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def kama(self, n, array=False):
        """自适应均线"""
        result = talib.KAMA(self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def std(self, n, array=False):
        """标准差"""
        result = talib.STDDEV(self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def cci(self, n, array=False):
        """CCI指标"""
        result = talib.CCI(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def atr(self, n, array=False):
        """ATR指标"""
        result = talib.ATR(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def rsi(self, n, array=False):
        """RSI指标"""
        result = talib.RSI(self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def macd(self, fastPeriod, slowPeriod, signalPeriod, array=False):
        """MACD指标"""
        macd, signal, hist = talib.MACD(self.close, fastPeriod,
                                        slowPeriod, signalPeriod)
        if array:
            return macd, signal, hist
        return macd[-1], signal[-1], hist[-1]

    #----------------------------------------------------------------------
    def adx(self, n, array=False):
        """ADX指标"""
        result = talib.ADX(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def boll(self, n, dev, array=False):
        """布林通道"""
        mid = self.sma(n, array)
        std = self.std(n, array)

        up = mid + std * dev
        down = mid - std * dev

        return up, down

    #----------------------------------------------------------------------
    def keltner(self, n, dev, array=False):
        """肯特纳通道"""
        mid = self.sma(n, array)
        atr = self.atr(n, array)

        up = mid + atr * dev
        down = mid - atr * dev

        return up, down

    #----------------------------------------------------------------------
    def donchian(self, n, array=False):
        """唐奇安通道"""
        up = talib.MAX(self.high, n)
        down = talib.MIN(self.low, n)

        if array:
            return up, down
        return up[-1], down[-1]

    #----------------------------------------------------------------------
    def er(self, n):
        """efficiency_ratio"""
        direction = self.close[-1] - self.close[-n]
        volatility = 0
        for i in range(1,n):
            volatility += abs( self.close[-i] - self.close[-i-1] )
        if volatility == 0:
            return 0
        else:
            return round( abs(direction/volatility), 2 )
