# encoding: UTF-8

import os
import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss
from nature import ArrayManager
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import Signal

MAX_PRODUCT_POS = 4         # 单品种最大持仓
MAX_DIRECTION_POS = 10      # 单方向最大持仓

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
filename_setting_fut = get_dss() + 'fut/cfg/setting_pz.csv'
with open(filename_setting_fut,encoding='utf-8') as f:
    r = DictReader(f)
    for d in r:
        contract_dict[ d['pz'] ] = Contract( d['pz'],int(d['size']),float(d['priceTick']),float(d['variableCommission']),float(d['fixedCommission']),float(d['slippage']),d['exchangeID'] )

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
class Fut_TurtleSignal(object):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        self.portfolio = portfolio      # 投资组合
        self.vtSymbol = vtSymbol        # 合约代码
        self.am = ArrayManager()        # K线容器
        self.bar = None                 # 最新K线

        # 策略参数
        self.initBars = 60              # 初始化数据所用的天数
        self.entryWindow = 20           # 入场通道周期数
        self.exitWindow = 50            # 出场通道周期数
        self.atrWindow = 5              # 计算ATR周期数
        self.profitCheck = True         # 是否检查上一笔盈利
        self.minx = 'day'

        # 策略临时变量
        self.atrVolatility = 0          # ATR波动率
        self.entryUp = 0                # 入场通道
        self.entryDown = 0
        self.exitUp = 0                 # 出场通道
        self.exitDown = 0

        self.longEntry1 = 0             # 多头入场位
        self.longEntry2 = 0
        self.longEntry3 = 0
        self.longEntry4 = 0
        self.longStop = 0               # 多头止损位

        self.shortEntry1 = 0            # 空头入场位
        self.shortEntry2 = 0
        self.shortEntry3 = 0
        self.shortEntry4 = 0
        self.shortStop = 0              # 空头止损位

        # 需要持久化保存的变量
        self.unit = 0

        self.result = None              # 当前的交易
        self.resultList = []            # 交易列表

        # 载入历史数据，并采用回放计算的方式初始化策略数值
        initData = self.portfolio.engine._bc_loadInitBar(self.vtSymbol, self.initBars, self.minx)
        for bar in initData:
            self.bar = bar
            self.am.updateBar(bar)


    #----------------------------------------------------------------------
    def load_param(self):
        filename = get_dss() +  'fut/cfg/signal_turtle_param.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df = df[ df.pz == get_contract(self.vtSymbol).pz ]
            if len(df) > 0:
                rec = df.iloc[0,:]
                self.rsiLength = rec.rsiLength
                self.trailingPercent = rec.trailingPercent
                self.victoryPercent = rec.victoryPercent

    #----------------------------------------------------------------------
    def set_param(self, param_dict):
        if 'atrMaLength' in param_dict:
            self.atrMaLength = param_dict['atrMaLength']
        if 'rsiLength' in param_dict:
            self.rsiLength = param_dict['rsiLength']
        if 'trailingPercent' in param_dict:
            self.trailingPercent = param_dict['trailingPercent']
        if 'victoryPercent' in param_dict:
            self.victoryPercent = param_dict['victoryPercent']

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='day'):
        """新推送过来一个bar，进行处理"""
        if minx != 'min1':
            self.on_bar_day(bar)

        if minx == 'min1':
            self.on_bar_min1(bar)

    #----------------------------------------------------------------------
    def on_bar_min1(self, bar):
        # 持有多头仓位
        if self.unit > 0:
            if bar.close <= self.stop:
                # print('平多: ', bar.datetime, self.intraTradeHigh, self.stop, bar.close)
                self.sell(bar.close, abs(self.unit))

        # 持有空头仓位
        elif self.unit < 0:
            if bar.close >= self.stop:
                # print('平空: ', bar.datetime, self.intraTradeLow, self.stop, bar.close)
                self.cover(bar.close, abs(self.unit))

    #----------------------------------------------------------------------
    def on_bar_day(self, bar):
        self.bar = bar
        self.am.updateBar(bar)
        if not self.am.inited:
            return

        self.generateSignal(bar)    # 触发信号，产生交易指令
        self.calculateIndicator()     # 计算指标

    #----------------------------------------------------------------------
    def generateSignal(self, bar):
        """
        判断交易信号
        要注意在任何一个数据点：buy/sell/short/cover只允许执行一类动作
        """
        # 如果指标尚未初始化，则忽略
        if self.longEntry1 == 0:
            return

        # 优先检查平仓
        if self.unit > 0:
            longExit = max(self.longStop, self.exitDown)

            if bar.low <= longExit:
                self.sell(longExit)
                return
        elif self.unit < 0:
            shortExit = min(self.shortStop, self.exitUp)
            if bar.high >= shortExit:
                self.cover(shortExit)
                return

        # 没有仓位或者持有多头仓位的时候，可以做多（加仓）
        if self.unit >= 0:
            trade = False

            if bar.high >= self.longEntry1 and self.unit < 1:
                self.buy(self.longEntry1, 1)
                trade = True

            if bar.high >= self.longEntry2 and self.unit < 2:
                self.buy(self.longEntry2, 1)
                trade = True

            if bar.high >= self.longEntry3 and self.unit < 3:
                self.buy(self.longEntry3, 1)
                trade = True

            if bar.high >= self.longEntry4 and self.unit < 4:
                self.buy(self.longEntry4, 1)
                trade = True

            if trade:
                return

        # 没有仓位或者持有空头仓位的时候，可以做空（加仓）
        if self.unit <= 0:
            if bar.low <= self.shortEntry1 and self.unit > -1:
                self.short(self.shortEntry1, 1)

            if bar.low <= self.shortEntry2 and self.unit > -2:
                self.short(self.shortEntry2, 1)

            if bar.low <= self.shortEntry3 and self.unit > -3:
                self.short(self.shortEntry3, 1)

            if bar.low <= self.shortEntry4 and self.unit > -4:
                self.short(self.shortEntry4, 1)

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""
        self.entryUp, self.entryDown = self.am.donchian(self.entryWindow)
        self.exitUp, self.exitDown = self.am.donchian(self.exitWindow)

        # 有持仓后，ATR波动率和入场位等都不再变化
        if self.unit == 0:
            self.atrVolatility = self.am.atr(self.atrWindow)

            self.longEntry1 = self.entryUp
            self.longEntry2 = self.entryUp + self.atrVolatility * 0.5
            self.longEntry3 = self.entryUp + self.atrVolatility * 1
            self.longEntry4 = self.entryUp + self.atrVolatility * 1.5
            self.longStop = 0

            self.shortEntry1 = self.entryDown
            self.shortEntry2 = self.entryDown - self.atrVolatility * 0.5
            self.shortEntry3 = self.entryDown - self.atrVolatility * 1
            self.shortEntry4 = self.entryDown - self.atrVolatility * 1.5
            self.shortStop = 0

    #----------------------------------------------------------------------
    def load_var(self):
        filename = get_dss() +  'fut/check/signal_turtle_var.csv'
        df = pd.read_csv(filename)
        df = df[df.vtSymbol == self.vtSymbol]
        if len(df) > 0:
            rec = df.iloc[-1,:]            # 取最近日期的记录
            self.unit = rec.unit
            self.cost = rec.cost
            self.intraTradeHigh = rec.intraTradeHigh
            self.intraTradeLow = rec.intraTradeLow
            self.stop = rec.stop
            if rec.has_result == 1:
                self.result = SignalResult()
                self.result.unit = rec.result_unit
                self.result.entry = rec.result_entry
                self.result.exit = rec.result_exit
                self.result.pnl = rec.result_pnl

#----------------------------------------------------------------------
    def save_var(self):
        r = []
        if self.result is None:
            r = [ [self.portfolio.result.date,self.vtSymbol, self.unit, self.cost, \
                   self.intraTradeHigh, self.intraTradeLow, self.stop, \
                   0, 0, 0, 0, 0 ] ]
        else:
            r = [ [self.portfolio.result.date,self.vtSymbol, self.unit, self.cost, \
                   self.intraTradeHigh, self.intraTradeLow, self.stop, \
                   1, self.result.unit, self.result.entry, self.result.exit, self.result.pnl ] ]
        df = pd.DataFrame(r, columns=['datetime','vtSymbol','unit','cost', \
                                      'intraTradeHigh','intraTradeLow','stop', \
                                      'has_result','result_unit','result_entry','result_exit', 'result_pnl'])
        filename = get_dss() +  'fut/check/signal_turtle_var.csv'
        df.to_csv(filename, index=False, mode='a', header=False)


    #----------------------------------------------------------------------
    def newSignal(self, direction, offset, price, volume):
        """调用组合中的接口，传递下单指令"""
        self.portfolio._bc_newSignal(self, direction, offset, price, volume)

#----------------------------------------------------------------------
    def buy(self, price, volume):
        """买入开仓"""
        price = self.calculateTradePrice(DIRECTION_LONG, price)

        self.open(price, volume)
        self.newSignal(DIRECTION_LONG, OFFSET_OPEN, price, volume)

        # 以最后一次加仓价格，加上两倍N计算止损
        self.longStop = price - self.atrVolatility * 2

    #----------------------------------------------------------------------
    def sell(self, price):
        """卖出平仓"""
        price = self.calculateTradePrice(DIRECTION_SHORT, price)

        volume = abs(self.unit)

        self.close(price)
        self.newSignal(DIRECTION_SHORT, OFFSET_CLOSE, price, volume)

    #----------------------------------------------------------------------
    def short(self, price, volume):
        """卖出开仓"""
        price = self.calculateTradePrice(DIRECTION_SHORT, price)

        self.open(price, -volume)
        self.newSignal(DIRECTION_SHORT, OFFSET_OPEN, price, volume)

        # 以最后一次加仓价格，加上两倍N计算止损
        self.shortStop = price + self.atrVolatility * 2

    #----------------------------------------------------------------------
    def cover(self, price):
        """买入平仓"""
        price = self.calculateTradePrice(DIRECTION_LONG, price)
        volume = abs(self.unit)

        self.close(price)
        self.newSignal(DIRECTION_LONG, OFFSET_CLOSE, price, volume)

    #----------------------------------------------------------------------
    def open(self, price, change):
        """开仓"""
        self.unit += change

        if not self.result:
            self.result = SignalResult()
        self.result.open(price, change)

        r = [ [self.portfolio.result.date, '多' if change>0 else '空', '开',  \
               abs(change), price, 0, \
               self.unit] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl',  \
                                      'unit'])
        filename = get_dss() +  'fut/deal/signal_turtle_' + self.vtSymbol + '.csv'
        df.to_csv(filename, index=False, mode='a', header=False)


    #----------------------------------------------------------------------
    def close(self, price):
        """平仓"""
        self.unit = 0
        self.result.close(price)

        r = [ [self.portfolio.result.date, '', '平',  \
               0, price, self.result.pnl, \
               self.unit] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl',  \
                                      'unit'])
        filename = get_dss() +  'fut/deal/signal_turtle_' + self.vtSymbol + '.csv'
        df.to_csv(filename, index=False, mode='a', header=False)

        self.resultList.append(self.result)
        self.result = None


    #----------------------------------------------------------------------
    def getLastPnl(self):
        """获取上一笔交易的盈亏"""
        if not self.resultList:
            return 0

        result = self.resultList[-1]
        return result.pnl

    #----------------------------------------------------------------------
    def calculateTradePrice(self, direction, price):
        """计算成交价格"""
        # 买入时，停止单成交的最优价格不能低于当前K线开盘价
        if direction == DIRECTION_LONG:
            tradePrice = max(self.bar.open, price)
        # 卖出时，停止单成交的最优价格不能高于当前K线开盘价
        else:
            tradePrice = min(self.bar.open, price)

        return tradePrice


########################################################################
class Fut_TurtlePortfolio(object):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.engine = engine                 # 所属引擎
        self.name = 'turtle'

        self.portfolioValue = 100E4          # 组合市值
        self.signalDict = defaultdict(list)  # 信号字典，code为键, signal列表为值
        self.posDict = {}                    # 真实持仓量字典,code为键,pos为值

        self.multiplierDict = {}             # 按照波动幅度计算的委托量单位字典
        self.totalLong = 0          # 总的多头持仓
        self.totalShort = 0         # 总的空头持仓

        self.result = DailyResult('00-00-00 00:00:00')
        self.resultList = []

        self.vtSymbolList = symbol_list

        # 初始化信号字典、持仓字典
        for vtSymbol in self.vtSymbolList:
            self.posDict[vtSymbol] = 0
            # 每个portfolio可以管理多种类型signal,暂只管理同一种类型的signal
            signal1 = Fut_TurtleSignal(self, vtSymbol)
            signal1.load_param()

            if vtSymbol in signal_param:
                param_dict = signal_param[vtSymbol]
                signal1.set_param(param_dict)

            l = self.signalDict[vtSymbol]
            l.append(signal1)

        print(u'投资组合的合约代码%s' %(self.vtSymbolList))

    #----------------------------------------------------------------------
    def init(self):
        pass

    #----------------------------------------------------------------------
    def onBar(self, bar, minx='min1'):
        """引擎新推送过来bar，传递给每个signal"""

        # 不处理不相关的品种
        if bar.vtSymbol not in self.vtSymbolList:
            return

        # 将bar推送给signal
        for signal in self.signalDict[bar.vtSymbol]:
            signal.onBar(bar, minx)

        if minx != 'min1':
            if self.result.date != bar.date + ' ' + bar.time:
                previousResult = self.result
                self.result = DailyResult(bar.date + ' ' + bar.time)
                self.resultList.append(self.result)
                if previousResult:
                    self.result.updateClose(previousResult.closeDict)

            self.result.updateBar(bar)
            self.result.updatePos(self.posDict)

    #----------------------------------------------------------------------
    def _bc_newSignal(self, signal, direction, offset, price, volume):
        """
        对交易信号进行过滤，符合条件的才发单执行。
        计算真实交易价格和数量。
        """
        pos = self.posDict.get(signal.vtSymbol, 0)

        # 如果当前无仓位，则重新根据波动幅度计算委托量单位
        if pos == 0:
            size = get_contract(signal.vtSymbol).size
            riskValue = self.portfolioValue * 0.01
            multiplier = riskValue / (signal.atrVolatility * size)
            multiplier = int(round(multiplier, 0))
            self.multiplierDict[signal.vtSymbol] = multiplier
        else:
            multiplier = self.multiplierDict[signal.vtSymbol]

        # 开仓
        if offset == OFFSET_OPEN:
            # 检查上一次是否为盈利
            if signal.profitCheck:
                pnl = signal.getLastPnl()
                if pnl > 0:
                    return

            # 买入
            if direction == DIRECTION_LONG:
                # 组合持仓不能超过上限
                if self.totalLong >= MAX_DIRECTION_POS:
                    return

                # 单品种持仓不能超过上限
                if signal.unit >= MAX_PRODUCT_POS:
                    return
            # 卖出
            else:
                if self.totalShort <= -MAX_DIRECTION_POS:
                    return

                if signal.unit <= -MAX_PRODUCT_POS:
                    return
        # 平仓
        else:
            if direction == DIRECTION_LONG:
                # 必须有空头持仓
                if signal.unit >= 0:
                    return

                # 平仓数量不能超过空头持仓
                volume = min(volume, abs(signal.unit))
            else:
                if signal.unit <= 0:
                    return

                volume = min(volume, abs(signal.unit))

        # 更新总持仓
        if direction == DIRECTION_LONG and offset == OFFSET_OPEN:
            self.totalLong += 1
        if direction == DIRECTION_SHORT and offset == OFFSET_OPEN:
            self.totalShort += 1
        if direction == DIRECTION_LONG and offset != OFFSET_OPEN:
            self.totalShort -= 1
        if direction == DIRECTION_SHORT and offset != OFFSET_OPEN:
            self.totalLong -= 1

        # 更新合约持仓
        if direction == DIRECTION_LONG:
            self.posDict[signal.vtSymbol] += volume * multiplier
        else:
            self.posDict[signal.vtSymbol] -= volume * multiplier

        # 对价格四舍五入
        priceTick = get_contract(signal.vtSymbol).price_tick
        price = int(round(price/priceTick, 0)) * priceTick

        self.engine._bc_sendOrder(signal.vtSymbol, direction, offset, price, volume*multiplier, self.name)

        # 记录成交数据
        trade = TradeData(self.result.date, signal.vtSymbol, direction, offset, price, volume*multiplier)
        # l = self.tradeDict.setdefault(self.result.date, [])
        # l.append(trade)

        self.result.updateTrade(trade)

    #----------------------------------------------------------------------
    def daily_open(self):
        # 从文件中读取posDict、portfolioValue
        filename = self.engine.dss + 'fut/check/portfolio_turtle_var.csv'
        df = pd.read_csv(filename, sep='$')
        #df = df.sort_values(by='datetime',ascending=False)
        if len(df) > 0:
            rec = df.iloc[-1,:]
            self.portfolioValue = rec.portfolioValue
            d = eval(rec.posDict)

            #self.posDict.update(d)
            for vtSymbol in self.vtSymbolList:
                if vtSymbol in d:
                    self.posDict[vtSymbol] = d[vtSymbol]

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

        # 保存posDict、portfolioValue到文件
        dt = self.result.date
        r = [ [dt, self.portfolioValue, str(self.posDict)] ]
        df = pd.DataFrame(r, columns=['datetime','portfolioValue','posDict'])
        filename = self.engine.dss + 'fut/check/portfolio_turtle_var.csv'
        df.to_csv(filename,index=False,sep='$',mode='a',header=False)

        # 保存组合市值
        tr = []
        totalTradeCount,totalTradingPnl,totalHoldingPnl,totalNetPnl = 0, 0, 0, 0
        n = len(self.resultList)
        #print(n)
        for i in range(n-1):
            result = self.resultList[i]

            # print(result.date)
            # print(result.posDict)
            # print(result.closeDict)

            result.calculatePnl()
            totalTradeCount += result.tradeCount
            totalTradingPnl += result.tradingPnl
            totalHoldingPnl += result.holdingPnl
            totalNetPnl += result.netPnl

            for vtSymbol, l in result.tradeDict.items():
                for trade in l:
                    tr.append( [trade.vtSymbol,trade.dt,trade.direction,trade.offset,trade.price,trade.volume] )

        r = [ [dt, totalTradeCount,totalTradingPnl,totalHoldingPnl,totalNetPnl] ]
        df = pd.DataFrame(r, columns=['datetime','tradeCount','tradingPnl','holdingPnl','netPnl'])
        filename = self.engine.dss + 'fut/check/portfolio_turtle_value.csv'
        df.to_csv(filename,index=False,mode='a',header=False)

        # 保存组合交易记录
        df = pd.DataFrame(tr, columns=['vtSymbol','datetime','direction','offset','price','volume'])
        filename = self.engine.dss + 'fut/deal/portfolio_turtle_deal.csv'
        df.to_csv(filename,index=False,mode='a',header=False)

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
            close = self.closeDict[vtSymbol]
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
                                  trade.volume * trade.price * variableCommission)
                slippageCost = trade.volume * slippage
                pnl = (close - trade.price) * trade.volume * side * size

                self.commission += commissionCost
                self.slippage += slippageCost
                self.tradingPnl += pnl

    #----------------------------------------------------------------------
    def calculateHoldingPnl(self):
        """计算当日持仓盈亏"""
        for vtSymbol, pos in self.posDict.items():
            #print(vtSymbol, pos)

            previousClose = self.previousCloseDict.get(vtSymbol, 0)
            close = self.closeDict[vtSymbol]
            ct = get_contract(vtSymbol)
            size = ct.size


            pnl = (close - previousClose) * pos * size
            self.holdingPnl += pnl

    #----------------------------------------------------------------------
    def calculatePnl(self):
        """计算总盈亏"""
        self.calculateHoldingPnl()
        self.calculateTradingPnl()
        self.totalPnl = self.holdingPnl + self.tradingPnl
        self.netPnl = self.totalPnl - self.commission - self.slippage

        return self.netPnl
