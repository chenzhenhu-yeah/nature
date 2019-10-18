# encoding: UTF-8

import pandas as pd
from csv import DictReader
from collections import OrderedDict, defaultdict

from nature import to_log, get_dss
from nature import ArrayManager
from nature import DIRECTION_LONG,DIRECTION_SHORT,OFFSET_OPEN,OFFSET_CLOSE,OFFSET_CLOSETODAY,OFFSET_CLOSEYESTERDAY
from nature import Signal

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
class TurtleResult(object):
    """一次完整的开平交易"""

    #----------------------------------------------------------------------
    def __init__(self, signal):
        """Constructor"""
        self.unit = 0
        self.entry = 0                  # 开仓均价
        self.exit = 0                   # 平仓均价
        self.pnl = 0                    # 盈亏

        self.signal = signal

    #----------------------------------------------------------------------
    def open(self, price, change):
        """开仓或者加仓"""
        cost = self.unit * self.entry    # 计算之前的开仓成本
        cost += change * price          # 加上新仓位的成本
        self.unit += change              # 加上新仓位的数量
        self.entry = cost / self.unit    # 计算新的平均开仓成本

        r = [ [self.signal.portfolio.result.date, '多' if change>0 else '空', '开',  \
               abs(change), price, 0, \
               self.signal.atrValue, self.signal.atrMa, self.signal.rsiValue, \
               self.signal.iswave, self.signal.intraTradeHigh, self.signal.intraTradeLow, \
               self.signal.stop] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl',  \
                                      'atrValue', 'atrMa', 'rsiValue', 'iswave', \
                                      'intraTradeHigh','intraTradeLow','stop'])
        filename = get_dss() +  'fut/check/signal_atrrsi_' + self.signal.vtSymbol + '.csv'
        df.to_csv(filename, index=False, mode='a', header=False)

    #----------------------------------------------------------------------
    def close(self, price):
        """平仓"""
        self.exit = price
        self.pnl = self.unit * (self.exit - self.entry)

        r = [ [self.signal.portfolio.result.date, '', '平',  \
               self.unit, price, self.pnl, \
               self.signal.atrValue, self.signal.atrMa, self.signal.rsiValue, \
               self.signal.iswave, self.signal.intraTradeHigh, self.signal.intraTradeLow, \
               self.signal.stop] ]
        df = pd.DataFrame(r, columns=['datetime','direction','offset','volume','price','pnl',  \
                                      'atrValue', 'atrMa', 'rsiValue', 'iswave', \
                                      'intraTradeHigh','intraTradeLow','stop'])
        filename = get_dss() +  'fut/check/signal_atrrsi_' + self.signal.vtSymbol + '.csv'
        df.to_csv(filename, index=False, mode='a', header=False)

########################################################################
class Fut_AtrRsiSignal(Signal):

    #----------------------------------------------------------------------
    def __init__(self, portfolio, vtSymbol):
        Signal.__init__(self, portfolio, vtSymbol)

        # 策略参数
        self.atrLength =1           # 计算ATR指标的窗口数
        self.atrMaLength = 14       # 计算ATR均线的窗口数
        self.rsiLength = 5           # 计算RSI的窗口数
        self.rsiEntry = 16           # RSI的开仓信号
        self.trailingPercent = 0.3   # 百分比移动止损
        self.victoryPercent = 0.8
        self.initBars = 90           # 初始化数据所用的天数
        self.fixedSize = 1           # 每次交易的数量
        self.ratio_atrMa = 0.85
        self.minx = 'min5'
        # 初始化RSI入场阈值
        self.rsiBuy = 50 + self.rsiEntry
        self.rsiSell = 50 - self.rsiEntry

        # 策略临时变量
        self.atrValue = 0                        # 最新的ATR指标数值
        self.atrMa = 0                           # ATR移动平均的数值
        self.rsiValue = 0                        # RSI指标的数值
        self.iswave = True

        # 需要持久化保存的变量
        self.unit = 0
        self.cost = 0
        self.intraTradeHigh = 0                  # 移动止损用的持仓期内最高价
        self.intraTradeLow = 0                   # 持仓期内的最低点
        self.stop = 0                            # 多头止损

        self.result = None              # 当前的交易
        self.resultList = []            # 交易列表

        # 载入历史数据，并采用回放计算的方式初始化策略数值
        initData = self.portfolio.engine._bc_loadInitBar(self.vtSymbol, self.initBars, self.minx)
        for bar in initData:
            self.bar = bar
            self.am.updateBar(bar)

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
    def onBar(self, bar):
        """新推送过来一个bar，进行处理"""
        #print(bar.time, self.vtSymbol)

        self.bar = bar
        self.am.updateBar(bar)
        if not self.am.inited:
            return

        #print('here')
        self.calculateIndicator()     # 计算指标
        self.generateSignal(bar)    # 触发信号，产生交易指令

    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""
        atrArray = self.am.atr(self.atrLength, array=True)
        # print(len(atrArray))

        self.atrValue = atrArray[-1]
        self.atrMa = atrArray[-self.atrMaLength:].mean()

        atrMa10 = atrArray[-10:].mean()
        atrMa50 = atrArray[-50:].mean()

        self.iswave = False if atrMa10 < self.ratio_atrMa * atrMa50  else True

        self.rsiValue = self.am.rsi(self.rsiLength)

    #----------------------------------------------------------------------
    def generateSignal(self, bar):

        # 当前无仓位
        if self.unit == 0:
            self.intraTradeHigh = bar.high
            self.intraTradeLow = bar.low

            # ATR数值上穿其移动平均线，说明行情短期内波动加大
            # 即处于趋势的概率较大，适合CTA开仓
            if self.atrValue > self.atrMa and self.iswave == False:
                # 使用RSI指标的趋势行情时，会在超买超卖区钝化特征，作为开仓信号
                if self.rsiValue > self.rsiBuy:
                    # 这里为了保证成交，选择超价5个整指数点下单
                    self.buy(bar.close, self.fixedSize)
                    self.cost = bar.close
                    self.stop = 0

                    # print(bar.datetime, '开多')
                    # print('rsi: ', self.rsiValue)
                    # print('atr: ', self.atrValue, self.atrMa)

                elif self.rsiValue < self.rsiSell:
                    self.short(bar.close, self.fixedSize)
                    self.cost = bar.close
                    self.stop = 100E4

                    # print(bar.datetime, '开空')
                    # print('rsi: ', self.rsiValue)
                    # print('atr: ', self.atrValue, self.atrMa)


        # 持有多头仓位
        elif self.unit > 0:
            # 计算多头持有期内的最高价，以及重置最低价
            self.intraTradeHigh = max(self.intraTradeHigh, bar.high)

            # 计算多头移动止损
            # if self.rsiValue < 35:
            #     self.stop = bar.close

            #self.stop = max( self.stop, self.intraTradeHigh * (1-self.victoryPercent/100) )
            if self.stop < self.cost:
                self.stop = max( self.stop, self.intraTradeHigh * (1-self.trailingPercent/100) )
            else:
                self.stop = max( self.stop, self.intraTradeHigh * (1-self.victoryPercent/100) )

            if bar.close <= self.stop:
                # print('平多: ', bar.datetime, self.intraTradeHigh, self.stop, bar.close)
                self.sell(bar.close, abs(self.unit))

        # 持有空头仓位
        elif self.unit < 0:
            self.intraTradeLow = min(self.intraTradeLow, bar.low)

            # if self.rsiValue > 65:
            #     self.stop = bar.close

            #self.stop = min( self.stop, self.intraTradeLow * (1+self.victoryPercent/100) )
            if self.stop > self.cost:
                self.stop = min( self.stop, self.intraTradeLow * (1+self.trailingPercent/100) )
            else:
                self.stop = min( self.stop, self.intraTradeLow * (1+self.victoryPercent/100) )

            if bar.close >= self.stop:
                # print('平空: ', bar.datetime, self.intraTradeLow, self.stop, bar.close)
                self.cover(bar.close, abs(self.unit))


#----------------------------------------------------------------------
    def load_var(self):
        filename = get_dss() +  'fut/check/signal_atrrsi_var.csv'
        df = pd.read_csv(filename)
        df = df[df.vtSymbol == self.vtSymbol]
        if len(df) > 0:
            rec = df.iloc[-1,:]
            self.unit = rec.unit
            self.cost = rec.cost
            self.intraTradeHigh = rec.intraTradeHigh
            self.intraTradeLow = rec.intraTradeLow
            self.stop = rec.stop

#----------------------------------------------------------------------
    def save_var(self):
        r = [ [self.portfolio.result.date,self.vtSymbol, self.unit, self.cost, self.intraTradeHigh, self.intraTradeLow, self.stop] ]
        df = pd.DataFrame(r, columns=['datetime','vtSymbol','unit','cost','intraTradeHigh','intraTradeLow','stop'])
        filename = get_dss() +  'fut/check/signal_atrrsi_var.csv'
        df.to_csv(filename, index=False, mode='a', header=False)

#----------------------------------------------------------------------
    def buy(self, price, volume):
        """买入开仓"""
        self.open(price, volume)
        self.newSignal(DIRECTION_LONG, OFFSET_OPEN, price, volume)

    #----------------------------------------------------------------------
    def sell(self, price, volume):
        """卖出平仓"""
        volume = abs(self.unit)

        self.close(price)
        self.newSignal(DIRECTION_SHORT, OFFSET_CLOSE, price, volume)

    #----------------------------------------------------------------------
    def short(self, price, volume):
        """卖出开仓"""
        self.open(price, -volume)
        self.newSignal(DIRECTION_SHORT, OFFSET_OPEN, price, volume)

    #----------------------------------------------------------------------
    def cover(self, price, volume):
        """买入平仓"""
        volume = abs(self.unit)

        self.close(price)
        self.newSignal(DIRECTION_LONG, OFFSET_CLOSE, price, volume)

    #----------------------------------------------------------------------
    def open(self, price, change):
        """开仓"""
        self.unit += change

        if not self.result:
            self.result = TurtleResult(self)
        self.result.open(price, change)

    #----------------------------------------------------------------------
    def close(self, price):
        """平仓"""
        self.unit = 0

        self.result.close(price)
        self.resultList.append(self.result)
        self.result = None

class Fut_AtrRsiPortfolio(object):

    #----------------------------------------------------------------------
    def __init__(self, engine, symbol_list, signal_param={}):
        self.engine = engine                 # 所属引擎
        self.name = 'atrrsi'

        self.portfolioValue = 100E4          # 组合市值
        self.signalDict = defaultdict(list)  # 信号字典，code为键, signal列表为值
        self.posDict = {}                    # 真实持仓量字典,code为键,pos为值

        self.result = DailyResult('00-00-00 00:00:00')
        self.resultList = []

        self.vtSymbolList = symbol_list

        # 初始化信号字典、持仓字典
        for vtSymbol in self.vtSymbolList:
            self.posDict[vtSymbol] = 0
            # 每个portfolio可以管理多种类型signal,暂只管理同一种类型的signal
            signal1 = Fut_AtrRsiSignal(self, vtSymbol)

            if vtSymbol in signal_param:
                param_dict = signal_param[vtSymbol]
                signal1.set_param(param_dict)
                #print('here')

            l = self.signalDict[vtSymbol]
            l.append(signal1)

        print(u'投资组合的合约代码%s' %(self.vtSymbolList))

    #----------------------------------------------------------------------
    def init(self):
        pass

    #----------------------------------------------------------------------
    def onBar(self, bar):
        """引擎新推送过来bar，传递给每个signal"""

        if self.result.date != bar.date + ' ' + bar.time:
            previousResult = self.result
            self.result = DailyResult(bar.date + ' ' + bar.time)
            self.result.updatePos(self.posDict)
            self.resultList.append(self.result)
            if previousResult:
                self.result.updatePreviousClose(previousResult.closeDict)

        self.result.updateBar(bar)

        for signal in self.signalDict[bar.vtSymbol]:
            signal.onBar(bar)
            #self.portfolioValue += self.result.calculatePnl()

        #print('come here')

    #----------------------------------------------------------------------
    def _bc_newSignal(self, signal, direction, offset, price, volume):
        """
        对交易信号进行过滤，符合条件的才发单执行。
        计算真实交易价格和数量。
        """
        multiplier = self.portfolioValue * 0.01 / get_contract(signal.vtSymbol).size
        multiplier = int(round(multiplier, 0))
        #print(multiplier)
        # multiplier = 1

        # 计算合约持仓
        if direction == DIRECTION_LONG:
            self.posDict[signal.vtSymbol] += volume*multiplier
        else:
            self.posDict[signal.vtSymbol] -= volume*multiplier

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
        filename = self.engine.dss + 'fut/check/portfolio_atrrsi_var.csv'
        df = pd.read_csv(filename, sep='$')
        #df = df.sort_values(by='datetime',ascending=False)
        if len(df) > 0:
            rec = df.iloc[-1,:]
            self.portfolioValue = rec.portfolioValue
            d = eval(rec.posDict)
            self.posDict.update(d)

        # 所有Signal读取保存到文件的变量
        for code in self.vtSymbolList:
            for signal in self.signalDict[code]:
                signal.load_var()


    #----------------------------------------------------------------------
    def daily_close(self):
        # 保存posDict、portfolioValue到文件
        dt = self.result.date
        r = [ [dt, self.portfolioValue, str(self.posDict)] ]
        df = pd.DataFrame(r, columns=['datetime','portfolioValue','posDict'])
        filename = self.engine.dss + 'fut/check/portfolio_atrrsi_var.csv'
        df.to_csv(filename,index=False,sep='$',mode='a',header=False)

        # 保存组合市值
        tr = []
        totalTradeCount,totalTradingPnl,totalHoldingPnl,totalNetPnl = 0, 0, 0, 0
        n = len(self.resultList)
        print(n)
        for i in range(n-1):
            result = self.resultList[i]
            
            print(result.date)
            print(result.posDict)
            print(result.closeDict)

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
        filename = self.engine.dss + 'fut/check/portfolio_atrrsi_value.csv'
        df.to_csv(filename,index=False,mode='a',header=False)

        # 保存组合交易记录
        df = pd.DataFrame(tr, columns=['vtSymbol','datetime','direction','offset','price','volume'])
        filename = self.engine.dss + 'fut/check/portfolio_atrrsi_deal.csv'
        df.to_csv(filename,index=False,mode='a',header=False)

        # 保存Signal变量到文件
        for code in self.vtSymbolList:
            for signal in self.signalDict[code]:
                signal.save_var()

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
    def updatePreviousClose(self, d):
        """更新昨收盘"""
        self.previousCloseDict.update(d)

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
