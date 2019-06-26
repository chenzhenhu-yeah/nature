# encoding: UTF-8
from __future__ import print_function

from csv import DictReader
from datetime import datetime
from collections import OrderedDict, defaultdict

import numpy as np
import matplotlib.pyplot as plt

from nature import get_stk_hfq
from nature import VtBarData, DIRECTION_LONG, DIRECTION_SHORT

#from ipdb import set_trace


SIZE_DICT = {}
PRICETICK_DICT = {}
VARIABLE_COMMISSION_DICT = {}
FIXED_COMMISSION_DICT = {}
SLIPPAGE_DICT = {}

dss = '../../../data/'


########################################################################
class BacktestingEngine(object):
    """组合类CTA策略回测引擎"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.portfolio = None

        # 合约配置信息
        self.vtSymbolList = []
        self.sizeDict = {}                  # 合约大小字典
        self.priceTickDict = {}             # 最小价格变动字典
        self.variableCommissionDict = {}    # 变动手续费字典
        self.fixedCommissionDict = {}       # 固定手续费字典
        self.slippageDict = {}              # 滑点成本字典

        self.portfolioValue = 0
        self.startDt = None
        self.endDt = None
        self.currentDt = None

        self.dataDict = OrderedDict()
        self.tradeDict = OrderedDict()

        self.result = None
        self.resultList = []

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
        print(self.startDt)
        print(self.endDt)
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

        print('self.result: ')
        if self.result is not None:
            self.result.print_dailyresult()

        print('self.resultList: ')
        print(len(self.resultList))
        for r in self.resultList:
            if r.tradeCount > 0:
                r.print_dailyresult()
                break

    #----------------------------------------------------------------------
    def setPeriod(self, startDt, endDt):
        """设置回测周期"""
        self.startDt = startDt
        self.endDt = endDt

    #----------------------------------------------------------------------
    def initPortfolio(self, pf, portfolioValue=100E4):
        """初始化投资组合"""
        self.portfolioValue = portfolioValue
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

        self.portfolio = pf
        self.portfolio.init(portfolioValue, self.vtSymbolList, SIZE_DICT)

        self.output(u'投资组合的合约代码%s' %(self.vtSymbolList))
        self.output(u'投资组合的初始价值%s' %(portfolioValue))

    #----------------------------------------------------------------------
    def loadData(self, dbName):
        """加载数据"""

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
                date = d['date'].split('-')
                date = ''.join(date)
                bar.date = date
                bar.time = '00:00:00'
                bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M:%S')
                bar.volume = d['volume']

                barDict = self.dataDict.setdefault(bar.datetime, OrderedDict())
                barDict[bar.vtSymbol] = bar

        self.output(u'全部数据加载完成')

    #----------------------------------------------------------------------
    def runBacktesting(self):
        """运行回测"""
        self.output(u'开始回放K线数据')

        for dt, barDict in self.dataDict.items():
            if dt >= self.startDt and dt <= self.endDt:
                #print(dt)

                self.currentDt = dt

                previousResult = self.result

                self.result = DailyResult(dt)
                self.result.updatePos(self.portfolio.posDict)
                self.resultList.append(self.result)

                if previousResult:
                    self.result.updatePreviousClose(previousResult.closeDict)

                for bar in barDict.values():
                    self.portfolio.onBar(bar)
                    self.result.updateBar(bar)
                    #set_trace()


        self.output(u'K线数据回放结束')

    #----------------------------------------------------------------------
    def calculateResult(self, annualDays=240):
        """计算结果"""
        self.output(u'开始统计回测结果')

        for result in self.resultList:
            result.calculatePnl()

        resultList = self.resultList
        dateList = [result.date for result in resultList]

        startDate = dateList[0]
        endDate = dateList[-1]
        totalDays = len(dateList)

        profitDays = 0
        lossDays = 0
        endBalance = self.portfolioValue
        highlevel = self.portfolioValue
        totalNetPnl = 0
        totalCommission = 0
        totalSlippage = 0
        totalTradeCount = 0

        netPnlList = []
        balanceList = []
        highlevelList = []
        drawdownList = []
        ddPercentList = []
        returnList = []

        for result in resultList:
            if result.netPnl > 0:
                profitDays += 1
            elif result.netPnl < 0:
                lossDays += 1
            netPnlList.append(result.netPnl)

            prevBalance = endBalance
            endBalance += result.netPnl
            balanceList.append(endBalance)
            returnList.append(endBalance/prevBalance - 1)

            highlevel = max(highlevel, endBalance)
            highlevelList.append(highlevel)

            drawdown = endBalance - highlevel
            drawdownList.append(drawdown)
            ddPercentList.append(drawdown/highlevel*100)

            totalCommission += result.commission
            totalSlippage += result.slippage
            totalTradeCount += result.tradeCount
            totalNetPnl += result.netPnl

        maxDrawdown = min(drawdownList)
        maxDdPercent = min(ddPercentList)
        totalReturn = (endBalance / self.portfolioValue - 1) * 100
        dailyReturn = np.mean(returnList) * 100
        annualizedReturn = dailyReturn * annualDays
        returnStd = np.std(returnList) * 100

        if returnStd:
            sharpeRatio = dailyReturn / returnStd * np.sqrt(annualDays)
        else:
            sharpeRatio = 0

        # 返回结果
        result = {
            'startDate': startDate,
            'endDate': endDate,
            'totalDays': totalDays,
            'profitDays': profitDays,
            'lossDays': lossDays,
            'endBalance': endBalance,
            'maxDrawdown': maxDrawdown,
            'maxDdPercent': maxDdPercent,
            'totalNetPnl': totalNetPnl,
            'dailyNetPnl': totalNetPnl/totalDays,
            'totalCommission': totalCommission,
            'dailyCommission': totalCommission/totalDays,
            'totalSlippage': totalSlippage,
            'dailySlippage': totalSlippage/totalDays,
            'totalTradeCount': totalTradeCount,
            'dailyTradeCount': totalTradeCount/totalDays,
            'totalReturn': totalReturn,
            'annualizedReturn': annualizedReturn,
            'dailyReturn': dailyReturn,
            'returnStd': returnStd,
            'sharpeRatio': sharpeRatio
            }

        timeseries = {
            'balance': balanceList,
            'return': returnList,
            'highLevel': highlevel,
            'drawdown': drawdownList,
            'ddPercent': ddPercentList,
            'date': dateList,
            'netPnl': netPnlList
        }

        return timeseries, result

    #----------------------------------------------------------------------
    def showResult(self):
        """显示回测结果"""
        timeseries, result = self.calculateResult()

        # 输出统计结果
        self.output('-' * 30)
        self.output(u'首个交易日：\t%s' % result['startDate'])
        self.output(u'最后交易日：\t%s' % result['endDate'])

        self.output(u'总交易日：\t%s' % result['totalDays'])
        self.output(u'盈利交易日\t%s' % result['profitDays'])
        self.output(u'亏损交易日：\t%s' % result['lossDays'])

        self.output(u'起始资金：\t%s' % self.portfolioValue)
        self.output(u'结束资金：\t%s' % formatNumber(result['endBalance']))

        self.output(u'总收益率：\t%s%%' % formatNumber(result['totalReturn']))
        self.output(u'年化收益：\t%s%%' % formatNumber(result['annualizedReturn']))
        self.output(u'总盈亏：\t%s' % formatNumber(result['totalNetPnl']))
        self.output(u'最大回撤: \t%s' % formatNumber(result['maxDrawdown']))
        self.output(u'百分比最大回撤: %s%%' % formatNumber(result['maxDdPercent']))

        self.output(u'总手续费：\t%s' % formatNumber(result['totalCommission']))
        self.output(u'总滑点：\t%s' % formatNumber(result['totalSlippage']))
        self.output(u'总成交笔数：\t%s' % formatNumber(result['totalTradeCount']))

        self.output(u'日均盈亏：\t%s' % formatNumber(result['dailyNetPnl']))
        self.output(u'日均手续费：\t%s' % formatNumber(result['dailyCommission']))
        self.output(u'日均滑点：\t%s' % formatNumber(result['dailySlippage']))
        self.output(u'日均成交笔数：\t%s' % formatNumber(result['dailyTradeCount']))

        self.output(u'日均收益率：\t%s%%' % formatNumber(result['dailyReturn']))
        self.output(u'收益标准差：\t%s%%' % formatNumber(result['returnStd']))
        self.output(u'Sharpe Ratio：\t%s' % formatNumber(result['sharpeRatio']))

        # 绘图
        fig = plt.figure(figsize=(10, 16))

        pBalance = plt.subplot(4, 1, 1)
        pBalance.set_title('Balance')
        plt.plot(timeseries['date'], timeseries['balance'])

        pDrawdown = plt.subplot(4, 1, 2)
        pDrawdown.set_title('Drawdown')
        pDrawdown.fill_between(range(len(timeseries['drawdown'])), timeseries['drawdown'])

        pPnl = plt.subplot(4, 1, 3)
        pPnl.set_title('Daily Pnl')
        plt.bar(range(len(timeseries['drawdown'])), timeseries['netPnl'])

        pKDE = plt.subplot(4, 1, 4)
        pKDE.set_title('Daily Pnl Distribution')
        plt.hist(timeseries['netPnl'], bins=50)

        plt.show()

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

        self.result.updateTrade(trade)

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

    def print_dailyresult(self):

        print(self.date)

        print(self.closeDict)                     # 收盘价字典
        print(self.previousCloseDict)             # 昨收盘字典

        print(self.tradeDict)                    # 成交字典
        for k,v in self.tradeDict.items():
            v[0].print_tradedata()
        print(self.posDict)                       # 持仓字典（开盘时）

        print(self.tradingPnl)                     # 交易盈亏
        print(self.holdingPnl)                     # 持仓盈亏
        print(self.totalPnl)                       # 总盈亏
        print(self.commission)                     # 佣金
        print(self.slippage)                       # 滑点
        print(self.netPnl)                        # 净盈亏
        print(self.tradeCount)                     # 成交笔数

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
            size = SIZE_DICT[vtSymbol]

            slippage = SLIPPAGE_DICT[vtSymbol]
            variableCommission = VARIABLE_COMMISSION_DICT[vtSymbol]
            fixedCommission = FIXED_COMMISSION_DICT[vtSymbol]

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
            previousClose = self.previousCloseDict.get(vtSymbol, 0)
            close = self.closeDict[vtSymbol]
            size = SIZE_DICT[vtSymbol]

            pnl = (close - previousClose) * pos * size
            self.holdingPnl += pnl

    #----------------------------------------------------------------------
    def calculatePnl(self):
        """计算总盈亏"""
        self.calculateHoldingPnl()
        self.calculateTradingPnl()
        self.totalPnl = self.holdingPnl + self.tradingPnl
        self.netPnl = self.totalPnl - self.commission - self.slippage


#----------------------------------------------------------------------
def formatNumber(n):
    """格式化数字到字符串"""
    rn = round(n, 2)        # 保留两位小数
    return format(rn, ',')  # 加上千分符
