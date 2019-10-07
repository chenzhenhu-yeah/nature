# encoding: UTF-8
from __future__ import print_function

from datetime import datetime
from collections import OrderedDict, defaultdict
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from nature import get_stk_hfq, to_log, get_dss
from nature import VtBarData, DIRECTION_LONG, DIRECTION_SHORT
from nature import Fut_AtrRsiPortfolio, Fut_CciPortfolio, Fut_BollPortfolio


########################################################################
class BacktestingEngine(object):
    """组合类CTA策略回测引擎"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.dss = get_dss()

        self.portfolio = None                # 一对一
        self.startDt = None
        self.endDt = None
        self.currentDt = None

        self.barDict = OrderedDict()

        # 加载配置
        config = open(get_dss()+'fut/cfg/config.json')
        setting = json.load(config)
        symbols = setting['symbols']
        self.vtSymbol_list = symbols.split(',')


    #----------------------------------------------------------------------
    def loadPortfolio(self, PortfolioClass, name, signal_param):
        """每日重新加载投资组合"""
        print('\n')

        p = PortfolioClass(self, name, signal_param)
        p.init()
        self.portfolio = p

    #----------------------------------------------------------------------
    def setPeriod(self, startDt, endDt):
        """设置回测周期"""
        self.startDt = startDt
        self.endDt = endDt

    #----------------------------------------------------------------------
    def loadData(self, vtSymbol, filename):
        """加载数据"""

        df = pd.read_csv(filename)
        for i, d in df.iterrows():
            #print(d)

            bar = VtBarData()
            bar.vtSymbol = vtSymbol
            bar.symbol = vtSymbol
            bar.open = float(d['open'])
            bar.high = float(d['high'])
            bar.low = float(d['low'])
            bar.close = float(d['close'])
            bar.volume = d['volume']

            date = str(d['date'])
            bar.date = date
            bar.time = str(d['time'])
            if '-' in date:
                bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y-%m-%d %H:%M:%S')
            else:
                bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M:%S')

            #bar.time = '00:00:00'
            #bar.datetime = bar.date + ' ' + bar.time
            bar.datetime = datetime.strftime(bar.datetime, '%Y%m%d %H:%M:%S')

            self.barDict[bar.datetime] = bar
            # print(bar.__dict__)
            # break

        self.output(u'全部数据加载完成')

    #----------------------------------------------------------------------
    def _bc_loadInitBar(self, vtSymbol, initBars):
        """读取startDt前n条Bar数据，用于初始化am"""

        dt_list = self.barDict.keys()
        #print(len(dt_list))
        dt_list = [x for x in dt_list if x<self.startDt]
        #print(len(dt_list))
        dt_list = dt_list[-initBars:]
        dt_list = sorted(dt_list)
        #print(dt_list)

        r = []
        for dt in dt_list:
            bar = self.barDict[dt]
            r.append(bar)

        return r

    #----------------------------------------------------------------------
    def runBacktesting(self, barType='min1'):
        """运行回测"""
        #self.output(u'开始回放K线数据  '+ barType)

        # 初始化回测结果

        for dt, bar in self.barDict.items():
            if dt >= self.startDt and dt <= self.endDt:
                #print(dt)
                self.currentDt = dt
                self.portfolio.onBar(bar)

        #self.output(u'K线数据回放结束')

        # td_list = self.getTradeData()
        # with open(barType+'_t1.txt','w') as f:
        #     for td in td_list:
        #         print(td.__dict__, file=f)


    #----------------------------------------------------------------------
    def calculateResult(self, annualDays=240):
        """计算结果"""
        self.output(u'开始统计回测结果')

        for result in self.portfolio.resultList:
            result.calculatePnl()

        resultList = self.portfolio.resultList
        dateList = [result.date for result in resultList]

        startDate = dateList[0]
        endDate = dateList[-1]
        totalDays = len(dateList)

        profitDays = 0
        lossDays = 0
        endBalance = self.portfolio.portfolioValue
        highlevel = self.portfolio.portfolioValue
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
        totalReturn = (endBalance / self.portfolio.portfolioValue - 1) * 100
        dailyReturn = np.mean(returnList) * 100
        annualizedReturn = dailyReturn * annualDays
        returnStd = np.std(returnList) * 100

        if returnStd:
            sharpeRatio = dailyReturn / returnStd * np.sqrt(annualDays*72)
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

        self.output(u'起始资金：\t%s' % self.portfolio.portfolioValue)
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
    def _bc_sendOrder(self, vtSymbol, direction, offset, price, volume, pfName):
        """记录交易数据（由portfolio调用）"""

        pass

    #----------------------------------------------------------------------
    def output(self, content):
        """输出信息"""
        print(content)

    #----------------------------------------------------------------------
    def calc_btKey(self):
        """返回回测信息"""
        timeseries, result = self.calculateResult()

        # 输出统计结果
        self.output('-' * 30)
        # self.output(u'首个交易日：\t%s' % result['startDate'])
        # self.output(u'最后交易日：\t%s' % result['endDate'])

        # self.output(u'起始资金：\t%s' % self.portfolio.portfolioValue)
        # self.output(u'结束资金：\t%s' % formatNumber(result['endBalance']))

        self.output(u'总收益率：\t%s%%' % formatNumber(result['totalReturn']))
        # self.output(u'总盈亏：\t%s' % formatNumber(result['totalNetPnl']))
        # self.output(u'最大回撤: \t%s' % formatNumber(result['maxDrawdown']))
        self.output(u'百分比最大回撤: %s%%' % formatNumber(result['maxDdPercent']))

        # self.output(u'总手续费：\t%s' % formatNumber(result['totalCommission']))
        # self.output(u'总滑点：\t%s' % formatNumber(result['totalSlippage']))
        self.output(u'总成交笔数：\t%s' % formatNumber(result['totalTradeCount']))

        self.output(u'Sharpe Ratio：\t%s' % formatNumber(result['sharpeRatio']))

        return result

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



#----------------------------------------------------------------------
def formatNumber(n):
    """格式化数字到字符串"""
    rn = round(n, 2)        # 保留两位小数
    return format(rn, ',')  # 加上千分符


def run_once(symbol,start_date,end_date,signal_param,filename):
    # 创建回测引擎对象
        e = BacktestingEngine()
        e.setPeriod(start_date, end_date)
        e.loadData(symbol,filename)
        e.loadPortfolio(Fut_AtrRsiPortfolio, [symbol], signal_param)

        e.runBacktesting()

        #return e.calc_btKey()

        e.calc_btKey()
        #e.showResult()


if __name__ == '__main__':

        symbol = 'ag1912'
        #symbol = 'c2001'
        start_date = '20190923 00:00:00'
        end_date   = '20191001 00:00:00'
        start_date = '20180101 00:00:00'
        end_date   = '20181231 00:00:00'
        #symbol_list = ['c2001','ag1912','CF001','SR001','rb2001']

        vtSymbol = 'c1805'
        symbol_list = [vtSymbol]

        r = []
        #filename = self.dss+'fut/bar/min5_'+vtSymbol+'.csv'
        filename = 'data/min5_'+vtSymbol+'.csv'
        # for symbol in symbol_list:
        #     for atrMaLength in [14]:
        #         for rsiLength in [5,10,15]:
        #             for trailingPercent in [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
        #                 signal_param = {symbol:{'atrMaLength':atrMaLength, 'rsiLength':rsiLength, 'trailingPercent':trailingPercent}}
        #                 result = run_once(symbol,start_date,end_date,signal_param,filename)
        #                 r.append([ atrMaLength,rsiLength,trailingPercent,result['totalReturn'],result['maxDdPercent'],result['totalTradeCount'],result['sharpeRatio'] ])
        #
        # df = pd.DataFrame(r)
        # df.to_csv('a6.csv', index=False)

        signal_param = {vtSymbol:{'atrMaLength':14, 'rsiLength':5, 'trailingPercent':0.8}}
        run_once(vtSymbol,start_date,end_date,signal_param,filename)
