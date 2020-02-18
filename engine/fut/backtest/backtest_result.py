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

#----------------------------------------------------------------------
def formatNumber(n):
    """格式化数字到字符串"""
    rn = round(n, 2)        # 保留两位小数
    return format(rn, ',')  # 加上千分符

########################################################################
class Backtest_Result(object):

    #----------------------------------------------------------------------
    def __init__(self, p):
        self.portfolio = p

    #----------------------------------------------------------------------
    def calculateResult(self, annualDays=240):
        """计算结果"""
        self.output(u'开始统计回测结果')

        for result in self.portfolio.resultList:
            result.calculatePnl()

        resultList = self.portfolio.resultList
        dateList = [result.date for result in resultList]
        #print(dateList)

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

        # r = [[item] for item in balanceList]
        # r = [[item] for item in netPnlList]
        # print(self.date, self.netPnl, self.holdingPnl, self.tradingPnl, self.commission, self.slippage)
        r = [[item.date, item.netPnl, item.holdingPnl, item.tradingPnl, item.commission, item.slippage] for item in resultList ]
        df = pd.DataFrame(r)
        df.to_csv('a.csv',index=False)

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

        # df = pd.DataFrame(timeseries)
        # df.to_csv('b.csv',index=False)

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

        # plt.show()

    #----------------------------------------------------------------------
    def output(self, content):
        """输出信息"""
        print(content)

    #----------------------------------------------------------------------
    def show_result_key(self):
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

        # print(timeseries)

        return result

if __name__ == '__main__':
    pass
