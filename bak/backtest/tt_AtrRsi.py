
from nature import Fut_AtrRsiPortfolio, Fut_CciPortfolio, Fut_BollPortfolio
from backtestEngine import BacktestingEngine

def bt_single(strategyClass, code):
    e = BacktestingEngine()
    e.setPeriod('20171023 00:00:00', '20191130 00:00:00')
    e.loadData_min1(code)

    # 在1分钟的数据集上回测
    e.loadPortfolio(strategyClass, code)
    e.runBacktesting('min1')
    e.calc_btKey()

    # 在5分钟的数据集上回测
    e.calcData_min5()
    e.loadPortfolio(strategyClass, code)
    e.runBacktesting('min5')
    e.calc_btKey()

    # 在15分钟的数据集上回测
    e.calcData_min15()
    e.loadPortfolio(strategyClass, code)
    e.runBacktesting('min15')
    e.calc_btKey()

    # 在30分钟的数据集上回测
    e.calcData_min30()
    e.loadPortfolio(strategyClass, code)
    e.runBacktesting('min30')
    e.calc_btKey()

if __name__ == '__main__':
    strategyClass = Fut_AtrRsiPortfolio
    #codes = ['c1901','IF88','CF901','rb1901','ru1901','SR901']
    codes = ['CF901','rb1901','ru1901','SR901']
    for code in codes:
        bt_single(strategyClass, code)
