
from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt

from nature import BacktestingEngine
from nature import UpBollPortfolio

#from ipdb import set_trace

# 创建回测引擎对象
engine = BacktestingEngine()
#engine.print_engine()


engine.setPeriod(datetime(2019,5, 1), datetime(2019, 6, 20))
#engine.print_engine()

pf = UpBollPortfolio(engine)

engine.initPortfolio(pf, 100E4)
#engine.print_engine()

dbName = 'VnTrader_Daily_Db'
#dbName = 'VnTrader_1Min_Db'
engine.loadData(dbName)
#engine.print_engine()

engine.runBacktesting()
#engine.print_engine()
#set_trace()

#engine.showResult()
