
from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt

from backtestEngine import BacktestingEngine
from strategyTurtle import Test_TurtlePortfolio

if __name__ == '__main__':
    engine = BacktestingEngine()
    engine.setPeriod(datetime(2016, 1, 1), datetime(2018, 12, 30))
    engine.initPortfolio(Test_TurtlePortfolio, 'turtle')


    engine.loadData()
    engine.runBacktesting()
    engine.showResult()
