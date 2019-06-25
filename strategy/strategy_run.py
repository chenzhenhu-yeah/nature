
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta



import sys
sys.path.append(r'../')
from strategy.simmer.simmer import simmer_run
from strategy.home_run.home_run import home_run
from strategy.up_overlap_up.up_overlap_up import up_run
from strategy.daily_view.daily_view import daily_view_run


def strategy_run(dss):
    # simmer_run(dss)
    # home_run(dss)
    # up_run(dss)
    # daily_view_run(dss)
    pass

if __name__ == "__main__":
    strategy_run(r'../../data/')
