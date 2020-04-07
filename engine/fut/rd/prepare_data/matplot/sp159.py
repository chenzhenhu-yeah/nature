
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

from nature import get_dss

def sp(title, symbol1, symbol2, start_dt, end_dt, fn1, fn2):
    df1 = pd.read_csv(fn1)
    df1 = df1[(df1.date >= start_dt) & (df1.date <= end_dt)]
    df1 = df1.reset_index()
    # print(df1.head(3))

    df2 = pd.read_csv(fn2)
    df2 = df2[(df2.date >= start_dt) & (df2.date <= end_dt)]
    df2 = df2.reset_index()
    # print(df2.head(3))

    df1['close'] = df1.close - df2.close
    # print(df1.close)
    # price_min = df1['close'].min() - 100
    # price_max =  df1['close'].max() + 100

    price_min = df1['close'].min()
    price_max =  df1['close'].max()

    df1 = df1.set_index('date') 

    plt.figure(figsize=(15,7))
    plt.xticks(rotation=45)
    plt.plot(df1.close)

    plt.title(title)
    plt.grid(True, axis='y')
    ax = plt.gca()

    for label in ax.get_xticklabels():
        label.set_visible(False)
    for label in ax.get_xticklabels()[1::7]:
        label.set_visible(True)

    # plt.show()
    plt.savefig('fig1.jpg')

#------------------------------------------------------------------------------
def m59():
    year = '2019'
    year = '2020'

    title = 'm59'
    symbol1 = 'm05'
    symbol2 = 'm09'
    start_dt = year + '-01-01'
    end_dt   = year + '-04-31'
    fn1 = get_dss() +'backtest/fut/m/day_' + symbol1 + '.csv'
    fn2 = get_dss() +'backtest/fut/m/day_' + symbol2 + '.csv'


    fn1 = get_dss() +'backtest/bar/day_m2005.csv'
    fn2 = get_dss() +'backtest/bar/day_m2009.csv'

    sp(year+' - '+title, symbol1, symbol2, start_dt, end_dt, fn1, fn2)

#------------------------------------------------------------------------------
def m91():
    year = '2019'
    title = 'm91'
    symbol1 = 'm09'
    symbol2 = 'm01'
    start_dt = year + '-05-01'
    end_dt   = year + '-08-31'
    fn1 = get_dss() +'backtest/fut/m/day_' + symbol1 + '.csv'
    fn2 = get_dss() +'backtest/fut/m/day_' + symbol2 + '.csv'

    sp(year+' - '+title, symbol1, symbol2, start_dt, end_dt, fn1, fn2)

#------------------------------------------------------------------------------
def m15():
    year = '2019'
    title = 'm15'
    symbol1 = 'm01'
    symbol2 = 'm05'
    start_dt = year + '-09-01'
    end_dt   = year + '-12-31'
    fn1 = get_dss() +'backtest/fut/m/day_' + symbol1 + '.csv'
    fn2 = get_dss() +'backtest/fut/m/day_' + symbol2 + '.csv'

    sp(year+' - '+title, symbol1, symbol2, start_dt, end_dt, fn1, fn2)

#------------------------------------------------------------------------------

def y59():
    year = '2019'
    year = '2020'

    title = 'y59'
    symbol1 = 'y05'
    symbol2 = 'y09'
    start_dt = year + '-01-01'
    end_dt   = year + '-04-31'
    fn1 = get_dss() +'backtest/fut/y/day_' + symbol1 + '.csv'
    fn2 = get_dss() +'backtest/fut/y/day_' + symbol2 + '.csv'

    fn1 = get_dss() +'backtest/bar/day_y2005.csv'
    fn2 = get_dss() +'backtest/bar/day_y2009.csv'

    sp(year+' - '+title, symbol1, symbol2, start_dt, end_dt, fn1, fn2)

#------------------------------------------------------------------------------
def y91():
    year = '2019'

    title = 'y91'
    symbol1 = 'y09'
    symbol2 = 'y01'
    start_dt = year + '-05-01'
    end_dt   = year + '-08-31'
    fn1 = get_dss() +'backtest/fut/y/day_' + symbol1 + '.csv'
    fn2 = get_dss() +'backtest/fut/y/day_' + symbol2 + '.csv'

    sp(year+' - '+title, symbol1, symbol2, start_dt, end_dt, fn1, fn2)

#------------------------------------------------------------------------------
def y15():
    year = '2018'

    title = 'y15'
    symbol1 = 'y01'
    symbol2 = 'y05'
    start_dt = year + '-09-01'
    end_dt   = year + '-12-31'
    fn1 = get_dss() +'backtest/fut/y/day_' + symbol1 + '.csv'
    fn2 = get_dss() +'backtest/fut/y/day_' + symbol2 + '.csv'

    sp(year+' - '+title, symbol1, symbol2, start_dt, end_dt, fn1, fn2)

#------------------------------------------------------------------------------

def p59():
    year = '2019'
    # year = '2020'

    title = 'p59'
    symbol1 = 'p05'
    symbol2 = 'p09'
    start_dt = year + '-01-01'
    end_dt   = year + '-04-31'
    fn1 = get_dss() +'backtest/fut/p/day_' + symbol1 + '.csv'
    fn2 = get_dss() +'backtest/fut/p/day_' + symbol2 + '.csv'

    # fn1 = get_dss() +'backtest/bar/day_p2005.csv'
    # fn2 = get_dss() +'backtest/bar/day_p2009.csv'

    sp(year+' - '+title, symbol1, symbol2, start_dt, end_dt, fn1, fn2)

#------------------------------------------------------------------------------
def p91():
    year = '2018'

    title = 'p91'
    symbol1 = 'p09'
    symbol2 = 'p01'
    start_dt = year + '-05-01'
    end_dt   = year + '-08-31'
    fn1 = get_dss() +'backtest/fut/p/day_' + symbol1 + '.csv'
    fn2 = get_dss() +'backtest/fut/p/day_' + symbol2 + '.csv'

    sp(year+' - '+title, symbol1, symbol2, start_dt, end_dt, fn1, fn2)

#------------------------------------------------------------------------------
def p15():
    year = '2019'

    title = 'p15'
    symbol1 = 'p01'
    symbol2 = 'p05'
    start_dt = year + '-09-01'
    end_dt   = year + '-12-31'
    fn1 = get_dss() +'backtest/fut/p/day_' + symbol1 + '.csv'
    fn2 = get_dss() +'backtest/fut/p/day_' + symbol2 + '.csv'

    sp(year+' - '+title, symbol1, symbol2, start_dt, end_dt, fn1, fn2)

#------------------------------------------------------------------------------
def al12():
    year = '2019'

    title = 'al12'
    symbol1 = 'al01'
    symbol2 = 'al02'
    start_dt = year + '-03-01'
    end_dt   = year + '-12-31'
    fn1 = get_dss() +'backtest/fut/al/day_' + symbol1 + '.csv'
    fn2 = get_dss() +'backtest/fut/al/day_' + symbol2 + '.csv'

    sp(year+' - '+title, symbol1, symbol2, start_dt, end_dt, fn1, fn2)

#------------------------------------------------------------------------------
def al23():
    year = '2019'

    title = 'al23'
    symbol1 = 'al02'
    symbol2 = 'al03'
    start_dt = year + '-04-01'
    end_dt   = year + '-12-31'
    fn1 = get_dss() +'backtest/fut/al/day_' + symbol1 + '.csv'
    fn2 = get_dss() +'backtest/fut/al/day_' + symbol2 + '.csv'

    sp(year+' - '+title, symbol1, symbol2, start_dt, end_dt, fn1, fn2)

al23()
