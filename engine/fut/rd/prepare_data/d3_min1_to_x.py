
import os
import time
import datetime
import json
import pandas as pd
import schedule
import threading
from multiprocessing.connection import Client


from nature import CtpTrade
from nature import CtpQuote
from nature import Tick

from nature import VtBarData, to_log, BarGenerator
from nature import SOCKET_BAR, get_dss

########################################################################


def one2x(pz,symbol,filename):
    df = pd.read_csv(filename)

    # 生成minx
    g5 = BarGenerator('min5')
    g15 = BarGenerator('min15')
    g30 = BarGenerator('min30')
    g_day = BarGenerator('day')
    for i, row in df.iterrows():
        new_bar = VtBarData()
        new_bar.vtSymbol = symbol
        new_bar.date = row.date
        new_bar.time = row.time
        new_bar.open = row.open
        new_bar.high = row.high
        new_bar.low =  row.low
        new_bar.close = row.close
        g5.update_bar(new_bar)
        g15.update_bar(new_bar)
        g30.update_bar(new_bar)
        g_day.update_bar(new_bar)

    # 保存min5
    r5 = g5.r_dict[symbol]
    df_symbol = pd.DataFrame(r5, columns=['date','time','open','high','low','close','volume'])
    fname = get_dss() +'backtest/fut/' + pz + '/min5_' + symbol+'.csv'
    if os.path.exists(fname):
        df_symbol.to_csv(fname, index=False, mode='a', header=False)
    else:
        df_symbol.to_csv(fname, index=False, mode='a')

    # 保存min15
    r15 = g15.r_dict[symbol]
    df_symbol = pd.DataFrame(r15, columns=['date','time','open','high','low','close','volume'])
    fname = get_dss() +'backtest/fut/' + pz + '/min15_' + symbol+'.csv'
    if os.path.exists(fname):
        df_symbol.to_csv(fname, index=False, mode='a', header=False)
    else:
        df_symbol.to_csv(fname, index=False, mode='a')

    # 保存min30
    r30 = g30.r_dict[symbol]
    df_symbol = pd.DataFrame(r30, columns=['date','time','open','high','low','close','volume'])
    fname = get_dss() +'backtest/fut/' + pz + '/min30_' + symbol+'.csv'
    if os.path.exists(fname):
        df_symbol.to_csv(fname, index=False, mode='a', header=False)
    else:
        df_symbol.to_csv(fname, index=False, mode='a')

    # 保存day
    r_day = g_day.r_dict[symbol]
    df_symbol = pd.DataFrame(r_day, columns=['date','time','open','high','low','close','volume'])
    fname = get_dss() +'backtest/fut/' + pz + '/day_'+symbol+'.csv'
    if os.path.exists(fname):
        df_symbol.to_csv(fname, index=False, mode='a', header=False)
    else:
        df_symbol.to_csv(fname, index=False, mode='a')


if __name__ == "__main__":
    pz = 'y'
    symbol = pz + '09'
    fn = get_dss() +'backtest/fut/' + pz + '/' + symbol + '.csv'

    one2x(pz,symbol,fn)
