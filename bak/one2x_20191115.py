
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

from nature import VtBarData, to_log
from nature import SOCKET_BAR, get_dss

########################################################################
class BarGenerator(object):

    #----------------------------------------------------------------------
    def __init__(self, minx):
        """Constructor"""
        self.minx = minx
        self.bar_minx_dict = {}

    #----------------------------------------------------------------------
    def update_bar(self, new_bar):

        id = new_bar.vtSymbol
        if id in self.bar_minx_dict:
            bar = self.bar_minx_dict[id]
        else:
            bar = new_bar
            self.bar_minx_dict[id] = bar
            return None

        # 更新数据
        if bar.high < new_bar.high:
            bar.high = new_bar.high
        if bar.low > new_bar.low:
            bar.low =  new_bar.low
        bar.close = new_bar.close

        if self.minx == 'min5' and new_bar.time[3:5] in ['05','10','15','20','25','30','35','40','45','50','55','00']:
            # 将 bar的分钟改为整点，推送并保存bar
            bar.time = new_bar.time[:-2] + '00'
            self.bar_minx_dict.pop(id)
            return bar
        elif self.minx == 'min15' and new_bar.time[3:5] in ['15','30','45','00']:
            # 将 bar的分钟改为整点，推送并保存bar
            bar.time = new_bar.time[:-2] + '00'
            self.bar_minx_dict.pop(id)
            return bar
        else:
            self.bar_minx_dict[id] = bar

        return None

    #----------------------------------------------------------------------
    def save_bar(self, bar):
        df = pd.DataFrame([bar.__dict__])
        cols = ['date','time','open','high','low','close','volume']
        df = df[cols]

        fname = get_dss() + 'fut/put/rec/' + self.minx + '_' + bar.vtSymbol + '.csv'
        if os.path.exists(fname):
            df.to_csv(fname, index=False, mode='a', header=False)
        else:
            df.to_csv(fname, index=False, mode='a')

########################################################################

def Generate_Bar_Min5(new_bar, temp_bar, r):
    """生成、推送、保存Bar"""
    if temp_bar != []:
        bar = temp_bar.pop()
    else:
        bar = new_bar
        temp_bar.append(bar)
        return

    if bar.high < new_bar.high:
            bar.high = new_bar.high
    if bar.low > new_bar.low:
            bar.low =  new_bar.low
    bar.close = new_bar.close

    if new_bar.time[3:5] in ['05','10','15','20','25','30','35','40','45','50','55','00']:
        # 将 bar的分钟改为整点，推送并保存bar
        bar.date = new_bar.date
        bar.time = new_bar.time[:-2] + '00'
        r.append( [bar.date, bar.time, bar.open, bar.high, bar.low, bar.close, 0] )
    else:
        temp_bar.append(bar)

#----------------------------------------------------------------------
def Generate_Bar_Min15(new_bar, temp_bar, r):
    """生成、推送、保存Bar"""
    if temp_bar != []:
        bar = temp_bar.pop()
    else:
        bar = new_bar
        temp_bar.append(bar)
        return

    if bar.high < new_bar.high:
            bar.high = new_bar.high
    if bar.low > new_bar.low:
            bar.low =  new_bar.low
    bar.close = new_bar.close

    if new_bar.time[3:5] in ['15','30','45','00']:
        # 将 bar的分钟改为整点，推送并保存bar
        bar.date = new_bar.date
        bar.time = new_bar.time[:-2] + '00'
        r.append( [bar.date, bar.time, bar.open, bar.high, bar.low, bar.close, 0] )
    else:
        temp_bar.append(bar)

def one2five(symbol):
    filename = 'min1_'+symbol+'.csv'
    df = pd.read_csv(filename)

    # 生成min5
    r5 = []
    temp_bar = []
    for i, row in df.iterrows():
        dt = row.datetime
        new_bar = VtBarData()
        new_bar.date = dt[:10]
        new_bar.time = dt[11:19]
        new_bar.open = row.open
        new_bar.high = row.high
        new_bar.low =  row.low
        new_bar.close = row.close
        Generate_Bar_Min5(new_bar, temp_bar, r5)

    df_symbol = pd.DataFrame(r5, columns=['date','time','open','high','low','close','volume'])
    fname = 'min5_'+symbol+'.csv'
    if os.path.exists(fname):
        df_symbol.to_csv(fname, index=False, mode='a', header=False)
    else:
        df_symbol.to_csv(fname, index=False, mode='a')


if __name__ == "__main__":
    symbol = 'CF901'
    one2five(symbol)
