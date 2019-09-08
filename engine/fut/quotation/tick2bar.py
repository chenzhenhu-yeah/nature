
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
from nature import VtBarData
from nature import SOCKET_BAR, get_dss


#----------------------------------------------------------------------
def _Generate_Bar_MinOne(tick, temp_bar, r, today):
    """生成、推送、保存Bar"""

    new_bar = VtBarData()
    new_bar.date = today
    new_bar.time = tick.UpdateTime
    new_bar.open = tick.LastPrice
    new_bar.high = tick.LastPrice
    new_bar.low =  tick.LastPrice
    new_bar.close = tick.LastPrice

    if temp_bar != []:
        bar = temp_bar.pop()
    else:
        bar = new_bar
        temp_bar.append(bar)
        return

    # 更新数据
    if bar.high < new_bar.high:
        bar.high = new_bar.high
    if bar.low > new_bar.low:
        bar.low =  new_bar.low
    bar.close = new_bar.close

    if bar.time[3:5] != new_bar.time[3:5] :
        # 将 bar的分钟改为整点，推送并保存bar
        bar.time = new_bar.time[:-2] + '00'
        r.append( [bar.date, bar.time, bar.open, bar.high, bar.low, bar.close] )


    temp_bar.append(bar)

def proc_segment(df1,begin,end,num):
    r =[]
    temp_bar = []
    today = ''          # 每段的today都一样。
    n = len(df1)
    for k, tick in df1.iterrows():
        if k == 0:
            today = tick.Localtime[:10]
        _Generate_Bar_MinOne(tick, temp_bar, r, today)
        if k == n-1:
            # 收尾处理
            if end == '23:59:59':
                # 处理交易时段跨日的情况，如品种 ag, 使生成最后一根bar
                tick.UpdateTime = '00:00:00'
                dt_today = datetime.datetime.strptime(today,'%Y-%m-%d')
                dt_today += datetime.timedelta(days=1)
                _Generate_Bar_MinOne(tick, temp_bar, r, dt_today.strftime('%Y-%m-%d'))
            else:
                tick.UpdateTime = end[:-2] + '00'
                _Generate_Bar_MinOne(tick, temp_bar, r, today)

    tm_begin = datetime.datetime.strptime(today+' '+begin,'%Y-%m-%d %H:%M:%S')
    oneminute = datetime.timedelta(minutes=1)
    next = tm_begin + oneminute
    i = 0
    while i < num:
        date = next.strftime('%Y-%m-%d')
        tm   = next.strftime('%H:%M:%S')
        #print(date,tm)
        #print(i)
        row = r[i]
        if row[0] == date and row[1] == tm:
            pass
        else:
            # 缺少bar，补齐
            bar1 = [ date, tm, row[2], row[3], row[4], row[5] ]
            r.insert(i,bar1)
        next = next +oneminute
        i += 1

    assert len(r) == num
    return r

if __name__ == "__main__":
    tradeDay = '20190904'

    symbol = 'SR001'
    symbol = 'SR909'

    fn = get_dss() + 'fut/tick/tick_' + tradeDay + '_' + symbol + '.csv'
    df = pd.read_csv(fn)
    print(df.head(3))

    fn = get_dss() + 'fut/cfg/trade_time.csv'
    df2 = pd.read_csv(fn)
    df2 = df2[df2.symbol=='SR'].sort_values(by='seq')
    print(df2)


    r = []
    for i,row in df2.iterrows():
        df1 = df[(df.UpdateTime>=row.begin) & (df.UpdateTime<=row.end)]
        df1 = df1.reset_index()
        # print(i,len(df1))
        # print(df1.head(9))
        r += proc_segment(df1, row.begin, row.end, row.num)


        #break

    df = pd.DataFrame(r, columns=['date','time','open','high','low','low'])
    df.to_csv('c1.csv',index=False)


#print(bar.date, bar.time, bar.open, bar.high, bar.low, bar.close)
        #bar.time[6:8] = '00'

        # self._Generate_Bar_Min5(bar)
        # self._Generate_Bar_Min15(bar)

        # fname = self.dss + 'fut/bar/min1_' + self.tradeDay + '_' + id + '.csv'
        # if os.path.exists(fname):
        #     df.to_csv(fname, index=False, mode='a', header=False)
        # else:
        #     df.to_csv(fname, index=False, mode='a')
