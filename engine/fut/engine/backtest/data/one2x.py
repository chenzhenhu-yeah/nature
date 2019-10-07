
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

    if bar.time[3:5] != new_bar.time[3:5] :
        # 将 bar的分钟改为整点，推送并保存bar
        bar.date = new_bar.date
        bar.time = new_bar.time[:-2] + '00'
        r.append( [bar.date, bar.time, bar.open, bar.high, bar.low, bar.close, 0] )
        bar.open = new_bar.open
        bar.high = new_bar.high
        bar.low = new_bar.low
        bar.close = new_bar.close
    else:
        # 更新数据
        if bar.high < new_bar.high:
            bar.high = new_bar.high
        if bar.low > new_bar.low:
            bar.low =  new_bar.low
        bar.close = new_bar.close

    temp_bar.append(bar)

def proc_segment(df1,begin,end,num):
    """
    df1:该时段内的tick明细
    begin:该时段起始时间
    end:该时段结束时间
    num:该时段应包含的bar数目
    """
    r =[]
    temp_bar = []
    begin_day = ''
    end_day = ''
    n = len(df1)
    for k, tick in df1.iterrows():
        # 第一条记录，确定关键变量的初始值
        if k == 0:
            if end > begin:
                # begin_day与end_day为同一天
                begin_day = tick.UpdateDate
                end_day = tick.UpdateDate
            else:
                # 跨零点
                if tick.UpdateTime>='00:00:00' and tick.UpdateTime <= '02:30:01':
                    # 正常情况下，这段逻辑用不着。仅适用于数据缺失的情况。
                    end_day = tick.UpdateDate

                    dt1 = datetime.datetime.strptime(end_day,'%Y-%m-%d')
                    weekday = int(dt1.strftime('%w'))
                    if weekday == 1:
                        dt0 = dt1 - datetime.timedelta(days=3)
                    else:
                        dt0 = dt1 - datetime.timedelta(days=1)

                    begin_day = dt0.strftime('%Y-%m-%d')
                else:
                    begin_day = tick.UpdateDate
                    dt0 = datetime.datetime.strptime(begin_day,'%Y-%m-%d')
                    weekday = int(dt0.strftime('%w'))
                    if weekday == 5:
                        dt1 = dt0 + datetime.timedelta(days=3)
                    else:
                        dt1 = dt0 + datetime.timedelta(days=1)

                    end_day = dt1.strftime('%Y-%m-%d')
            #print(begin_day,end_day)

        _Generate_Bar_MinOne(tick, temp_bar, r, tick.UpdateDate)

        if k == n-1:
            # 收尾处理
            tick.UpdateTime = end[:-2] + '00'
            _Generate_Bar_MinOne(tick, temp_bar, r, end_day)

    # if num == 60:
    #     print(r)
    #     print(len(r), num)

    # 如果有数据缺失，补全。
    tm_begin = datetime.datetime.strptime(begin_day+' '+begin,'%Y-%m-%d %H:%M:%S')
    oneminute = datetime.timedelta(minutes=1)
    next = tm_begin + oneminute
    i = 0
    while i < num:
        date = next.strftime('%Y-%m-%d')
        tm   = next.strftime('%H:%M:%S')

        # 周末，要加两天。节假日通常不开夜盘。
        if tm == '00:00:00' and date != end_day:
            #print(date, end_day)
            #print(next)

            two_days = datetime.timedelta(days=2)
            next += two_days
            date = next.strftime('%Y-%m-%d')

            #print(next)


        #print(date,tm)
        #print(i)
        row = r[i]
        if row[0] == date and row[1] == tm:
            pass
        else:
            # 缺少bar，补齐
            to_log( '当tick2bar时数据有缺失：'+ date + ' ' + tm + ' ' + str(row[2]) )
            bar1 = [ date, tm, row[2], row[3], row[4], row[5], 0 ]
            r.insert(i,bar1)
        next = next + oneminute
        i += 1

    # if num == 60:
    #      print(r)
    #      print(len(r), num)

    # print(len(r), num)
    assert len(r) == num
    return r

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

def one2five(filename):

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
    fname = 'min5_c1805.csv'
    if os.path.exists(fname):
        df_symbol.to_csv(fname, index=False, mode='a', header=False)
    else:
        df_symbol.to_csv(fname, index=False, mode='a')


if __name__ == "__main__":
    filename = 'min1_c1805.csv'
    one2five(filename)
