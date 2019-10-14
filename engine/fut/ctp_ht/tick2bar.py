
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
            #print(date)


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

def tick2bar(tradeDay):

    #读取交易时段文件
    fn = get_dss() + 'fut/cfg/trade_time.csv'
    df_tm = pd.read_csv(fn)

    # 加载配置，目前盯市哪些业务品种
    config = open(get_dss()+'fut/cfg/config.json')
    setting = json.load(config)
    symbols = setting['symbols']
    symbol_list = symbols.split(',')

    #symbol_list = ['ag1912']

    # 逐一处理每个业务品种
    for symbol in symbol_list:
        # 读取品种的tick文件
        fn = get_dss() + 'fut/tick/tick_' + tradeDay + '_' + symbol + '.csv'
        print(fn)
        if os.path.exists(fn):
            df = pd.read_csv(fn)
            # print(df.head(3))

            # 读品种配置文件，获取四个交易时段
            pz = symbol[:2]
            if pz.isalpha():
                pass
            else:
                # 大连交易所业务品种只有一个字母
                pz = symbol[:1]

            # 逐个时段遍历处理
            df2 = df_tm[df_tm.symbol==pz].sort_values(by='seq')
            r1 = []
            for i,row in df2.iterrows():
                if row.end > row.begin:
                    df1 = df[(df.UpdateTime>=row.begin) & (df.UpdateTime<=row.end)]
                else:
                    # 夜盘跨零点交易品种，作特殊处理
                    df11 = df[(df.UpdateTime>=row.begin) & (df.UpdateTime<='23:59:59')]
                    df12 = df[(df.UpdateTime>='00:00:00') & (df.UpdateTime<=row.end)]
                    df1 = pd.concat([df11, df12])

                if len(df1) > 0:
                    # 排序很重要，因为tick送过来的顺序可能是乱的
                    df1 = df1.sort_values(by=['UpdateDate','UpdateTime'])
                    df1 = df1.reset_index()
                    # print(i,len(df1))
                    # print(df1.head(9))
                    r1 += proc_segment(df1, row.begin, row.end, row.num)


            df_symbol = pd.DataFrame(r1, columns=['date','time','open','high','low','close','volume'])
            fname = get_dss() + 'fut/bar/min1_' + symbol + '.csv'
            if os.path.exists(fname):
                df_symbol.to_csv(fname, index=False, mode='a', header=False)
            else:
                df_symbol.to_csv(fname, index=False, mode='a')

            # 生成min5
            r5 = []
            temp_bar = []
            for row in r1:
                new_bar = VtBarData()
                new_bar.date = row[0]
                new_bar.time = row[1]
                new_bar.open = row[2]
                new_bar.high = row[3]
                new_bar.low =  row[4]
                new_bar.close = row[5]
                Generate_Bar_Min5(new_bar, temp_bar, r5)

            df_symbol = pd.DataFrame(r5, columns=['date','time','open','high','low','close','volume'])
            fname = get_dss() + 'fut/bar/min5_' + symbol + '.csv'
            if os.path.exists(fname):
                df_symbol.to_csv(fname, index=False, mode='a', header=False)
            else:
                df_symbol.to_csv(fname, index=False, mode='a')

            # 生成min15
            r15 = []
            temp_bar = []
            for row in r1:
                new_bar = VtBarData()
                new_bar.date = row[0]
                new_bar.time = row[1]
                new_bar.open = row[2]
                new_bar.high = row[3]
                new_bar.low =  row[4]
                new_bar.close = row[5]
                Generate_Bar_Min15(new_bar, temp_bar, r15)

            df_symbol = pd.DataFrame(r15, columns=['date','time','open','high','low','close','volume'])
            fname = get_dss() + 'fut/bar/min15_' + symbol + '.csv'
            if os.path.exists(fname):
                df_symbol.to_csv(fname, index=False, mode='a', header=False)
            else:
                df_symbol.to_csv(fname, index=False, mode='a')

if __name__ == "__main__":
    tradeDay = '20191014'
    tick2bar(tradeDay)
