
import os
import time
import datetime
import json
import pandas as pd
import schedule
import threading
from multiprocessing.connection import Client
import traceback

from nature import CtpTrade
from nature import CtpQuote
from nature import Tick

from nature import VtBarData, to_log, BarGenerator
from nature import SOCKET_BAR, get_dss, get_contract
from nature import get_symbols_quote

#----------------------------------------------------------------------
def _Generate_Bar_MinOne(tick, temp_bar, r, today):
    """
    从tick加工生成bar_min1
    temp_bar：存储上一个bar
    r：存储数据加工结果
    today：tradeDay
    """

    new_bar = VtBarData()
    new_bar.date = today
    new_bar.time = tick.UpdateTime
    new_bar.open = tick.LastPrice
    new_bar.high = tick.LastPrice
    new_bar.low =  tick.LastPrice
    new_bar.close = tick.LastPrice

    # 上一个bar存储在变量bar中，最新bar存储在变量new_bar中。
    if temp_bar != []:
        bar = temp_bar.pop()
    else:
        bar = new_bar
        temp_bar.append(bar)
        return

    # if bar.time[3:5] != new_bar.time[3:5] :
    if bar.time[:5] != new_bar.time[:5] :
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

def proc_segment(df1,begin,end,num,symbol):
    """
    df1:该时段内的tick明细
    begin:该时段起始时间
    end:该时段结束时间
    num:该时段应包含的bar数目
    """
    r =[]
    temp_bar = []
    begin_day = ''            # 该时段起始交易所在日期
    end_day = ''              # 该时段结束交易所在日期
    n = len(df1)
    # part1: 初加工，将tick转成bar_min1。(这段逻辑写得有点绕，应该还有改进的空间。)
    for k, tick in df1.iterrows():
        # 第一条记录，确定关键变量的初始值
        if k == 0:
            if end > begin:
                # begin_day与end_day为同一天
                begin_day = tick.UpdateDate
                end_day = tick.UpdateDate
            else:
                # 跨零点的时段
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

        _Generate_Bar_MinOne(tick, temp_bar, r, tick.UpdateDate)

        # 收尾处理
        if k == n-1:
            # 将更新时间设为该时段的结束时间
            tick.UpdateTime = end[:-2] + '00'
            _Generate_Bar_MinOne(tick, temp_bar, r, end_day)

    # if num == 240:
    #     # print(r)
    #     print(len(r), num)

    # part2: 该时段内每个bar的时间是确定的，如果有数据缺失，补全。
    tm_begin = datetime.datetime.strptime(begin_day+' '+begin,'%Y-%m-%d %H:%M:%S')
    oneminute = datetime.timedelta(minutes=1)
    next = tm_begin + oneminute
    i = 0
    while i < num:
        date = next.strftime('%Y-%m-%d')
        tm   = next.strftime('%H:%M:%S')

        # 周末，要加两天。节假日通常不开夜盘!!!。
        if tm == '00:00:00' and date != end_day:
            #print(date, end_day)
            #print(next)

            two_days = datetime.timedelta(days=2)
            next += two_days
            date = next.strftime('%Y-%m-%d')

        row = r[i]
        if row[0] == date and row[1] == tm:
            pass
        else:
            # 缺少bar，补齐
            # to_log( 'tick数据缺失，已补齐：'+ date + ' ' + tm + ' ' + symbol + ' ' + str(row[2]) )
            bar1 = [ date, tm, row[2], row[3], row[4], row[5], 0 ]
            r.insert(i,bar1)
        next = next + oneminute
        i += 1

    assert len(r) == num
    return r

def save_bar(r1, symbol):
    # 保存min1
    df_symbol = pd.DataFrame(r1, columns=['date','time','open','high','low','close','volume'])
    fname = get_dss() + 'fut/bar/min1_' + symbol + '.csv'
    if os.path.exists(fname):
        df_symbol.to_csv(fname, index=False, mode='a', header=False)
    else:
        df_symbol.to_csv(fname, index=False, mode='a')

    # 根据min1的结果集生成minx
    g5 = BarGenerator('min5')
    g15 = BarGenerator('min15')
    g30 = BarGenerator('min30')
    g_day = BarGenerator('day')
    for row in r1:
        new_bar = VtBarData()
        new_bar.vtSymbol = symbol
        new_bar.date = row[0]
        new_bar.time = row[1]
        new_bar.open = row[2]
        new_bar.high = row[3]
        new_bar.low =  row[4]
        new_bar.close = row[5]
        g5.update_bar(new_bar)
        g15.update_bar(new_bar)
        g30.update_bar(new_bar)
        g_day.update_bar(new_bar)

    # 保存min5
    r5 = g5.r_dict[symbol]
    df_symbol = pd.DataFrame(r5, columns=['date','time','open','high','low','close','volume'])
    fname = get_dss() + 'fut/bar/min5_' + symbol + '.csv'
    if os.path.exists(fname):
        df_symbol.to_csv(fname, index=False, mode='a', header=False)
    else:
        df_symbol.to_csv(fname, index=False, mode='a')

    # 保存min15
    r15 = g15.r_dict[symbol]
    df_symbol = pd.DataFrame(r15, columns=['date','time','open','high','low','close','volume'])
    fname = get_dss() + 'fut/bar/min15_' + symbol + '.csv'
    if os.path.exists(fname):
        df_symbol.to_csv(fname, index=False, mode='a', header=False)
    else:
        df_symbol.to_csv(fname, index=False, mode='a')

    # 保存min30
    r30 = g30.r_dict[symbol]
    df_symbol = pd.DataFrame(r30, columns=['date','time','open','high','low','close','volume'])
    fname = get_dss() + 'fut/bar/min30_' + symbol + '.csv'
    if os.path.exists(fname):
        df_symbol.to_csv(fname, index=False, mode='a', header=False)
    else:
        df_symbol.to_csv(fname, index=False, mode='a')

    # 保存day
    r_day = g_day.r_dict[symbol]
    df_symbol = pd.DataFrame(r_day, columns=['date','time','open','high','low','close','volume'])
    fname = get_dss() + 'fut/bar/day_' + symbol + '.csv'
    if os.path.exists(fname):
        df_symbol.to_csv(fname, index=False, mode='a', header=False)
    else:
        df_symbol.to_csv(fname, index=False, mode='a')


def tick2bar(tradeDay):
    #读取交易时段文件
    fn = get_dss() + 'fut/cfg/trade_time.csv'
    df_tm = pd.read_csv(fn)

    symbol_list = get_symbols_quote()
    # symbol_list = ['ag1912']

    # 逐个处理每个合约
    for symbol in symbol_list:
        try:
            # 读取品种的tick文件
            fn = get_dss() + 'fut/tick/tick_' + tradeDay + '_' + symbol + '.csv'
            if os.path.exists(fn) == False:
                # print(fn+' not exists')
                continue

            # print(fn+' begin ... ')
            df = pd.read_csv(fn)

            # 获取品种交易时段
            pz = get_contract(symbol).pz
            df2 = df_tm[df_tm.symbol==pz].sort_values(by='seq')
            r1 = []
            # 逐个时段遍历处理
            for i,row in df2.iterrows():
                # 组装该时段数据，进行数据清洗
                if row.end > row.begin:
                    # 非夜盘跨零点交易时段
                    df1 = df[(df.UpdateTime>=row.begin) & (df.UpdateTime<=row.end)]
                    if len(df1) > 0:
                        # 处理tick异常数据，删除盘后非交易时段推送的数据
                        df1 = df1.sort_values(by=['UpdateDate','UpdateTime'])
                        df1 = df1.reset_index()
                        dt = df1.at[0,'UpdateDate']
                        df1 = df1[df1.UpdateDate == dt]
                else:
                    # 夜盘跨零点交易时段，作特殊拼接处理
                    df11 = df[(df.UpdateTime>=row.begin) & (df.UpdateTime<='23:59:59')]
                    df12 = df[(df.UpdateTime>='00:00:00') & (df.UpdateTime<=row.end)]
                    df1 = pd.concat([df11, df12])

                # 加工生成该时段的 bar_min1 数据，至少有2根tick才能加工成bar
                if len(df1) > 3:
                    # 排序很重要，因为tick送过来的顺序可能是乱的
                    df1 = df1.sort_values(by=['UpdateDate','UpdateTime'])
                    df1 = df1.reset_index()
                    # 处理该时段的tick，加工返回bar数据集
                    r1 += proc_segment(df1, row.begin, row.end, row.num, symbol)
                else:
                    if len(symbol) < 9:
                        to_log( symbol + ' 时段数据缺失：'+ tradeDay + ' ' + str(row.seq) )

            # 该合约处理完毕，保存各周期bar到文件
            if len(r1) > 0:
                save_bar(r1, symbol)

        except Exception as e:
            s = traceback.format_exc()
            to_log(s)
            to_log('error: ' + symbol)
            continue

if __name__ == "__main__":
    now = datetime.datetime.now()
    tradeDay = now.strftime('%Y%m%d')
    tradeDay = '20201127'

    tick2bar(tradeDay)
