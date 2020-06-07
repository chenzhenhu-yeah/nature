import numpy as np
import pandas as pd

import smtplib
from email.mime.text import MIMEText

import os
import re
from datetime import datetime
import time
from csv import DictReader
from nature import get_dss, get_trading_dates, get_daily, get_stk_hfq, get_contract, to_log
from nature import get_symbols_quote


def update_date():
    dirname = get_dss() + 'fut/engine/opt/'
    listfile = os.listdir(dirname)

    for filename in listfile:
        if filename[:4] == 'book':
            print(filename)
            fn = dirname + filename
            df = pd.read_csv(fn)
            date_list = list(df.date.astype('str'))
            date_list = [x[:4]+'-'+x[4:6]+'-'+x[6:8] for x in date_list]
            df['date'] = date_list
            # print(df)
            df.to_csv(fn, index=False)
            # break

def update_rec_price(rec):
    dss = get_dss()
    net_pnl = rec.netPnl
    pos_dict = eval(rec.posDict)
    close_dict = eval(rec.closeDict)

    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    rec.date = today

    for symbol in pos_dict:
        # 读 day_symbol.csv 文件，获取最新收盘价，再做其他处理
        size = int(get_contract(symbol).size)
        fn = dss + 'opt/' + today[:7] + '.csv'
        if os.path.exists(fn):
            df = pd.read_csv(fn)
            df = df[df.Instrument == symbol]
            row = df.iloc[-1,:]
            net_pnl += (row.LastPrice - close_dict[symbol]) * pos_dict[symbol] * size
            close_dict[symbol] = row.LastPrice

    rec.netPnl = net_pnl
    rec.closeDict = str(close_dict)

def fresh_book():
    """更新booking文件，计算当日盈亏"""
    """booking文件转成booked文件"""

    # 获取opt目录下全部booking文件
    dirname = get_dss() + 'fut/engine/opt/'
    listfile = os.listdir(dirname)

    for filename in listfile:
        # 逐个文件更新收盘价，并计算当日盈亏
        if filename[:7] == 'booking':
            fn = dirname + filename
            df = pd.read_csv(fn)
            rec = df.iloc[-1,:]

            # 如果已结清，转成booked文件，否则更新价格
            booked = True
            pos_dict = eval(rec.posDict)
            for symbol in pos_dict:
                if pos_dict[symbol] != 0:
                    booked = False

            if booked == True:
                fn_booked = fn_book.replace('booking', 'booked')
                os.rename(fn_book,fn_booked)
            else:
                update_rec_price(rec)
                df = pd.DataFrame([rec])
                df.to_csv(fn, index=False, header=None, mode='a')

def alter_book_by_rec(row):
    margin = 0
    net_pnl = 0
    pos_dict = {}
    close_dict = {}
    size = int(get_contract(row.Instrument).size)

    fn_book = dss + 'fut/engine/opt/' + row.book + '.csv'
    if os.path.exists(fn_book):
        df_book = pd.read_csv(fn_book)
        rec = df_book.iloc[-1,:]
        margin = rec.margin
        net_pnl = rec.netPnl
        pos_dict = eval(rec.posDict)
        close_dict = eval(rec.closeDict)

    margin += row.margin

    # 如果是book中不存在的新合约
    if row.InstrumentID not in pos_dict:
        pos_dict[row.InstrumentID] = 0
        close_dict[row.InstrumentID] = row.Price

    # 更新pos_dict
    if row.Direction == 'Buy':
        pos_dict[row.InstrumentID] += row.Volume
    if row.Direction == 'Sell':
        pos_dict[row.InstrumentID] += -row.Volume

    # 更新net_pnl
    if row.Offset == 'Open':
        if row.Direction == 'Buy':
            net_pnl += (row.Price - close_dict[row.InstrumentID]) * row.Volume * size
        if row.Direction == 'Sell':
            net_pnl -= (row.Price - close_dict[row.InstrumentID]) * row.Volume * size

    if row.Offset == 'Close':
        if row.Direction == 'Buy':
            net_pnl -= (row.Price - close_dict[row.InstrumentID]) * row.Volume * size
        if row.Direction == 'Sell':
            net_pnl += (row.Price - close_dict[row.InstrumentID]) * row.Volume * size

    df_book = pd.DataFrame([[row.TradingDay,margin,net_pnl,str(pos_dict),str(close_dict)]], columns=['date','margin','netPnl','posDict','closeDict'])
    if os.path.exists(fn_book):
        df_book.to_csv(fn_book, index=False, header=None, mode='a')
    else:
        df_book.to_csv(fn_book, index=False)


def trade2book():
    """分析opt_trade文件，生成新的booking文件，或转成booked文件"""

    dss = get_dss()
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    r = []
    fn = dss + 'fut/engine/opt/opt_trade.csv'
    df = pd.read_csv(fn)
    for i, row in df.iterrows():
        if row.book != row.book:           # 值为nan
            r.append(row)                  # 此记录无需处理，回写文件即可
            continue

        alter_book_by_rec(row)

        # 设置p与booking的关系
        p = row.portfolio
        if p != p:           # 值为nan
            pass
        else:
            fn_p = ''
            pz = str(get_contract(row.Instrument).pz)
            if p[:7] == 'daliopt':
                fn_p = dss + 'fut/engine/daliopt/portfolio_' + p + '_' + pz + '_var.csv'
            if p[:4] == 'star':
                fn_p = dss + 'fut/engine/star/portfolio_' + p + '_' + pz + '_var.csv'
            if p[:6] == 'mutual':
                fn_p = dss + 'fut/engine/mutual/portfolio_' + p + '_' + pz + '_var.csv'
            if fn_p != '':
                df_p = pd.read_csv(fn_p)
                rec = df_p.iloc[-1,:]
                pos_dict = eval(rec.posDict)
                if row.book not in pos_dict:
                    pos_dict[row.book] = 1
                    rec.posDict = str(pos_dict)
                    rec.datetime = today + ' 15:00:00'
                    df_p = pd.DataFrame([rec])
                    df_p.to_csv(fn_p, index=False, header=None, mode='a')

    # 回写文件
    if r == []:
        df = pd.DataFrame([], columns=['Direction','ExchangeID','InstrumentID','Offset','Price','TradeID','TradeTime','TradingDay','Volume','book','portfolio','margin'])
    else:
        df = pd.DataFrame(r)
    df.to_csv(fn, index=False)

def get_trade():
    """从成交反馈文件中提取期权成交记录，追加到opt_trade文件中"""

    now = datetime.now()
    today = int( now.strftime('%Y%m%d') )

    fn = get_dss() +  'fut/engine/gateway_trade.csv'
    df = pd.read_csv(fn)
    df = df[df.TradingDay == today]                 # 当日交易记录
    df = df[df.InstrumentID.str.len() >= 9]         # 期权合约
    # print(df)
    if len(df) > 0:
        df = df.drop_duplicates()
        del df['OrderID']
        del df['SysID']
        df['book'] = ''
        df['portfolio'] = ''
        df['margin'] = 0
        date_list = list(df.TradingDay.astype('str'))
        date_list = [x[:4]+'-'+x[4:6]+'-'+x[6:8] for x in date_list]
        df['TradingDay'] = date_list
        # print(df)
        fn = get_dss() + 'fut/engine/opt/opt_trade.csv'
        if os.path.exists(fn):
            df.to_csv(fn, index=False, header=None, mode='a')
        else:
            df.to_csv(fn, index=False)

def book_opt_run():
    # 以下调用顺序不能乱！
    fresh_book()
    trade2book()
    get_trade()

if __name__ == '__main__':
    # book_opt_run()
    # update_date()
    pass
