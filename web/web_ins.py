
import numpy as np
import pandas as pd
from flask import Flask, render_template, request, redirect
from flask import url_for
from datetime import datetime, timedelta
from multiprocessing.connection import Client
import time
import tushare as ts
import json
import os
import traceback


from nature import del_blank, check_symbols_p
from nature import read_log_today, get_dss, get_symbols_quote, get_contract, send_email
from nature import draw_web, ic_show, ip_show, smile_show, opt, dali_show, yue, mates, iv_ts, star
from nature import iv_straddle_show, hv_show, skew_show, book_min5_show, book_min5_now_show
from nature import open_interest_show, hs300_spread_show, straddle_diff_show, iv_show, iv_min5_show
from nature import get_file_lock, release_file_lock, r_file, a_file

app = Flask(__name__)
# app = Flask(__name__, static_url_path="/render")
# app = Flask(__name__,template_folder='tpl') # 指定一个参数使用自己的模板目录

@app.route('/')
def index():
 return 'log file ins'

@app.route('/fut')
def fut():
    config = open(get_dss()+'fut/cfg/config.json')
    setting = json.load(config)
    symbol = setting['symbols_quote_canary']
    assert symbol[:2] == 'ag'
    # symbol = 'ag2012'

    # 只生成ag的min1文件，其他品种不产生
    filename = get_dss() + 'fut/put/min1_' + symbol + '.csv'
    df = pd.read_csv(filename, dtype='str')
    r_q = [ list(df.columns) ]
    for i, row in df.iterrows():
        r_q.append( list(row) )

    filename = get_dss() + 'fut/put/rec/min5_' + symbol + '.csv'
    df = pd.read_csv(filename, dtype='str')
    r_t = [ list(df.columns) ]
    row = df.iloc[-1,:]
    r_t.append( list(row) )

    filename = get_dss() + 'fut/engine/engine_deal.csv'
    df = pd.read_csv(filename, dtype='str')
    r = []
    for i, row in df.iterrows():
        r.append( list(row) )
    r.append( list(df.columns) )
    r = list( reversed(r) )
    r = r[:12]

    return render_template("fut.html",title=symbol,rows_q=r_q,rows_t=r_t,rows=r)

@app.route('/fut_csv')
def fut_csv():
    return render_template("fut_csv.html",title="fut_csv")

@app.route('/show_fut_csv', methods=['post'])
def show_fut_csv():
    filename = get_dss() + request.form.get('filename')
    df = pd.read_csv(filename, dtype='str')

    r = []
    for i, row in df.iterrows():
        r.append( list(row) )
    r.append( list(df.columns) )
    r = reversed(r)

    return render_template("show_fut_csv.html",title="Show Log",rows=r)

@app.route('/fut_config', methods=['get','post'])
def fut_config():
    tips = '提示：'
    filename = get_dss() + 'fut/cfg/config.json'
    if request.method == "POST":
        key = del_blank( request.form.get('key') )
        value = del_blank( request.form.get('value') )

        s = check_symbols_p(key, value)
        if s != '':
            tips += s
        else:
            with open(filename,'r') as f:
                load_dict = json.load(f)

            kind = request.form.get('kind')
            if kind in ['add', 'alter']:
                load_dict[key] = value
                if key in ['symbols_ratio','symbols_straddle']:
                    load_dict['symbols_trade'] += ',' + value
                if key in ['symbols_skew_bili']:
                    load_dict['symbols_trade'] += ',' + value
                    load_dict['symbols_ratio'] += ',' + value
                if key in ['symbols_sdiffer','symbols_skew_strd']:
                    load_dict['symbols_trade'] += ',' + value
                    load_dict['symbols_straddle'] += ',' + value

            if kind == 'del':
                load_dict.pop(key)
            with open(filename,"w") as f:
                json.dump(load_dict,f)

    # 显示配置文件的内容
    r = [ ['key', 'value'] ]
    with open(filename,'r') as f:
        load_dict = json.load(f)
        for (key,value) in load_dict.items():
            if key not in ['front_trade','front_quote','broker','investor','pwd','appid','auth_code']:
                r.append( [key,value] )

    return render_template("fut_config.html",tip=tips,rows=r)

@app.route('/fut_setting_pz', methods=['get','post'])
def fut_setting_pz():
    setting_dict = {'pz':'','size':'','priceTick':'','variableCommission':'','fixedCommission':'','slippage':'','exchangeID':'','margin':''}
    filename = get_dss() + 'fut/cfg/setting_pz.csv'
    if request.method == "POST":
        pz = del_blank( request.form.get('pz') )
        size = del_blank( request.form.get('size') )
        priceTick = del_blank( request.form.get('priceTick') )
        variableCommission = del_blank( request.form.get('variableCommission') )
        fixedCommission = del_blank( request.form.get('fixedCommission') )
        slippage = del_blank( request.form.get('slippage') )
        exchangeID = del_blank( request.form.get('exchangeID') )
        margin = del_blank( request.form.get('margin') )

        kind = request.form.get('kind')

        r = [[pz,size,priceTick,variableCommission,fixedCommission,slippage,exchangeID,margin]]
        cols = ['pz','size','priceTick','variableCommission','fixedCommission','slippage','exchangeID','margin']
        if kind == 'add':
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, mode='a', header=False, index=False)
        if kind == 'del':
            df = pd.read_csv(filename, dtype='str')
            df = df[df.pz != pz ]
            df.to_csv(filename, index=False)
        if kind == 'alter':
            # 删
            df = pd.read_csv(filename, dtype='str')
            df = df[df.pz != pz ]
            df.to_csv(filename, index=False)
            # 增
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, mode='a', header=False, index=False)
        if kind == 'query':
            df = pd.read_csv(filename, dtype='str')
            df = df[df.pz == pz ]
            if len(df) > 0:
                rec = df.iloc[0,:]
                setting_dict = dict(rec)

    df = pd.read_csv(filename, dtype='str')
    r = [ list(df.columns) ]
    for i, row in df.iterrows():
        r.append( list(row) )

    return render_template("fut_setting_pz.html",title="fut_setting_pz",rows=r,words=setting_dict)

@app.route('/fut_trade_time', methods=['get','post'])
def fut_trade_time():
    tips = '提示：'
    filename = get_dss() + 'fut/cfg/trade_time.csv'
    if request.method == "POST":
        symbol = del_blank( request.form.get('symbol') )
        name = del_blank( request.form.get('name') )
        seq_list = request.form.getlist('seq')
        #return str(seq_list)
        r =[]
        for seq in seq_list:
            begin = del_blank( request.form.get('begin'+'_'+seq) )
            end = del_blank( request.form.get('end'+'_'+seq) )
            num = del_blank( request.form.get('num'+'_'+seq) )
            r.append( [name,symbol,seq,begin,end,num] )

            if seq == '1':
                tips += symbol + ' 夜盘结束时间为：' + end

        #return str(r)

        cols = ['name','symbol','seq','begin','end','num']
        kind = request.form.get('kind')
        if kind == 'add':
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, mode='a', header=False, index=False)
        if kind == 'del':
            df = pd.read_csv(filename, dtype='str')
            df = df[df.symbol != symbol ]
            df.to_csv(filename, index=False)
        if kind == 'alter':
            # 删
            df = pd.read_csv(filename, dtype='str')
            df = df[df.symbol != symbol ]
            df.to_csv(filename, index=False)
            # 增
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, mode='a', header=False, index=False)

    df = pd.read_csv(filename, dtype='str')
    r = [ list(df.columns) ]
    for i, row in df.iterrows():
        r.append( list(row) )

    return render_template("fut_trade_time.html",tip=tips,rows=r)

@app.route('/fut_signal_pause', methods=['get','post'])
def fut_signal_pause():
    tips = '提示：'
    filename = get_dss() + 'fut/cfg/signal_pause_var.csv'
    if request.method == "POST":
        signal = del_blank( request.form.get('signal') )
        symbols = del_blank( request.form.get('symbols') )
        kind = request.form.get('kind')

        tips += '请确认 ' + symbols + ' 是否为字典'

        r = [[signal,symbols]]
        cols = ['signal','symbols']
        if kind == 'add':
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, mode='a', header=False, index=False)
        if kind == 'del':
            df = pd.read_csv(filename, dtype='str')
            df = df[df.signal != signal ]
            df.to_csv(filename, index=False)
        if kind == 'alter':
            # 删
            df = pd.read_csv(filename, dtype='str')
            df = df[df.signal != signal ]
            df.to_csv(filename, index=False)
            # 增
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, mode='a', header=False, index=False)

    df = pd.read_csv(filename, dtype='str')
    r = [ list(df.columns) ]
    for i, row in df.iterrows():
        r.append( list(row) )

    return render_template("fut_signal_pause.html",tip=tips,rows=r)

@app.route('/opt_mature', methods=['get','post'])
def opt_mature():
    pz = ''
    tips = '提示：'
    setting_dict = {'pz':'','symbol':'','mature':'','flag':'','obj':'','strike_min1':'','strike_max1':'','gap1':'','strike_min2':'','strike_max2':'','gap2':'','dash_c':'','dash_p':''}
    filename = get_dss() + 'fut/cfg/opt_mature.csv'
    if request.method == "POST":
        pz = del_blank( request.form.get('pz') )
        symbol = del_blank( request.form.get('symbol') )
        mature = del_blank( request.form.get('mature') )
        flag = del_blank( request.form.get('flag') )
        obj = del_blank( request.form.get('obj') )
        strike_min1 = del_blank( request.form.get('strike_min1') )
        strike_max1 = del_blank( request.form.get('strike_max1') )
        gap1 = del_blank( request.form.get('gap1') )
        strike_min2 = del_blank( request.form.get('strike_min2') )
        strike_max2 = del_blank( request.form.get('strike_max2') )
        gap2 = del_blank( request.form.get('gap2') )
        dash_c = del_blank( request.form.get('dash_c') )
        dash_p = del_blank( request.form.get('dash_p') )
        kind = request.form.get('kind')

        r = [[pz,symbol,mature,flag,obj,strike_min1,strike_max1,gap1,strike_min2,strike_max2,gap2,dash_c,dash_p]]
        cols = ['pz','symbol','mature','flag','obj','strike_min1','strike_max1','gap1','strike_min2','strike_max2','gap2','dash_c','dash_p']
        if kind == 'add':
            df = pd.read_csv(filename, dtype='str')
            df = df[df.symbol == symbol ]
            if df.empty:
                df = pd.DataFrame(r, columns=cols)
                df.to_csv(filename, mode='a', header=False, index=False)
            else:
                tips = '合约已存在，无法新增！'
        if kind == 'del':
            df = pd.read_csv(filename, dtype='str')
            df = df[df.symbol != symbol ]
            df.to_csv(filename, index=False)
            tips = '合约删除成功'
        if kind == 'alter':
            # 删
            df = pd.read_csv(filename, dtype='str')
            df = df[df.symbol != symbol ]
            df.to_csv(filename, index=False)
            # 增
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, mode='a', header=False, index=False)
            tips = '合约修改成功'
        if kind == 'query':
            df = pd.read_csv(filename, dtype='str')
            df = df[df.symbol == symbol ]
            if len(df) > 0:
                rec = df.iloc[0,:]
                setting_dict = dict(rec)
                pz = rec.pz

    # 显示配置文件的内容
    df = pd.read_csv(filename, dtype='str')
    if pz == '':
        df = df.sort_values(by='mature', ascending=False)
        df = df.iloc[:10, :]
    else:
        df = df[df.pz == pz]
        df = df.sort_values(by='symbol', ascending=False)

    r = [ list(df.columns) ]
    for i, row in df.iterrows():
        r.append( list(row) )

    return render_template("opt_mature.html",tip=tips,rows=r,words=setting_dict)

@app.route('/fut_owl', methods=['get','post'])
def fut_owl():
    tips = ''
    if request.method == "POST":
        if request.form.get('token') != '9999':
            tips = 'token error'
        else:
            code = del_blank( request.form.get('code') )
            ins_type = del_blank( request.form.get('ins_type') )
            price = del_blank( float(request.form.get('price')) )
            num = del_blank( int(request.form.get('num')) )

            ins = str({'ins':ins_type,'price':price,'num':num})
            fn = get_dss() + 'fut/engine/owl/signal_owl_mix_var_' + code + '.csv'
            # fn = get_dss() + 'fut/engine/owl/signal_owl_mix_var_' + code + '.csv'
            a_file(fn,ins)

            # 历史维护记录
            now = datetime.now()
            today = now.strftime('%Y-%m-%d %H:%M:%S')
            ins = str({'date':today,'code':code,'ins':ins_type,'price':price,'num':num})
            fn = get_dss() + 'fut/engine/owl/history.csv'
            a_file(fn,ins)

            tips = 'append success'


    # 动态加载新维护的symbol
    config = open(get_dss()+'fut/cfg/config.json')
    setting = json.load(config)
    symbols = setting['symbols_owl']
    symbols_list = symbols.split(',')

    r = [['code', 'ins', 'price', 'num']]
    for symbol in symbols_list:
        fn = get_dss() + 'fut/engine/owl/signal_owl_mix_var_' + symbol + '.csv'
        ins_list = r_file(fn)
        for ins in ins_list:
            r.append([symbol] + list(dict(ins).values()))

    return render_template("owl.html",tip=tips,rows=r)

@app.route('/opt_trade', methods=['get','post'])
def opt_trade():
    tips = '提示：'
    fn = get_dss() + 'fut/engine/opt/opt_trade.csv'
    if request.method == "POST":
        index = int( del_blank(request.form.get('index')) )
        book = del_blank( request.form.get('book') )
        portfolio = del_blank( request.form.get('portfolio') )
        margin = del_blank( request.form.get('margin') )
        kind = request.form.get('kind')

        if kind == 'alter':
            df = pd.read_csv(fn, dtype='str')
            df.at[index, 'book'] = book
            df.at[index, 'portfolio'] = portfolio
            df.at[index, 'margin'] = margin
            df.to_csv(fn, index=False)

    # 显示文件的内容
    df = pd.read_csv(fn, dtype='str')
    df = df.reset_index()
    r = [ list(df.columns) ]
    for i, row in df.iterrows():
        r.append( list(row) )

    return render_template("opt_trade.html",tip=tips,rows=r)

@app.route('/opt_book', methods=['get','post'])
def opt_book():
    tips = '提示：'
    dirname = get_dss() + 'fut/engine/opt/'
    if request.method == "POST":
        index = int( del_blank(request.form.get('index')) )

        if kind == 'close':
            # 将booking文件关仓为booked文件
            # 逻辑很复杂，暂时没有思路，先放一放
            pass

    # 显示文件列表
    r = [ ['index', 'fn'] ]
    listfile = os.listdir(dirname)
    i = 0
    for filename in listfile:
        if filename[:7] == 'booking':
            r.append( [i,filename] )
            i += 1

    return render_template("opt_book.html",tip=tips,rows=r)

@app.route('/market_date', methods=['get','post'])
def market_date():
    filename = get_dss() + 'fut/engine/market_date.csv'
    if request.method == "POST":
        date = del_blank( request.form.get('date') )
        morning = del_blank( request.form.get('morning') )
        night = del_blank( request.form.get('night') )

        kind = request.form.get('kind')

        r = [[date,morning,night]]
        cols = ['date','morning','night']
        if kind == 'add':
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, mode='a', header=False, index=False)
        if kind == 'del':
            df = pd.read_csv(filename, dtype='str')
            df = df[df.date != date ]
            df.to_csv(filename, index=False)
        if kind == 'alter':
            # 删
            df = pd.read_csv(filename, dtype='str')
            df = df[df.date != date ]
            df.to_csv(filename, index=False)
            # 增
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, mode='a', header=False, index=False)

    df = pd.read_csv(filename, dtype='str')
    df = df.sort_values(by='date')
    r = [ list(df.columns) ]
    for i, row in df.iterrows():
        r.append( list(row) )

    return render_template("market_date.html",title="market_date",rows=r)

@app.route('/follow', methods=['get','post'])
def follow():
    filename = get_dss() + 'fut/engine/follow/portfolio_follow_param.csv'
    if request.method == "POST":
        symbol_o = del_blank( request.form.get('symbol_o') )
        symbol_c = del_blank( request.form.get('symbol_c') )
        symbol_p = del_blank( request.form.get('symbol_p') )
        flag_c = del_blank( request.form.get('flag_c') )
        flag_p = del_blank( request.form.get('flag_p') )
        strike_high = del_blank( request.form.get('strike_high') )
        strike_low = del_blank( request.form.get('strike_low') )
        fixed_size = del_blank( request.form.get('fixed_size') )
        switch_state = del_blank( request.form.get('switch_state') )
        percent = del_blank( request.form.get('percent') )
        gap = del_blank( request.form.get('gap') )

        kind = request.form.get('kind')

        r = [[symbol_o,symbol_c,symbol_p,flag_c,flag_p,strike_high,strike_low,fixed_size,switch_state,percent,gap]]
        cols = ['symbol_o','symbol_c','symbol_p','flag_c','flag_p','strike_high','strike_low','fixed_size','switch_state','percent','gap']
        if kind == 'add':
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, mode='a', header=False, index=False)
        if kind == 'del':
            df = pd.read_csv(filename, dtype='str')
            df = df[(df.symbol_c != symbol_c) & (df.symbol_p != symbol_p)]
            df.to_csv(filename, index=False)
        if kind == 'alter':
            # 删
            df = pd.read_csv(filename, dtype='str')
            df = df[(df.symbol_c != symbol_c) & (df.symbol_p != symbol_p)]
            df.to_csv(filename, index=False)
            # 增
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, mode='a', header=False, index=False)

    df = pd.read_csv(filename, dtype='str')
    r = [ list(df.columns) ]
    for i, row in df.iterrows():
        r.append( list(row) )

    return render_template("follow.html",title="follow",rows=r)

@app.route('/ratio', methods=['get','post'])
def ratio():
    filename = get_dss() + 'fut/engine/ratio/portfolio_ratio_param.csv'
    if request.method == "POST":
        symbol_b = del_blank( request.form.get('symbol_b') )
        symbol_s = del_blank( request.form.get('symbol_s') )
        num_b = del_blank( request.form.get('num_b') )
        num_s = del_blank( request.form.get('num_s') )
        gap = del_blank( request.form.get('gap') )
        profit = del_blank( request.form.get('profit') )
        state = del_blank( request.form.get('state') )
        source = del_blank( request.form.get('source') )

        kind = request.form.get('kind')

        r = [[symbol_b,symbol_s,num_b,num_s,gap,profit,0,0,state,source,'','','','','','00:00:00','','']]
        cols = ['symbol_b','symbol_s','num_b','num_s','gap','profit','hold_b','hold_s','state','source','price_b','price_s','profit_b','profit_s','profit_o','tm','delta','theta']
        if kind == 'add':
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, mode='a', header=False, index=False)
        if kind == 'del':
            df = pd.read_csv(filename, dtype='str')
            df = df[~( (df.symbol_b == symbol_b) & (df.symbol_s == symbol_s) & (df.num_b == num_b) & (df.num_s == num_s) & (df.source == source) )]
            df.to_csv(filename, index=False)
        if kind == 'alter':
            df = pd.read_csv(filename, dtype='str')
            for i, row in df.iterrows():
                if row.symbol_b == symbol_b and row.symbol_s == symbol_s and row.num_b == num_b and row.num_s == num_s and row.source == source:
                    df.at[i, 'gap'] = gap
                    df.at[i, 'profit'] = profit
                    df.at[i, 'state'] = state
            df.to_csv(filename, index=False)

    df = pd.read_csv(filename, dtype='str')
    r = [ list(df.columns) ]
    for i, row in df.iterrows():
        r.append( list(row) )

    return render_template("ratio.html",title="ratio",rows=r)

@app.route('/straddle', methods=['get','post'])
def straddle():
    filename = get_dss() + 'fut/engine/straddle/portfolio_straddle_param.csv'
    # while get_file_lock(filename) == False:
        # time.sleep(0.01)

    try:
        if request.method == "POST":
            kind = request.form.get('kind')
            basic_c = del_blank( request.form.get('basic_c') )
            strike_c = del_blank( request.form.get('strike_c') )
            basic_p = del_blank( request.form.get('basic_p') )
            strike_p = del_blank( request.form.get('strike_p') )
            num_c = del_blank( request.form.get('num_c') )
            num_p = del_blank( request.form.get('num_p') )
            direction = del_blank( request.form.get('direction') )
            hold_c = del_blank( request.form.get('hold_c') )
            hold_p = del_blank( request.form.get('hold_p') )
            profit = del_blank( request.form.get('profit') )
            state = del_blank( request.form.get('state') )
            source = del_blank( request.form.get('source') )

            now = datetime.now()
            date = now.strftime('%Y-%m-%d')
            r = [['00:00:00',basic_c,strike_c,basic_p,strike_p,num_c,num_p,direction,hold_c,hold_p,profit,state,source,'','','','','',date]]
            cols = ['tm','basic_c','strike_c','basic_p','strike_p','num_c','num_p','direction','hold_c','hold_p','profit','state','source','price_c','price_p','profit_c','profit_p','profit_o','date']
            if kind == 'add':
                df = pd.DataFrame(r, columns=cols)
                df.to_csv(filename, mode='a', header=False, index=False)
            if kind == 'del':
                df = pd.read_csv(filename, dtype='str')
                df = df[~( (df.basic_c == basic_c) & (df.strike_c == strike_c) & (df.basic_p == basic_p) & (df.strike_p == strike_p) & (df.direction == direction) & (df.source == source) )]
                df.to_csv(filename, index=False)
            if kind == 'alter':
                df = pd.read_csv(filename, dtype='str')
                for i, row in df.iterrows():
                    if row.basic_c == basic_c and row.strike_c == strike_c and row.basic_p == basic_p and row.strike_p == strike_p and row.direction == direction and row.source == source:
                        df.at[i, 'profit'] = profit
                        df.at[i, 'state'] = state
                df.to_csv(filename, index=False)

        df = pd.read_csv(filename, dtype='str')
        r = [ list(df.columns) ]
        for i, row in df.iterrows():
            r.append( list(row) )
    except Exception as e:
        s = traceback.format_exc()
        to_log(s)

    # release_file_lock(filename)
    return render_template("straddle.html",title="straddle",rows=r)

@app.route('/sdiffer', methods=['get','post'])
def sdiffer():
    filename = get_dss() + 'fut/engine/sdiffer/portfolio_sdiffer_param.csv'
    # while get_file_lock(filename) == False:
    #     time.sleep(1)

    if request.method == "POST":
        kind = request.form.get('kind')
        basic_m0 = del_blank( request.form.get('basic_m0') )
        basic_m1 = del_blank( request.form.get('basic_m1') )
        strike = del_blank( request.form.get('strike') )
        fixed_size = del_blank( request.form.get('fixed_size') )
        hold_m0 = del_blank( request.form.get('hold_m0') )
        hold_m1 = del_blank( request.form.get('hold_m1') )
        d_low_open = del_blank( request.form.get('d_low_open') )
        d_high_open= del_blank( request.form.get('d_high_open') )
        profit = del_blank( request.form.get('profit') )
        state = del_blank( request.form.get('state') )
        source = del_blank( request.form.get('source') )

        now = datetime.now()
        date = now.strftime('%Y-%m-%d')
        r = [[date,basic_m0,basic_m1,strike,fixed_size,hold_m0,hold_m1,'','','','',d_low_open,d_high_open,100.0,0,-100.0,0,profit,state,source,'','','']]
        cols = ['date','basic_m0','basic_m1','strike','fixed_size','hold_m0','hold_m1','price_c_m0','price_p_m0','price_c_m1','price_p_m1','d_low_open','d_high_open','d_max','dida_max','d_min','dida_min','profit','state','source','profit_m0','profit_m1','profit_o']
        if kind == 'add':
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, mode='a', header=False, index=False)
        if kind == 'del':
            df = pd.read_csv(filename, dtype='str')
            df = df[~((df.basic_m0 == basic_m0) & (df.basic_m1 == basic_m1) & (df.strike == strike) & (df.hold_m0 == hold_m0) & (df.hold_m1 == hold_m1) & (df.source == source))]
            df.to_csv(filename, index=False)
        if kind == 'alter':
            df = pd.read_csv(filename, dtype='str')
            for i, row in df.iterrows():
                if row.basic_m0 == basic_m0 and row.basic_m1 == basic_m1 and row.strike == strike and row.hold_m0 == hold_m0 and row.hold_m1 == hold_m1 and row.source == source:
                    df.at[i, 'profit'] = profit
                    df.at[i, 'state'] = state
            df.to_csv(filename, index=False)


    df = pd.read_csv(filename, dtype='str')
    r = [ list(df.columns) ]
    for i, row in df.iterrows():
        r.append( list(row) )
    # release_file_lock(filename)

    return render_template("sdiffer.html",title="sdiffer",rows=r)


@app.route('/skew_strd', methods=['get','post'])
def skew_strd():
    filename = get_dss() + 'fut/engine/skew_strd/portfolio_skew_strd_param.csv'

    if request.method == "POST":
        kind = request.form.get('kind')
        basic_m0 = del_blank( request.form.get('basic_m0') )
        basic_m1 = del_blank( request.form.get('basic_m1') )
        strike_m0 = del_blank( request.form.get('strike_m0') )
        strike_m1 = del_blank( request.form.get('strike_m1') )
        fixed_size = del_blank( request.form.get('fixed_size') )
        hold_m0 = del_blank( request.form.get('hold_m0') )
        hold_m1 = del_blank( request.form.get('hold_m1') )
        skew_low_open = del_blank( request.form.get('skew_low_open') )
        skew_high_open= del_blank( request.form.get('skew_high_open') )
        profit = del_blank( request.form.get('profit') )
        state = del_blank( request.form.get('state') )
        source = del_blank( request.form.get('source') )

        now = datetime.now()
        date = now.strftime('%Y-%m-%d')
        r = [['00:00:00',basic_m0,basic_m1,strike_m0,strike_m1,fixed_size,profit,state,source,hold_m0,hold_m1,-100.0,100.0,'','','','',100.0,0,-100.0,0,'','','',date]]
        cols = ['tm','basic_m0','basic_m1','strike_m0','strike_m1','fixed_size','profit','state','source','hold_m0','hold_m1','skew_low_open','skew_high_open','price_c_m0','price_p_m0','price_c_m1','price_p_m1','skew_max','dida_max','skew_min','dida_min','profit_m0','profit_m1','profit_o','date']
        if kind == 'add':
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, mode='a', header=False, index=False)
        if kind == 'del':
            df = pd.read_csv(filename, dtype='str')
            df = df[~((df.basic_m0 == basic_m0) & (df.basic_m1 == basic_m1) & (df.strike_m0 == strike_m1) & (df.strike_m1 == strike_m1) & (df.hold_m0 == hold_m0) & (df.hold_m1 == hold_m1) & (df.source == source))]
            df.to_csv(filename, index=False)
        if kind == 'alter':
            df = pd.read_csv(filename, dtype='str')
            for i, row in df.iterrows():
                if row.basic_m0 == basic_m0 and row.basic_m1 == basic_m1 and row.strike_m0 == strike_m0 and row.strike_m1 == strike_m1 and row.hold_m0 == hold_m0 and row.hold_m1 == hold_m1 and row.source == source:
                    df.at[i, 'profit'] = profit
                    df.at[i, 'state'] = state
            df.to_csv(filename, index=False)

    df = pd.read_csv(filename, dtype='str')
    r = [ list(df.columns) ]
    for i, row in df.iterrows():
        r.append( list(row) )

    return render_template("skew_strd.html",title="skew_strd",rows=r)

@app.route('/skew_bili', methods=['get','post'])
def skew_bili():
    filename = get_dss() + 'fut/engine/skew_bili/portfolio_skew_bili_param.csv'

    if request.method == "POST":
        kind = request.form.get('kind')
        symbol_b = del_blank( request.form.get('symbol_b') )
        symbol_s = del_blank( request.form.get('symbol_s') )
        num_b = del_blank( request.form.get('num_b') )
        num_s = del_blank( request.form.get('num_s') )
        skew_low_open = del_blank( request.form.get('skew_low_open') )
        skew_high_open= del_blank( request.form.get('skew_high_open') )
        profit = del_blank( request.form.get('profit') )
        state = del_blank( request.form.get('state') )
        source = del_blank( request.form.get('source') )

        now = datetime.now()
        date = now.strftime('%Y-%m-%d')
        r = [['00:00:00',symbol_b,symbol_s,num_b,num_s,profit,state,source,0,0,-100.0,100.0,'','',100.0,0,-100.0,0,'','','',date]]
        cols = ['tm','symbol_b','symbol_s','num_b','num_s','profit','state','source','hold_b','hold_s','skew_low_open','skew_high_open','price_b','price_s','skew_max','dida_max','skew_min','dida_min','profit_b','profit_s','profit_o','date']
        if kind == 'add':
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, mode='a', header=False, index=False)
        if kind == 'del':
            df = pd.read_csv(filename, dtype='str')
            df = df[~((df.symbol == symbol_b) & (df.symbol_s == symbol_s)  & (df.num_b == num_b) & (df.num_s == num_s) & (df.source == source))]
            df.to_csv(filename, index=False)
        if kind == 'alter':
            df = pd.read_csv(filename, dtype='str')
            for i, row in df.iterrows():
                if row.symbol_b == symbol_b and row.symbol_s == symbol_s and row.num_b == num_b and row.num_s == num_s and row.source == source:
                    df.at[i, 'profit'] = profit
                    df.at[i, 'state'] = state
            df.to_csv(filename, index=False)

    df = pd.read_csv(filename, dtype='str')
    r = [ list(df.columns) ]
    for i, row in df.iterrows():
        r.append( list(row) )

    return render_template("skew_bili.html",title="skew_bili",rows=r)


@app.route('/gateway_trade', methods=['get'])
def gateway_trade():
    fn = get_dss() + 'fut/engine/gateway_trade.csv'

    # 显示文件的内容
    df = pd.read_csv(fn, dtype='str')
    df = df.iloc[-30:, :]
    df = df.sort_index(ascending=False)
    df = df.reset_index()
    df = df[['TradingDay','TradeTime','InstrumentID','Offset','Direction','Price','Volume']]
    r = [ list(df.columns) ]
    for i, row in df.iterrows():
        r.append( list(row) )

    return render_template("gateway_trade.html", rows=r)


@app.route('/upload_statement', methods=['get', 'post'])
def upload_statement():
    tips = ''
    if request.method == "POST":
        sm = request.files.get('sm')
        if sm is not None:
            # 指定上传路径
            dirname = get_dss() + 'fut/statement'
            # 拼接文件全路径
            dt = sm.filename[-12:-4]
            dt = dt[:4] + '-' + dt[4:6] + '-' + dt[6:8]
            fn1 = os.path.join(dirname, dt+'.txt')
            # 上传文件到指定路径
            sm.save(fn1)
            tips = '文件上传成功'

            #　解读对账单，以df结构保存到临时文件中
            i = 0
            fn2 = os.path.join(dirname, 'tmp_'+dt+'.txt')
            fw = open(fn2, 'w')
            with open(fn1, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    if i > 0:
                        if line.startswith('--------------------------------'):
                            i += 1
                        elif line.strip() == '':
                            pass
                        else:
                            # print(line)
                            fw.write(line)
                    if i == 4:
                        break
                    if line.strip().startswith('持仓汇总'):
                        i += 1
            fw.close()                        # 完成临时文件的写入

            # 分类汇总，计算保证金占用， 计算greeks值
            df = pd.read_csv(fn2, encoding='gbk', sep='|', skiprows=2, header=None)
            df = df.drop(columns=[0,14])
            fn_greeks = get_dss() + 'opt/' + dt[:7] + '_greeks.csv'
            df_greeks = pd.read_csv(fn_greeks)

            pz_list = []
            opt_list = []
            delta_list = []
            gamma_list = []
            vega_list = []
            for i, row in df.iterrows():
                symbol = row[2].strip()
                num = row[3] - row[5]
                pz = get_contract(symbol).pz
                pz_list.append(pz)
                if get_contract(symbol).be_opt:
                    df2 = df_greeks[df_greeks.Instrument == symbol]
                    # df = df[df.Localtime.str.slice(0,10) == dt]
                    df2 = df2.drop_duplicates(subset=['Instrument'],keep='last')
                    if df2.empty:
                        delta_list.append(0)
                        gamma_list.append(0)
                        vega_list.append(0)
                    else:
                        rec = df2.iloc[0,:]
                        # print(rec.Instrument, num, rec.delta, rec.gamma, rec.vega)
                        delta_list.append(int(100 * num * rec.delta))
                        gamma_list.append(round(100 * num * rec.gamma,2))
                        vega_list.append(round(num * rec.vega,2))

                    opt_list.append('期权')
                else:
                    opt_list.append('期货')
                    delta_list.append(100*num)
                    gamma_list.append(0)
                    vega_list.append(0)

            df['pz'] = pz_list
            df['opt'] = opt_list
            df['delta'] = delta_list
            df['gamma'] = gamma_list
            df['vega'] = vega_list
            df['magin'] = df[10].apply(int)

            df2 = df.groupby(by=['pz','opt']).agg({'magin':np.sum, 'delta':np.sum, 'gamma':np.sum, 'vega':np.sum})
            fn3 = os.path.join(dirname, 'risk_'+dt+'.csv')
            df2.to_csv(fn3)

            send_email(get_dss(), '结算单_'+dt, '', [fn1, fn3])

    return render_template("upload_statement.html",title="upload statement",tip=tips)


@app.route('/value_dali_csv', methods=['get','post'])
def value_dali_csv():
    fn = get_dss() + 'fut/engine/star/value_dali.csv'
    df = pd.read_csv(fn, dtype='str')

    r = []
    for i, row in df.iterrows():
        r.append( list(row) )
    r.append( list(df.columns) )
    r = reversed(r)

    return render_template("show_fut_csv.html",title="value_dali",rows=r)


@app.route('/plot_dali', methods=['get','post'])
def plot_dali():
    if request.method == "POST":
        pz = request.form.get('pz')
        return dali_show(pz)

    return render_template("plot_dali.html", title="plot_dali")

@app.route('/ic_y_m', methods=['get','post'])
def ic_y_m():
    symbol1 = 'y2005'
    symbol2 = 'm2005'
    start_dt = '2020-01-01'
    draw_web.ic(symbol1, symbol2, start_dt)
    time.sleep(1)
    fn = 'ic_' + symbol1 + '_'+ symbol2+ '.html'
    return app.send_static_file(fn)

@app.route('/mates_config', methods=['get','post'])
def mates_config():
    setting_dict = {}
    fn = 'mates.csv'
    if request.method == "POST":
        df = pd.read_csv(fn, dtype='str')
        kind = request.form.get('kind')
        if kind == 'alter':
            for i, row in df.iterrows():
                df.iat[i,1] = del_blank( request.form.get(df.iat[i,0]+'_mate1') )
                df.iat[i,2] = del_blank( request.form.get(df.iat[i,0]+'_mate2') )
            df.to_csv(fn, index=False)

    df = pd.read_csv(fn, dtype='str')
    for i, row in df.iterrows():
        setting_dict[row.seq + '_mate1'] =  row.mate1
        setting_dict[row.seq + '_mate2'] =  row.mate2

    return render_template("mates_config.html",title="mates_config",words=setting_dict)

@app.route('/mates_show', methods=['get','post'])
def mates_show():
    setting_dict = {}
    fn = 'mates.csv'

    if request.method == "POST":
        df = pd.read_csv(fn, dtype='str')
        kind = request.form.get('kind')
        if kind is not None:
            if kind[:2] == 'ic':
                mate1 = del_blank( request.form.get(kind + '_mate1') )
                mate2 = del_blank( request.form.get(kind + '_mate2') )
                return ic_show(mate1, mate2)
            if kind[:2] == 'ip':
                mate1 = del_blank( request.form.get(kind + '_mate1') )
                mate2 = del_blank( request.form.get(kind + '_mate2') )
                return ic_show(mate1, mate2)
                # return request.form.get(kind + '_mate2')
            if kind[:2] == 'ma':
                symbol =  request.form.get( kind + '_mate1' )
                draw_web.bar_ma(symbol)
                fn2 = 'bar_ma.html'
                time.sleep(1)
                return app.send_static_file(fn2)
            if kind[:3] == 'cci':
                symbol =  request.form.get( kind + '_mate1' )
                draw_web.bar_cci(symbol)
                fn2 = 'bar_cci.html'
                time.sleep(1)
                return app.send_static_file(fn2)
            if kind[:7] == 'dalicta':
                symbol =  request.form.get( kind + '_mate1' )
                draw_web.bar_dalicta(symbol)
                fn2 = 'bar_dalicta.html'
                time.sleep(1)
                return app.send_static_file(fn2)
            if kind[:10] == 'aberration':
                symbol =  request.form.get( kind + '_mate1' )
                draw_web.bar_aberration(symbol)
                fn2 = 'bar_aberration.html'
                time.sleep(1)
                return app.send_static_file(fn2)

    df = pd.read_csv(fn, dtype='str')
    for i, row in df.iterrows():
        setting_dict[row.seq + '_mate1'] =  row.mate1
        setting_dict[row.seq + '_mate2'] =  row.mate2

    return render_template("mates_show.html",title="mates_show",words=setting_dict)

@app.route('/show_opt', methods=['get'])
def show_opt():
    opt()
    r = []
    dirname = 'static/'
    file_list = os.listdir(dirname)
    for fn in file_list:
        if fn.startswith('opt'):
            r.append(dirname + fn)

    # return str(r)
    return render_template("show_jpg.html",header="opt",items=r)

@app.route('/show_dali', methods=['get'])
def show_dali():
    dali()
    r = []
    dirname = 'static/'
    file_list = os.listdir(dirname)
    for fn in file_list:
        if fn.startswith('dali'):
            r.append(dirname + fn)

    # return str(r)
    return render_template("show_jpg.html",header="dali",items=r)

@app.route('/show_star', methods=['get'])
def show_star():
    star()
    r = []
    dirname = 'static/'
    file_list = os.listdir(dirname)
    for fn in file_list:
        if fn.startswith('star'):
            r.append(dirname + fn)

    # return str(r)
    return render_template("show_jpg.html",header="star",items=r)

@app.route('/show_yue', methods=['get'])
def show_yue():
    yue()
    r = []
    dirname = 'static/'
    file_list = os.listdir(dirname)
    for fn in file_list:
        if fn.startswith('yue'):
            r.append(dirname + fn)

    # return str(r)
    return render_template("show_jpg.html",header="yue",items=r)

@app.route('/show_mates', methods=['get'])
def show_mates():
    mates()
    r = []
    dirname = 'static/'
    file_list = os.listdir(dirname)
    for fn in file_list:
        if fn.startswith('ic'):
            r.append(dirname + fn)

    # return str(r)
    return render_template("show_jpg.html",header="ic",items=r)

@app.route('/show_smile', methods=['get'])
def show_smile():
    smile()
    r = []
    dirname = 'static/'
    file_list = os.listdir(dirname)
    for fn in file_list:
        if fn.startswith('smile'):
            r.append(dirname + fn)

    # return str(r)
    return render_template("show_jpg.html",header="smile",items=r)

@app.route('/show_iv_ts', methods=['get'])
def show_iv_ts():
    iv_ts()
    r = []
    dirname = 'static/'
    file_list = os.listdir(dirname)
    for fn in file_list:
        if fn.startswith('iv_ts'):
            r.append(dirname + fn)

    # return str(r)
    return render_template("show_jpg.html",header="iv_ts",items=r)


@app.route('/T', methods=['get','post'])
def T():
    tips = ''
    r = [['隐波', '时间价值', '内在价值', '卖价', '买价', '最新价',
          '行权价',
          '最新价', '买价', '卖价', '内在价值', '时间价值','隐波']]

    if request.method == "POST":
        symbol = del_blank( request.form.get('symbol') )
        date = del_blank( request.form.get('date') )
        if date == '':
            now = datetime.now()
            date = now.strftime('%Y-%m-%d')
            # date = '2020-08-06'

        tips += '合约： ' + symbol + '  日期：' + date
        fn = get_dss() + 'opt/' + date[:7] + '_greeks.csv'
        # fn = get_dss() + 'opt/2020-08_greeks.csv'
        if os.path.exists(fn):
            df = pd.read_csv(fn)
            df = df[df.Instrument.str.slice(0,len(symbol)) == symbol]
            df = df[df.Localtime.str.slice(0,10) == date]
            df = df.drop_duplicates(subset=['Instrument'],keep='last')
            df = df.sort_values('Instrument')
            # print(df.tail())
            obj = 0
            n = int( len(df)/2 )
            if n > 0:
                for i in range(n):
                    row_c = df.iloc[i,:]
                    row_p = df.iloc[i+n,:]
                    strike_c = get_contract(row_c.Instrument).strike
                    strike_p = get_contract(row_p.Instrument).strike
                    assert strike_c == strike_p
                    theory_value_c = int( float(row_c.obj) - strike_c )
                    theory_value_c = theory_value_c if theory_value_c > 0 else 0
                    time_value_c = int( float(row_c.AskPrice) - theory_value_c )
                    theory_value_p = int( strike_p - float(row_p.obj) )
                    theory_value_p = theory_value_p if theory_value_p > 0 else 0
                    time_value_p = int( float(row_p.AskPrice) - theory_value_p )
                    obj = row_c.obj
                    r.append([round(100*row_c.iv,2), int(time_value_c), int(theory_value_c),
                              row_c.AskPrice, row_c.BidPrice, round(row_c.LastPrice,1),
                              strike_c,
                              round(row_p.LastPrice,1), row_p.BidPrice, row_p.AskPrice,
                              theory_value_p, time_value_p, round(100*row_p.iv,2)])

                tips += ' 标的价格：' + str(obj)
    else:
        pass

    return render_template("T.html",title="T",rows=r,tip=tips)


@app.route('/hv', methods=['get', 'post'])
def hv():
    if request.method == "POST":
        code = request.form.get('code')
        return hv_show(code)

    return render_template("hv.html", title="hv")


@app.route('/iv', methods=['get', 'post'])
def iv():
    if request.method == "POST":
        basic0 = request.form.get('basic0')
        basic1 = request.form.get('basic1')
        both = request.form.get('both')
        date_end = request.form.get('date_end')

        basic_list = []
        if basic0 != '':
            basic_list.append(basic0)
        if basic1 != '':
            basic_list.append(basic1)

        if date_end == '':
            now = datetime.now()
            date_end = now.strftime('%Y-%m-%d')

        return iv_show(basic_list, both, date_end)

    return render_template("iv.html", title="iv")


@app.route('/iv_min5', methods=['get', 'post'])
def iv_min5():
    if request.method == "POST":
        symbol1 = request.form.get('symbol1')
        symbol2 = request.form.get('symbol2')
        symbol3 = request.form.get('symbol3')
        symbol4 = request.form.get('symbol4')
        date = request.form.get('date')

        symbol_list = []
        if symbol1 != '':
            symbol_list.append(symbol1)
        if symbol2 != '':
            symbol_list.append(symbol2)
        if symbol3 != '':
            symbol_list.append(symbol3)
        if symbol4 != '':
            symbol_list.append(symbol4)

        if date == '':
            now = datetime.now()
            date = now.strftime('%Y-%m-%d')

        return iv_min5_show(symbol_list, date)

    return render_template("iv_min5.html", title="iv_min5")

@app.route('/skew', methods=['get', 'post'])
def skew():
    if request.method == "POST":
        kind = request.form.get('kind')
        basic = request.form.get('basic')
        date = request.form.get('date')
        if date == '':
            now = datetime.now()
            date = now.strftime('%Y-%m-%d')

        return skew_show(basic, date, kind)

    return render_template("skew.html", title="skew")


@app.route('/smile', methods=['get', 'post'])
def smile():
    if request.method == "POST":
        pz = request.form.get('pz')
        type = request.form.get('type')
        date = request.form.get('date')
        kind = request.form.get('kind')
        symbol = request.form.get('symbol')
        if date == '':
            now = datetime.now()
            date = now.strftime('%Y-%m-%d')
        return smile_show(pz, type, date, kind, symbol)

    return render_template("smile.html", title='smile')


@app.route('/greeks', methods=['get','post'])
def greeks():
    tips = ''
    r = []

    if request.method == "POST":
        symbol1 = del_blank( request.form.get('symbol1') )
        symbol2 = del_blank( request.form.get('symbol2') )
        symbol3 = del_blank( request.form.get('symbol3') )
        symbol4 = del_blank( request.form.get('symbol4') )
        num1 = del_blank( request.form.get('num1') )
        num2 = del_blank( request.form.get('num2') )
        num3 = del_blank( request.form.get('num3') )
        num4 = del_blank( request.form.get('num4') )

        symbol_num_list = []
        if symbol1 != '':
            symbol_num_list.append( (symbol1, int(num1)) )
        if symbol2 != '':
            symbol_num_list.append( (symbol2, int(num2)) )
        if symbol3 != '':
            symbol_num_list.append( (symbol3, int(num3)) )
        if symbol4 != '':
            symbol_num_list.append( (symbol4, int(num4)) )

        a = ['date', 'price_obj', 'price_p', 'delta', 'gamma', 'theta', 'vega']
        for symbol, num in symbol_num_list:
            a.append(symbol)
            a.append('price')
            a.append('delta')
            a.append('gamma')
            a.append('theta')
            a.append('vega')
            a.append('iv')
        r.append(a)

        date_end = del_blank( request.form.get('date_end') )
        if date_end == '':
            now = datetime.now()
            date_end = now.strftime('%Y-%m-%d')
            # date_end = '2020-09-06'

        now = datetime.strptime(date_end, '%Y-%m-%d')
        # 本月第一天
        first_day = datetime(now.year, now.month, 1)
        #前一个月最后一天
        pre_month = first_day - timedelta(days = 1)
        today = now.strftime('%Y-%m-%d')
        pre = pre_month.strftime('%Y-%m-%d')

        fn = get_dss() + 'opt/' +  pre[:7] + '_greeks.csv'
        df_pre = pd.read_csv(fn)
        fn = get_dss() + 'opt/' +  today[:7] + '_greeks.csv'
        df_today = pd.read_csv(fn)
        df = pd.concat([df_pre, df_today])
        df = df[df.Localtime.str[:10] <= date_end]

        date_list = sorted(list(set(df.Localtime.str[:10])), reverse=True)
        # print(date_list)

        for date in date_list:
            a = [date]
            price_obj, price_p, delta_p, gamma_p, theta_p, vega_p = 0, 0, 0, 0, 0, 0
            for symbol, num in symbol_num_list:
                df1 = df[(df.Instrument == symbol) & (df.Localtime.str[:10] == date)]
                if df1.empty:
                    continue
                rec = df1.iloc[0,:]
                price_obj = rec.obj
                price_p += rec.LastPrice * num
                delta_p += rec.delta * num
                gamma_p += rec.gamma * num
                theta_p += rec.theta * num
                vega_p += rec.vega * num
                a.append(num)
                a.append(round(rec.LastPrice,2))
                a.append(int(100*rec.delta))
                a.append(round(100*rec.gamma,2))
                a.append(round(0.1*rec.theta,1))
                a.append(int(rec.vega))
                a.append(round(100*rec.iv,2))
            a.insert(1, int(vega_p))
            a.insert(1, round(0.1*theta_p,1))
            a.insert(1, round(100*gamma_p,2))
            a.insert(1, int(100*delta_p))
            a.insert(1, int(price_p))
            a.insert(1, int(price_obj))
            r.append(a)
        # print(r)

    return render_template("greeks.html",title="greeks",rows=r,tip=tips)


@app.route('/iv_straddle', methods=['get', 'post'])
def iv_straddle():
    if request.method == "POST":
        kind = request.form.get('kind')
        symbol = request.form.get('symbol')
        strike1 = request.form.get('strike1')
        strike2 = request.form.get('strike2')
        strike3 = request.form.get('strike3')
        strike4 = request.form.get('strike4')
        strike_list = []
        if strike1 != '':
            strike_list.append(strike1)
        if strike2 != '':
            strike_list.append(strike2)
        if strike3 != '':
            strike_list.append(strike3)
        if strike4 != '':
            strike_list.append(strike4)

        startdate = request.form.get('startdate')
        if startdate == '':
            now = datetime.now() - timedelta(days = 3)
            startdate = now.strftime('%Y-%m-%d')

        return iv_straddle_show(symbol, strike_list, startdate, kind)

    return render_template("iv_straddle.html", title="iv_straddle")

@app.route('/book_min5', methods=['get', 'post'])
def book_min5():
    if request.method == "POST":
        kind = request.form.get('kind')
        startdate = request.form.get('startdate')
        if startdate == '':
            now = datetime.now() - timedelta(days = 3)
            startdate = now.strftime('%Y-%m-%d')

        r = []
        seq_list = request.form.getlist('seq')
        for seq in seq_list:
            symbol_a = del_blank( request.form.get('symbol_a'+seq) )
            num_a = del_blank( request.form.get('num_a'+seq) )
            symbol_b = del_blank( request.form.get('symbol_b'+seq) )
            num_b = del_blank( request.form.get('num_b'+seq) )
            r.append( [symbol_a, num_a, symbol_b, num_b]  )

        if kind == 'now':
            return book_min5_now_show(startdate, r)
        else:
            return book_min5_show(startdate, r)

    return render_template("book_min5.html", title="book_min5")


@app.route('/open_interest', methods=['get', 'post'])
def open_interest():
    if request.method == "POST":
        basic = request.form.get('basic')
        type = request.form.get('type')
        date = request.form.get('date')
        kind = request.form.get('kind')

        if date == '':
            now = datetime.now()
            date = now.strftime('%Y-%m-%d')

        return open_interest_show(basic, type, date, kind)

    return render_template("open_interest.html", title='open_interest')


@app.route('/hs300_spread', methods=['get', 'post'])
def hs300_spread():
    if request.method == "POST":
        startdate = request.form.get('startdate')
        if startdate == '':
            now = datetime.now() - timedelta(days = 30)
            startdate = now.strftime('%Y-%m-%d')

        return hs300_spread_show(startdate)

    return render_template("hs300_spread.html", title="hs300_spread")


@app.route('/straddle_diff', methods=['get', 'post'])
def straddle_diff():
    if request.method == "POST":
        kind = request.form.get('kind')
        basic_m0 = request.form.get('basic_m0')
        basic_m1 = request.form.get('basic_m1')
        startdate = request.form.get('startdate')
        if startdate == '':
            now = datetime.now()
            startdate = now.strftime('%Y-%m-%d')
        enddate = request.form.get('enddate')
        if enddate == '':
            enddate = startdate

        return straddle_diff_show(basic_m0, basic_m1, startdate, enddate, kind)

    return render_template("straddle_diff.html", title="straddle_diff")

@app.route('/log')
def show_log():
    items = read_log_today()
    return render_template("show_log.html",title="Show Log",items=items)

@app.route('/ins')
def ins():
    return render_template("ins.html",title="ins")

@app.route('/confirm_ins', methods=['post'])
def confirm_ins():
    if request.form.get('token') != '9999':
        return redirect('/')

    ins_type = request.form.get('ins_type')
    code = request.form.get('code')
    num = int(request.form.get('num'))
    price = float(request.form.get('price'))
    cost = int(num*price)
    portfolio = request.form.get('portfolio')
    agent = request.form.get('agent')

    df = ts.get_realtime_quotes(code)
    name = df.at[0,'name']
    if ins_type in ['down_sell','sell_order','buy_order']:
        ins = str({'ins':ins_type,'portfolio':portfolio,'code':code,'num':num,'price':price,'cost':cost,'agent':agent,'name':name})
    if ins_type in ['up_warn','down_warn','del']:
        ins = str({'ins':ins_type,'code':code,'num':num,'price':price,'name':name})

    filename = get_dss() + 'csv/ins.txt'

    a_file(filename,ins)
    return 'success: ' + ins

if __name__ == '__main__':
    # app.run(debug=True)

    app.run(host='0.0.0.0')
