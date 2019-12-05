

import pandas as pd
from flask import Flask, render_template, request, redirect
from datetime import datetime
from multiprocessing.connection import Client
import time
import tushare as ts
import json

from nature import read_log_today, a_file, get_dss

app = Flask(__name__)
# app = Flask(__name__,template_folder='tpl') # 指定一个参数使用自己的模板目录

@app.route('/')
def index():
 return 'log file ins'

@app.route('/fut')
def fut():
    symbol = 'ag2006'
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
    if 'var' in filename:
        df = pd.read_csv(filename, sep='$', dtype='str')
    else:
        df = pd.read_csv(filename, dtype='str')
    r = []
    for i, row in df.iterrows():
        r.append( list(row) )
    r.append( list(df.columns) )
    r = list( reversed(r) )
    r = r[:9]

    return render_template("fut.html",title=symbol,rows_q=r_q,rows_t=r_t,rows=r)

@app.route('/fut_csv')
def fut_csv():
    return render_template("fut_csv.html",title="fut_csv")

@app.route('/show_fut_csv', methods=['post'])
def show_fut_csv():
    filename = get_dss() + request.form.get('filename')
    if 'var' in filename:
        df = pd.read_csv(filename, sep='$', dtype='str')
    else:
        #df = pd.read_csv(filename, sep=',', dtype='str')
        df = pd.read_csv(filename, dtype='str')

    r = []
    for i, row in df.iterrows():
        r.append( list(row) )
    r.append( list(df.columns) )
    r = reversed(r)

    return render_template("show_fut_csv.html",title="Show Log",rows=r)

@app.route('/fut_config', methods=['get','post'])
def fut_config():
    filename = get_dss() + 'fut/cfg/config.json'
    if request.method == "POST":
        key = request.form.get('key')
        value = request.form.get('value')
        with open(filename,'r') as f:
            load_dict = json.load(f)

        kind = request.form.get('kind')
        if kind in ['add', 'alter']:
            load_dict[key] = value
        if kind == 'del':
            load_dict.pop(key)
        with open(filename,"w") as f:
            json.dump(load_dict,f)

    r = [ ['key', 'value'] ]
    with open(filename,'r') as f:
        load_dict = json.load(f)
        for (key,value) in load_dict.items():
            r.append( [key,value] )

    return render_template("fut_config.html",title="fut_config",rows=r)

@app.route('/fut_setting_pz', methods=['get','post'])
def fut_setting_pz():
    filename = get_dss() + 'fut/cfg/setting_pz.csv'
    if request.method == "POST":
        pz = request.form.get('pz')
        size = request.form.get('size')
        priceTick = request.form.get('priceTick')
        variableCommission = request.form.get('variableCommission')
        fixedCommission = request.form.get('fixedCommission')
        slippage = request.form.get('slippage')
        exchangeID = request.form.get('exchangeID')
        kind = request.form.get('kind')

        r = [[pz,size,priceTick,variableCommission,fixedCommission,slippage,exchangeID]]
        cols = ['pz','size','priceTick','variableCommission','fixedCommission','slippage','exchangeID']
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

    df = pd.read_csv(filename, dtype='str')
    r = [ list(df.columns) ]
    for i, row in df.iterrows():
        r.append( list(row) )

    return render_template("fut_setting_pz.html",title="fut_setting_pz",rows=r)

@app.route('/fut_trade_time', methods=['get','post'])
def fut_trade_time():
    filename = get_dss() + 'fut/cfg/trade_time.csv'
    if request.method == "POST":
        symbol = request.form.get('symbol')
        name = request.form.get('name')
        seq_list = request.form.getlist('seq')
        #return str(seq_list)
        r =[]
        for seq in seq_list:
            begin = request.form.get('begin'+'_'+seq)
            end = request.form.get('end'+'_'+seq)
            num = request.form.get('num'+'_'+seq)
            r.append( [name,symbol,seq,begin,end,num] )
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

    return render_template("fut_trade_time.html",title="fut_trade_time",rows=r)

@app.route('/fut_signal_pause', methods=['get','post'])
def fut_signal_pause():
    filename = get_dss() + 'fut/cfg/signal_pause_var.csv'
    if request.method == "POST":
        signal = request.form.get('signal')
        symbols = request.form.get('symbols')
        kind = request.form.get('kind')

        r = [[signal,symbols]]
        cols = ['signal','symbols']
        if kind == 'add':
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, sep='$',  mode='a', header=False, index=False)
        if kind == 'del':
            df = pd.read_csv(filename, sep='$',  dtype='str')
            df = df[df.signal != signal ]
            df.to_csv(filename, sep='$',  index=False)
        if kind == 'alter':
            # 删
            df = pd.read_csv(filename, sep='$',  dtype='str')
            df = df[df.signal != signal ]
            df.to_csv(filename, sep='$',  index=False)
            # 增
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, sep='$', mode='a', header=False, index=False)

    df = pd.read_csv(filename, sep='$', dtype='str')
    r = [ list(df.columns) ]
    for i, row in df.iterrows():
        r.append( list(row) )

    return render_template("fut_signal_pause.html",title="fut_signal_pause",rows=r)

@app.route('/fut_signal_atrrsi', methods=['get','post'])
def fut_signal_atrrsi():
    filename = get_dss() + 'fut/cfg/signal_atrrsi_param.csv'
    if request.method == "POST":
        pz = request.form.get('pz')
        rsiLength = request.form.get('rsiLength')
        trailingPercent = request.form.get('trailingPercent')
        victoryPercent = request.form.get('victoryPercent')

        kind = request.form.get('kind')

        r = [[pz,rsiLength,trailingPercent,victoryPercent]]
        cols = ['pz','rsiLength','trailingPercent','victoryPercent']
        if kind == 'add':
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, mode='a', header=False, index=False)
        if kind == 'del':
            df = pd.read_csv(filename, dtype='str')
            df = df[df.pz != pz ]
            df.to_csv(filename,  index=False)
        if kind == 'alter':
            # 删
            df = pd.read_csv(filename,  dtype='str')
            df = df[df.pz != pz ]
            df.to_csv(filename, index=False)
            # 增
            df = pd.DataFrame(r, columns=cols)
            df.to_csv(filename, mode='a', header=False, index=False)

    df = pd.read_csv(filename, dtype='str')
    r = [ list(df.columns) ]
    for i, row in df.iterrows():
        r.append( list(row) )

    #return str(r)
    return render_template("fut_signal_atrrsi.html",title="fut_signal_atrrsi",rows=r)

@app.route('/log')
def show_log():
    items = read_log_today()
    return render_template("show_log.html",title="Show Log",items=items)

@app.route('/file')
def upload_file():
    return render_template("upload_file.html",title="upload file")

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

    filename = 'csv/ins.txt'

    a_file(filename,ins)
    return 'success: ' + ins

if __name__ == '__main__':
    #app.run(debug=True)

    app.run(host='0.0.0.0')
