
import pandas as pd
from flask import Flask, render_template, request, redirect
from datetime import datetime
from multiprocessing.connection import Client
import time
import tushare as ts
import json
import os

from nature import read_log_today, a_file, get_dss, get_symbols_quote
from nature import draw_web, draw_web_plot

def del_blank(c):
    s = str(c).strip()
    s = s.replace('\t','')
    s = s.replace(' ','')
    return s

app = Flask(__name__)
#app = Flask(__name__, static_url_path="/render")
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

def check_symbols_p(key, value):
    r = ''

    if key == 'gateway_pf':
        v  = eval(value)
        # if type(v) != type({}):
        if isinstance(v, dict) == False:
            r = '非字典'

    if key == 'symbols_aberration_enhance':
        if len(value) > 0:
            symbol_list = value.split(',')
        else:
            symbol_list = []

        for symbol in symbol_list:
            fn = get_dss() + 'fut/put/rec/day_' + symbol + '.csv'
            if os.path.exists(fn):
                df = pd.read_csv(fn)
                if len(df) <= 30:
                    r = 'day_' + symbol + '.csv 记录数不足30'
            else:
                r = 'day_' + symbol + '.csv 记录数不足30'

    if key in ['symbols_dalicta', 'symbols_dualband']:
        if len(value) > 0:
            symbol_list = value.split(',')
        else:
            symbol_list = []

        for symbol in symbol_list:
            fn = get_dss() + 'fut/put/rec/day_' + symbol + '.csv'
            if os.path.exists(fn):
                df = pd.read_csv(fn)
                if len(df) <= 60:
                    r = 'day_' + symbol + '.csv 记录数不足60'
            else:
                r = 'day_' + symbol + '.csv 记录数不足60'

    if key == 'symbols_cci_raw':
        if len(value) > 0:
            symbol_list = value.split(',')
        else:
            symbol_list = []

        for symbol in symbol_list:
            fn = get_dss() + 'fut/put/rec/day_' + symbol + '.csv'
            if os.path.exists(fn):
                df = pd.read_csv(fn)
                if len(df) <= 100:
                    r = 'day_' + symbol + '.csv 记录数不足100'
            else:
                r = 'day_' + symbol + '.csv 记录数不足100'

    if key in ['symbols_dali', 'symbols_owl']:
        if len(value) > 0:
            symbol_list = value.split(',')
        else:
            symbol_list = []

        for symbol in symbol_list:
            fn = get_dss() + 'fut/put/rec/min5_' + symbol + '.csv'
            if os.path.exists(fn):
                df = pd.read_csv(fn)
                if len(df) <= 60:
                    r = 'min5_' + symbol + '.csv 记录数不足60'
            else:
                r = 'min5_' + symbol + '.csv 记录数不足60'

    if key in ['symbols_rsiboll', 'symbols_cciboll']:
        if len(value) > 0:
            symbol_list = value.split(',')
        else:
            symbol_list = []

        for symbol in symbol_list:
            fn = get_dss() + 'fut/put/rec/min15_' + symbol + '.csv'
            if os.path.exists(fn):
                df = pd.read_csv(fn)
                if len(df) <= 60:
                    r = 'min15_' + symbol + '.csv 记录数不足100'
            else:
                r = 'min15_' + symbol + '.csv 记录数不足100'

    if key == 'symbols_trade':
        if len(value) > 0:
            symbol_list = value.split(',')
        else:
            symbol_list = []

        for symbol in symbol_list:
            fn = get_dss() + 'fut/put/min1_' + symbol + '.csv'
            if os.path.exists(fn):
                pass
            else:
                r = 'min1_' + symbol + '.csv 文件不存在'

    if key in ['symbols_quote','symbols_quote_01','symbols_quote_05','symbols_quote_06','symbols_quote_09','symbols_quote_10','symbols_quote_12']:
        if len(value) > 0:
            symbol_list = value.split(',')
            for symbol in symbol_list:
                pz = symbol[:2]
                if pz.isalpha():
                    pass
                else:
                    pz = symbol[:1]

                fn = get_dss() + 'fut/cfg/setting_pz.csv'
                df = pd.read_csv(fn)
                pz_set = set(df.pz)
                if pz in pz_set:
                    pass
                else:
                    r = pz + '未在setting_pz中维护'

                fn = get_dss() + 'fut/cfg/trade_time.csv'
                df = pd.read_csv(fn)
                pz_set = set(df.symbol)
                if pz in pz_set:
                    pass
                else:
                    r = pz + '未在trade_time中维护'

    if key not in ['symbols_quote','symbols_quote_01','symbols_quote_05','symbols_quote_06','symbols_quote_09','symbols_quote_10','symbols_quote_12','symbols_trade','gateway_pz','gateway_pf']:
        config = open(get_dss() + 'fut/cfg/config.json')
        setting = json.load(config)
        symbols = setting['symbols_trade']
        symbols_trade_list = symbols.split(',')
        if len(value) > 0:
            symbol_list = value.split(',')
            for symbol in symbol_list:
                if symbol not in  symbols_trade_list:
                    r = symbol + ' 未在symbols_trade中维护'

    symbols_all = ['symbols_quote','symbols_quote_01','symbols_quote_05','symbols_quote_06','symbols_quote_09','symbols_quote_10','symbols_quote_12',
                   'symbols_trade','gateway_pz','gateway_pf','symbols_owl','symbols_cci_raw','symbols_aberration_enhance',
                   'symbols_cciboll','symbols_dali','symbols_rsiboll','symbols_atrrsi','symbols_turtle','symbols_dalicta',
                   'symbols_dualband','symbols_ic','symbols_ma',
                  ]
    if key not in symbols_all:
        r = '新symbols，未在web端进行风控'

    return r

@app.route('/fut_config', methods=['get','post'])
def fut_config():
    tips = '提示：'
    filename = get_dss() + 'fut/cfg/config.json'
    if request.method == "POST":
        key = del_blank( request.form.get('key') )
        value = del_blank( request.form.get('value') )
        try:
            s = check_symbols_p(key, value)
        except:
            s = '该项设置出错了 ！'

        if s != '':
            tips += s
        else:
            with open(filename,'r') as f:
                load_dict = json.load(f)

            kind = request.form.get('kind')
            if kind in ['add', 'alter']:
                load_dict[key] = value
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

from flask import url_for

@app.route('/value_p_csv', methods=['get','post'])
def value_p_csv():
    fn = get_dss() + 'fut/engine/value_p.csv'
    df = pd.read_csv(fn, dtype='str')

    r = []
    for i, row in df.iterrows():
        r.append( list(row) )
    r.append( list(df.columns) )
    r = reversed(r)

    return render_template("show_fut_csv.html",title="value_p",rows=r)

@app.route('/value_dali_csv', methods=['get','post'])
def value_dali_csv():
    fn = get_dss() + 'fut/engine/value_dali.csv'
    df = pd.read_csv(fn, dtype='str')

    r = []
    for i, row in df.iterrows():
        r.append( list(row) )
    r.append( list(df.columns) )
    r = reversed(r)

    return render_template("show_fut_csv.html",title="value_dali",rows=r)

@app.route('/value_p_render', methods=['get','post'])
def value_p_render():
    fn = get_dss() + 'fut/engine/value_p.csv'
    df = pd.read_csv(fn)
    df['close'] = df['year_ratio']
    df['symbol'] = df['p']
    df = df[ df.symbol.isin(['dali','star','opt']) ]

    draw_web.value(df)
    time.sleep(1)
    fn = 'value.html'
    return app.send_static_file(fn)

@app.route('/value_dali_render', methods=['get','post'])
def value_dali_render():
    fn = get_dss() + 'fut/engine/value_dali.csv'
    df = pd.read_csv(fn)
    df['close'] = df['year_ratio']
    df['symbol'] = df['pz']

    draw_web.value(df)
    time.sleep(1)
    fn = 'value.html'
    return app.send_static_file(fn)

@app.route('/value_dali_m_render', methods=['get','post'])
def value_dali_m_render():
    fn = get_dss() + 'fut/engine/value_dali_m.csv'
    df = pd.read_csv(fn)
    df['close'] = df['year_ratio']
    df['symbol'] = df['name']

    draw_web.value(df)
    time.sleep(1)
    fn = 'value.html'
    return app.send_static_file(fn)

@app.route('/value_dali_RM_render', methods=['get','post'])
def value_dali_RM_render():
    fn = get_dss() + 'fut/engine/value_dali_RM.csv'
    df = pd.read_csv(fn)
    df['close'] = df['year_ratio']
    df['symbol'] = df['name']

    draw_web.value(df)
    time.sleep(1)
    fn = 'value.html'
    return app.send_static_file(fn)

@app.route('/value_dali_MA_render', methods=['get','post'])
def value_dali_MA_render():
    fn = get_dss() + 'fut/engine/value_dali_MA.csv'
    df = pd.read_csv(fn)
    df['close'] = df['year_ratio']
    df['symbol'] = df['name']

    draw_web.value(df)
    time.sleep(1)
    fn = 'value.html'
    return app.send_static_file(fn)

@app.route('/risk_p_render', methods=['get','post'])
def risk_p_render():
    fn = get_dss() + 'fut/engine/value_p.csv'
    df = pd.read_csv(fn)
    df['close'] = df['risk']
    df['symbol'] = df['p']

    draw_web.value(df)
    time.sleep(1)
    fn = 'value.html'
    return app.send_static_file(fn)

@app.route('/risk_dali_render', methods=['get','post'])
def risk_dali_render():
    fn = get_dss() + 'fut/engine/value_dali.csv'
    df = pd.read_csv(fn)
    df['close'] = df['risk']
    df['symbol'] = df['pz']

    draw_web.value(df)
    time.sleep(1)
    fn = 'value.html'
    return app.send_static_file(fn)

@app.route('/bar_ma_m', methods=['get','post'])
def bar_ma_m():
    symbol = 'm2005'
    draw_web.bar_ma_m(symbol)
    fn = 'bar_ma_m.html'
    time.sleep(1)
    return app.send_static_file(fn)

@app.route('/bar_ma_CF', methods=['get','post'])
def bar_ma_CF():
    symbol = 'CF005'
    draw_web.bar_ma_CF(symbol)
    fn = 'bar_ma_CF.html'
    time.sleep(1)
    return app.send_static_file(fn)

@app.route('/bar_ma_ru', methods=['get','post'])
def bar_ma_ru():
    symbol = 'ru2009'
    draw_web.bar_ma_ru(symbol)
    fn = 'bar_ma_ru.html'
    time.sleep(1)
    return app.send_static_file(fn)


@app.route('/resid_day_CF', methods=['get','post'])
def resid_day_CF():
    symbol = 'CF009'
    draw_web.resid_day_CF(symbol)
    fn = 'resid_day_CF.html'
    time.sleep(1)
    return app.send_static_file(fn)

@app.route('/resid_min30_CF', methods=['get','post'])
def resid_min30_CF():
    symbol = 'CF005'
    draw_web.resid_min30_CF(symbol)
    fn = 'resid_min30_CF.html'
    time.sleep(1)
    return app.send_static_file(fn)

@app.route('/resid_day_m', methods=['get','post'])
def resid_day_m():
    symbol = 'm2009'
    draw_web.resid_day_m(symbol)
    fn = 'resid_day_m.html'
    time.sleep(1)
    return app.send_static_file(fn)


@app.route('/resid_min30_m', methods=['get','post'])
def resid_min30_m():
    symbol = 'm2005'
    draw_web.resid_min30_m(symbol)
    fn = 'resid_min30_m.html'
    time.sleep(1)
    return app.send_static_file(fn)


@app.route('/resid_day_ru', methods=['get','post'])
def resid_day_ru():
    symbol = 'ru2009'
    draw_web.resid_day_ru(symbol)
    fn = 'resid_day_ru.html'
    time.sleep(1)
    return app.send_static_file(fn)


@app.route('/resid_min30_ru', methods=['get','post'])
def resid_min30_ru():
    symbol = 'ru2009'
    draw_web.resid_min30_ru(symbol)
    fn = 'resid_min30_ru.html'
    time.sleep(1)
    return app.send_static_file(fn)

@app.route('/bar_aberration_CF', methods=['get','post'])
def bar_aberration_CF():
    symbol = 'CF009'
    draw_web.bar_aberration_CF(symbol)
    fn = 'bar_aberration_CF.html'
    time.sleep(1)
    return app.send_static_file(fn)

@app.route('/bar_cci_CF', methods=['get','post'])
def bar_cci_CF():
    symbol = 'CF009'
    draw_web.bar_cci_CF(symbol)
    fn = 'bar_cci_CF.html'
    time.sleep(1)
    return app.send_static_file(fn)

@app.route('/bar_dalicta_m', methods=['get','post'])
def bar_dalicta_m():
    symbol = 'm2009'
    draw_web.bar_dalicta_m(symbol)
    fn = 'bar_dalicta_m.html'
    time.sleep(1)
    return app.send_static_file(fn)

@app.route('/ic_y_m', methods=['get','post'])
def ic_y_m():
    symbol1 = 'y2005'
    symbol2 = 'm2005'
    start_dt = '2020-01-01'
    draw_web.ic(symbol1, symbol2, start_dt)
    time.sleep(1)
    fn = 'ic_' + symbol1 + '_'+ symbol2+ '.html'
    return app.send_static_file(fn)

def ic(seq):
    r = ''
    seq = 'ic' + str(seq)
    fn = 'mates.csv'
    df = pd.read_csv(fn)
    df = df[df.seq == seq]
    if len(df) > 0:
        rec = df.iloc[0,:]
        symbol1 = rec.mate1
        symbol2 = rec.mate2
        start_dt = rec.start_dt
        draw_web_plot.ic(symbol1, symbol2, start_dt)
        fn = 'ic_' + symbol1 + '_'+ symbol2+ '.jpg'
        now = str(int(time.time()))
        r = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return r

@app.route('/ic0', methods=['get','post'])
def ic0():
    return ic(0)

@app.route('/ic1', methods=['get','post'])
def ic1():
    return ic(1)

@app.route('/ic2', methods=['get','post'])
def ic2():
    return ic(2)

@app.route('/ic3', methods=['get','post'])
def ic3():
    return ic(3)

@app.route('/ic4', methods=['get','post'])
def ic4():
    return ic(4)

@app.route('/ic5', methods=['get','post'])
def ic5():
    return ic(5)

@app.route('/ic6', methods=['get','post'])
def ic6():
    return ic(6)

@app.route('/ic7', methods=['get','post'])
def ic7():
    return ic(7)

@app.route('/ic8', methods=['get','post'])
def ic8():
    return ic(8)

@app.route('/ic9', methods=['get','post'])
def ic9():
    return ic(9)

def ip(seq):
    r = ''
    seq = 'ip' + str(seq)
    fn = 'mates.csv'
    df = pd.read_csv(fn)
    df = df[df.seq == seq]
    if len(df) > 0:
        rec = df.iloc[0,:]
        symbol1 = rec.mate1
        symbol2 = rec.mate2
        start_dt = rec.start_dt
        draw_web_plot.ic(symbol1, symbol2, start_dt)
        fn = 'ic_' + symbol1 + '_'+ symbol2+ '.jpg'
        now = str(int(time.time()))
        r = '<img src=\"static/' + fn + '?rand=' + now + '\" />'
    return r

@app.route('/ip0', methods=['get','post'])
def ip0():
    return ip(0)

@app.route('/ip1', methods=['get','post'])
def ip1():
    return ip(1)

@app.route('/ip2', methods=['get','post'])
def ip2():
    return ip(2)

@app.route('/ip3', methods=['get','post'])
def ip3():
    return ip(3)

@app.route('/ip4', methods=['get','post'])
def ip4():
    return ip(4)

@app.route('/ip5', methods=['get','post'])
def ip5():
    return ip(5)

@app.route('/ip6', methods=['get','post'])
def ip6():
    return ip(6)

@app.route('/ip7', methods=['get','post'])
def ip7():
    return ip(7)

@app.route('/ip8', methods=['get','post'])
def ip8():
    return ip(8)

@app.route('/ip9', methods=['get','post'])
def ip9():
    return ip(9)

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
    # app.run(debug=True)

    app.run(host='0.0.0.0')
