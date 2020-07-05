
import pandas as pd
from flask import Flask, render_template, request, redirect
from flask import url_for
from datetime import datetime
from multiprocessing.connection import Client
import time
import tushare as ts
import json
import os

from nature import read_log_today, a_file, get_dss, get_symbols_quote, get_contract
from nature import draw_web, ic_show, ip_show, smile, opt, dali, yue, mates, iv_ts, star, vol
from nature import del_blank, check_symbols_p


app = Flask(__name__)
# app = Flask(__name__, static_url_path="/render")
# app = Flask(__name__,template_folder='tpl') # 指定一个参数使用自己的模板目录

@app.route('/')
def index():
 return 'log file ins'

@app.route('/fut')
def fut():
    symbol = 'ag2012'
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
        # try:
        #     s = check_symbols_p(key, value)
        # except:
        #     s = '该项设置出错了 ！'

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

@app.route('/opt_mature', methods=['get','post'])
def opt_mature():
    pz = ''
    tips = '提示：'
    setting_dict = {'pz':'','symbol':'','mature':'','flag':'',}
    filename = get_dss() + 'fut/cfg/opt_mature.csv'
    if request.method == "POST":
        pz = del_blank( request.form.get('pz') )
        symbol = del_blank( request.form.get('symbol') )
        mature = del_blank( request.form.get('mature') )
        flag = del_blank( request.form.get('flag') )
        kind = request.form.get('kind')

        r = [[pz,symbol,mature,flag]]
        cols = ['pz','symbol','mature','flag']
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
        if kind == 'query':
            df = pd.read_csv(filename, dtype='str')
            df = df[df.symbol == symbol ]
            if len(df) > 0:
                rec = df.iloc[0,:]
                setting_dict = dict(rec)
                pz = rec.pz

    # 显示配置文件的内容
    df = pd.read_csv(filename, dtype='str')
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
            fn = 'fut/engine/owl/signal_owl_mix_var_' + code + '.csv'
            # fn = get_dss() + 'fut/engine/owl/signal_owl_mix_var_' + code + '.csv'
            a_file(fn,ins)

            # 历史维护记录
            now = datetime.now()
            today = now.strftime('%Y-%m-%d %H:%M:%S')
            ins = str({'date':today,'ins':ins_type,'price':price,'num':num})
            fn = 'fut/engine/owl/history.csv'
            a_file(fn,ins)

            tips = 'append success'

    return render_template("owl.html",tip=tips)

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

@app.route('/show_vol', methods=['get'])
def show_vol():
    vol()
    r = []
    dirname = 'static/'
    file_list = os.listdir(dirname)
    for fn in file_list:
        if fn.startswith('vol'):
            r.append(dirname + fn)

    # return str(r)
    return render_template("show_jpg.html",header="vol",items=r)


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
