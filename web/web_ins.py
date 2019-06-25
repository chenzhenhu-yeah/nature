
import pandas as pd
from flask import Flask, render_template, request, redirect
from datetime import datetime
from multiprocessing.connection import Client
import time
import tushare as ts


def read_log():
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    #today = '2019-03-02'
    logfile= '../auto_trade/log/autotrade.log'
    #df = pd.read_csv(logfile,sep=' ',header=None,encoding='ansi')
    df = pd.read_csv(logfile,sep=' ',header=None,encoding='gbk')
    df = df[df[0]==today]

    r = []
    for i, row in df.iterrows():
        r.append(str(list(row)))

    return r


#{'ins':'a','filename':'ins.txt','content':'aaaaaa'}
def a_ins_file(content):
    r = []
    ins_dict = {'ins':'a','filename':'ini/ins.txt','content':content}
    address = ('localhost', 9001)
    again = True
    while again:
        time.sleep(1)
        try :
            with Client(address, authkey=b'secret password') as conn:
                conn.send(ins_dict)
                r = conn.recv()
                again = False
        except:
            pass

    return r

app = Flask(__name__)
# app = Flask(__name__,template_folder='tpl') # 指定一个参数使用自己的模板目录

@app.route('/')
def index():
 return 'log file ins'

@app.route('/log')
def show_log():
    items = read_log()
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
    
    a_ins_file(ins)
    return 'success: ' + ins

if __name__ == '__main__':
    #app.run(debug=True)
    app.run(host='0.0.0.0')
