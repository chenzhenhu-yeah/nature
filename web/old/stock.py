import json
import pandas as pd
from datetime import datetime, timedelta
from flask import Flask,render_template

def read_log(today):
    logfile= r'E:\win7_data\Me\python_project\auto_trade\log\autotrade.log'
    df = pd.read_csv(logfile,sep=' ',header=None)
    df = df[df[0]==today]
    #print(df)

    r = []
    for i, row in df.iterrows():
        #print(list(row))
        r.append(list(row))

    #print(r)
    return r

app = Flask(__name__)   #创建一个wsgi应用

@app.route('/')             #添加路由：根
def hello_world():
    s = r'<a href="/log">log</a>'
    return s            #输出一个字符串

@app.route('/log')
def hello():
    today = datetime.now().strftime('%Y-%m-%d')
    showlist = ['今天是'+today] + read_log(today)
    return render_template("log.html",title="log",items=showlist)

@app.route('/hello')           #添加路由：hello
def do_hello():
    return '<h1>Hello, stranger!</h1>'      #输出一个字符串

@app.route('/json')
def do_json():
    hello = {"name":"stranger", "say":"hello"}
    return json.dumps(hello)

if __name__ == '__main__':
    #app.run(debug=True)             #启动app的调试模式
    app.run(host="192.168.31.204", port=5000)
