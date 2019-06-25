
import pandas as pd
import json
from flask import Flask
from flask import make_response
from flask import redirect
from flask import abort


app = Flask(__name__) #创建一个wsgi应用
@app.route('/') #添加路由：根
def hello_world():
    return 'Hello World!' #输出一个字符串

@app.route('/hello') #添加路由：hello
def do_hello():
    return '<h1>Hello, stranger!</h1>' #输出一个字符串

@app.route('/json')
def do_json():
    hello = {"name":"stranger", "say":"hello"}
    return json.dumps(hello)

@app.route('/status_500')
def status_500():
    return "hello",500

@app.route('/set_header')
def set_header():
    resp = make_response('<h1>This document has a modified header!</h1>')
    resp.headers['X-Something'] = 'A value'
    resp.headers['Server'] = 'My special http server'
    return resp

@app.route('/set_cookie')
def set_cookie():
    response = make_response('<h1>This document carries a cookie!</h1>')
    response.set_cookie('username', 'evancss')
    return response

@app.route('/redir')
def redir():
    return redirect('http://www.baidu.com')

@app.route('/user/<id>')
def get_user(id):
    if int(id)>10:
        abort(404)
    return '<h1>Hello, %s</h1>' % id


if __name__ == '__main__':
    app.run(debug=True) #
