import pandas as pd
from flask import Flask, render_template, request

app = Flask(__name__)
# app = Flask(__name__,template_folder='tpl') # 指定一个参数使用自己的模板目录

@app.route('/')
def index():
 return render_template('test1.html')

# @app.route('/login', methods=['post'])
# def login():
#  name = request.form.get('name')
#  password = request.form.get('password')
#  if name == 'admin' and password == '123':
#       return render_template('test1.html', name=name)
#  return render_template('test1.html')

@app.route('/login', methods=['get', 'post'])
def login():
 name = request.form.get('name')
 password = request.form.get('password')
 if name == 'admin' and password == '123':
     return redirect(url_for('login'))
 return render_template('test1.html')

if __name__ == '__main__':
    #app.run(debug=True)
    app.run(host='0.0.0.0')
