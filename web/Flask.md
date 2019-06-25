
启动VirtualEnv，输入pip install flask即可

## 安装Flask
- pip install flask
## Hello World
```
from flask import Flask
app = Flask(__name__) #创建一个wsgi应用

@app.route('/') #添加路由：根
def hello_world():
    return 'Hello World!' #输出一个字符串

if __name__ == '__main__':
    app.run(debug=True) #调试模式
```
- 打开浏览器访问http://localhost:5000
## Json接口
```
import json
...
@app.route('/json')
def do_json():
  hello = {"name":"stranger", "say":"hello"}
  return json.dumps(hello)
```
## 自定义HTTP响应头
```
from flask import make_response
...
@app.route('/set_header')
def set_header():
    resp = make_response('<h1>This document has a modified header!</h1>')
    resp.headers['X-Something'] = 'A value'
    resp.headers['Server'] = 'My special http server'
    return resp
```
## 设置Cookie
```
@app.route('/set_cookie')
def set_cookie():
    response = make_response('<h1>This document carries a cookie!</h1>')
    response.set_cookie('username', 'evancss')
    return response
```
## 重定向
```
from flask import redirect
...

@app.route('/redir')
def redir():
    return redirect('http://www.baidu.com')
```
## 抛出异常把控制权交给Web服务器
```
@app.route('/user/<id>')
def get_user(id):
    if int(id)>10:
        abort(404)
    return '<h1>Hello, %s</h1>' % id
```
## Jinja2模板
- 一个典型的Jinja2模板，该html文件默认存放在template目录下面
```
<title>{{title}}</title>
<ul>
{% for user in users %}
<li><a href="{{ user.url }}">{{ user.username }}</a></li>
{% endfor %}
</ul>
```
- 使用模板
  - 参数在调用模板时指定
```
from flask import Flask,render_template

app = Flask(__name__)
# app = Flask(__name__,template_folder='tpl') # 指定一个参数使用自己的模板目录

@app.route('/hello')
def hello():
    users = [
    {"username":"users1","url":"/users/user1"},
    {"username":"users2","url":"/users/user2"}]
    return render_template("hello.html",title="User List",users=users)

if __name__ == '__main__':
    app.run(debug=True)
```
## 基本的模板用法
- 以使用一对双大括号显示变量
```
<title>{{title}}</title>
```
- 循环
```
{% for user in users %}
<li>{{ user }}</a></li>
{% endfor %}
```
- 字典可以使用点操作符或者下标访问的方式取得值
```
Username:{{ user['name'] }}
Email: {{ user.email }}
```
- 访问对象属性和方法
```
<h5>{{obj.name}}:</h5>
<p>{{obj.say()}}</p>
```
## 循环内置对象loop
- loop对象的详细说明见下表
属性值 | 描述
---   |  ---
loop.index  | 当前迭代的索引，从1开始算
loop.index0  | 当前迭代的索引，从0开始算
loop.revindex  | 相对于序列末尾的索引，从1开始算
loop.revindex0  | 相对于序列末尾的索引，从0开始算
loop.first  | 相当于 loop.index == 1.
loop.last  | 相当于 loop.index == len(seq) - 1
loop.length  | 序列的长度.
loop.cycle  | 可以接受两个字符串参数，如果当前循环索引是偶数，则显示第一个字符串，是奇数则显示第二个字符串。
- loop.cycle常被在表格中用不同的背景色区分相邻的行
```
{% for row in rows %}
<li class="{{ loop.cycle('odd', 'even') }}">{{ row }}</li>
{% endfor %}
```
## if-else
```
{% if kenny.sick %}
Kenny is sick.
{% elif kenny.dead %}
You killed Kenny! You bastard!!!
{% else %}
Kenny looks okay --- so far
{% endif %}
```
## 格式化
- Jinjia2中的格式化操作最常见的是使用过滤器进行的。
- 字符串
```
<h1>Hello {{user}}</h1>
<hr />
<p>转为首字母大写: Hello, {{ user|capitalize }}</p>
<p>转为大写: Hello, {{ user|upper }}</p>
<p>转为小写: Hello, {{ user|lower }}</p>
<p>转为标题样式（每个主题词首字母大写）: Hello, {{ user|title }}</p>
<p>去掉首尾空格（页面上无效果）: Hello, {{ user|trim }}</p>
<p>默认的HTML过滤操作: {{ myhtmlstr }}</p>
<p>过滤掉HTML标记: {{ myhtmlstr|striptags }}</p>
<hr />
```
- 数字
```
<h2>和数字相关的过滤器</h2>
<p>四舍五入：{{n1|round}}</p>
<p>保留3位小数并四舍五入：{{n1|round(3)}}</p>
<p>保留1位小数下取整：{{n1|round(1,'floor')}}</p>
<p>保留1位小数上取整：{{n1|round(1,'ceil')}}</p>
<p>取整：{{n1|int}}</p>
<p>使用格式化字符串：{{"%.2f" % n1}}</p>
<p>使用string.format函数进行千分位显示：{{"{:,}".format(n2)}}</p>
<p>使用string.format函数对多个数字进行格式化：{{"Number1:{0:.2%}, Number2:{1:,}".format(n1,n2)}}</p>
```
