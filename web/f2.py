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
