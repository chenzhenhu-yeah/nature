#  -*- coding: utf-8 -*-
import pandas as pd
import time
from datetime import datetime
import json
import smtplib
from email.mime.text import MIMEText

from nature import to_log

def send_email(dss, subject, content):
    to_log('in send_email')
    # # 第三方 SMTP 服务
    # mail_host = 'smtp.yeah.net'              # 设置服务器
    # mail_username = 'chenzhenhu@yeah.net'   # 用户名
    # mail_auth_password = "852299"       # 授权密码

    # 加载配置
    config = open(dss+'csv/config.json')
    setting = json.load(config)
    mail_host = setting['mail_host']              # 设置服务器
    mail_username = setting['mail_username']          # 用户名
    mail_auth_password = setting['mail_auth_password']     # 授权密码
    # print(mail_host, mail_username, mail_auth_password)

    sender = setting['sender']
    receivers = setting['receivers']         # 一个收件人
    #receivers = '270114497@qq.com, zhenghaishu@126.com' # 多个收件人

    try:
        message = MIMEText(content, 'plain', 'utf-8')
        message['From'] = sender
        message['To'] =  receivers
        message['Subject'] = str(subject)
        #smtpObj = smtplib.SMTP(mail_host, 25)                               # 生成smtpObj对象，使用非SSL协议端口号25
        smtpObj = smtplib.SMTP_SSL(mail_host, 465)                         # 生成smtpObj对象，使用SSL协议端口号465
        smtpObj.login(mail_username, mail_auth_password)                    # 登录邮箱
        # smtpObj.sendmail(sender, receivers, message.as_string())          # 发送给一人
        smtpObj.sendmail(sender, receivers.split(','), message.as_string()) # 发送给多人
        print ("邮件发送成功")
    except smtplib.SMTPException as e:
        print ("Error: 无法发送邮件")
        print(e)

def is_trade_time():
    #to_log('in is_trade_time')

    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    weekday = int(now.strftime('%w'))
    #print(weekday)
    if 1 <= weekday <= 5:
        t = time.localtime()
        if (t.tm_hour>9 and t.tm_hour<15) or (t.tm_hour==9 and t.tm_min>20) :
            return True
    else:
        return False

def is_price_time():
    #to_log('in is_price_time')

    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    weekday = int(now.strftime('%w'))
    #print(weekday)
    if 1 <= weekday <= 5:
        t = time.localtime()
        if (t.tm_hour>9 and t.tm_hour<15) or (t.tm_hour==9 and t.tm_min>31) :
            return True
    else:
        return False

#----------------------------------------------------------------------
def is_trade_day():
    #now = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0)
    now = datetime.now()
    today = now.strftime('%Y%m%d')
    today = datetime.strptime(today + ' ' + '00:00:00', '%Y%m%d %H:%M:%S')
    weekday = int(now.strftime('%w'))
    #print(weekday)

    if 1 <= weekday <= 5:
        return True, today
    else:
        return False, today

if __name__ == '__main__':
    pass
