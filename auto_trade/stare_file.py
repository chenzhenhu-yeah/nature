import time
import os
import tushare as ts
import smtplib
from email.mime.text import MIMEText
from multiprocessing.connection import Client
from datetime import datetime

def to_log(s):
    address = ('localhost', 9000)
    again = True
    while again:
        time.sleep(1)
        try :
            with Client(address, authkey=b'secret password') as conn2:
                conn2.send(s)
            again = False
        except:
            pass

def is_trade_time():
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    weekday = int(now.strftime('%w'))
    #print(weekday)
    if 0 <= weekday <= 6:
        t = time.localtime()
        if (t.tm_hour>9 and t.tm_hour<15) or (t.tm_hour==9 and t.tm_min>20) :
            return True
    else:
        return False

def send_instruction(ins_dict):
    address = ('localhost', 9002)
    again = True
    while again:
        time.sleep(1)
        try :
            with Client(address, authkey=b'secret password') as conn:
                # to_log('stare send ins: '+str(ins_dict))
                conn.send(ins_dict)
                again = False
        except:
            pass

def deal_order(ins_dict):
    if is_trade_time():
        send_instruction(ins_dict)
        return True
    else:
        return False

def send_email(subject, content):
    # 第三方 SMTP 服务
    mail_host = 'smtp.yeah.net'              # 设置服务器
    mail_username = 'chenzhenhu@yeah.net'   # 用户名
    mail_auth_password = "852299"       # 授权密码

    sender = 'chenzhenhu@yeah.net'
    receivers = 'chenzhenhu@yeah.net'         # 一个收件人
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
    except smtplib.SMTPException:
        print ("Error: 无法发送邮件")

#{'ins':'r','filename':'ins.txt'}
#{'ins':'rc','filename':'ins.txt'}
def rc_ins_file():
    r = []
    ins_dict = {'ins':'rc','filename':'ini/ins.txt'}
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

def stare_reach():
    print('begin stare_reach')
    fly_dict = {}
    item_list = []
    cycle = True
    while cycle:
        try:
            time.sleep(3)

            if is_trade_time():
                # 读入ins文件后清空
                new_ins = rc_ins_file()

                # 处理 'del' 指令
                d = {}
                c = []
                for i,item in enumerate(new_ins):
                    if item['ins'] == 'del':
                        d = item
                    else:
                        c += [item]

                # 将ins加入到stare列表中
                item_list += c

                if d != {}:
                    for i,item in enumerate(item_list):
                        if item['code'] == d['code'] and item['num'] == d['num'] and item['price'] == d['price']:
                            item_list.pop(i)
                            break

                for i,item in enumerate(item_list):
                    if item['ins'] == 'down_sell':
                        df = ts.get_realtime_quotes(item['code'])
                        b3_price = float(df.at[0,'b3_p'])
                        if b3_price <= item['price'] and b3_price > 0:
                            # to_log('reach cost {}: '.format(item['price'])+ str(item))
                            item['ins'] = 'sell_order'
                            deal_order(item)
                            item_list.pop(i)
                            break
                    if item['ins'] == 'up_warn':
                        df = ts.get_realtime_quotes(item['code'])
                        b3_price = float(df.at[0,'b3_p'])
                        if b3_price >= item['price']:
                            # to_log('reach price {}: '.format(item['price'])+ str(item))
                            send_email('up_warn', str(item))
                            item_list.pop(i)
                            break
                    if item['ins'] == 'down_warn':
                        df = ts.get_realtime_quotes(item['code'])
                        b3_price = float(df.at[0,'b3_p'])
                        if b3_price <= item['price'] and b3_price > 0 :
                            # to_log('reach price {}: '.format(item['price'])+ str(item))
                            send_email('down_warn', str(item))
                            item_list.pop(i)
                            break
                    if item['ins'] in ['sell_order','buy_order']:
                        if deal_order(item):
                            item_list.pop(i)
                            break
                    if item['ins'] == 'fly_buy':
                        '''
                        短时直线拉升买入策略：
                        1、开盘价介于前收盘的1%之间
                        2、5分种内变动幅度达3%，不超5%
                        3、买入价为当前价加1%
                        '''
                        code = item['code']
                        df = ts.get_realtime_quotes(item['code'])
                        open = float(df.at[0,'open'])
                        pre_close = float(df.at[0,'pre_close'])
                        price = float(df.at[0,'price'])
                        if (open-pre_close)/pre_close > -0.01 and (open-pre_close)/pre_close < 0.01:
                            if code in fly_dict:
                                price_list = fly_dict[code]
                                price_list.insert(0,price)
                                if len(price_list) > 90:
                                    price_list.pop()
                                fly_dict[code] = price_list
                                if (price_list[0]-price_list[-1])/pre_close >= 0.03:
                                    if (price_list[0]-price_list[-1])/pre_close <= 0.05:
                                        order_price = round(price*1.01,2)
                                        order_cost  = order_price * item['num']
                                        order = {'ins':'buy_order','portfolio':item['portfolio'],'code':code,'num':item['num'],'price':order_price,'cost':order_cost,'agent':'pingan'}
                                        if deal_order(order):
                                            item_list.pop(i)
                                            break
                            else:
                                fly_dict[code] = [price]
                        else:
                            if code in fly_dict:
                                del fly_dict[code]
                    if item['ins'] == 'cross_sell':
                        '''
                        高位跳空大阴线、十字星卖出策略：
                        1、跳空高开
                        2、后高十字星或阴线
                        '''
                        code = item['code']
                        df = ts.get_realtime_quotes(item['code'])
                        open = float(df.at[0,'open'])
                        high = float(df.at[0,'high'])
                        low = float(df.at[0,'low'])
                        pre_close = float(df.at[0,'pre_close'])
                        price = float(df.at[0,'price'])
                        if open > pre_close:
                            t = time.localtime()
                            if t.tm_hour==14 and t.tm_min>45:
                                if high > low:
                                    if (high-price)/(high-low)>0.8:
                                        order_price = price
                                        order_cost  = order_price * item['num']
                                        order = {'ins':'sell_order','portfolio':item['portfolio'],'code':code,'num':item['num'],'price':order_price,'cost':order_cost,'agent':'pingan'}
                                        if deal_order(order):
                                            item_list.pop(i)
                                            break
            else:
                t = time.localtime()
                if t.tm_hour==15 and t.tm_min>5 and t.tm_min<10:
                    r = []
                    for i,item in enumerate(item_list):
                        a_ins_file(str(item))
                        #r.append(str(item))
                    #send_email('show ins ', '\n'.join(r))
                    item_list = []
                    time.sleep(300)


        except Exception as e:
            print("\n error info: ")
            print(e)
            to_log('stare_reach error!')


if __name__ == "__main__":
    stare_reach()
