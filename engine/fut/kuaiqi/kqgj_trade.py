"""
快期交易系统V2
使用前，对软件进行必要设置，以减少弹出对话框。
选项-选项设置-下单-下单板：去掉勾选项目
选项-选项设置-提示及反馈： 去掉勾选项目
"""

from pywinauto import application
import time
import json

from nature import to_log, get_dss

dss = get_dss()
config = open(dss+'csv/config.json')
setting = json.load(config)

kqgj_window_handle = setting['kqgj_window_handle']
kqgj_app_path = setting['kqgj_app_path']
#print(kqgj_window_handle)
#print(kqgj_app_path)

app = application.Application()
app.connect(path = kqgj_app_path)
dlg_spec = app.window(handle = kqgj_window_handle)

def kqgj_buy(code,price,num):
    """买开仓"""
    r = True
    try:
        to_log('in kqgj_buy')
        time.sleep(0.1)

        dlg_spec.Edit1.set_text(code)    #合约
        time.sleep(0.1)

        dlg_spec['卖出'].uncheck()
        time.sleep(0.1)
        dlg_spec['买入'].check()            #买卖
        time.sleep(0.1)

        dlg_spec['平仓'].uncheck()
        time.sleep(0.1)
        dlg_spec['平今'].uncheck()
        time.sleep(0.1)
        dlg_spec['开仓'].check()            #开平
        time.sleep(0.1)

        dlg_spec.Edit2.set_text(num)       #手数
        time.sleep(0.1)
        dlg_spec.Edit3.set_text(price)     #价格
        time.sleep(0.1)

        dlg_spec['下单'].click()
    except:
        r = False
        print('error, 下单异常')

    return r

def kqgj_sell(code,price,num):
    """卖平仓"""
    r = True
    try:
        to_log('in kqgj_sell')
        time.sleep(0.1)

        dlg_spec.Edit1.set_text(code)    #合约
        time.sleep(0.1)

        dlg_spec['买入'].uncheck()
        time.sleep(0.1)
        dlg_spec['卖出'].check()            #买卖
        time.sleep(0.1)

        dlg_spec['开仓'].uncheck()
        time.sleep(0.1)
        dlg_spec['平今'].uncheck()
        time.sleep(0.1)
        dlg_spec['平仓'].check()            #开平
        time.sleep(0.1)

        dlg_spec.Edit2.set_text(num)       #手数
        time.sleep(0.1)
        dlg_spec.Edit3.set_text(price)     #价格
        time.sleep(0.1)

        dlg_spec['下单'].click()
    except:
        r = False
        print('error, 下单异常')

    return r


def kqgj_short(code,price,num):
    """卖开仓"""
    r = True
    try:
        to_log('in kqgj_short')
        time.sleep(0.1)

        dlg_spec.Edit1.set_text(code)    #合约
        time.sleep(0.1)

        dlg_spec['买入'].uncheck()
        time.sleep(0.1)
        dlg_spec['卖出'].check()            #买卖
        time.sleep(0.1)

        dlg_spec['平仓'].uncheck()
        time.sleep(0.1)
        dlg_spec['平今'].uncheck()
        time.sleep(0.1)
        dlg_spec['开仓'].check()            #开平
        time.sleep(0.1)

        dlg_spec.Edit2.set_text(num)       #手数
        time.sleep(0.1)
        dlg_spec.Edit3.set_text(price)     #价格
        time.sleep(0.1)

        dlg_spec['下单'].click()
    except:
        r = False
        print('error, 下单异常')

    return r

def kqgj_cover(code,price,num):
    """买平仓"""
    r = True
    try:
        to_log('in kqgj_cover')
        time.sleep(0.1)

        dlg_spec.Edit1.set_text(code)      #合约
        time.sleep(0.1)

        dlg_spec['卖出'].uncheck()
        time.sleep(0.1)
        dlg_spec['买入'].check()            #买卖
        time.sleep(0.1)

        dlg_spec['开仓'].uncheck()
        time.sleep(0.1)
        dlg_spec['平今'].uncheck()
        time.sleep(0.1)
        dlg_spec['平仓'].check()            #开平
        time.sleep(0.1)

        dlg_spec.Edit2.set_text(num)       #手数
        time.sleep(0.1)
        dlg_spec.Edit3.set_text(price)     #价格
        time.sleep(0.1)

        dlg_spec['下单'].click()
    except:
        r = False
        print('error, 下单异常')

    return r

def zggj_buy(code,price,num):
    #r = False
    #try:
        app = application.Application()
        app.connect(path = r"C:\Program Files (x86)\中国国际期货网上交易终端(标准版)\FastTrader.exe")
        dlg_spec = app.window(handle = zggj_window_handle)
        time.sleep(1)
        print('here')

        # dlg_spec.Edit1.set_text('c1909')
        # dlg_spec.Edit2.set_text('1236')
        # dlg_spec.Edit3.set_text('10')

        #dlg_spec.wxWindowClass1.select('卖出')


        dlg_spec.comboBox1.select(0)
        # t = dlg_spec.Edit1
        # print(dir(t))
        # #print(help(t))
        # print(type(t))

        # #dlg_spec.Edit1.type_keys(code)
        # dlg_spec.Edit1.set_text(code)
        # time.sleep(1)
        # dlg_spec.Edit2.set_text(price)
        # time.sleep(1)
        # dlg_spec.Edit3.set_text(num)
        # time.sleep(2)
        # #dlg_spec.button1.click()
        # dlg_spec['买入确认'].click()
        #
        # time.sleep(3)
        # app['交易确认']['确认'].click()
        #
        # time.sleep(6)
        # app['提示']['确认'].click()
        # r = True
    #except:
        #print('error')

    #return r

if __name__ == "__main__":
    #测试用
    code = 'c1909'
    price = '1235'
    num = '1'

    #kqgj_buy(code,price,num)

    #gtja_sell(code,price,num)
    #pingan_buy(code,price,num)
    #pingan_sell(code,price,num)
    #cf_buy(code,price,num)
    #cf_sell(code,price,num)

    #dlg_spec.print_control_identifiers()
