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
config = open(dss+'fut/cfg/config.json')
setting = json.load(config)

# 海通
# app = application.Application()
# app.connect(path = setting['kq_ht_app_path'])
# dlg_spec = app.window( handle = int(setting['kq_ht_window_handle'], 16) )

# simnow
app = application.Application()
app.connect(path = setting['kq_simnow_app_path'])
dlg_spec = app.window( handle = int(setting['kq_simnow_window_handle'], 16) )

# dlg_spec = app.window(handle = 0x408E8)
# dlg_spec.print_control_identifiers()
# dlg_spec.print_ctrl_ids()

def kq_buy(code,price,num):
    """买开仓"""
    r = True
    try:
        to_log('in kq_buy')
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

def kq_sell(code,price,num):
    """卖平仓"""
    r = True
    try:
        to_log('in kq_sell')
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


def kq_short(code,price,num):
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

def kq_cover(code,price,num):
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
    code = 'c2001'
    price = '1851'
    num = '1'

    kq_buy(code,price,num)
