

from pywinauto import application
import time

pingan_window_handle = 0x2C0616
gtja_window_handle = 0xA085C
cf_window_handle = 0x20C60

def pingan_avoid_idle():
    r = False
    try:
        app = application.Application()
        app.connect(path = r"C:\tc_pazq\tc.exe")
        dlg = app.window(handle = pingan_window_handle)
        time.sleep(1)
        dlg['买入确认'].click()
        time.sleep(6)
        app['提示']['确认'].click()
        r = True

    except Exception as e:
        print('error')
        print(e)

    return r

def pingan_buy(code,price,num):
    r = False
    try:
        app = application.Application()
        app.connect(path = r"C:\tc_pazq\tc.exe")
        dlg_spec = app.window(handle = pingan_window_handle)
        time.sleep(1)

        #dlg_spec.Edit1.set_text('')
        #dlg_spec.Edit1.type_keys(code)
        dlg_spec.Edit1.set_text(code)
        time.sleep(1)
        dlg_spec.Edit2.set_text(price)
        time.sleep(1)
        dlg_spec.Edit3.set_text(num)
        time.sleep(2)
        #dlg_spec.button1.click()
        dlg_spec['买入确认'].click()

        time.sleep(3)
        app['交易确认']['确认'].click()

        time.sleep(6)
        app['提示']['确认'].click()
        r = True
    except:
        pass

    return r

def pingan_sell(code,price,num):
    r = False
    try:
        app = application.Application()
        app.connect(path = r"C:\tc_pazq\tc.exe")
        dlg_spec = app.window(handle = pingan_window_handle)
        time.sleep(1)

        #dlg_spec.Edit4.set_text('')
        #dlg_spec.Edit4.type_keys(code)
        dlg_spec.Edit4.set_text(code)
        time.sleep(1)
        dlg_spec.Edit5.set_text(price)
        time.sleep(1)
        dlg_spec.Edit6.set_text(num)
        time.sleep(2)
        dlg_spec.button2.click()

        time.sleep(3)
        app['交易确认']['确认'].click()

        time.sleep(6)
        app['提示']['确认'].click()
        r = True
    except:
        pass

    return r


def gtja_buy(code,price,num):
    r = False
    try:
        app = application.Application()
        app.connect(path = r"C:\RichEZ\RichET\bin\RichEZ.exe")
        dlg_spec = app.window(handle = gtja_window_handle)
        time.sleep(1)

        dlg_spec.Edit1.set_text('')
        dlg_spec.Edit1.type_keys(code)  #买入股票代码
        dlg_spec.Edit8.set_text(price)    #买入价格
        dlg_spec.Edit10.set_text(num)   #买入数量
        time.sleep(3)
        dlg_spec['买入（B）'].click()

        time.sleep(3)
        app['委托确认']['确定'].click()

        time.sleep(3)
        app['富易']['确定'].click()
        r = True
    except:
        pass

    return r


def gtja_sell(code,price,num):
    r = False
    try:
        app = application.Application()
        app.connect(path = r"C:\RichEZ\RichET\bin\RichEZ.exe")
        dlg_spec = app.window(handle = gtja_window_handle)
        time.sleep(1)

        dlg_spec.Edit2.set_text('')
        dlg_spec.Edit2.type_keys(code)    #卖出股票代码
        dlg_spec.Edit4.set_text(price)   #卖出价格
        time.sleep(1)
        dlg_spec.Edit6.set_text(num)     #卖出数量

        time.sleep(3)
        dlg_spec['卖出（S）'].click()

        time.sleep(3)
        app['委托确认']['确定'].click()

        time.sleep(3)
        app['富易']['确定'].click()
        r = True
    except:
        pass

    return r

def cf_avoid_idle():
    r = False
    try:
        app = application.Application()
        app.connect(path = r"C:\财富证券独立委托（聚财版）\xiadan.exe")
        dlg = app.window(handle = cf_window_handle)
        time.sleep(1)
        dlg['买入[B]'].click()
        time.sleep(3)
        dlg_spec = app.top_window()
        dlg_spec['确定'].click()
        r = True
    except:
        pass

    return r

def cf_buy(code,price,num):
    r = False
    try:
        app = application.Application()
        app.connect(path = r"C:\财富证券独立委托（聚财版）\xiadan.exe")

        dlg_spec = app.window(handle = cf_window_handle)
        time.sleep(1)

        dlg_spec.Edit1.set_text('')
        dlg_spec.Edit1.type_keys(code)  #买入股票代码
        time.sleep(3)
        dlg_spec.Edit2.set_text(price)    #买入价格
        time.sleep(1)
        dlg_spec.Edit3.set_text(num)   #买入数量
        time.sleep(1)
        dlg_spec['买入[B]'].click()

        time.sleep(3)
        #app['委托确认']['是(&Y)'].click()
        dlg_spec = app.top_window()
        dlg_spec['是(Y)'].click()

        time.sleep(3)
        dlg_spec = app.top_window()
        dlg_spec['确定'].click()
        r = True
    except:
        pass

    return r

def cf_sell(code,price,num):
    r = False
    try:
        app = application.Application()
        app.connect(path = r"C:\财富证券独立委托（聚财版）\xiadan.exe")

        dlg_spec = app.window(handle = cf_window_handle)
        time.sleep(1)

        dlg_spec.Edit4.set_text('')
        dlg_spec.Edit4.type_keys(code)  #买入股票代码
        time.sleep(3)
        dlg_spec.Edit5.set_text(price)    #买入价格
        time.sleep(1)
        dlg_spec.Edit6.set_text(num)   #买入数量
        time.sleep(1)
        dlg_spec['卖出[S]'].click()

        time.sleep(3)
        #app['委托确认']['是(&Y)'].click()
        dlg_spec = app.top_window()
        dlg_spec['是(Y)'].click()

        time.sleep(3)
        dlg_spec = app.top_window()
        dlg_spec['确定'].click()
        r = True
    except:
        pass

    return r

if __name__ == "__main__":
    #测试用
    code = '300408'
    price = '15.99'
    num = '100'

    #gtja_buy(code,price,num)
    gtja_sell(code,price,num)
    #pingan_buy(code,price,num)
    #pingan_sell(code,price,num)
    #cf_buy(code,price,num)
    #cf_sell(code,price,num)

    #dlg_spec.print_control_identifiers()
