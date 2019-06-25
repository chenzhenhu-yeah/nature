
def a_ins_file():
    c = "{'ins':'up_warn','code':'300408','num':1500,'price':20.20,'name':'三环集团'}" + '\n' + \
     "{'ins':'down_warn','code':'002475','num':1000,'price':13.88,'name':'立讯精密'}"
    ins_dict = {'ins':'a','filename':'ini/ins.txt','content':c}
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
