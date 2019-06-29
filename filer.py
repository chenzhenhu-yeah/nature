
from multiprocessing.connection import Listener
from multiprocessing.connection import Client
import pandas as pd
import time
import json

from nature import SOCKET_FILER

dss = '../data/'
address = ('localhost', SOCKET_FILER)

#{'ins':'r','filename':'ins.txt'}
#{'ins':'rc','filename':'ins.txt'}
def rc_file(filename):
    r = []
    ins_dict = {'ins':'rc','filename':filename}
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
def a_file(filename, content):
    r = []
    ins_dict = {'ins':'a','filename':filename,'content':content}
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

#{'ins':'r','filename':'ins.txt'}
#{'ins':'rc','filename':'ins.txt'}
#{'ins':'a','filename':'ins.txt','content':'aaaaaa'}
def deal_file(ins):
    r = []
    filename = dss + ins['filename']

    if ins['ins']=='r':
        with open(filename, 'r', encoding='utf-8') as f:
            line = f.readline()
            while line:
                if line[0] == '{':
                    line_dict = eval(line)
                    r.append(line_dict)
                line = f.readline()

    if ins['ins']=='rc':
        with open(filename, 'r+', encoding='utf-8') as f:
            line = f.readline()
            while line:
                if line[0] == '{':
                    line_dict = eval(line)
                    r.append(line_dict)
                line = f.readline()
            #清空文件内容
            f.seek(0)
            f.truncate()

    if ins['ins']=='a':
        with open(filename, 'a', encoding='utf-8') as f:
            f.write(str(ins['content'])+'\n')

    return r

if __name__ == "__main__":
    print('beging filer')
    while True:
        with Listener(address, authkey=b'secret password') as listener:
            with listener.accept() as conn:
                # print('connection accepted from', listener.last_accepted)
                ins_dict = conn.recv(); #print(ins_dict)
                conn.send( deal_file(ins_dict) )
