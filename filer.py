
from multiprocessing.connection import Listener
from multiprocessing.connection import Client
import pandas as pd
import os
import time
import json
import traceback

from nature import SOCKET_FILER
from nature import to_log

dss = '../data/'
address = ('localhost', SOCKET_FILER)


file_lock = {}

def get_file_lock(fn):
    r = False
    ins_dict = {'ins':'get_file_lock','filename':fn}
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

def release_file_lock(fn):
    r = False
    ins_dict = {'ins':'release_file_lock','filename':fn}
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
    global file_lock

    r = []
    fn = ins['filename']
    if os.path.exists(fn) == False:
        return False

    if ins['ins']=='r':
        with open(fn, 'r', encoding='utf-8') as f:
            line = f.readline()
            while line:
                if line[0] == '{':
                    line_dict = eval(line)
                    r.append(line_dict)
                line = f.readline()

    if ins['ins']=='rc':
        with open(fn, 'r+', encoding='utf-8') as f:
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
        with open(fn, 'a', encoding='utf-8') as f:
            f.write(str(ins['content'])+'\n')

    if ins['ins']=='get_file_lock':
            if fn not in file_lock:
                file_lock[fn] = True
                r = True
            else:
                if file_lock[fn] == True :
                    r = False
                else:
                    file_lock[fn] = True
                    r = True
            # print(file_lock)

    if ins['ins']=='release_file_lock':
            if fn not in file_lock:
                file_lock[fn] = False
                r = False
            else:
                if file_lock[fn] == True:
                    file_lock[fn] = False
                    r = True
                else:
                    r = False
            # print(file_lock)

    return r

def file_service():
    print('beging filer')
    while True:
        try:
            with Listener(address, authkey=b'secret password') as listener:
                with listener.accept() as conn:
                    # print('connection accepted from', listener.last_accepted)
                    ins_dict = conn.recv(); #print(ins_dict)
                    conn.send( deal_file(ins_dict) )
        except Exception as e:
            print('error')
            print(e)

            s = traceback.format_exc()
            to_log(s)

if __name__ == "__main__":
    file_service()
