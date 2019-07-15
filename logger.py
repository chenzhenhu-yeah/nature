from multiprocessing.connection import Listener
from multiprocessing.connection import Client
import json

import time
import os
import configparser
import logging
import logging.config

from nature import SOCKET_LOGGER

dss = '../data/'

address = ('localhost', SOCKET_LOGGER)

def to_log(s):
    again = True
    while again:
        time.sleep(1)
        try :
            with Client(address, authkey=b'secret password') as conn2:
                conn2.send(s)
            again = False
        except Exception as e:
            print('error')
            print(e)

def read_log_today():
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    #today = '2019-03-02'
    logfile= dss + 'log/autotrade.log'
    #df = pd.read_csv(logfile,sep=' ',header=None,encoding='ansi')
    df = pd.read_csv(logfile,sep=' ',header=None,encoding='gbk')
    df = df[df[0]==today]

    r = []
    for i, row in df.iterrows():
        r.append(str(list(row)))

    return r

def log_service():
    print('beging logging... ')

    cfg = configparser.ConfigParser()
    # cfg.read(config_filename)
    # if cfg.getboolean('init','DEBUG'):
    #     logconfigfile =  cfg.get('debug','logconfigfile')
    # else:
    #     print('here')
    #     logconfigfile =  cfg.get('work','logconfigfile')
    #     print(logconfigfile)

    logconfigfile = dss + 'log/logconfigwork.ini'
    logging.config.fileConfig(logconfigfile)

    while True:
        with Listener(address, authkey=b'secret password') as listener:
            with listener.accept() as conn:
                #print('connection accepted from', listener.last_accepted)
                s = conn.recv();
                logger = logging.getLogger('autotrade')
                logger.info(s)

if __name__ == "__main__":
    try:
        log_service()
    except Exception as e:
        print('error')
        print(e)
