from multiprocessing.connection import Listener
from multiprocessing.connection import Client

import time
import os
import configparser
import logging
import logging.config

dss = '../data/'

def to_log(s):
    address = ('localhost', 9000)
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

if __name__ == "__main__":
    print('beging logging... ')

    config_filename = dss + 'log/appconfig.ini'
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


    address = ('localhost', 9000)     # family is deduced to be 'AF_INET'
    while True:
        with Listener(address, authkey=b'secret password') as listener:
            with listener.accept() as conn:
                #print('connection accepted from', listener.last_accepted)
                s = conn.recv();
                logger = logging.getLogger('autotrade')
                logger.info(s)
