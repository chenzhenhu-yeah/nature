from multiprocessing.connection import Listener
import time

import os
import configparser
import logging
import logging.config

if __name__ == "__main__":
    print('beging logging')
    cfg = configparser.ConfigParser()
    cfg.read('ini//appconfig.ini')

    if cfg.getboolean('init','DEBUG'):
        logconfigfile =  cfg.get('debug','logconfigfile')
    else:
        logconfigfile =  cfg.get('work','logconfigfile')

    logging.config.fileConfig(logconfigfile)


    address = ('localhost', 9000)     # family is deduced to be 'AF_INET'
    while True:
        with Listener(address, authkey=b'secret password') as listener:
            with listener.accept() as conn:
                print('connection accepted from', listener.last_accepted)

                s = conn.recv();
                logger = logging.getLogger('autotrade')
                logger.info(s)
