import time
from datetime import datetime
from multiprocessing.connection import Client


def avoid_idle():
    print('avoid_idle begin ...')
    address = ('localhost', 9002)
    time.sleep(60)
    again = True
    while again:
        try :
            with Client(address, authkey=b'secret password') as conn:
                #print('here  2')
                ins_dict = {'ins':'avoid_idle','agent':'pingan'}
                conn.send(ins_dict)

                # time.sleep(9)
                # ins_dict = {'ins':'avoid_idle','agent':'cf'}
                # conn.send(ins_dict)
        except:
            pass

        time.sleep(900)
        #print('here  1')

if __name__ == '__main__':
    avoid_idle()
