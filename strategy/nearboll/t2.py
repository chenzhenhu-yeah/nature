
#from ipdb import set_trace


from datetime import datetime

import numpy as np


from csv import DictReader

with open('setting.csv')as f:
    r = DictReader(f)
    print(r)
    #set_trace()
    print('/n')

    for d in r:
        print(d)
        print('/n')
