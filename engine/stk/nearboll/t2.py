
#from ipdb import set_trace


from datetime import datetime

import numpy as np


from nature import GatewayPingan, NearBollPortfolio, CciEngine

def testClass(engine, className):
    p1 = className(engine)
    print(p1)
    print(type(p1))

def start():
    dss = '../../../data/'
    engine = CciEngine(dss, GatewayPingan(), 'cci')
    #engine.run()

    testClass(engine, NearBollPortfolio)

if __name__ == '__main__':
    start()
