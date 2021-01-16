import time
from datetime import datetime
import numpy as np
import pandas as pd
import csv


from nature import get_dss


from io import StringIO

from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/api_fut_bar/{item_id}")
def read_api_fut_bar(item_id: int, minx: str = None, symbol: str = None):
    # print(minx, symbol)
    r = {'detail':'error'}
    if minx is None or symbol is None:
        pass
    else:
        try:
            fn = get_dss() +'fut/bar/' + minx + '_' + symbol + '.csv'
            with open(fn, 'r') as csv_file:
                reader = csv.reader(csv_file)
                for i, row in enumerate(reader):
                    r[i] = str(row)
                    # break
            r.pop('detail')
        except:
            pass

    return r
