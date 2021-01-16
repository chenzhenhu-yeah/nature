import time
from datetime import datetime
import numpy as np
import pandas as pd
import csv


from nature import get_dss

import requests
import pprint

# minx = 'day'
minx = 'min5'
symbol = 'm2105'

response = requests.get('http://127.0.0.1:8000/api_fut_bar/1?minx='+minx+'&symbol='+symbol)
result = response.json()
# print(type(result))
if 'detail' in result:
    print('error')
else:
    # print(result['0'])
    print(len(result))
# pprint.pprint(result)
