import time
from datetime import datetime
import numpy as np
import scipy.stats as si

from nature import get_file_lock, release_file_lock

release_file_lock('t3')

get_file_lock('t32')
