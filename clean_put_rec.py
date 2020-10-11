
import os
import time
from datetime import datetime, timedelta

from nature import get_dss


print('in clean_put_rec')
strdirname = get_dss() + 'fut/put/rec/'
listfile = os.listdir(strdirname)

pre = datetime.now() - timedelta(days = 90)
pre = pre.strftime('%Y-%m-%d')
print(pre)

for strfilename in listfile:
    f_src = os.path.join(strdirname, strfilename)
    if os.path.isfile(f_src):
        fileinfo = os.stat(f_src)
        dt_alter = time.strftime("%Y-%m-%d",time.localtime(fileinfo.st_mtime))
        # print(strfilename, dt_alter)

        if dt_alter < pre:
            print(strfilename, dt_alter)
            os.remove(f_src)

print('task completed')
while 1:
    time.sleep(1)
