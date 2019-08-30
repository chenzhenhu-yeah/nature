
import pandas as pd
from datetime import datetime, timedelta


class Point():
    """连接点"""
    #----------------------------------------------------------------------
    def __init__(self, x, y):
        """Constructor"""
        self.x = x
        self.y = y

class Bi():
    """笔"""
    #----------------------------------------------------------------------
    def __init__(self, start_point, end_point):
        """Constructor"""
        self.start_point = start_point
        self.end_point = end_point
        self.up = True if end_point.y>start_point.y else False

class Duan():
    """线段"""
    #----------------------------------------------------------------------
    def __init__(self, start_point, end_point):
        """Constructor"""
        self.start_point = start_point
        self.end_point = end_point
        self.up = True if end_point.y>start_point.y else False

def get_std_tz(bi_list):
    """获得标准特征序列"""

    # 取当前笔的特征序列
    tz = [bi_list[n] for n in range(1,len(bi_list),2)]
    #print(tz)

    # 对序列进行半包含处理（左包含右）
    tz_std = [tz[0]]
    for bi in tz:
        last_bi = tz_std[-1]
        # 若特征序列为向下笔
        if last_bi.up == False:
            if last_bi.start_point.y >= bi.start_point.y and last_bi.end_point.y <= bi.end_point.y:
                last_bi.end_point.y = bi.end_point.y
            else:
                tz_std.append(bi)

        # 若特征序列为向上笔
        if last_bi.up == True:
            if last_bi.start_point.y <= bi.start_point.y and last_bi.end_point.y >= bi.end_point.y:
                last_bi.end_point.y = bi.end_point.y
            else:
                tz_std.append(bi)

    return tz_std

def is_duan(bi_list):
    """找到序列中的第一段"""
    try:
        # 取当前笔的特征序列
        tz = [bi_list[n] for n in range(1,len(bi_list),2)]
        print(tz)

        # 遍历特征序列
        # 向上笔，
        if bi_list[0].up == True:
            bi1 = tz[0]
            bi2 = tz[1]            # 前2笔不做包含处理
            bi3 = None

            for i in range(2, len(tz)):
                bi3 = tz[i]

                # 特征序列为向下笔，做包含处理
                if bi2.start_point.y >= bi3.start_point.y and bi2.end_point.y <= bi3.end_point.y:
                    bi2.end_point.y = bi3.end_point.y
                    continue

                # 特征序列是否出现准顶分型
                if bi2.start_point.y >= bi1.start_point.y and bi2.start_point.y >= bi3.start_point.y and bi2.end_point.y > bi3.end_point.y:
                    #print('here')
                    n = i*2-1
                    # 若没有缺口， 成功
                    if bi2.end_point.y <= bi1.start_point.y:
                        return True, n
                    # 若有缺口，以bi2作为向下笔，查找底分型
                    else:
                        new_list = bi_list[n:]
                        assert new_list[0].up == False
                        new_tz_std = get_std_tz(new_list)
                        for j in range(2,len(new_tz_std)):
                            new_bi1 = new_tz_std[j-2]
                            new_bi2 = new_tz_std[j-1]
                            new_bi3 = new_tz_std[j]
                            if new_bi2.start_point.y <= new_bi1.start_point.y and new_bi2.start_point.y < new_bi3.start_point.y and new_bi2.end_point.y < new_bi3.end_point.y:
                                if new_bi2.end_point.y < new_list[0].start_point.y:
                                    return True, n
                bi1 = bi2
                bi2 = bi3

        # 向下笔，
        else:
            bi1 = tz[0]
            bi2 = tz[1]            # 前2笔不做包含处理
            bi3 = None

            for i in range(2, len(tz)):
                bi3 = tz[i]

                # 特征序列为向上笔，做包含处理
                if bi2.start_point.y <= bi3.start_point.y and bi2.end_point.y >= bi3.end_point.y:
                    bi2.end_point.y = bi3.end_point.y
                    continue

                # 特征序列是否出现准底分型
                if bi2.start_point.y <= bi1.start_point.y and bi2.start_point.y <= bi3.start_point.y and bi2.end_point.y < bi3.end_point.y:
                    n = i*2-1
                    # 若没有缺口， 成功
                    if bi2.end_point.y >= bi1.start_point.y:
                        return True, n
                    # 若有缺口，以bi2作为向上笔，查找顶分型
                    else:
                        new_list = bi_list[n:]
                        assert new_list[0].up == True
                        new_tz_std = get_std_tz(new_list)
                        for j in range(2,len(new_tz_std)-1):
                            new_bi1 = new_tz_std[j-2]
                            new_bi2 = new_tz_std[j-1]
                            new_bi3 = new_tz_std[j]
                            if new_bi2.start_point.y >= new_bi1.start_point.y and new_bi2.start_point.y > new_bi3.start_point.y and new_bi2.end_point.y > new_bi3.end_point.y:
                                if new_bi2.end_point.y > new_list[0].start_point.y:
                                    return True, n
                bi1 = bi2
                bi2 = bi3

    except:
        print('error')

    return False, 0

# 将点转成笔
df_p = pd.read_csv('bi.csv')
bi_list = []
for i, row in df_p.iterrows():
    s_p = Point(row.s_x, row.s_y)
    e_p = Point(row.e_x, row.e_y)
    bi_list.append( Bi(s_p, e_p) )
#print(bi_list)

n = 0
c= True
d = []
while c:
    c, m = is_duan(bi_list[n:])
    print(m)

    # 保存当前线段
    if c == True:
        d.append( Duan(bi_list[n].start_point, bi_list[n+m].start_point) )
    n += m

duan_point_array = []
for item in d:
    duan_point_array.append( [item.start_point.x,item.start_point.y,item.end_point.x,item.end_point.y] )

df = pd.DataFrame(duan_point_array, columns=['s_x','s_y','e_x','e_y'])
df.to_csv('duan.csv', index=False)
