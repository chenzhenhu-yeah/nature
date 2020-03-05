import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar, Grid, Scatter

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib

from nature import get_dss


def gen_line(df1, symbol, price_min, price_max):
    #df1['datetime'] = df1['date'] + ' ' + df1['time']
    df1['datetime'] = df1['date']
    dt_list =  list(df1['datetime'])
    close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list = np.array(close_list)

    line1 = Line(init_opts=opts.InitOpts(height='700px',width='1300px'))
    line1.set_global_opts( yaxis_opts=opts.AxisOpts(min_=price_min,
                                                    #min_=999,
                                                    max_=price_max,
                                                    splitarea_opts=opts.SplitAreaOpts(is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)),
                                                    ),
                           datazoom_opts=[opts.DataZoomOpts(is_show=True,type_="slider",range_start=0,range_end=100,),],
                         )
    line1.add_xaxis( xaxis_data=dt_list )
    line1.add_yaxis( symbol, y_axis=close_list, )
    line1.set_series_opts(label_opts=opts.LabelOpts(is_show=False))

    return line1


def gen_ma(df1,n):
    df1['datetime'] = df1['date']
    dt_list =  list(df1['datetime'])
    close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
    close_list = np.array(close_list)
    ma_n = talib.SMA(close_list, n)
    # print(ma_120)
    # print( type(ma_120) )

    line = Line()
    line.add_xaxis( xaxis_data=dt_list )
    line.add_yaxis( 'MA'+str(n), y_axis=ma_n,label_opts=opts.LabelOpts(is_show=False),)

    return line

def ic(symbol1, symbol2, start_dt='2020-01-01'):
    fn = get_dss() +'fut/bar/day_' + symbol1 + '.csv'
    df1 = pd.read_csv(fn)
    df1 = df1[df1.date >= start_dt]
    df1 = df1.reset_index()
    # print(df1.head(3))

    fn = get_dss() +'fut/bar/day_' + symbol2 + '.csv'
    df2 = pd.read_csv(fn)
    df2 = df2[df2.date >= start_dt]
    df2 = df2.reset_index()
    # print(df2.head(3))

    df1['close'] = df1.close - df2.close
    # print(df1.close)
    price_min = df1['close'].min() - 100
    price_max =  df1['close'].max() + 100
    line1 = gen_line(df1, symbol1+'-'+symbol2, price_min, price_max)
    line_ma  = gen_ma(df1, 10)

    fn = 'static/ic_' + symbol1 + '_'+ symbol2+ '.html'
    line1.overlap(line_ma).render(fn)


def value(df):

    price_min = float(df['close'].min()) - 3
    price_max = float(df['close'].max()) + 3

    df0 = df.loc[:,['date']]
    df0 = df0.drop_duplicates()
    df0['close'] = np.nan
    line = gen_line(df0, 'null', price_min, price_max)

    symbol_set = set(df.symbol)
    for symbol in symbol_set:
        df1 = df[df.symbol == symbol]
        line1 = gen_line(df1, symbol, price_min, price_max)
        line = line.overlap(line1)

    fn = 'static/value.html'
    line.render(fn)

#################################################################################
class Bar_Ma():
    def gen_kline(self, df1, symbol):
        dt_list =  list(df1['datetime'])
        #print(dt_list)
        k_plot_value = df1.apply(lambda record: [record['open'], record['close'], record['low'], record['high']], axis=1).tolist()
        #print(k_plot_value)

        kline = Kline(init_opts=opts.InitOpts(width='1500px',height="700px",))
        kline.add_xaxis( list(df1['datetime']) )
        kline.add_yaxis(symbol, k_plot_value)
        kline.set_global_opts(title_opts=opts.TitleOpts(title='Min30'),
                              datazoom_opts=[opts.DataZoomOpts(range_start=0,range_end=100)],)
                              #xaxis_opts=opts.AxisOpts(type_='time'))
        return kline

    def gen_ma(self, df1,n):
        close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
        close_list = np.array(close_list)
        ma_n = talib.SMA(close_list, n)
        # print(ma_120)
        # print( type(ma_120) )

        line = Line()
        line.add_xaxis( xaxis_data=list(df1['datetime']) )
        line.add_yaxis( str(n), y_axis=ma_n,label_opts=opts.LabelOpts(is_show=False),)

        return line

    def draw(self, symbol, fn_render):
        fn = get_dss() +'fut/bar/min30_' + symbol + '.csv'
        df1 = pd.read_csv(fn)
        df1['datetime'] = df1['date'] + ' ' + df1['time']
        print(df1.head())

        kline = self.gen_kline(df1,symbol)
        line1  = self.gen_ma(df1, 90)
        line2  = self.gen_ma(df1, 120)
        line3  = self.gen_ma(df1, 240)

        d = kline.overlap(line1)
        d = d.overlap(line2)
        d = d.overlap(line3)

        kline.render(fn_render)

def bar_ma_m(symbol):
    fn_render = 'static/bar_ma_m.html'
    bar_ma = Bar_Ma()
    bar_ma.draw(symbol, fn_render)

def bar_ma_CF(symbol):
    fn_render = 'static/bar_ma_CF.html'
    bar_ma = Bar_Ma()
    bar_ma.draw(symbol, fn_render)

def bar_ma_ru(symbol):
    fn_render = 'static/bar_ma_ru.html'
    bar_ma = Bar_Ma()
    bar_ma.draw(symbol, fn_render)


#################################################################################

class Bar_Aberration():
    def gen_kline(self, df1,symbol):
        df1['datetime'] = df1['date']
        dt_list =  list(df1['datetime'])
        k_plot_value = df1.apply(lambda record: [record['open'], record['close'], record['low'], record['high']], axis=1).tolist()
        #print(k_plot_value)

        kline = Kline(init_opts=opts.InitOpts(width='1500px',height="700px",))
        kline.add_xaxis( dt_list )
        kline.add_yaxis( symbol, k_plot_value )
        kline.set_global_opts(title_opts=opts.TitleOpts(title='日线'),
                              datazoom_opts=[opts.DataZoomOpts(range_start=0,range_end=100)],)
                              #xaxis_opts=opts.AxisOpts(type_='time'))
        return kline
    #----------------------------------------------------------------------
    def sma(self, close_list, n, array=False):
        """简单均线"""
        result = talib.SMA(close_list, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def std_here(self, close_list, n, array=False):
        """标准差"""
        result = talib.STDDEV(close_list, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def boll(self, close_list, n, dev, array=True):
        """布林通道"""
        mid = self.sma(close_list, n, array)
        std = self.std_here(close_list, n, array)

        up = mid + std * dev
        down = mid - std * dev

        return up, down

    #----------------------------------------------------------------------
    def gen_boll(self, df1, n, dev):
        close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
        close_list = np.array(close_list)
        dt_list = df1['datetime'].tolist()

        up, down = self.boll(close_list, n, dev)

        line = Line()
        line.add_xaxis( xaxis_data=dt_list )
        line.add_yaxis( 'boll_up',
                        y_axis=up,
                        label_opts=opts.LabelOpts(is_show=False),
                      )
        line.add_yaxis( 'boll_down',
                        y_axis=down,
                        label_opts=opts.LabelOpts(is_show=False),
                      )
        line.set_global_opts(
                            xaxis_opts=opts.AxisOpts(is_show=False),
                            legend_opts=opts.LegendOpts(is_show=True,pos_right="40%")
                            )

        return line

    def draw(self, symbol, fn_render):
        fn = get_dss() +'fut/bar/day_' + symbol + '.csv'
        df1 = pd.read_csv(fn)
        # print(df1.head())
        price_min = int( df1.close.min() * 0.99 )
        price_max = df1.close.max()


        kline = self.gen_kline(df1,symbol)
        line_boll = self.gen_boll(df1, 10, 2)
        kline = kline.overlap(line_boll)
        kline.render(fn_render)

def bar_aberration_CF(symbol):
    fn_render = 'static/bar_aberration_CF.html'
    bar_ma = Bar_Aberration()
    bar_ma.draw(symbol, fn_render)


#################################################################################
class Bar_Dalicta():
    def gen_kline(self, df1, symbol):
        df1['datetime'] = df1['date']
        dt_list =  list(df1['datetime'])
        k_plot_value = df1.apply(lambda record: [record['open'], record['close'], record['low'], record['high']], axis=1).tolist()
        #print(k_plot_value)

        kline = Kline(init_opts=opts.InitOpts(width='1500px',height="700px",))
        kline.add_xaxis( dt_list )
        kline.add_yaxis(symbol, k_plot_value)
        kline.set_global_opts(title_opts=opts.TitleOpts(title='日线'),
                              datazoom_opts=[opts.DataZoomOpts(range_start=0,range_end=100)],)
                              #xaxis_opts=opts.AxisOpts(type_='time'))
        return kline

    def gen_ma(self, df1,n):
        close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
        close_list = np.array(close_list)
        ma_n = talib.SMA(close_list, n)
        # print(ma_120)
        # print( type(ma_120) )

        line = Line()
        line.add_xaxis( xaxis_data=list(df1['datetime']) )
        line.add_yaxis( str(n), y_axis=ma_n,label_opts=opts.LabelOpts(is_show=False),)

        return line

    def draw(self, symbol, fn_render):
        fn = get_dss() +'fut/bar/day_' + symbol + '.csv'
        df1 = pd.read_csv(fn)
        df1['datetime'] = df1['date'] + ' ' + df1['time']
        print(df1.head())

        kline = self.gen_kline(df1,symbol)
        line1  = self.gen_ma(df1, 10)
        line2  = self.gen_ma(df1, 30)
        line3  = self.gen_ma(df1, 60)

        d = kline.overlap(line1)
        d = d.overlap(line2)
        d = d.overlap(line3)

        kline.render(fn_render)

def bar_dalicta_m(symbol):
    fn_render = 'static/bar_dalicta_m.html'
    bar_ma = Bar_Dalicta()
    bar_ma.draw(symbol, fn_render)

################################################################################

class Bar_Cci():
    def gen_kline(self, df1):
        df1['datetime'] = df1['date']
        dt_list =  list(df1['datetime'])
        k_plot_value = df1.apply(lambda record: [record['open'], record['close'], record['low'], record['high']], axis=1).tolist()
        #print(k_plot_value)

        kline = Kline(init_opts=opts.InitOpts(width='1500px',height="700px",))
        kline.add_xaxis( dt_list )
        kline.add_yaxis( 'bar', k_plot_value )
        kline.set_global_opts(title_opts=opts.TitleOpts(title='日线'),
                              datazoom_opts=[opts.DataZoomOpts(is_show=True,type_="slider",xaxis_index=[0,1],range_start=0,range_end=100,),
                                             opts.DataZoomOpts(is_show=False,type_="inside",xaxis_index=[0,1],range_start=0,range_end=100,), ],
                              tooltip_opts=opts.TooltipOpts( axis_pointer_type="cross" ),
                              axispointer_opts=opts.AxisPointerOpts( is_show=True, link=[{"xAxisIndex": "all"}], ),
                              )

        return kline


    def gen_cci(self, df1, n):
        high_list = df1.apply(lambda record: float(record['high']), axis=1).tolist()
        high_list = np.array(high_list)

        low_list = df1.apply(lambda record: float(record['low']), axis=1).tolist()
        low_list = np.array(low_list)

        close_list = df1.apply(lambda record: float(record['close']), axis=1).tolist()
        close_list = np.array(close_list)

        rsi_n = talib.CCI(high_list, low_list, close_list, n)

        line = Line()
        line.add_xaxis( xaxis_data=list(df1['datetime']) )
        line.add_yaxis( 'cci_'+str(n),
                        y_axis=rsi_n,
                        xaxis_index=1,
                        yaxis_index=1,
                        label_opts=opts.LabelOpts(is_show=False),
                      )

        line.set_global_opts(yaxis_opts=opts.AxisOpts(min_=-150,max_=150),
                             # xaxis_opts=opts.AxisOpts(is_show=False),
                             xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(is_show=False),),
                             legend_opts=opts.LegendOpts(is_show=True,pos_right="38%"),
                            )
        line.set_series_opts(
                            label_opts=opts.LabelOpts(is_show=False),
                            )

        return line


    def draw(self, symbol, fn_render):
        fn = get_dss() +'fut/bar/day_' + symbol + '.csv'
        df1 = pd.read_csv(fn)
        # print(df1.head())
        price_min = int( df1.close.min() * 0.99 )
        price_max = df1.close.max()

        kline = self.gen_kline(df1)
        line_cci = self.gen_cci(df1, 100)

        grid_chart = Grid(
            init_opts=opts.InitOpts(
                width="1390px",
                height="700px",
                animation_opts=opts.AnimationOpts(animation=False),
            )
        )
        grid_chart.add(
            kline,
            grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", height="60%"),
        )
        grid_chart.add(
            line_cci,
            grid_opts=opts.GridOpts(
                pos_left="10%", pos_right="8%", pos_top="75%", height="17%" ),
        )

        grid_chart.render(fn_render)

def bar_cci_CF(symbol):
    fn_render = 'static/bar_cci_CF.html'
    bar_ma = Bar_Cci()
    bar_ma.draw(symbol, fn_render)


################################################################################
class Resid():
    def gen_line_one(self, df1, symbol, s1):

        df1['datetime'] = df1['date'] + ' ' + df1['time']
        dt_list1 =  list(df1['datetime'])
        # print( len(dt_list1) )
        # dt_list1 = [s[5:10] for s in dt_list1]
        close_list1 = df1.apply(lambda record: float(record['close']), axis=1).tolist()
        close_list1 = np.array(close_list1)


        kline = Line(init_opts=opts.InitOpts(width='1500px'))
        kline.add_xaxis( dt_list1 )
        kline.add_yaxis(symbol, close_list1,label_opts=opts.LabelOpts(is_show=False))
        kline.set_global_opts(title_opts=opts.TitleOpts(title=s1),
                              #datazoom_opts=[opts.DataZoomOpts()],
                              #xaxis_opts=opts.AxisOpts(type_='time'))
                              yaxis_opts=opts.AxisOpts(is_scale=True,splitarea_opts=opts.SplitAreaOpts(is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)) ),
                              datazoom_opts=[opts.DataZoomOpts(is_show=True,type_="slider",xaxis_index=[0,1],range_start=0,range_end=100,),
                                             opts.DataZoomOpts(is_show=False,type_="inside",xaxis_index=[0,1],range_start=0,range_end=100,), ],
                              tooltip_opts=opts.TooltipOpts( trigger="axis",axis_pointer_type="cross" ),
                              #axispointer_opts=opts.AxisPointerOpts(is_show=True, link=[{"xAxisIndex": "all"}], ),
                             )

        return kline

    def gen_line_two(self, df1):

        df1['datetime'] = df1['date'] + ' ' + df1['time']
        dt_list1 =  list(df1['datetime'])
        # print( len(dt_list1) )
        # dt_list1 = [s[5:10] for s in dt_list1]
        close_list1 = df1.apply(lambda record: float(record['close']), axis=1).tolist()
        close_list1 = np.array(close_list1)


        kline = Line()
        kline.add_xaxis( dt_list1 )
        kline.add_yaxis('resid', close_list1, xaxis_index=1,yaxis_index=1,label_opts=opts.LabelOpts(is_show=False))
        kline.set_global_opts(yaxis_opts=opts.AxisOpts(is_scale=True,splitarea_opts=opts.SplitAreaOpts(is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)) ),
                              legend_opts=opts.LegendOpts(is_show=True,pos_right="40%")
                              #xaxis_opts=opts.AxisOpts(type_='time'))
                             )

        return kline

    def draw(self, fn, symbol, fn_render, s1):

        df = pd.read_csv(fn)
        df1 = df.loc[:,['date','time','close']]

        n = 30
        close_list = df.apply(lambda record: float(record['close']), axis=1).tolist()
        close_list = np.array(close_list)
        ma_arr = talib.SMA(close_list, n)
        df['ma'] = ma_arr
        df['close'] = df['close'] - df['ma']
        df2 = df.loc[:,['date','time','close']]

        line1 = self.gen_line_one(df1, symbol, s1)
        line2 = self.gen_line_two(df2)

        grid_chart = Grid(
            init_opts=opts.InitOpts(
                width="1300px",
                height="700px",
                #animation_opts=opts.AnimationOpts(animation=False),
            )
        )
        grid_chart.add(
            line1,
            grid_opts=opts.GridOpts(pos_left="5%", pos_right="3%", height="39%"),
        )
        grid_chart.add(
            line2,
            grid_opts=opts.GridOpts(
                pos_left="5%", pos_right="3%", pos_top="53%", height="39%" ),
        )

        grid_chart.render(fn_render)


def resid_day_CF(symbol):
    fn = get_dss() +'fut/bar/day_' + symbol + '.csv'
    fn_render = 'static/resid_day_CF.html'
    resid = Resid()
    resid.draw(fn, symbol, fn_render, 'day')

def resid_day_m(symbol):
    fn = get_dss() +'fut/bar/day_' + symbol + '.csv'
    fn_render = 'static/resid_day_m.html'
    resid = Resid()
    resid.draw(fn, symbol, fn_render, 'day')

def resid_day_ru(symbol):
    fn = get_dss() +'fut/bar/day_' + symbol + '.csv'
    fn_render = 'static/resid_day_ru.html'
    resid = Resid()
    resid.draw(fn, symbol, fn_render, 'day')


def resid_min30_CF(symbol):
    fn = get_dss() +'fut/bar/min30_' + symbol + '.csv'
    fn_render = 'static/resid_min30_CF.html'
    resid = Resid()
    resid.draw(fn, symbol, fn_render, 'min30')

def resid_min30_m(symbol):
    fn = get_dss() +'fut/bar/min30_' + symbol + '.csv'
    fn_render = 'static/resid_min30_m.html'
    resid = Resid()
    resid.draw(fn, symbol, fn_render, 'min30')

def resid_min30_ru(symbol):
    fn = get_dss() +'fut/bar/min30_' + symbol + '.csv'
    fn_render = 'static/resid_min30_ru.html'
    resid = Resid()
    resid.draw(fn, symbol, fn_render, 'min30')

if __name__ == '__main__':
    pass
