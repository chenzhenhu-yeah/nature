import pyecharts.options as opts
from pyecharts.charts import Line, Kline, Bar, Grid

import pandas as pd
from datetime import datetime, timedelta

def gen_kline(df1):

    dt_list =  list(df1['datetime'])
    kline_data = df1.apply(lambda record: [record['open'], record['close'], record['low'], record['high']], axis=1).tolist()

    kline = Kline(init_opts=opts.InitOpts(width='1200px'))
    kline.add_xaxis( list(df1['datetime']) )
    kline.add_yaxis('日K', kline_data)

    kline.set_global_opts(title_opts=opts.TitleOpts(title='Kline-基本示例'),
                          datazoom_opts=[opts.DataZoomOpts(is_show=True,type_="slider",xaxis_index=[0,1,2],range_start=98,range_end=100,),
                                         opts.DataZoomOpts(is_show=False,type_="inside",xaxis_index=[0,1,2],range_start=98,range_end=100,), ],
                          legend_opts=opts.LegendOpts(is_show=True),
                          yaxis_opts=opts.AxisOpts(is_scale=True,splitarea_opts=opts.SplitAreaOpts(is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)) ),
                          tooltip_opts=opts.TooltipOpts(
                                                        trigger="axis",
                                                        axis_pointer_type="cross",
                                                        background_color="rgba(245, 245, 245, 0.8)",
                                                        border_width=1,
                                                        border_color="#ccc",
                                                        textstyle_opts=opts.TextStyleOpts(color="#000"),
                                                        ),
                          # visualmap_opts=opts.VisualMapOpts(
                          #                                   is_show=False,
                          #                                   dimension=2,
                          #                                   series_index=5,
                          #                                   is_piecewise=True,
                          #                                   pieces=[
                          #                                       {"value": 1, "color": "#ec0000"},
                          #                                       {"value": -1, "color": "#00da3c"}, ],
                          #                                  ),
                          axispointer_opts=opts.AxisPointerOpts(
                                                                is_show=True,
                                                                link=[{"xAxisIndex": "all"}],
                                                                label=opts.LabelOpts(background_color="#777"),
                                                               ),
                          # brush_opts=opts.BrushOpts(
                          #                             x_axis_index="all",
                          #                             brush_link="all",
                          #                             out_of_brush={"colorAlpha": 0.1},
                          #                             brush_type="lineX",
                          #                          ),
                      )

    return kline

def gen_rsi(df1):

    bar_data = df1.apply(lambda record: record['close'], axis=1).tolist()

    bar = (
        Bar()
        .add_xaxis(xaxis_data=list(df1['datetime']))
        .add_yaxis(
            series_name="Volume",
            yaxis_data=bar_data,
            xaxis_index=1,
            yaxis_index=1,
            label_opts=opts.LabelOpts(is_show=False),
        )
        .set_global_opts(
            xaxis_opts=opts.AxisOpts(
                type_="category",
                is_scale=True,
                grid_index=1,
                boundary_gap=False,
                axisline_opts=opts.AxisLineOpts(is_on_zero=False),
                axistick_opts=opts.AxisTickOpts(is_show=False),
                splitline_opts=opts.SplitLineOpts(is_show=False),
                axislabel_opts=opts.LabelOpts(is_show=False),
                split_number=20,
                min_="dataMin",
                max_="dataMax",
            ),
            yaxis_opts=opts.AxisOpts(
                grid_index=1,
                is_scale=True,
                split_number=2,
                axislabel_opts=opts.LabelOpts(is_show=False),
                axisline_opts=opts.AxisLineOpts(is_show=False),
                axistick_opts=opts.AxisTickOpts(is_show=False),
                splitline_opts=opts.SplitLineOpts(is_show=False),
            ),
            legend_opts=opts.LegendOpts(is_show=False),
        )
    )

    return bar


def gen_atr(df1):

    bar_data = df1.apply(lambda record: record['close'], axis=1).tolist()

    line = Line()
    line.add_xaxis( xaxis_data=list(df1['datetime']) )
    line.add_yaxis( 'price',
                    y_axis=bar_data,
                    xaxis_index=2,
                    yaxis_index=2,
                    label_opts=opts.LabelOpts(is_show=False),
                  )

    return line

def draw_charts():

    fn = 'bar/min1_SR001.csv'
    df1 = pd.read_csv(fn)
    df1['datetime'] = df1['date'] + ' ' + df1['time']
    # print(df1.head())

    kline = gen_kline(df1)
    bar = gen_rsi(df1)
    line = gen_atr(df1)

    grid_chart = Grid(
        init_opts=opts.InitOpts(
            width="1000px",
            height="800px",
            animation_opts=opts.AnimationOpts(animation=False),
        )
    )
    grid_chart.add(
        kline,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", height="50%"),
    )
    grid_chart.add(
        bar,
        grid_opts=opts.GridOpts(
            pos_left="10%", pos_right="8%", pos_top="63%", height="16%" ),
    )

    grid_chart.add(
        line,
        grid_opts=opts.GridOpts(
            pos_left="10%", pos_right="8%", pos_top="83%", height="16%" ),
    )

    grid_chart.render("brush.html")


if __name__ == "__main__":
    draw_charts()
