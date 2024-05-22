
from jinja2 import Markup, Environment, FileSystemLoader
from pyecharts.globals import CurrentConfig
from flask import(Blueprint, flash, g, redirect, render_template, request, url_for)
from random import randrange

from flask import Flask, render_template

from pyecharts import options as opts
from pyecharts.charts import Bar
# 关于 CurrentConfig，可参考 [基本使用-全局变量]
from pyecharts.charts import Bar
import json

bp=Blueprint('test_pye',__name__,url_prefix='/aidb/test_pye')


def bar_base() -> Bar:
    c = (
        Bar()
        .add_xaxis(["衬衫", "羊毛衫", "雪纺衫", "裤子", "高跟鞋", "袜子"])
        .add_yaxis("商家A", [randrange(0, 100) for _ in range(6)])
        .add_yaxis("商家B", [randrange(0, 100) for _ in range(6)])
        .set_global_opts(title_opts=opts.TitleOpts(title="Bar-基本示例", subtitle="我是副标题"))
    )
    return c


@bp.route('/index',methods=['get'])
def index():
    query=request.args.get('query')
    print(query)
    c = bar_base()
    print(type(c.dump_options_with_quotes())) 
    json_data=c.dump_options_with_quotes()
    aa=json.loads(json_data)
    return json.dumps({
            "code":20000,
            "data":aa
    })
