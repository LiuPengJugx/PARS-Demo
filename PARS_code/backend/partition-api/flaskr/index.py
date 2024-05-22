from flask import(Blueprint, flash, g, redirect, render_template, request, url_for)
from werkzeug.exceptions import abort
import json
from flaskr.db import Postgres
import decimal
import numpy as np
import pickle
import random
import os
bp=Blueprint('index',__name__,url_prefix='/aidb/index')

# 数据类型转换器
class NpEncoder(json.JSONEncoder):
    def default(self,o):
        if isinstance(o,decimal.Decimal):
            return float(o)
        elif isinstance(o, np.integer):
            return int(o)
        elif isinstance(o, np.floating):
            return float(o)
        elif isinstance(o, np.ndarray):
            return o.tolist()
        super(NpEncoder,self).default(o)

@bp.route('/tabledata',methods=['GET'])
def getTableData():
    query=request.args.get('query')
    tablename=request.args.get('tablename')
    pg=Postgres()
    pg.getPgConnCur()
    res,_=pg.executeQuery(query+" limit 10 offset 0;")
    table_columns,_=pg.executeQuery("SELECT attname FROM pg_attribute WHERE attrelid = ( SELECT oid FROM pg_class WHERE relname = '%s' ) AND attnum > 0;"%tablename)
    table_columns=[item[0] for item in table_columns]
    pg.close()
    tabledata=[]
    for row in res:
        row_data={}
        for idx,col in enumerate(table_columns):
            row_data[col]=row[idx]
        tabledata.append(row_data)
    print(tabledata)
    return json.dumps({
            "code": 20000,
            "data": {
                "columns":table_columns,
                "tabledata":tabledata
            }
        }, cls=NpEncoder
    )


@bp.route('/recommand',methods=['GET'])
def fetchIndexRecommand():
    maxIndexCount=int(request.args.get('maxIndexCount'))
    queryDistribution=request.args.get('queryDistribution')
    base_dir = os.path.dirname(__file__)
    
    cf = open(base_dir+'/static/index/cands14.pickle', 'rb')
    index_candidates = pickle.load(cf)

    randIndexList=random.sample(range(1,len(index_candidates)), maxIndexCount)
    return {
        "code":20000,
        "data":[index_candidates[i] for i in randIndexList]
    }