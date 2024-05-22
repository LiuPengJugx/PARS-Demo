import imp
from flask import(Blueprint, request)
# from data.workload_generator import main

from flaskr.storage import NpEncoder
bp=Blueprint('dypartition',__name__,url_prefix='/aidb/dypartition')
import sys
import json
import copy
sys.path.append('./algorithms')
from controller.executer import getResultByExperimentSet 
from controller.util import Util
import os
from flaskr.db import Postgres
import pandas as pd

@bp.route('/test',methods=['GET'])
def testDynamicFun():
    getResultByExperimentSet([0],[0],[0,50],[0],[])
    return {"data":'ok'}

@bp.route('/progress',methods=['GET'])
def getProgressByTimer():
    return {
        "code":20000,
        'progress':Util.get_experiment_progress()
    }
    
@bp.route('/workloadSize/<path>',methods=['GET'])
def getWorkloadSize(path):
    base_dir = os.path.dirname(__file__)
    df=pd.read_csv(base_dir+"/static/dynamicWorkload/"+path+".csv",header=None)
    return json.dumps(
        {
            "code":20000,
            "count":df.shape[0]
        },cls=NpEncoder
    )
@bp.route('/workload/export/<path>',methods=['GET'])
def exportWorkload(path):
    base_dir = os.path.dirname(__file__)
    df=pd.read_csv(base_dir+"/static/dynamicWorkload/"+path+".csv",header=None)
    queries=[]
    columnNames=json.loads(getTableColumns(path))['columns']
    for row in range(df.shape[0]):
        scan_keys=[]
        # if math.isnan(df.iloc[row][2]):
        if (df.iloc[row][2])!=(df.iloc[row][2]):
            pass
        else:
            scan_keys=[columnNames[int(x)] for x in df.iloc[row][2].split(",")]
        ## end
        queries.append({
            'accessed attributes':[columnNames[int(x)] for x in df.iloc[row][0].split(",")],
            'frequency':df.iloc[row][1],
            'scan keys':scan_keys,
            'selectivity':round(df.iloc[row][3],3),
            'arrive time':df.iloc[row][4]
        })
    return json.dumps(
        {
            "code":20000,
            "data":queries
        },cls=NpEncoder
    )

@bp.route('/viewWorkload',methods=['GET'])
def readWorkload():
    pageIndex=request.args.get('currentPage',type=int)
    pageSize=request.args.get('pageSize',type=int)
    path=request.args.get('tabName',type=str)
    base_dir = os.path.dirname(__file__)
    df=pd.read_csv(base_dir+"/static/dynamicWorkload/"+path+".csv",header=None)
    querys=[]
    begin=(pageIndex-1)*pageSize
    end=pageIndex*pageSize
    if end>df.shape[0]: end=df.shape[0]
    for row in range(begin,end):
        scan_keys=[]
        # if math.isnan(df.iloc[row][2]):
        if (df.iloc[row][2])!=(df.iloc[row][2]):
            pass
        else:
            scan_keys=[int(x) for x in df.iloc[row][2].split(",")]
        ## end
        querys.append({
            'value':[int(x) for x in df.iloc[row][0].split(",")],
            'freq':df.iloc[row][1],
            # 'scan_key':df.iloc[row][2]-1,
            'scan_key':scan_keys,
            'selectivity':round(df.iloc[row][3],3),
            'arrive_time':df.iloc[row][4]
        })
    return json.dumps(
        {
            "code":20000,
            "queries":querys
        },cls=NpEncoder
    )

@bp.route('/columns/<tablename>',methods=['GET'])
def getTableColumns(tablename):
    pg=Postgres()
    pg.getPgConnCur()
    relnamespace=734308
    if tablename[:3]=='SYN':
        tablename='tt_tab'
        relnamespace=2200
    else:
        tablename=tablename[:-4]

    table_columns,_=pg.executeQuery("SELECT attname FROM pg_attribute WHERE attrelid = ( SELECT oid FROM pg_class WHERE relname = '%s' and relnamespace=%d) AND attnum > 0;"%(tablename,relnamespace))
    pg.close()
    return json.dumps(
        {
            "code":20000,
            "name":tablename,
            "columns":[col[0] for col in table_columns]
        }
    )

@bp.route('/tableProfile/<tablename>',methods=['GET'])
def getTableProfile(tablename):
    pg=Postgres()
    pg.getPgConnCur()
    relnamespace=734308
    if tablename[:3]=='SYN':
        tablename='tt_tab'
        relnamespace=2200
    else:
        tablename=tablename[:-4]
    table_profiles,_=pg.executeQuery("SELECT a.attname as name, format_type(a.atttypid,a.atttypmod) as type, a.attnotnull as notnull FROM pg_class as c,pg_attribute as a where c.relname = '%s' and c.relnamespace=%d and a.attrelid = c.oid and a.attnum>0"%(tablename,relnamespace))
    pg.close()
    return json.dumps(
        {
            "code":20000,
            "name":tablename,
            "column_count":len(table_profiles),
            "primary_key":table_profiles[0][0],
            "profile":table_profiles
        }
    )

def addHeadDict(dt):
    firstTime=list(dt.keys())[0]
    if firstTime>=2:
        for time in range(1,firstTime):
            dt[time]=dt[firstTime]
    return dt

@bp.route('/start',methods=['POST'])
def runDynamicAlgorithms():
    jsonData = request.get_json(silent=True)
    selectedAlgos=jsonData['selectedAlgos']
    selectedDatasets=jsonData['selectedDatasets']
    timeRange=jsonData['timeRange']

    result,timeRange,selected_tables=getResultByExperimentSet(selectedAlgos,selectedDatasets,timeRange)
    # result,timeRange=getResultByExperimentSet([0,1,2,3],[0],[10,50])
    # 对结果进行解析
    datasets=list(result.keys())
    algos=list(result[datasets[0]].keys())
    parData={}
    lineRaceData=dict()
    barRaceData=dict()
    lineData={
        'xlabel':datasets,
        'data':dict()
    }
    barData={
        'ylabel':datasets,
        'data':dict()
    }
    heatmapData=dict()

    for algo in algos:
        lineData['data'][algo]=list()
        barData['data'][algo]=list()

    for idx,dataset in enumerate(datasets):
        lineRaceData[dataset]={'label':algos,'data':{}}
        lineRaceData[dataset]['data']=[["QueryCost","Algorithm","Time"]]
        barRaceData[dataset]={'label':algos,'data':{}}
        barRaceData[dataset]['data']=[["RepartitionCost","Algorithm","Time"]]        
        heatmapData[dataset]=dict()
        parData[dataset]={'tableProfile':json.loads(getTableColumns(selected_tables[idx]))}
        for algo in result[dataset].keys():
            for time in sorted(addHeadDict(result[dataset][algo]['avg_query_cost_per_time']).keys()):
                if time>=timeRange[0]:
                    lineRaceData[dataset]['data'].append([result[dataset][algo]['avg_query_cost_per_time'][time],algo,time])
            for time in sorted(addHeadDict(result[dataset][algo]['repartition_cost_per_time']).keys()):
                if time>=timeRange[0]:
                    barRaceData[dataset]['data'].append([result[dataset][algo]['repartition_cost_per_time'][time],algo,time])
            lineData['data'][algo].append(result[dataset][algo]['avg_query_cost'])
            barData['data'][algo].append(result[dataset][algo]['repartition_cost'])
            parData[dataset][algo]={}
            par_list_per_time=result[dataset][algo]['par_list_per_time']
            # formatting dict
            # 对分区方案进行格式化处理
            rep_time_points=list(par_list_per_time.keys())
            first_time=-1
            for idx,time in enumerate(rep_time_points):
                # next_solved_attrs=[]
                par_schema=par_list_per_time[time]
                if not par_list_per_time[time]:
                    par_list_per_time.pop(time)
                    continue

                if first_time>0:
                    raw_par_schema=copy.deepcopy(parData[dataset][algo][first_time])
                    cur_par_schema=[]
                    for raw_par in par_schema:
                        cur_par_schema.append(sorted(raw_par))
                        for attr in raw_par: 
                            raw_par_schema.remove([attr])
                    cur_par_schema+=raw_par_schema
                    if len(cur_par_schema)>1:
                        parData[dataset][algo][time]=cur_par_schema
                else:
                    parData[dataset][algo][time]=[[attr] for attr in par_schema[0]]
                    first_time=time
                

                # version 1: str formatting partition
                # new_par_desc=[]
                # for par in par_schema:
                #     [new_par_desc.append(str(attr)) for attr in sorted(par)]
                #     new_par_desc.append('')
                # new_par_desc.pop(-1)
                # par_list_per_time[time]=new_par_desc


            heatmapData[dataset][algo]={
                # 'pars':result[dataset][algo]['par_list_per_time'],
                'data':result[dataset][algo]['headmap_per_time']
            }
    return json.dumps(
        {
            "code":20000,
            "par_data":parData,
            "plot":{
                "lineRaceData":lineRaceData,
                "barRaceData":barRaceData,
                "lineData":lineData,
                "barData":barData,
                "heatmapData":heatmapData,
                "adjusted_time_range":timeRange
            }
        },cls=NpEncoder
    )