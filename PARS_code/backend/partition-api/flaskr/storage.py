# from crypt import methods
from flask import Blueprint,g,current_app,request
import json
import decimal
import numpy as np
import os
import ray
from flaskr.db import Postgres
import pandas as pd
import sys
import random
import datetime
import pickle
# 垂直分区模块核心控制器
import copy
sys.path.append('./flaskr/algorithms/SCVP')
sys.path.append('./flaskr/algorithms')
from SCVP.demo import run
from SCVP.demo_plus import run_baseline,splitTableName
from SCVPplus.query_encoder import Q_Encoder
from SCVPplus.hp_partitioner import Scvp_Plus
from SCVPplus.Conf import col_inf,schema_inf
from SCVPplus.q_parser import get_tab_from_conf
from SCVPplus.utils import write_csv,encode_pred_val,load_dict
bp=Blueprint('storage',__name__,url_prefix='/aidb/storage')

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
        elif isinstance(o, datetime.date):
            return str(o)
        super(NpEncoder,self).default(o)



@bp.route('/createTab',methods=['POST'])
def createTable():
    tabData=request.get_json(silent=True)
    pg=Postgres()
    tableName=tabData['tabName']
    fieldStatements=''
    colNames=[]
    for colIdx,colItem in enumerate(tabData['dataSource']):
        type=colItem['type']
        length=colItem['length']
        if type in ['Decimal','Numeric']: s_type=f"{type}({length},2)"
        elif type in ['Date','Int','Integer']: s_type=f"{type}"
        else: s_type=f"{type}({length})"
        colNames.append(colItem['name'])
        fieldStatements+=f"{colItem['name']} {s_type}"
        if colIdx==len(tabData['dataSource'])-1: fieldStatements+=""
        else: fieldStatements+=","
    pg.getPgConnCur()
    createStatement=f"CREATE TABLE \"ADPS\".{tableName}({fieldStatements});"
    message=f'Create {tableName} success!'
    status_code=1
    try:
        pg.cur.execute(createStatement)
        pg.conn.commit()
    except:
        message=f'Table {tableName} exists!'
        status_code=0
    
    return {
        "code":20000,
        "status":status_code,
        "message":message
    }

@bp.route('/insertTab',methods=['POST'])
def insertTable():
    pg=Postgres()
    pg.getPgConnCur()
    tableName = request.form['tabName']
    data=request.files['file']
    data = pd.read_csv(data,encoding='utf-8',header=None)
    data=np.array(data).tolist()
    status_code=1
    try:
        pg.cur.executemany("INSERT INTO \"ADPS\"."+tableName+" VALUES("+','.join(['%s']*len(data[0]))+")",data)
        pg.conn.commit()
        message=f"Update table success, affect {pg.cur.rowcount} rows."
    except:
        status_code=0
        message="Error!"
    status_code=0
    return {
        "code":20000,
        "status":status_code,
        "message":message
    }
    

@bp.route('/tabList')
def getSchemaTables():
    pg=Postgres()
    pg.getPgConnCur()
    res,row_num=pg.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'ADPS'")
    # res,row_num=pg.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'tpcd'")
    pg.close()
    benchmarks={
        'TPC-H':["customer","lineitem","nation","orders","part","partsupp","region","supplier",],
        'JOB':["cast_info","movie_info_idx","movie_info","movie_keyword","movie_companies","title","company_name",'name',"aka_name"],
        'WDT':["widetable","widetable30","widetable50","widetable100"],
        'delted':["table_name","asin_0","asin_1","asin_2","tt_tab","order_par_key_1","orders_par","order_par_key_2","order_par_key_3","order_par_key_4",],
    }
    bm_keys=['TPC-H','JOB','WDT','Others']
    tab_list={}
    for k in bm_keys: tab_list[k]=[]
    for row in res:
        flag=0
        for key in benchmarks:
            if row[0] in benchmarks[key]:
                if key!='delted': tab_list[key].append(row[0])
                flag=1
                break
        if flag==0: tab_list['Others'].append(row[0])
    # data={}
    # for k in bm_keys: data[k]=tab_list[k]
    # print(tab_list)
    return {
        "code":20000,
        "data":{"bm_keys":bm_keys,"tab_data":tab_list}
    }


@bp.route('/pev2',methods=['GET'])
def getSampleData():
    no=request.args.get("no",type=int)
    base_dir = os.path.dirname(__file__)
    with open(base_dir+"/static/samples/plan_%d.txt"%(no)) as f:
        plan=f.read()
    # with open(base_dir+"/static/samples/plan_%d.sql"%(no)) as f:
    #     query=f.read()
    return {
        "code":20000,
        "data":{
            "query":'',
            "plan":plan
        }
    }

@bp.route('/comp',methods=['POST','GET'])
def testConfigAlogorithm():
    table=request.form['table']
    method=request.form['method']
    # print(request.headers)
    # data = request.get_json()
    # print(data)
    # table=data['table']
    # method=data['method']
    return {
        "code":20000,
        "data":{}
    }

@bp.route('/')
def method_name():
   pass

@bp.route('/tableInfo/columns/<tablename>',methods=['GET'])
def getTableColumns(tablename):
    pg=Postgres()
    pg.getPgConnCur()
    relnamespace=734308  #734308
    if tablename=='widetable':
        tablename='tt_tab'
    #     relnamespace=2200
    table_columns,_=pg.executeQuery("SELECT attname FROM pg_attribute WHERE attrelid = ( SELECT oid FROM pg_class WHERE relname = '%s' and relnamespace=%d) AND attnum > 0;"%(tablename,relnamespace))
    pg.close()
    return json.dumps(
        {
            "code":20000,
            "data":{
                'columns':[col[0] for col in table_columns]
            }
        }
    )
@bp.route('/tableCount/<tablename>',methods=['GET'])
def getTableCount(tablename):
    pg=Postgres()
    pg.getPgConnCur()
    count,_=pg.executeQuery("SELECT COUNT(*) FROM \"ADPS\".%s"%tablename)
    pg.close()
    return json.dumps(
        {
        "code":20000,
        "count":count[0][0]
        } ,cls=NpEncoder
    )

@bp.route('/tableInfo/<tablename>',methods=['GET'])
def getTableInfo(tablename):
    # tab_name=request.args.get('tabName')
    pageSize=request.args.get('pageSize',type=int)
    currentPage=request.args.get('currentPage',type=int)
    rowNum=request.args.get('rowNum',type=int)

    offset=(currentPage-1)*pageSize
    limit=pageSize
    if rowNum-offset<pageSize:
        limit=rowNum-offset

    pg=Postgres()
    pg.getPgConnCur()
    relnamespace=734308

    if tablename=='widetable':
        tablename='tt_tab'
    #     relnamespace=2200
    table_columns,_=pg.executeQuery("SELECT attname FROM pg_attribute WHERE attrelid = ( SELECT oid FROM pg_class WHERE relname = '%s' and relnamespace=%d ) AND attnum > 0;"%(tablename,relnamespace))
    table_data,_=pg.executeQuery("SELECT * FROM \"ADPS\".%s LIMIT %d OFFSET %d;"%(tablename,limit,offset))
    
    pg.close()

    # print("%s %d %d"%(tab_name,page,limit))
    
    # columns=["date","name","address"]
    # student_info=[['2016-05-02','王小虎','上海市普陀区金沙江路 1518 弄'],
    #     ['2016-05-02','王小虎','上海市普陀区金沙江路 1518 弄'],
    #     ['2016-05-02','王小虎','上海市普陀区金沙江路 1518 弄']
    # ]
    return json.dumps(
        {
        "code":20000,
        "data":{
            'columns':[col[0] for col in table_columns],
            'student_info':table_data
        }
        } ,cls=NpEncoder
    )

def parse_sql_embs_col(embs,benchmark):
    parsed_embs=copy.deepcopy(embs)
    lg_ops=['and','or']
    def encode_simple_cols(cols):
        new_cols=[]
        for col in cols:
            tab,col_id=get_tab_from_conf(col)
            new_cols.append(col_id)
        return new_cols
    def encode_lg_preds(preds):
        if len(preds)==0: return []
        if preds[0] in lg_ops:
            new_preds=[preds[0],[]]
            for pred in preds[1]:
                new_preds[1].append(encode_lg_preds(pred)) 
        else:
            tab,col_id=get_tab_from_conf(preds[0])
            return [col_id,preds[1],encode_pred_val(col_id,preds[2],load_dict(benchmark,tab))]
        return new_preds

    for qid,p_emb in enumerate(parsed_embs):
        tab=p_emb['tab']
        for pid,pred in enumerate(p_emb['preds']):
            col_id=col_inf[tab]['name'].index(pred[0])
            parsed_embs[qid]['preds'][pid]=[col_id,pred[1],encode_pred_val(col_id,pred[2],load_dict(benchmark,tab))]
        parsed_embs[qid]['lg_preds']=encode_lg_preds(parsed_embs[qid]['lg_preds'])
        parsed_embs[qid]['filter']=encode_simple_cols(p_emb['filter'])
        parsed_embs[qid]['scan']=encode_simple_cols(p_emb['scan'])
        parsed_embs[qid]['gp_ob']=encode_simple_cols(p_emb['gp_ob'])
    return parsed_embs

def beauty_sql_embs(my_embs):
    embs=copy.deepcopy(my_embs)
    lg_ops=['and','or']
    def beauty_lg_preds(preds,depth,p_id):
        if len(preds)==0: return {'title': 'None', 'key': f'{depth}-{p_id}', 'slots': { 'icon': 'drag' },'children':[]}
        if preds[0] in lg_ops:
            my_preds={'title': preds[0], 'key': f'{depth}-{p_id}', 'slots': { 'icon': 'drag' },'children':[]}
            depth+=1
            for cp_id,pred in enumerate(preds[1]):
                my_preds['children'].append(beauty_lg_preds(pred,depth,cp_id))
            return my_preds
        else:
            return {'title': f'{preds[0]}{preds[1]}{preds[2]}', 'key': f'{depth}-{p_id}', 'slots': { 'icon': 'italic' } }
    for qid,emb in enumerate(embs):
        beauty_preds=[]
        for pred in emb['preds']:
            beauty_preds.append(f'{pred[0]}{pred[1]}{pred[2]}')
        embs[qid]['preds']=beauty_preds
        embs[qid]['lg_preds']=[beauty_lg_preds(emb['lg_preds'],0,0)]
    return embs


@bp.route('/vertical/partitions/parse',methods=['GET'])
def getParsedRes():
    benchmark='tpch'
    sql=request.args.get('sql',type=str)
    current_app.logger.info(sql)
    embs=Q_Encoder(benchmark).encode(sql,keep_colnames=True)
    beauty_embs=beauty_sql_embs(embs)
    parsed_embs=parse_sql_embs_col(embs,benchmark)
    return json.dumps(
        {
        "code":20000,
        "data":[beauty_embs,parsed_embs]
        } ,cls=NpEncoder
    )

@ray.remote
class RayInitializer:
    def __init__(self):
        self.ray_initialized = False

    def init_ray(self):
        if not self.ray_initialized:
            ray.init()
            self.ray_initialized = True

    def shutdown_ray(self):
        if self.ray_initialized:
            ray.shutdown()
            print("Ray has been shut!")
            self.ray_initialized = False
        else:
            print("Ray has not been initialized.")

def getPieStatistic(worklaods):
    baseDir='http://127.0.0.1:5000/static/tempParsedSqls/'
    tab_statistic_dict={}
    q_num_list=[]
    for workload_file_name in worklaods:
        tablename=workload_file_name[:-4]
        benchmark='tpch'
        for k in schema_inf.keys():
            if tablename in schema_inf[k]: benchmark=k
        path=baseDir+workload_file_name
        encode_dicts=load_dict(benchmark,tablename)
        df=pd.read_csv(path)
        queries=df[df['tab']==tablename].to_dict('records')
        tot_pred_num=0
        tot_q_num=len(queries)
        tot_encoded_pred_num=0
        tot_col_num=len(col_inf[tablename]['name'])
        accessed_col_idx=[]
        for qid,q in enumerate(queries):
            for pred in eval(q['preds']):
                tot_pred_num+=1
                if pred[0] not in accessed_col_idx: accessed_col_idx.append(pred[0])
                # new_pred_val=encode_pred_val(pred[0],pred[2],encode_dicts)
                # if pred[2]!=new_pred_val:
                if pred[0] not in col_inf[tablename]['num_col']:
                    tot_encoded_pred_num+=1
        tot_access_col_num=len(accessed_col_idx)
        q_num_list.append(tot_q_num)
        q_num_arr=np.array(q_num_list)
        tab_statistic_dict[tablename]={'tot_col_num':tot_col_num,'tot_access_col_num':tot_access_col_num,'tot_pred_num':tot_pred_num,'tot_encoded_pred_num':tot_encoded_pred_num}
    tot_col_num,tot_access_col_num,tot_pred_num,encoded_pred_num=0,0,0,0
    for tab in tab_statistic_dict.keys():
        tab_statistic= tab_statistic_dict[tab]
        tot_col_num+=tab_statistic['tot_col_num']
        tot_access_col_num+=tab_statistic['tot_access_col_num']
        tot_pred_num+=tab_statistic['tot_pred_num']
        encoded_pred_num+=tab_statistic['tot_encoded_pred_num']
    pie_statistic={'tab_query_ratio':q_num_arr/np.sum(q_num_arr),'access_unaccess_ratio':[tot_access_col_num/tot_col_num,(tot_col_num-tot_access_col_num)/tot_col_num],'numeric_text_ratio':[encoded_pred_num/tot_pred_num,(tot_pred_num-encoded_pred_num)/tot_pred_num]}
    return pie_statistic

@bp.route('/vertical/partitions/V2/analysis',methods=['POST'])
def getSchemeEvaluateRes():
    js_data=request.get_json(silent=True)
    # ray.init()
    benchmark=js_data['benchmark']
    # result=run_baseline(js_data['workloads'],js_data['methods'],'Huang','http://127.0.0.1:5000/static/tempParsedSqls/')
    with open(f'D:/PycharmProjects/partition-api/flaskr/pre-results/{benchmark}.pickle','rb') as f:
        result=pickle.load(f)
    print(result)
    # ray.shutdown()

    # shutdown Ray
    # ray_init.shutdown_ray.remote()
    # current_app.config['WOODS_DICT']=result['woods']

    # 6：NSM， 1：SCVP, 11: AVP-RL
    # find the SCVP 相比 AVP-RL 和 NSM 减少的总latency （顺便计算一下提升比例）
    SCVPVR_IDX,SCVP_IDX,HILLCLIMB_IDX,ROW_IDX=result['methods'].index('SCVP-RV'),result['methods'].index('SCVP'),result['methods'].index('AVP-RL'),result['methods'].index('ROW')
    latency_reduce,time_reduce,cost_reduce,time_dict={},{},{},{}
    for item in result['result']:
        tablename=item['tablename']
        scvp_latency=item['latency'][SCVP_IDX]
        svcpvr_latency=item['latency'][SCVPVR_IDX]
        row_latency=item['latency'][ROW_IDX]
        hillclimb_latency=item['latency'][HILLCLIMB_IDX]
        latency_reduce[tablename]=[[row_latency-svcpvr_latency,hillclimb_latency-svcpvr_latency,scvp_latency-svcpvr_latency],
                                   [(row_latency-svcpvr_latency)/row_latency,(hillclimb_latency-svcpvr_latency)/hillclimb_latency,(scvp_latency-svcpvr_latency)/scvp_latency],
                                    [(row_latency-hillclimb_latency)/row_latency,(row_latency-scvp_latency)/row_latency,(row_latency-svcpvr_latency)/row_latency]
                                   ]
        # find SCVP 相比 AVP-RL 减少的总执行时间 （顺便计算一下倍数）
        svcp_overhead=item['overhead'][SCVP_IDX]
        svcpvr_overhead=item['overhead'][SCVPVR_IDX]
        row_overhead=item['overhead'][ROW_IDX]
        hillclimb_overhead=item['overhead'][HILLCLIMB_IDX]
        time_reduce[tablename]=[[row_overhead-svcpvr_overhead,hillclimb_overhead-svcpvr_overhead,svcp_overhead-svcpvr_overhead],
                                [(row_overhead-svcpvr_overhead)/row_overhead,(hillclimb_overhead-svcpvr_overhead)/hillclimb_overhead,(svcp_overhead-svcpvr_overhead)/svcp_overhead],
                                [(row_overhead-hillclimb_overhead)/row_overhead,(row_overhead-svcp_overhead)/row_overhead,(row_overhead-svcpvr_overhead)/row_overhead]
                                ]
        time_dict[tablename]=[hillclimb_overhead,svcp_overhead,svcpvr_overhead]

        scvp_cost = item['costs'][SCVP_IDX]
        svcpvr_cost = item['costs'][SCVPVR_IDX]
        row_cost = item['costs'][ROW_IDX]
        hillclimb_cost = item['costs'][HILLCLIMB_IDX]
        cost_reduce[tablename] = [[row_cost - svcpvr_cost, hillclimb_cost - svcpvr_cost, scvp_cost - svcpvr_cost],
                       [(row_cost - svcpvr_cost) / row_cost, (hillclimb_cost - svcpvr_cost) / hillclimb_cost,(scvp_cost - svcpvr_cost) / scvp_cost],
                        [(row_cost - hillclimb_cost) / row_cost, (row_cost - scvp_cost) / row_cost,(row_cost - svcpvr_cost) / row_cost]
                       ]

    avg_latency_reduce=[[np.average(np.array([latency_reduce[_][0][0] for _ in cost_reduce.keys()])),np.average(np.array([latency_reduce[_][0][1] for _ in cost_reduce.keys()])),np.average(np.array([latency_reduce[_][0][2] for _ in cost_reduce.keys()]))],
                        [np.average(np.array([latency_reduce[_][1][0] for _ in cost_reduce.keys()])),np.average(np.array([latency_reduce[_][1][1] for _ in cost_reduce.keys()])),np.average(np.array([latency_reduce[_][1][2] for _ in cost_reduce.keys()]))],
                        [np.average(np.array([latency_reduce[_][2][0] for _ in cost_reduce.keys()])),np.average(np.array([latency_reduce[_][2][1] for _ in cost_reduce.keys()])),np.average(np.array([latency_reduce[_][2][2] for _ in cost_reduce.keys()]))]
                        ]
    avg_time_reduce = [[np.average(np.array([time_reduce[_][0][0] for _ in cost_reduce.keys()])), np.average(np.array([time_reduce[_][0][1] for _ in cost_reduce.keys()])), np.average(np.array([time_reduce[_][0][2] for _ in cost_reduce.keys()]))  ],
                            [np.average(np.array([time_reduce[_][1][0] for _ in cost_reduce.keys()])), np.average(np.array([time_reduce[_][1][1] for _ in cost_reduce.keys()])), np.average(np.array([time_reduce[_][1][2] for _ in cost_reduce.keys()])) ],
                                [np.average(np.array([time_reduce[_][2][0] for _ in cost_reduce.keys()])), np.average(np.array([time_reduce[_][2][1] for _ in cost_reduce.keys()])), np.average(np.array([time_reduce[_][2][2] for _ in cost_reduce.keys()])) ]
                        ]
    
    time_list=[np.average(np.array([time_dict[_][0] for _ in time_dict.keys()])),np.average(np.array([time_dict[_][1] for _ in time_dict.keys()])),np.average(np.array([time_dict[_][2] for _ in time_dict.keys()]))]
    avg_time_reduce2=[np.sum([time_dict[_][0] for _ in time_dict])/np.sum([time_dict[_][2] for _ in time_dict]) , np.sum([time_dict[_][1] for _ in time_dict])/np.sum([time_dict[_][2] for _ in time_dict])]
    avg_cost_reduce = [
        [np.average(np.array([cost_reduce[_][0][0] for _ in cost_reduce.keys()])),np.average(np.array([cost_reduce[_][0][1] for _ in cost_reduce.keys()])),np.average(np.array([cost_reduce[_][0][2] for _ in cost_reduce.keys()]))],
        [np.average(np.array([cost_reduce[_][1][0] for _ in cost_reduce.keys()])),np.average(np.array([cost_reduce[_][1][1] for _ in cost_reduce.keys()])),np.average(np.array([cost_reduce[_][1][2] for _ in cost_reduce.keys()]))],
        [np.average(np.array([cost_reduce[_][2][0] for _ in cost_reduce.keys()])),np.average(np.array([cost_reduce[_][2][1] for _ in cost_reduce.keys()])),np.average(np.array([cost_reduce[_][2][2] for _ in cost_reduce.keys()]))]
                    ]

    pie_statistic = getPieStatistic(js_data['workloads'])
    print('pie_statistic: ',pie_statistic)
    result['pie_statistic']=pie_statistic
    result['pref']={'latency':avg_latency_reduce,'time':avg_time_reduce,'time2':avg_time_reduce2,'time3':time_list,'cost':avg_cost_reduce}
    row_score=1
    w1,w2,w3=0.7,0.1,0.2
    print("avg_latency_reduce:",avg_latency_reduce)
    print("avg_time_reduce:",avg_time_reduce)
    print("avg_cost_reduce:",avg_cost_reduce)
    avp_score=avg_latency_reduce[2][0]*w1+avg_time_reduce[2][0]*w2+avg_cost_reduce[2][0]*w3+1
    scvp_score=avg_latency_reduce[2][1]*w1+avg_time_reduce[2][1]*w2+avg_cost_reduce[2][1]*w3+1
    scvpvr_score=avg_latency_reduce[2][2]*w1+avg_time_reduce[2][2]*w2+avg_cost_reduce[2][2]*w3+1
    avp_score=0 if avp_score<0 else avp_score
    scvp_score=0 if scvp_score<0 else scvp_score
    scvpvr_score=0 if scvpvr_score<0 else scvpvr_score
    # avp_score=((1-avg_latency_reduce[1][0])/(1-avg_latency_reduce[1][1])-1)*0.5+((1-avg_time_reduce[1][0])/(1-avg_cost_reduce[1][1])-1)*0.25+((1-avg_cost_reduce[1][0])/(1-avg_cost_reduce[1][1])-1)*0.25+1
    # scvp_score=((1-avg_latency_reduce[1][0])/(1-avg_latency_reduce[1][2])-1)*0.5+((1-avg_time_reduce[1][0])/(1-avg_cost_reduce[1][2])-1)*0.25+((1-avg_cost_reduce[1][0])/(1-avg_cost_reduce[1][2])-1)*0.25+1
    score_list=[row_score,scvp_score,avp_score,scvpvr_score]
    max_score=max(score_list)
    result['score']={6:round(row_score*100/max_score,1),10:round(scvp_score*100/max_score,1),11:round(avp_score*100/max_score,1),12:round(scvpvr_score*100/max_score,1)}
    result['raw_score']=score_list
    
    # scale lineitem values
    for rid,load_dict in enumerate(result['result']):
        if load_dict['workload']=='lineitem.csv':
            load_dict['costs']=[val/7.5 for val in load_dict['costs']]
            load_dict['latency']=[val/7.5 for val in load_dict['latency']]
            result['result'][rid]=load_dict
            break
    # result['score_list']=list(result['score'].values())
    del result['woods']
    return json.dumps(
        {
        "code":20000,
        "data":result
        },cls=NpEncoder
    )


@bp.route('/vertical/partitions/V2/random/sql',methods=['GET'])
def pickRandomSql():
    r_no=-1
    while r_no==-1 or (r_no in [8,9,15,22]):
        # r_no=random.randint(1,22)  #
        # r_no=random.choice([1,2,3,4,5,6,7,10])
        r_no=random.choice([3])
    base_dir='queries/tpch-queries'
    sql=''
    with open(f'{base_dir}/d1-{r_no}.sql','r') as reader:
        sql=reader.read()    
    return json.dumps(
        {
        "code":20000,
        "data":sql
        } ,cls=NpEncoder
    )

def getTree(benchmark,tablename,method):
    if method==10: strategy=1
    else: strategy=0
    current_app.logger.info(f'Fetch tree:{tablename},{strategy}')
    # current_app.logger.info(current_app.config['WOODS_DICT'])
    # wood=current_app.config['WOODS_DICT'][strategy][tablename]
    wood=None
    if strategy in current_app.config:
        if tablename in current_app.config[strategy]:
            wood=current_app.config[strategy][tablename]
        else:
            with open(f'D:/PycharmProjects/partition-api/flaskr/pre-results/{benchmark}_wood_{strategy}_{tablename}.pickle','rb') as f:
                wood=pickle.load(f)
            current_app.config[strategy][tablename]=wood
    else:
        with open(f'D:/PycharmProjects/partition-api/flaskr/pre-results/{benchmark}_wood_{strategy}_{tablename}.pickle','rb') as f:
            wood = pickle.load(f)
        current_app.config[strategy]={}
        current_app.config[strategy][tablename] = wood
    # wood=woods[strategy][tablename]
    return list(wood.child_trees.values())[0],wood

@bp.route('/vertical/partitions/V2/node/<nid>',methods=['GET'])
def fetchNodeInfo(nid):
    tablename=request.args.get('tabName',type=str)
    method=request.args.get('method',type=int)
    benchmark=request.args.get('benchmark',type=str)
    tree,wood=getTree(benchmark,tablename,method)
    node=tree.nid_node_dict[int(nid)]
    col_names=col_inf[tablename]['name']
    node_profile={
        'no':node.nid,
        # 'boundary':', '.join([f'{col_names[bid]}:{bound}' for bid,bound in enumerate(node.boundary)]),
        'boundary':[f'{col_names[bid]}:{bound}' if bound is None or len(bound)<=12 else f'{col_names[bid]}:{bound[:12]}...' for bid,bound in enumerate(node.boundary)],
        'depth':node.depth,
        'is_leaf':'yes' if node.is_leaf else 'no',
        'node_size':node.node_size,
    }
    return json.dumps(
        {
        "code":20000,
        "data":node_profile
        } ,cls=NpEncoder
    )

@bp.route('/vertical/partitions/V2/tree',methods=['GET'])
def getTreeStructure():
    method=request.args.get('method',type=int)
    workload=request.args.get('workload',type=str)
    benchmark=request.args.get('benchmark',type=str)
    # root_url=request.args.get('rootUrl',type=str)
    # leaf_url=request.args.get('leafUrl',type=str)
    max_depth=request.args.get('maxDepth',type=int)
    extend_depth=request.args.get('extendDepth',type=int)
    extend_split_depth=4
    max_node_num=request.args.get('maxNodeNum',type=int)
    max_attr_num=request.args.get('maxAttrNum',type=int)
    # node image dict
    root_url='/public/tree.svg'
    leaf_url='/public/leaf.svg'
    avg_split_url='/public/avg_split.svg'
    median_split_url='/public/median_split.svg'
    pred_split_url='/public/pre_split.svg'

    tablename=splitTableName(workload)
    tree,wood=getTree(benchmark,tablename,method)
    select_attr_ids=[]
    for aid,boundary in enumerate(tree.boundary):
        if len(select_attr_ids)>=max_attr_num: break
        if isinstance(boundary,np.ndarray): select_attr_ids.append(aid)
        
    # split route?
    # showing split point / split benefit
    split_scheme_dicts={}
    tree_structure={}
    has_been_extended=False

    def traverse_split_tree(has_been_extended,node_no, node, depth):
        if depth > max_depth: return None
        if not node.is_leaf:
            # node_name = f"{col_inf[tablename]['name'][node.split_scheme[0]]} {node.split_scheme[1]}, Rew: {node.split_scheme[2]}"
            if node.split_scheme[1] == 'avg_split':
                image_url = avg_split_url
            elif node.split_scheme[1] == 'median_split':
                image_url = median_split_url
            else:
                image_url = pred_split_url
            if node.split_scheme[1] == 'median_split':
                node_suffix="Median Split: (STEP {node.nid+1})"
                node_name = f"{col_inf[tablename]['name'][node.split_scheme[0]]}"
            else:
                node_suffix=f"Predicate Split: (STEP {node.nid+1})"
                node_name = f"{col_inf[tablename]['name'][node.split_scheme[0]]}{node.split_scheme[1]}"

            extend_flag=False
            if depth<=1: extend_flag=True
            elif depth <=2:
                if node_no==1 and not has_been_extended:
                    extend_flag=True
                    has_been_extended=True
            elif depth<=3:
                extend_flag = True
            # elif depth<=4:
            #     if node_no==0: extend_flag=True
            split_scheme = {
                'suffix': node_suffix,
                'name': node_name,
                'image_url': image_url,
                'extend': extend_flag,
                'children': []
            }
            for no, child_nid in enumerate(node.children_ids):
                if no <= max_node_num - 1:
                    child_dict = traverse_split_tree(has_been_extended,no,tree.nid_node_dict[child_nid], depth + 1)
                    if child_dict is not None:
                        split_scheme['children'].append(child_dict)
            if len(split_scheme['children']) == 0: del split_scheme['children']
        else:
            split_scheme = None
        return split_scheme


    # def traverse_split_tree(node,depth):
    #     if depth>max_depth: return None
    #     if not node.is_leaf:
    #         node_name=f"{col_inf[tablename]['name'][node.split_scheme[0]]} {node.split_scheme[1]}, Rew: {node.split_scheme[2]}"
    #         if node.split_scheme[1]=='avg_split': image_url=avg_split_url
    #         elif node.split_scheme[1]=='median_split': image_url=median_split_url
    #         else: image_url=predictate_split_url
    #         split_scheme={
    #                 'name':node_name,
    #                 'image_url':image_url,
    #                 'extend':True if depth<=extend_depth-1 else False,
    #                 'children':[]
    #             }
    #
    #         for no,child_nid in enumerate(node.children_ids):
    #             if no<=max_node_num-1:
    #                 child_dict=traverse_split_tree(tree.nid_node_dict[child_nid],depth+1)
    #                 if child_dict is not None:
    #                     split_scheme['children'].append(child_dict)
    #         if len(split_scheme['children'])==0: del split_scheme['children']
    #     else:
    #         split_scheme=None
    #     return split_scheme
    
    def traverse_tree(node,node_th,depth):
        if depth>max_depth: return None
        boundary_str=''
        for aid in select_attr_ids:
            boundary_str+=f"{col_inf[tablename]['name'][aid]}->{node.boundary[aid]} "
        # node_name=f"ID:{node.nid} {boundary_str}"
        if node.nid==0:
            node_name = 'Root 1'
        elif node.is_leaf:
            node_name = f'Leaf Node {node.nid+1}'
        else:
            node_name =f'Node {node.nid+1}'
        extend_flag=False
        if depth<extend_depth-1:
            extend_flag=True
        elif depth==extend_depth-1 and node_th==1:
            extend_flag=True
        node_structure={
            'name':node_name,
            'image_url':root_url if depth==1 else leaf_url,
            'extend':extend_flag,
            'nid':node.nid,
            'children':[]
        }
        if len(node.children_ids)>0:
            for no,child_nid in enumerate(node.children_ids):
                if no<=max_node_num-1:
                    child_dict=traverse_tree(tree.nid_node_dict[child_nid],no,depth+1)
                    if child_dict is not None:
                        node_structure['children'].append(child_dict)
        else:
            del node_structure['children']
        return node_structure
    current_app.logger.info(tree.pt_root.is_leaf)
    split_scheme_dicts=traverse_split_tree(has_been_extended,0,tree.pt_root,1)
    tree_structure=traverse_tree(tree.pt_root,0,1)
    # showing (part) tree(node->boundary)  
    return json.dumps(
        {
        "code":20000,
        "data":{
            'split_scheme':split_scheme_dicts,
            'tree_structure':tree_structure,
        }
        } ,cls=NpEncoder
    )

@bp.route('/vertical/partitions/V2/blockList',methods=['GET'])
def fetchAllBlocks():
    tablename=request.args.get('tabName',type=str)
    method=request.args.get('method',type=int)
    benchmark=request.args.get('benchmark',type=str)
    tree,wood=getTree(benchmark,tablename,method)
    result=[]
    cnt=1
    for node in tree.get_leaves():
        result.append({'no':cnt,'row_index':node.raw_row_ids})
        cnt+=1
    return json.dumps(
        {
        "code":20000,
        "data":result
        } ,cls=NpEncoder
    )

@bp.route('/vertical/partitions/V2/genSql',methods=['GET'])
def writeDeploymentSql():
    tablename=request.args.get('tabName',type=str)
    method=request.args.get('method',type=str)
    partitions=request.args.get('partitions',type=str)
    base_dir = os.path.dirname(__file__)
    lineList=None
    with open(base_dir+"/static/deployment/par_deployment.sql",'r') as f:
        lineList=f.readlines()
    lineList[1]=lineList[1].replace("%s",tablename)
    lineList[2]=lineList[2].replace("%s",partitions)
    lineList[3]=lineList[3].replace("%s",method)
    return json.dumps(
        {
        "code":20000,
        "data":"".join(lineList)
        } ,cls=NpEncoder
    )

@bp.route('/vertical/partitions/V2/blocks',methods=['POST'])
def getBlocks():
    jsonData=request.get_json(silent=True)
    pageSize=jsonData['pageSize']
    tablename=jsonData['tabName']
    benchmark=jsonData['benchmark']
    current_app.logger.info(f'Get blocks:{tablename}')
    
    currentPage=jsonData['currentPage']
    totBlockNum=jsonData['pageCount']
    method=jsonData['method']
    vps=jsonData['pars']
    columns=col_inf[tablename]['name']
    partitions=[]
    for vp in vps:
        partitions.append([columns.index(attr) for attr in vp ])
    tree,wood=getTree(benchmark,tablename,method)
    leaf_nodes = []
    
    offset=(currentPage-1)*pageSize
    limit=pageSize
    if totBlockNum-offset<pageSize:
        limit=totBlockNum-offset
    for nid, node in tree.nid_node_dict.items():
        if node.is_leaf:
            leaf_nodes.append(node)
            if len(leaf_nodes)>=offset+limit: break
    block_data=[]
    for block in leaf_nodes[offset:(offset+limit)]:
        block_data.append({
            'row_ids':block.raw_row_ids,
            'dataset':[pd.DataFrame(block.dataset[:,par],columns=vps[pid]).to_dict(orient='records') for pid,par in enumerate(partitions)]
        })
    
    return json.dumps(
        {
        "code":20000,
        "data":{
            'columns':columns,
            'blocks':block_data
        }
        } ,cls=NpEncoder
    )


@bp.route('/vertical/partitions/analysis',methods=['POST'])
def getAnalysisResult():
    jsonData=request.get_json(silent=True)
    workloads=jsonData['workloads']
    algorithm_indexs=jsonData['methods']
    costModels=['Huang']
    baseDir='http://127.0.0.1:5000/static/tempWorkload/'
    result=run(workloads,algorithm_indexs,costModels[0],baseDir)
    pg=Postgres()
    relnamespace=734308
    pg.getPgConnCur()
    for schema in result['result']:
        tablename=splitTableName(schema['workload'])
        if tablename=='widetable':
            tablename='tt_tab'
        #     relnamespace=2200
        table_columns,_=pg.executeQuery("SELECT attname FROM pg_attribute WHERE attrelid = ( SELECT oid FROM pg_class WHERE relname = '%s' and relnamespace=%d) AND attnum > 0;"%(tablename,relnamespace))
        partitionsName=[]
        for idx,pars in enumerate(schema['partitions']):
            for idy,par in enumerate(pars):
                schema['partitions'][idx][idy]=[table_columns[i][0] for i in par]
            # partitionsName.append()
    pg.close()
    return json.dumps(
        {
        "code":20000,
        "data":result
        },cls=NpEncoder
    )


@bp.route('/workload/V2/<path>',methods=['GET'])
def readWorkloadV2(path):
    base_dir = os.path.dirname(__file__)
    benchmark='others'
    for bm in schema_inf:
        if path in schema_inf[bm]: benchmark=bm
    querys=Scvp_Plus().load_encode_queryset(path=base_dir+"/static/tempParsedSqls/"+path+".csv",benchmark=benchmark,tab=path)
    return json.dumps(
        {
            "code":20000,
            "data":querys
        },cls=NpEncoder
    )

@bp.route('/workload/<path>',methods=['GET'])
def readWorkload(path):
    base_dir = os.path.dirname(__file__)
    df=pd.read_csv(base_dir+"/static/tpch/"+path+".csv",header=None)
    querys=[]
    for row in range(df.shape[0]):
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
            'selectivity':df.iloc[row][3]
        })
    return json.dumps(
        {
            "code":20000,
            "data":querys
        },cls=NpEncoder
    )

@bp.route('/workload/V2/add',methods=['POST'])
def addWorkloadV2():
    jsonData=request.get_json(silent=True)
    fname=jsonData['fname']
    workload_data=jsonData['data']
    base_dir = os.path.dirname(__file__)
    filepath=base_dir+"/static/tempParsedSqls/"+fname
    write_csv(filepath,workload_data,workload_data[0].keys())
    return json.dumps(
        {
            "code":20000,
            "data":{
                'msg':f'Add {fname} success！',
                'filename':fname
            }
        }
    )

@bp.route('/workload/add',methods=['POST'])
def addWorkload():
    jsonData=request.get_json(silent=True)
    workload_data=jsonData['data']
    fname=jsonData['fname']

    workload_data_list=[]
    for query in workload_data:
        temp_query=[]
        if len(query['value'])==0:
            temp_query.append('')
        else:
            temp_query.append(','.join([str(item) for item in query['value']]))
        temp_query.append(query['freq'])
        
        if len(query['scan_key'])==0:
            temp_query.append('')
        else:
            temp_query.append(','.join([str(item) for item in query['scan_key']]))
        temp_query.append(query['selectivity'])
        workload_data_list.append(temp_query)
    res_pd=pd.DataFrame(workload_data_list)
    base_dir = os.path.dirname(__file__)
    filepath=base_dir+"/static/tempWorkload/"+fname+".csv"
    res_pd.to_csv(filepath,encoding="utf-8",header=0,index=0)
    return json.dumps(
        {
            "code":20000,
            "data":{
                'msg':f'Add {fname} success！',
                'filename':fname+".csv"
            }
        }
    )


# @bp.route('/vertical/go',methods=['POST'])
# def executeAnalysis():

#     # methods  key
#     # costmodel   key
#     # workload 
#     # tablename 
    
#     return json.dumps({
#         "code":20000,
#         "data":{
#             "keys":[0,1,2,3],
#             "partitions":[
#                     [[0,1,2],[3,4,5]],
#                     [[0,1],[2,3],[4,5]],
#                     [[0,1,2,3],[4,5]],
#                     [[0],[1],[2,3],[4,5]]
#             ],
#             "estimateIoCost":[5204,6780,5890,8032],
#             "estimateCpuTime":[0.06,0.12,0.09,0.15],
#             "estimateRunTime":[0.5,0.7,0.8,0.9]
#         }
#     })

@bp.route('/compress/getjson',methods=['GET'])
def getJSON():
    path=request.args.get('path')
    base_dir = os.path.dirname(__file__)
    with open(base_dir+"/static/compress"+path,'r',encoding='utf-8') as fp:
        json_data=json.load(fp)
        return json.dumps({
            "code":20000,
            "data":json_data
        })


