import numpy as np
import globalvar as gl
import warnings
import time
import ray
from Conf import col_inf
from baseline.call_java_baseline import runBaseLine,init_jvm
import sys
from flaskr.algorithms.SCVPplus.baseline.drl_partitioner import Drl_Partitioner
# 忽略警告
warnings.filterwarnings("ignore")
# 初始化负载参数
attrs_length_dict={
    'randomAttr':{
        '30attr':[8]*30,
        '50attr':[8]*50,
        '100attr':[8]*100,
        '200attr':[8]*200,
        '500attr':[8]*500,
    },
    'randomQue':{
        '50query':[8]*50,
        '200query':[8]*50,
        '500query':[8]*50,
        '1000query':[8]*50,
        '2000query':[8]*50,
        '5000query':[8]*50,
    },
    'tpch':{
        'customer':[4,25,40,4,15,4,10,117],
        'lineitem':[4,8,8,4,15,15,15,15,1,1,10,10,10,25,10,44],
        'nation':[4,25,4,152],
        'orders':[4,4,1,4,10,15,15,4,79],
        'part':[4,55,25,10,25,4,10,4,23],
        'partsupp':[4,4,4,4,199],
        'region':[4,25,152],
        'supplier':[4,25,40,4,15,4,101],
        'airplanes':[20,100,20,100,30,30,30,30,30,30,30,30,30,30,20],
        'widetable':[8]*50
    },
    'ssb':{
        'customer':[4,25,25,10,15,12,15,10],
        'date':[4, 18, 8, 9, 4, 4, 7, 4, 4, 4, 4, 4, 12, 1, 1, 1, 1],
        'lineorder':[4, 4, 4, 4, 4, 4, 15, 1, 4, 4, 4, 4, 4, 4, 4, 4, 10],
        'part':[4, 22, 6, 7, 9, 11, 25, 4, 10],
        'supplier':[4, 25, 25, 10, 15, 12, 15],
    }
}
test_cases={
        'randomAttr':[
            ("data/random/30attr.csv",30),
            ("data/random/50attr.csv",50),
            ("data/random/100attr.csv",100),
            ("data/random/200attr.csv",200),
        ],
        'randomQue':[
            ("data/random/200query.csv",50),
            ("data/random/500query.csv",50),
            ("data/random/1000query.csv",50),
            ("data/random/2000query.csv",50),
            ("data/random/5000query.csv",50),
        ],
        'tpch':[
            ("data/tpch/customer.csv",8),
            ("data/tpch/lineitem.csv",16),
            ("data/tpch/nation.csv",4),
            ("data/tpch/orders.csv",9),
            ("data/tpch/part.csv",9),
            ("data/tpch/partsupp.csv",5),
            ("data/tpch/region.csv",3),
            ("data/tpch/supplier.csv",7),
            ("data/tpch/airplanes.csv",15),
            ("data/tpch/widetable.csv",50)
        ],
        'ssb':[
            ("data/ssb/customer.csv",8),
            ("data/ssb/date.csv",17),
            ("data/ssb/lineorder.csv",17),
            ("data/ssb/part.csv",9),
            ("data/ssb/supplier.csv",7)
        ]
    }
        
gl._init_()
gl.set_value("test_cases",test_cases)
gl.set_value("attrs_length_dict",attrs_length_dict)
import pandas as pd
from ClimbHill import compute_cost_by_climb_hill
from affinity_support import compute_cost_by_affinity_support 
from frequent_pattern import compute_cost_by_frequent_pattern
from ColumnCluster import compute_cost_by_spectal_cluster
from ColumnClusterPlus import compute_cost_by_spectal_cluster_R
from MemoryCost import cal_total_memory_cost
import DiskCost as DC
from DiskCost import cal_avg_tuple_reconstruction_joins,cal_column_group_effectiveness
from DiskCost import cal_total_cost_improvement as cal_total_cost_update
from utils import read_query_data
from Test_TPCH_DDL import cal_subtable_by_partition_time
def specialPartition(file_input_info):
    unsolved_paritions=[]
    complete_parition=[]
    for attr in range(file_input_info['attr_num']):
        complete_parition.append([attr])
        unsolved_paritions.append(attr)
    return [unsolved_paritions],complete_parition

def runAlgorithm(dataset,costmodel):
    cost_results=[]
    execution_cost_results=[]
    run_time_results=[]
    tuple_reconstruction_joins=[]
    column_group_effectiveness=[]
    # methods=["pure_NSM","pure_DSM","Climb Hill's","Lisbeth Rodriguez's","Huang Yin-fu's","SCVP's"]
    methods=["pure_NSM","pure_DSM","ClimbHill","Rodriguez","Huang","SCVP"]
    datasets=[]
    otherAlgorithmResult=runBaseLine(dataset,costmodel)
    
    for test_case in test_cases[dataset]:
        path,attr_num=test_case[0],test_case[1]
        print(path)
        data_name=path.split("/")[-1].replace(".csv","")
        datasets.append(data_name)
        attrs_length=attrs_length_dict[dataset][data_name]
        file_input_info={
            'path':path,
            "attr_num":attr_num,
            "attrs_length":attrs_length
        }
        # 成本模型estimated Cost
        case_cost=[]
        # 数据库实测 reudced time(s)
        case_time_cost=[]
        # 算法运行时间 run time(s)
        run_time_cost=[]

        partitions=[]
        time0=time.time()
        unsolved_paritions,complete_parition=specialPartition(file_input_info)
        partitions.append(unsolved_paritions)
        partitions.append(complete_parition)

        time1=time.time()
        run_time_cost.append(time1-time0)
        run_time_cost.append(time1-time0)

        print("```````````````````%s`````````````````"%("Climb Hill's"))
        if(data_name=='200attr'):
            partitions.append(complete_parition)
        else:
            partitions.append(compute_cost_by_climb_hill(file_input_info))

        time3=time.time()
        run_time_cost.append(time3-time1)
        
        print("```````````````````%s`````````````````"%("Lisbeth Rodríguez's"))
        partitions.append(compute_cost_by_affinity_support(file_input_info))
        time4=time.time()
        run_time_cost.append(time4-time3)

        print("```````````````````%s`````````````````"%("Huang Yin-fu's"))
        partitions.append(compute_cost_by_frequent_pattern(file_input_info))
        time5=time.time()
        run_time_cost.append(time5-time4)

        print("```````````````````%s`````````````````"%("Scvp's"))
        partitions.append(compute_cost_by_spectal_cluster(file_input_info))
        time6=time.time()
        run_time_cost.append(time6-time5)

        if data_name in otherAlgorithmResult['partitions']:
            for methodName in otherAlgorithmResult['partitions'][data_name].keys():
                if(methodName=='DREAM'):continue
                if(methodName not in methods): methods.append(methodName)
                partitions.append(otherAlgorithmResult['partitions'][data_name][methodName])
                run_time_cost.append(otherAlgorithmResult['runTimes'][data_name][methodName])
                if (data_name == 'lineitem' and methodName=='O2P'):
                    partitions.append([])
                    run_time_cost.append(0)

        affinity_matrix,querys=read_query_data(file_input_info['path'],file_input_info['attr_num'])
        case_reconstruction_joins=[]
        case_cg_effectiveness=[]
        for par in partitions:
            partitions_cost=cal_total_cost_update(querys,par,file_input_info['attrs_length'])
            # partitions_cost=cal_total_memory_cost(querys,par,file_input_info['attrs_length'])
            case_cost.append(partitions_cost)
            case_reconstruction_joins.append(cal_avg_tuple_reconstruction_joins(querys,par,file_input_info['attrs_length']))
            case_cg_effectiveness.append(cal_column_group_effectiveness(querys,par,file_input_info['attrs_length']))


        cost_results.append(case_cost)
        run_time_results.append(run_time_cost)
        tuple_reconstruction_joins.append(case_reconstruction_joins)
        column_group_effectiveness.append(case_cg_effectiveness)
        # execution_cost_results.append(case_time_cost)

    print(methods)
    for i in range(len(cost_results)):
        # print(cost_results[i])
        for j in range(len(cost_results[i])):
            print("%.2f "%(cost_results[i][j]),end='')
            # cost_results[i][j]=round(cost_results[i][j],3)
        print("")
    for i in range(len(execution_cost_results)):
        for j in range(len(execution_cost_results[i])):
            print("%.4f "%(execution_cost_results[i][j]),end='')
        print("")
    
    print(methods)
    for i in range(len(run_time_results)):
        for j in range(len(run_time_results[i])):
            print("%.4f "%(run_time_results[i][j]),end='')
        print("")
    tuple_reconstruction_joins=np.array(tuple_reconstruction_joins).transpose()
    column_group_effectiveness=np.array(column_group_effectiveness).transpose()
    avg_tuple_reconstruction_joins=[np.mean(alog) for alog in tuple_reconstruction_joins]
    avg_column_group_effectiveness=[np.mean(alog) for alog in column_group_effectiveness]

    # writeResToExcel(cost_results,methods,datasets,"%s-%s-cost-result"%(dataset,costmodel))
    # writeResToExcel(run_time_results,methods,datasets,"%s-%s-time-result"%(dataset,costmodel))

    # writeResToExcel([avg_tuple_reconstruction_joins], methods, [dataset], "%s-%s-reconstruction-join-cost" % (dataset, costmodel))
    # writeResToExcel([avg_column_group_effectiveness], methods, [dataset], "%s-%s-column-group-effectiveness" % (dataset, costmodel))
def writeResToExcel(cost_results,col_names,row_names,fname):
    res_pd=pd.DataFrame(cost_results,
    index=row_names,
    columns=col_names)
    res_pd.to_csv("result/"+fname+".csv",encoding="utf-8")

# 2023/03/08
import sys
sys.path.append('/home/liupengju/pycharmProjects/partition-api/')
from flaskr.algorithms.SCVPplus.hp_partitioner import Scvp_Plus
from flaskr.algorithms.SCVPplus.q_parser import get_all_tab_rows

def test_new_cost_model(benchmark,tab,benchmark_path):

    # benchmark='synthetic'
    # test_tab='widetable30'
    # benchmark='tpch'
    # test_tab='customer'
    # benchmark_path='tpchpro'
    # benchmark='job'
    # test_tab='title'
    # benchmark_path='job'
    vps={}
    queries=[]
    partitions=[]
    execution_time={'AVP-RL':[],'SCVP':[],'latency_list':[]}
    file_input_info={
        'path':f'/home/liupengju/pycharmProjects/partition-api/flaskr/algorithms/SCVP/data/{benchmark_path}/{tab}.csv',
        "attrs_length":col_inf[tab]['length'],
        "attr_num":len(col_inf[tab]['length']),
        'tab':tab
    }
    print('Current Tab:',tab)
    DC.tab_name=tab
    DC.table_rows=get_all_tab_rows(tab)
    NSM,DSM=specialPartition(file_input_info)
    partitions.append(NSM)
    partitions.append(DSM)
    time0=time.time()
    # partitions.append(Drl_Partitioner().load(tab))
    partitions.append(compute_cost_by_climb_hill(file_input_info))
    print('AVP-RL time:',time.time()-time0)
    execution_time['AVP-RL'].append(time.time()-time0)
    # partitions.append([[0], [1, 46, 21, 32, 42], [2], [3, 12, 55], [4], [5], [6], [7], [8], [9, 57, 87, 43, 53, 77], [10], [11], [13], [14, 44], [15], [16], [17], [18], [19], [20], [22], [23], [24, 82, 56], [25, 69, 59, 98, 90, 71], [26], [27], [28], [29], [30], [31], [33], [34], [35], [36], [37, 92, 91], [38], [39], [40], [41], [45], [47], [48], [49], [50], [51], [52], [54], [58], [60, 78], [61], [62], [63], [64], [65], [66], [67], [68], [70], [72], [73], [74], [75], [76], [79], [80], [81], [83], [84], [85], [86], [88], [89], [93], [94], [95], [96], [97], [99]])
    # partitions.append([[59, 90, 98], [14, 44], [95], [0], [2], [4], [5], [6], [7], [8], [10], [11], [13], [15], [16], [17], [18], [19], [20], [22], [23], [26], [27], [28], [29], [30], [31], [33], [34], [35], [38], [39], [40], [41], [45], [47], [49], [50], [51], [52], [54], [58], [61], [62], [63], [64], [65], [66], [67], [68], [70], [72], [73], [74], [75], [76], [79], [80], [81], [83], [84], [85], [86], [88], [89], [93], [94], [96], [97], [99], [36], [60, 78], [91, 92, 37], [9, 57, 87, 43, 53, 77], [56, 82, 24], [1, 21, 46, 32, 42], [25, 69, 71], [3, 48, 55, 12]])
    
    time1=time.time()
    partitions.append(compute_cost_by_spectal_cluster(file_input_info))
    print('SCVP time:',time.time()-time1)
    execution_time['SCVP'].append(time.time()-time1)
    vps[tab]=partitions
    _,querys=read_query_data(file_input_info['path'],file_input_info['attr_num'])
    for par_scheme in partitions:
        par_cost=cal_total_cost_update(querys,par_scheme,file_input_info['attrs_length'],print_info=False)
        print(par_cost,par_scheme)
    queries+=Scvp_Plus().load_encode_queryset(file_input_info['path'],benchmark,tab)
    # exit(-1)
    strategies=[0,0,0,1]
    scvp=Scvp_Plus()
    # queries=scvp.input_new_sqls(tabs=list(vps.keys()),benchmark=benchmark)
    latency_list=[]
    for tab in vps.keys():
        print('Current Tab:',tab)
        woods=[]
        for algo_id,par_scheme in enumerate(vps[tab]):
            # if algo_id!=3: continue
            refined_par_scheme=[list(range(len(col_inf[tab]['name'])))]
            if strategies[algo_id]==0 and len(woods)>=1:
                wood=woods[strategies[algo_id]]
                wood[tab].pages=par_scheme
            else:
                time2=time.time()
                wood={
                    tab:scvp.gen_trees_by_vp(benchmark,tab,refined_par_scheme,load_old=False,strategy=strategies[algo_id],mini_pages=par_scheme,queries=queries)
                }
                print('Tree construction time:',time.time()-time2)
                if strategies[algo_id]==0: execution_time['AVP-RL'].append(time.time()-time2)
                else: execution_time['SCVP'].append(time.time()-time2)
                woods.append(wood)
            total_latency=scvp.eval_tree_latency(wood,queries)
            # total_latency=scvp.eval_tree_blocks(wood,queries)
            latency_list.append(total_latency)
            print(algo_id,total_latency)
    print(f'Tab {tab} :{latency_list}')
    execution_time['latency_list']=latency_list
    return execution_time
# construct lineitem scvp tree: 243.026(s)  load raw data 3.687(s)
def main():
    # 启动ray
    ray.init(address='auto', _redis_password='5241590000000000')
    init_jvm('vp-demo-20230315.jar')
    # datasets=['randomAttr','randomQue']
    datasets=['tpch','ssb']
    costmodels=['Huang','Son']
    for item in datasets:
        runAlgorithm(item,costmodels[0])
    ray.shutdown()

if __name__=="__main__":
    # main()
    ray.init()
    execution_time_dict={}
    # for tab in ["title","movie_info_idx","cast_info","movie_info","movie_keyword","movie_companies"]:
        # execution_time_dict[tab]=test_new_cost_model('job',tab,'job')
    for tab in ["widetable100"]:
    # "widetable30","widetable50",
        execution_time_dict[tab]=test_new_cost_model('synthetic',tab,'tpchpro')
    # for tab in ["customer","lineitem","nation","orders","part","partsupp","region","supplier"]:
    #     execution_time_dict[tab]=test_new_cost_model('tpch',tab,'tpchpro')
    print(execution_time_dict)