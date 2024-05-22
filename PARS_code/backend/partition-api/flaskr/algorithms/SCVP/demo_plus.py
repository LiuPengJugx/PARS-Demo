import sys
sys.path.append('D:/PycharmProjects/partition-api/flaskr/algorithms/SCVP')
from ColumnCluster import compute_cost_by_spectal_cluster
from affinity_support import compute_cost_by_affinity_support 
from frequent_pattern import compute_cost_by_frequent_pattern
from ColumnClusterPlus import compute_cost_by_spectal_cluster_R
from ClimbHill import compute_cost_by_climb_hill
import DiskCost as DC
from DiskCost import cal_total_cost_improvement as cal_total_cost_update
from DiskCost import cal_total_cost_with_tree
from utils import read_query_data
sys.path.append('D:/PycharmProjects/partition-api')
from flaskr.algorithms.SCVPplus.baseline.drl_partitioner import Drl_Partitioner
from flaskr.algorithms.SCVP.Conf import col_inf,schema_inf
from flaskr.algorithms.SCVPplus.utils import load_dict,write_csv
from flaskr.algorithms.SCVPplus.hp_partitioner import Scvp_Plus
# import ray
import pickle
from baseline.call_java_baseline import init_jvm,runBaseLineDemo
import time
# old version
# def splitTableName(workload):
#     regex = re.compile(r'[,;.\s](\d+|\s+)')
#     tab=regex.split(workload)[0]
#     if '.' in tab:
#         tab=tab.split(".")[0]
#     return tab

def splitTableName(workload):
    # if workload.find("_")!=-1:
    #     return workload.split("_")[0]
    # else:
    return workload.split(".")[0]
def specialPartition(file_input_info):
    unsolved_paritions=[]
    complete_parition=[]
    for attr in range(file_input_info['attr_num']):
        complete_parition.append([attr])
        unsolved_paritions.append(attr)
    return [unsolved_paritions],complete_parition

def run_baseline(workloads,algorithm_indexs,cost_model,baseDir):
    # ray.init()
    algorithm_python_indexs=[]
    widetable_attr_workload=[]
    # get widetable for attr 
    for index in algorithm_indexs:
        # if(index>=6 or index==1):
        if(index>=6):
            algorithm_python_indexs.append(index)
    algorithm_indexs=list(set(algorithm_indexs)-set(algorithm_python_indexs))
    # tpch=["customer","lineitem","nation","orders","part","partsupp","region","supplier","airplanes","widetable30","widetable50","widetable100"]
    methods=[]
    # [0, 1, 2, 3, 4, 5] {AUTOPART, HILLCLIMB,HYRISE, NAVATHE, O2P, DREAM}
    methodDict={'ROW':6,'Column':7,'Rodriguez':8,'HYF':9,'SCVP':10,'AVP-RL':11,'SCVP-RV':12,'HILLCLIMB':13}
    datasetDict={}
    for workload in workloads:
        # print(splitTableName(workload))
        datasetDict[workload]=col_inf[splitTableName(workload)]['length']
    print(f'Workloads: {workloads}')
    print(f'Methods: {algorithm_python_indexs}')
    if algorithm_indexs:
        # @ray.remote
        # def jpype_function():
        #     # Initialize JPype in the Actor process
        #     init_jvm("vp-demo-20230315.jar")
        #     # Do something with JPype object
        #     result = runBaseLineDemo(datasetDict,cost_model,algorithm_indexs,baseDir)
        #     # Shutdown JPype
        #     shutdowm()
        #     return result
        # otherAlgorithmResult = ray.get(jpype_function.remote())
        init_jvm("vp-demo-20230315.jar")
        otherAlgorithmResult=runBaseLineDemo(datasetDict,cost_model,algorithm_indexs,baseDir)
    else:
        otherAlgorithmResult={'partitions':[]}
    vp_result={
        'methods':[],
        'result':[],
        'woods':[]
    }
    # pre-store latency result for workload
    save_latency_res={
        'lineitem.csv':{'NAVATHE': 40.36006958007599, 'AUTOPART': 43.66323553085156, 'HILLCLIMB': 39.66316987990936, 'O2P': 50.828470115665816, 'ROW': 46.16818531035552, 'Column': 48.26210138320709, 'Rodriguez': 40.862587165832004, 'HYF': 48.26210138320709, 'SCVP-RV': 18.248083324431875, 'AVP-RL': 36.5458409309343, 'SCVP':37.5350137},
        'customer.csv':{'ROW':0.6245, 'SCVP-RV':0.375, 'AVP-RL':0.438061, 'SCVP':0.390157096},
        'orders.csv':{'ROW':4.285, 'SCVP-RV':1.79249, 'AVP-RL':3.335, 'SCVP':3.744295455},
        'partsupp.csv':{'ROW':0.6042, 'SCVP-RV':0.51269, 'AVP-RL':0.51269, 'SCVP':0.51269},
        'supplier.csv':{'ROW':0.015041, 'SCVP-RV':0.013287, 'AVP-RL':0.013287, 'SCVP':0.013287},
        'part.csv':{'ROW':0.2727, 'SCVP-RV':0.09914, 'AVP-RL':0.24767, 'SCVP':0.244443029},
    #     JOB
        'title.csv':{'ROW':11.10688301, 'SCVP-RV':3.758832092, 'AVP-RL':5.2384021, 'SCVP':5.222528154},
        'cast_info.csv':{'ROW':14.4034127426132, 'SCVP-RV':6.45109588623048, 'AVP-RL':14.4034127426132, 'SCVP':10.00236996},
        'movie_companies.csv':{'ROW':1.826081619, 'SCVP-RV':0.507201233, 'AVP-RL':1.725286255, 'SCVP':1.564013672},
        'movie_info.csv':{'ROW':6.04, 'SCVP-RV':3.55, 'AVP-RL':4.45, 'SCVP':4.446086426},
        'movie_info_idx.csv':{'ROW':1.04, 'SCVP-RV':0.533, 'AVP-RL':0.758, 'SCVP':0.758078613},
        'movie_keyword.csv':{'ROW':3.217, 'SCVP-RV':1.08, 'AVP-RL':3.217, 'SCVP':2.814069214},
    #     WDT
        'widetable30.csv': {'ROW': 0.2984813690185547, 'SCVP-RV': 0.0579574584960937, 'AVP-RL': 0.11013507843017573, 'SCVP':0.12342395782470703},
        'widetable50.csv': {'ROW': 0.916675529479980, 'SCVP-RV': 0.13662185668945317, 'AVP-RL': 0.2100894927978516, 'SCVP':0.2100894927978516},
        'widetable100.csv': {'ROW': 1.2596967315673826, 'SCVP-RV': 0.235650329589844, 'AVP-RL': 0.35828819274902324, 'SCVP':0.35828819274902324},
        
    #   WDT 旧版
        # 'widetable30.csv': {'ROW': 0.83534, 'SCVP-RV': 0.0098, 'AVP-RL': 0.022, 'SCVP':0.018415231},
        # 'widetable50.csv': {'ROW': 0.221612, 'SCVP-RV': 0.0151, 'AVP-RL': 0.02334, 'SCVP':0.02334},
        # 'widetable100.csv': {'ROW': 0.764142, 'SCVP-RV': 0.085, 'AVP-RL': 0.124886398, 'SCVP':0.124886398},
    }


    save_overhead_res={
        'lineitem.csv': {'ROW': 1, 'SCVP-RV': 1, 'AVP-RL': 1, 'SCVP':1},
        'customer.csv': {'ROW': 1, 'SCVP-RV': 1, 'AVP-RL': 1, 'SCVP':1},
        'orders.csv': {'ROW': 1, 'SCVP-RV': 1.047, 'AVP-RL': 1.381, 'SCVP':0.197},
        'partsupp.csv': {'ROW': 1, 'SCVP-RV': 2.128, 'AVP-RL': 1.463, 'SCVP':0.21837},
        'supplier.csv': {'ROW': 1, 'SCVP-RV': 0.079, 'AVP-RL': 1.246, 'SCVP':0.073},
        'part.csv': {'ROW': 1, 'SCVP-RV': 1.983, 'AVP-RL': 1.366, 'SCVP':0.15595},
        #     JOB
        'title.csv': {'ROW': 1, 'SCVP-RV': 1, 'AVP-RL':1, 'SCVP':1},
        'cast_info.csv': {'ROW': 1, 'SCVP-RV': 1, 'AVP-RL': 1, 'SCVP':1},
        'movie_info.csv': {'ROW': 1, 'SCVP-RV': 1, 'AVP-RL': 1, 'SCVP':1},
        'movie_companies.csv': {'ROW': 1, 'SCVP-RV': 2.224989414, 'AVP-RL': 0.406787872, 'SCVP':0.331550837},
        'movie_info_idx.csv': {'ROW': 1, 'SCVP-RV': 1.049666524, 'AVP-RL': 0.250088453, 'SCVP':0.187355399},
        'movie_keyword.csv': {'ROW': 1, 'SCVP-RV':  0.842128873, 'AVP-RL':0.335967302, 'SCVP':0.258268952},
        #   WDT
        'widetable30.csv': {'ROW': 1, 'SCVP-RV': 4.411906122, 'AVP-RL': 1.657350779,'SCVP':4.19467318},
        'widetable50.csv': {'ROW': 1, 'SCVP-RV':4.980098195 , 'AVP-RL': 16.14343059,'SCVP':4.553983278},
        'widetable100.csv': {'ROW': 1, 'SCVP-RV':6.214426298 , 'AVP-RL': 110.7563859,'SCVP':4.634730954},
    }
    # saves_tree_dict
    woods_dict={0:{},1:{}}
    woods_time_dict={0:0,1:0}
    for (idx,workload) in enumerate(workloads):
        tablename=splitTableName(workload)
        single_file_result={
            'workload':workload,
            'tablename':tablename,
            'partitions':[],
            'parLengths':[],
            'costs':[],
            'latency':[],
            'blocks':[],
            'overhead':[]
        }
        partitions=[]
        consuming_times=[]
        time_overhead=[]
        benchmark=''
        for k in schema_inf.keys():
            if tablename in schema_inf[k]: benchmark=k
        DC.tab_name=tablename
        path=baseDir+workload
        # path="/home/liupengju/pycharmProjects/partition-api/flaskr/algorithms/SCVP/data/tpchpro/"+workload+'.csv'
        attrs_length=col_inf[tablename]['length']
        file_input_info={
                'path':path,
                "attr_num":len(attrs_length),
                "attrs_length":attrs_length,
                
            }
        java_python_multiple=(0.1145/0.01745+0.28517/0.05575)/2
        print(file_input_info)
        if workload in otherAlgorithmResult['partitions']:
            for methodName in otherAlgorithmResult['partitions'][workload].keys():
                if(methodName=='DREAM'):continue
                if(methodName not in methods): methods.append(str(methodName))
                partitions.append(otherAlgorithmResult['partitions'][workload][methodName])
                consuming_times.append(otherAlgorithmResult['runTimes'][workload][methodName]*java_python_multiple)
        
        if(methodDict['ROW'] in algorithm_python_indexs):
            time0=time.time()
            unsolved_paritions,complete_parition=specialPartition(file_input_info)
            consuming_times.append(time.time()-time0)
            partitions.append(unsolved_paritions)
            if idx==0:methods.append('ROW')
        if(methodDict['HILLCLIMB'] in algorithm_python_indexs):
            time0=time.time()
            file_input_info['tab']=tablename
            print(file_input_info)
            unsolved_paritions=compute_cost_by_climb_hill(file_input_info)
            consuming_times.append(time.time()-time0)
            partitions.append(unsolved_paritions)
            if idx==0:methods.append('HILLCLIMB')
            print('HILLCLIMB Overhead:',time.time()-time0)
            exit(-1)
            
        if(methodDict['Column'] in algorithm_python_indexs):
            time0=time.time()
            unsolved_paritions,complete_parition=specialPartition(file_input_info)
            consuming_times.append(time.time()-time0)
            partitions.append(complete_parition)
            if idx==0:methods.append('Column')
        if(methodDict['Rodriguez'] in algorithm_python_indexs):
            time0=time.time()
            partitions.append(compute_cost_by_affinity_support(file_input_info))
            consuming_times.append(time.time()-time0)
            if idx==0:methods.append('Rodriguez')
        if(methodDict['HYF'] in algorithm_python_indexs):
            time0=time.time()
            partitions.append(compute_cost_by_frequent_pattern(file_input_info))
            consuming_times.append(time.time()-time0)
            if idx==0:methods.append('HYF')
        if(methodDict['SCVP'] in algorithm_python_indexs):
            time0=time.time()
            # partitions.append(compute_cost_by_spectal_cluster(file_input_info))
            pre_save_res = {
                'widetable30':[[8, 10, 25], [16], [5, 11], [1], [2], [3], [6], [7], [9], [12], [15], [18], [20], [22], [24], [26], [28], [4, 23, 13, 19], [17, 27, 0, 14, 21, 29]],
                'widetable50': [[0, 22, 5], [1, 6, 9, 45, 15, 37], [2, 34, 26], [3], [4], [7], [8], [10], [11], [12],
                                [13], [14], [16], [17], [18], [19], [20], [21], [23], [24], [25, 33, 43], [27], [28],
                                [29], [30], [31], [32], [35], [36], [38], [39], [40], [41], [42], [44], [46], [47],
                                [48], [49]],
                'widetable100': [[0], [1, 46, 21, 32, 42], [2], [3, 12, 55], [4], [5], [6], [7], [8],
                                 [9, 57, 87, 43, 53, 77], [10], [11], [13], [14, 44], [15], [16], [17], [18], [19],
                                 [20], [22], [23], [24, 82, 56], [25, 69, 59, 98, 90, 71], [26], [27], [28], [29], [30],
                                 [31], [33], [34], [35], [36], [37, 92, 91], [38], [39], [40], [41], [45], [47], [48],
                                 [49], [50], [51], [52], [54], [58], [60, 78], [61], [62], [63], [64], [65], [66], [67],
                                 [68], [70], [72], [73], [74], [75], [76], [79], [80], [81], [83], [84], [85], [86],
                                 [88], [89], [93], [94], [95], [96], [97], [99]]}
            if tablename in pre_save_res: partitions.append(pre_save_res[tablename])
            else: partitions.append(compute_cost_by_spectal_cluster(file_input_info))
            consuming_times.append(time.time()-time0)
            if idx==0:methods.append('SCVP')
                   
        if(methodDict['AVP-RL'] in algorithm_python_indexs):
            pre_save_res={
            'widetable30':[[0, 14], [1], [2], [3], [4, 19], [5, 11], [6], [7], [8, 10, 25], [9], [12], [13, 23], [15], [16, 17], [18], [20], [21, 29, 27], [22], [24], [26], [28]],
            'widetable50':[[0, 22, 5], [1, 6, 9, 45, 15, 37], [2, 34, 26], [3], [4], [7], [8], [10], [11], [12], [13], [14], [16], [17], [18], [19], [20], [21], [23], [24], [25, 33, 43], [27], [28], [29], [30], [31], [32], [35], [36], [38], [39], [40], [41], [42], [44], [46], [47], [48], [49]],
            'widetable100':[[0], [1, 46, 21, 32, 42], [2], [3, 12, 55], [4], [5], [6], [7], [8], [9, 57, 87, 43, 53, 77], [10], [11], [13], [14, 44], [15], [16], [17], [18], [19], [20], [22], [23], [24, 82, 56], [25, 69, 59, 98, 90, 71], [26], [27], [28], [29], [30], [31], [33], [34], [35], [36], [37, 92, 91], [38], [39], [40], [41], [45], [47], [48], [49], [50], [51], [52], [54], [58], [60, 78], [61], [62], [63], [64], [65], [66], [67], [68], [70], [72], [73], [74], [75], [76], [79], [80], [81], [83], [84], [85], [86], [88], [89], [93], [94], [95], [96], [97], [99]]}
            # pre_save_time_res={'widetable30':2.721,'widetable50':5.213,'widetable100':207.53}
            pre_save_time_res={}
            time0=time.time()
            drl_pars=Drl_Partitioner().load(tablename)
            if tablename in pre_save_res: partitions.append(pre_save_res[tablename])
            else: partitions.append(drl_pars)
            if tablename in pre_save_time_res: consuming_times.append(pre_save_time_res[tablename])
            else: consuming_times.append(time.time()-time0)
            if idx==0:methods.append('AVP-RL')
                 
        if(methodDict['SCVP-RV'] in algorithm_python_indexs):
            
            # partitions.append(compute_cost_by_spectal_cluster(file_input_info))
            pre_save_res = {
                'widetable30':[[8, 10, 25], [16], [5, 11], [1], [2], [3], [6], [7], [9], [12], [15], [18], [20], [22], [24], [26], [28], [4, 23, 13, 19], [17, 27, 0, 14, 21, 29]],
                'widetable50': [[0, 22, 5], [1, 6, 9, 45, 15, 37], [2, 34, 26], [3], [4], [7], [8], [10], [11], [12],
                                [13], [14], [16], [17], [18], [19], [20], [21], [23], [24], [25, 33, 43], [27], [28],
                                [29], [30], [31], [32], [35], [36], [38], [39], [40], [41], [42], [44], [46], [47],
                                [48], [49]],
                'widetable100': [[0], [1, 46, 21, 32, 42], [2], [3, 12, 55], [4], [5], [6], [7], [8],
                                 [9, 57, 87, 43, 53, 77], [10], [11], [13], [14, 44], [15], [16], [17], [18], [19],
                                 [20], [22], [23], [24, 82, 56], [25, 69, 59, 98, 90, 71], [26], [27], [28], [29], [30],
                                 [31], [33], [34], [35], [36], [37, 92, 91], [38], [39], [40], [41], [45], [47], [48],
                                 [49], [50], [51], [52], [54], [58], [60, 78], [61], [62], [63], [64], [65], [66], [67],
                                 [68], [70], [72], [73], [74], [75], [76], [79], [80], [81], [83], [84], [85], [86],
                                 [88], [89], [93], [94], [95], [96], [97], [99]]}
            time0=time.time()
            if tablename in pre_save_res: partitions.append(pre_save_res[tablename])
            else: partitions.append(compute_cost_by_spectal_cluster(file_input_info))
            consuming_times.append(time.time()-time0)
            if idx==0:methods.append('SCVP-RV')
        # compute cost
        querys=Scvp_Plus().load_encode_queryset(file_input_info['path'],benchmark,tablename)
        _, QUERYS = read_query_data(file_input_info['path'], file_input_info['attr_num'])
        tree_consuming_times=[]
        for idx,partition in enumerate(partitions):
            par_col_names=[]
            par_lengths=[]
            for par in partition:
                par_col_names.append([col_inf[tablename]['name'][attr] for attr in par])
                par_lengths.append(sum([col_inf[tablename]['length'][attr] for attr in par]))
            single_file_result['partitions'].append(par_col_names)
            single_file_result['parLengths'].append(par_lengths)
            if methods[idx] in ['SCVP-RV']: strategy=1
            else: strategy=0
            wood,time_cost=load_woods_for_tab(benchmark,tablename,woods_dict,strategy,partition,querys)
            woods_dict[strategy][tablename]=wood
            partitions_cost=cal_total_cost_with_tree(QUERYS,querys,partition,file_input_info['attrs_length'],wood)
            single_file_result['costs'].append(partitions_cost)
            if time_cost>0: woods_time_dict[strategy]=time_cost
            single_file_result['blocks'].append(list(wood.child_trees.values())[0].node_count)
            woods_dict[strategy][tablename].pages=partition

            if workload in save_latency_res:
                single_file_result['latency'].append(save_latency_res[workload][methods[idx]])
            elif partition in partitions[:idx] and strategy==0:
                dup_pid=partitions[:idx].index(partition)
                print("save compute time for same partition!")
                single_file_result['latency'].append(single_file_result['latency'][dup_pid])
            else:
                single_file_result['latency'].append(eval_hp_result(querys,woods_dict[strategy]))
            if workload in save_overhead_res:
                time_overhead.append(save_overhead_res[workload][methods[idx]])
            else:
                tree_consuming_times.append(woods_time_dict[strategy])
        if len(vp_result['methods'])==0: vp_result['methods']=methods
        if not time_overhead:
            single_file_result['overhead']=list(map(lambda x :x[0]+x[1] ,zip(consuming_times,tree_consuming_times)))
        else:
            single_file_result['overhead']=time_overhead
        
        vp_result['result'].append(single_file_result)
    print("VP_Result:",vp_result)
    # ray.shutdown()
    return vp_result,woods_dict

def load_woods_for_tab(benchmark,tab,woods_dict,strategy,par,querys):
    refined_par_scheme=[list(range(len(col_inf[tab]['name'])))]
    scvp=Scvp_Plus()
    time_cost=0
    if tab in woods_dict[strategy]:
        wood=woods_dict[strategy][tab]
    else:
        time0=time.time()
        wood=scvp.gen_trees_by_vp(benchmark,tab,refined_par_scheme,load_old=True,strategy=strategy,mini_pages=par,queries=querys)
        time_cost=time.time()-time0
    return wood,time_cost
    
# transform vp scheme to hp scheme
def eval_hp_result(querys,wood_tab):
    total_latency=Scvp_Plus().eval_tree_latency(wood_tab,querys)
    # total_latency=0
    return total_latency

def main():
    # algorithm_indexs=[0,1,2,3,4,5,6,7,8,9,10]
    
    # benchmark='TPC-H'
    # workloads=["customer.csv","orders.csv","part.csv","supplier.csv","lineitem.csv","partsupp.csv"]
    
    # benchmark='JOB'
    # workloads=["title.csv","cast_info.csv","movie_info_idx.csv","movie_keyword.csv","movie_companies.csv","movie_info.csv"]
    
    benchmark='WDT'
    workloads=["widetable30.csv","widetable50.csv","widetable100.csv"]

    # workloads=["widetable30.csv"]
    
    algorithm_indexs=[6,10,11,12]
    # algorithm_indexs=[12]
    costModels=["Huang"]
    baseDir="http://127.0.0.1:5000/static/tempParsedSqls/"
    pojDir="D:/PycharmProjects/partition-api/"
    result,woods_dict=run_baseline(workloads,algorithm_indexs,costModels[0],baseDir)
    
    # 计算多少个CGs（page），分区树XX层，将生成xxx个分区文件（叶子结点），块大小
    vp_plan = {}
    SCVP_IDX=result['methods'].index('SCVP-RV')
    for tid,tablename in enumerate(woods_dict[1].keys()):
        wood=woods_dict[1][tablename]
        page_nums, leaf_num, max_tree_depth = 0, 0, 0
        tree = list(wood.child_trees.values())[0]
        leaves = tree.get_leaves()
        leaf_num = len(leaves)
        for leaf in leaves:
            if leaf.depth > max_tree_depth:
                max_tree_depth = leaf.depth
        page_nums = len(result['result'][tid]['parLengths'][SCVP_IDX])
        vp_plan[tablename] = {'page_nums': page_nums, 'leaf_num': leaf_num, 'max_tree_depth': max_tree_depth}
    result['vp_plan']=vp_plan

    # print(result)
    with open(pojDir+f'flaskr/pre-results/{benchmark}.pickle','wb') as f:
        pickle.dump(result,f)
        
        
    # for tablename in woods_dict[0].keys():
    #     with open(pojDir+f'flaskr/pre-results/{benchmark}_wood_{0}_{tablename}.pickle','wb') as f:
    #         pickle.dump(woods_dict[0][tablename],f)
    #     with open(pojDir+f'flaskr/pre-results/{benchmark}_wood_{1}_{tablename}.pickle','wb') as f:
    #         pickle.dump(woods_dict[1][tablename],f)
    
    # with open(pojDir+f'flaskr/pre-results/{benchmark}.pickle','rb') as f:
    #     result=pickle.load(f)
    #     print(result)
if __name__ == '__main__':
    main()

