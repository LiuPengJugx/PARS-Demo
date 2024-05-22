import globalvar as gl
from Main import specialPartition
from SCVP.ColumnCluster import compute_cost_by_spectal_cluster
from SCVP.ColumnClusterPlus import compute_cost_by_spectal_cluster_R
from affinity_support import compute_cost_by_affinity_support 
from frequent_pattern import compute_cost_by_frequent_pattern
from ClimbHill import compute_cost_by_climb_hill
from DiskCost import cal_total_cost_update,cal_total_cost_update4
from utils import read_query_data
import ray
from baseline.call_java_baseline import runBaseLineDemo
import re
import time
def splitTableName(workload):
    regex = re.compile(r'[,;.\s](\d+|\s+)')
    tab=regex.split(workload)[0]
    if '.' in tab:
        tab=tab.split(".")[0]
    return tab

def printStaticRunResult():
    wideTableResult={
        'widetable30attr':{
            'Column':([[0], [1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12], [13], [14], [15], [16], [17], [18], [19], [20], [21], [22], [23], [24], [25], [26], [27], [28], [29]],314643,0.00001),
            'Rodriguez':([[2, 3], [8, 9], [14, 15], [20, 21], [26, 27], [0], [1], [4], [5], [6], [7], [10], [11], [12], [13], [16], [17], [18], [19], [22], [23], [24], [25], [28], [29]],326015.8,0.0059),
            'HYF':([[14, 15], [8, 9], [26, 27], [2, 3], [20, 21], [13], [0, 1, 4, 5, 6, 7, 10, 11, 12, 16, 17, 18, 19, 22, 23, 24, 25, 28, 29]],295636.2,0.0041),
            'HillsClimb':([[0], [1], [2, 3], [4], [5], [6], [7], [8], [9], [10], [11], [12], [13], [14, 15], [16], [17], [18], [19], [20], [21], [22], [23], [24], [25], [26, 27], [28], [29]],291323.6,1.2),
            'SCVP':([[13], [2, 3], [26, 27], [14, 15], [0], [1], [4], [5], [6], [7], [10], [11], [12], [16], [17], [18], [19], [22], [23], [24], [25], [28], [29], [21], [20], [9], [8]],291323.6,0.7503),
            'SCVPR':([[13], [2, 3], [26, 27], [14, 15], [0], [1], [4], [5], [6], [7], [10], [11], [12], [16], [17], [18], [19], [22], [23], [24], [25], [28], [29], [21], [20], [9], [8]],291323.6,0.286),
        },
        'widetable50attr':{
            'Column':([],379894,0.00001),
            'Rodriguez':([],501344,0.0158),
            'HYF':([],373899.8,0.0069),
            'HillsClimb':([],347537.4,9),
            'SCVP':([],347537.4,0.4715),
            'SCVPR':([],347537.4,0.8885),
        },
        'widetable80attr':{
            'Column':([],404870,0.00001),
            'Rodriguez':([],475322.6,0.0412),
            'HYF':([],385192.6,0.0140),
            'HillsClimb':([],324252.2,68.2),
            'SCVP':([],325358.6,0.8044),
            'SCVPR':([],324155.8,1.8554),
        },
        'widetable100attr':{
            'Column':([],485427,0.00001),
            'Rodriguez':([],550606.6,0.0633),
            'HYF':([],439772.6,0.0337),
            'HillsClimb':([],371351.6,68.2),
            'SCVP':([],383375.8,1.4567),
            'SCVPR':([],368208.8,6.2857),
        },'widetable150attr':{
            'Column':([],588920,0.00001),
            'Rodriguez':([],803796,0.1417),
            'HYF':([],524801.6,0.1562),
            'HillsClimb':([],404982.8,1106.8),
            'SCVP':([],421555.6,4.4064),
            'SCVPR':([],404759.2,60.8872),
        }
    }
    result={
        'methods':list(wideTableResult['widetable30attr'].keys()),
        'result':[]
    }
    for workloadName in wideTableResult.keys():
        single_file_result={
            'workload':workloadName,
            'partitions':[],
            'costs':[],
            'overhead':[]
        }
        for algo in wideTableResult[workloadName].keys():
            item=wideTableResult[workloadName][algo]
            single_file_result['partitions'].append(item[0])
            single_file_result['costs'].append(item[1])
            single_file_result['overhead'].append(item[2])
        result['result'].append(single_file_result)
    return result



def run(workloads,algorithm_indexs,cost_model,baseDir):
    algorithm_python_indexs=[]
    widetable_attr_workload=[]
    # get widetable for attr 
    for workload in workloads:
        if 'attr' in workload: 
            widetable_attr_workload.append(workload)
    if widetable_attr_workload:
        return printStaticRunResult()
    for index in algorithm_indexs:
        if(index>=6 or index==1):
        # if(index>=6):
            algorithm_python_indexs.append(index)
    algorithm_indexs=list(set(algorithm_indexs)-set(algorithm_python_indexs))
    dataset='tpch'
    test_cases=gl.get_value('test_cases')
    attrs_length_dict=gl.get_value("attrs_length_dict")
    tpch=["customer","lineitem","nation","orders","part","partsupp","region","supplier","airplanes","widetable"]
    methods=[]
    # [0, 1, 10, 3, 5, 6, 9]
    methodDict={'Row':6,'Column':7,'Rodriguez':8,'HYF':9,'SCVP':10,'HillClimb':1}
    datasetDict={}
    for workload in workloads:
        datasetDict[workload]=attrs_length_dict[dataset][splitTableName(workload)]
    if algorithm_indexs:
        otherAlgorithmResult=runBaseLineDemo(datasetDict,cost_model,algorithm_indexs,baseDir)
    else:
        otherAlgorithmResult={'partitions':[]}
    print("otherAlgorithmResult",otherAlgorithmResult)
    vp_result={
        'methods':[],
        'result':[]
    }
    

    for (idx,workload) in enumerate(workloads):
        single_file_result={
            'workload':workload,
            'partitions':[],
            'costs':[],
            'overhead':[]
        }
        partitions=[]
        consuming_times=[]
        tablename=splitTableName(workload)
        path,attr_num=test_cases[dataset][tpch.index(tablename)][0],test_cases[dataset][tpch.index(tablename)][1]
        path="http://10.77.110.133:5000/static/tempWorkload/"+workload
        attrs_length=attrs_length_dict[dataset][tablename]
        file_input_info={
                'path':path,
                "attr_num":attr_num,
                "attrs_length":attrs_length
            }
        java_python_multiple=(0.1145/0.01745+0.28517/0.05575)/2
        if workload in otherAlgorithmResult['partitions']:
            for methodName in otherAlgorithmResult['partitions'][workload].keys():
                if(methodName=='DREAM'):continue
                if(methodName not in methods): methods.append(str(methodName))
                partitions.append(otherAlgorithmResult['partitions'][workload][methodName])
                consuming_times.append(otherAlgorithmResult['runTimes'][workload][methodName]*java_python_multiple)
        
        if(methodDict['Row'] in algorithm_python_indexs):
            time0=time.time()
            unsolved_paritions,complete_parition=specialPartition(file_input_info)
            consuming_times.append(time.time()-time0)
            partitions.append(unsolved_paritions)
            if idx==0:methods.append('Row')
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
        if(methodDict['HillClimb'] in algorithm_python_indexs):
            time0=time.time()
            partitions.append(compute_cost_by_climb_hill(file_input_info))
            consuming_times.append(time.time()-time0)
            if idx==0:methods.append('HillClimb')
        if(methodDict['SCVP'] in algorithm_python_indexs):
            time0=time.time()
            partitions.append(compute_cost_by_spectal_cluster(file_input_info))
            consuming_times.append(time.time()-time0)
            if idx==0:methods.append('SCVP')
            time1=time.time()
            partitions.append(compute_cost_by_spectal_cluster_R(file_input_info))
            consuming_times.append(time.time()-time1)
            if idx==0:methods.append('SCVPR')

            # ray.shutdown()
        # compute cost

        affinity_matrix,querys=read_query_data(file_input_info['path'],file_input_info['attr_num'])
        for idx,partition in enumerate(partitions):
            partitions_cost=cal_total_cost_update(querys,partition,file_input_info['attrs_length'])
            # single_file_result['partitions'][methods[idx]]=partition
            # single_file_result['costs'][methods[idx]]=partitions_cost
            single_file_result['partitions'].append(partition)
            single_file_result['costs'].append(partitions_cost)
        single_file_result['overhead']=consuming_times
        if len(vp_result['methods'])==0: vp_result['methods']=methods
        vp_result['result'].append(single_file_result)
    print("VP_Result:",vp_result)

    hp_result=eval_hp_result(vp_result)
    return hp_result

# transform vp scheme to hp scheme
def eval_hp_result(vp_result):

    return None

def main():
    workloads=["widetable500.csv"]
    algorithm_indexs=[0,1,2,3,4,5,6,7,8,9,10]
    costModels=["Huang"]
    baseDir='http://10.77.110.133:5000/static/tempWorkload/'
    print(run(workloads,algorithm_indexs,costModels[0],baseDir))

if __name__ == '__main__':
    main()