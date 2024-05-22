# import sys
# sys.path.append("..")
# from Main import *
from Main import writeResToExcel
import ray
from SCVP.ColumnClusterPlus import test_scvp_by_minsup_sensitivity
import numpy as np
from fp_growth_plus import Fp_growth_plus,load_data
import globalvar as gl
from DiskCost import cal_total_cost_update
from utils import read_query_data,prune_affinity_matrix
def getParByDataset(dataset,minsup_list):
    test_cases = gl.get_value("test_cases")
    attrs_length_dict = gl.get_value("attrs_length_dict")
    result={}
    for minsup in minsup_list:
        result[minsup]=[]
    for test_case in test_cases[dataset]:
        # 一个负载表
        path,attr_num=test_case[0],test_case[1]
        print(path)
        data_name=path.split("/")[-1].replace(".csv","")
        attrs_length = attrs_length_dict[dataset][data_name]
        file_input_info = {
            'path': path,
            "attr_num": attr_num,
            "attrs_length": attrs_length
        }
        # BASE_DIR='../'
        affinity_matrix, querys = read_query_data(file_input_info['path'], file_input_info['attr_num'])
        X, accessedAttr = prune_affinity_matrix(affinity_matrix)
        data_set, item_num = load_data(file_input_info['path'], accessedAttr, file_input_info['attr_num'])
        for minsup in minsup_list:
            min_support = minsup * item_num
            L, _ = Fp_growth_plus().generate_L(data_set, min_support)
            optimal_candidate_paritions = test_scvp_by_minsup_sensitivity(file_input_info,L)
            result[minsup].append(cal_total_cost_update(querys,optimal_candidate_paritions,attrs_length))
    return result

def Main():
    ray.init(address='auto', _redis_password='5241590000000000')
    # datasets=['tpch','ssb']
    datasets = ['randomAttr', 'randomQue']
    costmodel=['huang','son']
    minsup_list=np.arange(0,1,0.05)
    final_result=[]
    for dataset in datasets:
        temp=[]
        result=getParByDataset(dataset,minsup_list)
        for key in result:
            temp.append(np.mean(result[key]))
        final_result.append(temp)
    writeResToExcel(final_result,minsup_list,datasets,"scvp-random-%s-sensitivity-result"%(costmodel[0]))
    print(final_result)

if __name__ == '__main__':
    Main()