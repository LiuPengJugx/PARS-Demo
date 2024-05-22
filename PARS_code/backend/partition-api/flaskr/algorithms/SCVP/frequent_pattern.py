from utils import read_query_data
from utils import list_in_list
import globalvar as gl
import numpy as np
from DiskCost import cal_total_cost_update
from fp_growth_plus import load_data
from fp_growth_plus import Fp_growth_plus

def split_all_candidate_paritions(complete_column_range,querys):
    data_set,_=load_data(complete_column_range,querys)
    # min_support=0.2*item_num#最小支持度
    min_support=0 #最小支持度
    fp=Fp_growth_plus()
    L,support_data=fp.generate_L(data_set,min_support)
    total_candidate_paritions=[]
    for k in reversed(range(0,len(L))):
        update_column_range=complete_column_range.copy()
        complete_column_range_copy=complete_column_range.copy()
        candidate_paritions=[]
        for itemset in reversed(L[:k+1]):
            if len(update_column_range)==0:break
            # 生成所有候选方案
            while(True):
                current_selected_item=[]
                freq_max=0
                for key in itemset:
                    if itemset[key]>freq_max and list_in_list([x for x in key],update_column_range):
                        freq_max=itemset[key]
                        current_selected_item=[x for x in key]
                if freq_max==0:break
                for selected_item in current_selected_item:
                    update_column_range.remove(selected_item)
                candidate_paritions.append(current_selected_item)
                complete_column_range_copy=list(set(complete_column_range_copy)-set(current_selected_item))
        # print(candidate_paritions)
        if(len(complete_column_range_copy)>0):candidate_paritions.append(complete_column_range_copy)
        total_candidate_paritions.append(candidate_paritions)
    
    return total_candidate_paritions

def compute_cost_by_frequent_pattern(file_input_info):
    path,attr_num,attrs_length=file_input_info['path'],file_input_info['attr_num'],file_input_info['attrs_length']
    affinity_matrix,querys=read_query_data(path,attr_num)
    complete_column_range=[attr for attr in range(attr_num)]
    candidate_paritions=split_all_candidate_paritions(complete_column_range,querys)
    # 在所有的candidate_paritions中取最小值
    min_cost=float('inf')
    for candidate_parition in candidate_paritions:
        cost=cal_total_cost_update(querys,candidate_parition,attrs_length)
        # cost=cal_total_memory_cost(querys,candidate_parition,attrs_length)
        if cost<min_cost:
            min_cost=cost
            best_parition=candidate_parition
    # min_cost=cal_total_cost_update(querys,best_parition,attrs_length)
    # min_cost=cal_total_memory_cost(querys,best_parition,attrs_length)
    # execution_cost=cal_subtable_by_partition_time(querys,file_input_info['path'],best_parition)
    # final_schema_list.append(best_parition)
    # final_schema_list_cost.append(min_cost)
    # print(best_parition," ",min_cost)
    print(best_parition)
    return best_parition

