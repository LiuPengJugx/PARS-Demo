from utils import read_query_data
import numpy as np
# from DiskCost import cal_total_cost_update
# from MemoryCost import cal_total_memory_cost
# from Test_TPCH_DDL import cal_subtable_by_partition_time

def compute_cost_by_affinity_support(file_input_info):
    affinity_matrix,querys=read_query_data(file_input_info['path'],file_input_info['attr_num'])
    affinity_temp = affinity_matrix.astype(np.float64)
    attrs_length=file_input_info['attrs_length']
    
    total_access=sum([q['freq'] for q in querys])
    # 统计亲和度支持度并排序
    support_value_matrix={}
    for row in range(affinity_temp.shape[0]):
        for col in range(affinity_temp.shape[1]):
            affinity_temp[row][col]="%.2f"%np.true_divide(affinity_temp[row][col],total_access)
            if affinity_temp[row][col] in support_value_matrix.keys():
                support_value_matrix[affinity_temp[row][col]]+=1
            else:
                support_value_matrix[affinity_temp[row][col]]=0
    support_value_matrix_temp=sorted(support_value_matrix.items(),key=lambda dict:dict[0],reverse=False)
    support_value_matrix_sort=[]
    for tuple in support_value_matrix_temp:
        support_value_matrix_sort.append([tuple[0],tuple[1]])
    # vals=sorted(support_value_matrix.items(),key=lambda dict:dict[1],reverse=False)
    accumlated_value=0
    total_martix_size=len(affinity_temp)*len(affinity_temp)
    percent_high=total_martix_size*0.75
    percent_low=total_martix_size*0.71
    mini_sup=0
    for index,item in enumerate(support_value_matrix_sort):
        if index>0:
            support_value_matrix_sort[index][1]=support_value_matrix_sort[index][1]+support_value_matrix_sort[index-1][1]
        if support_value_matrix_sort[index][1]>=percent_high:
            if support_value_matrix_sort[index-1][1]>=percent_low:
                mini_sup=support_value_matrix_sort[index-1][0]
                break
            else:mini_sup=support_value_matrix_sort[index][0]
    # CBDA
    if mini_sup==0:mini_sup=0.001
    unallocated_attr=[i for i in range(file_input_info['attr_num'])]
    final_schema=[]
    for row in range(affinity_temp.shape[0]):
        if row not in unallocated_attr:continue
        candidate_attr_column=[row]
        for col in range(affinity_temp.shape[1]):
            if affinity_temp[row][col]>=mini_sup and col in unallocated_attr and col!=row:
                candidate_attr_column.append(col)
                unallocated_attr.remove(col)
        if len(candidate_attr_column)>1:
            final_schema.append(candidate_attr_column)
            unallocated_attr.remove(row)
    for attr in unallocated_attr:
        final_schema.append([attr])
    # cost=cal_total_cost_update(querys,final_schema,attrs_length)
    # cost=cal_total_memory_cost(querys,final_schema,attrs_length)
    # execution_cost=cal_subtable_by_partition_time(querys,file_input_info['path'],final_schema)
    print(final_schema)
    return final_schema