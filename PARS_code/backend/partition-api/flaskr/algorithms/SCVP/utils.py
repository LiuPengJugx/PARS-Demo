import pandas as pd
import numpy as np
from numpy import *
import math
def getUpdatedAvgSel(querys,workload_matrix,bc_par,b_size):
    n_avg_sel=[]
    bc_martix=mat(workload_matrix[:,bc_par])
    bc_array=np.asarray(bc_martix)
    n_matrix_ind=[[],[],[]]
    for ind,mr in enumerate(bc_array):
        if sum(mr)==0:continue
        elif querys[ind]['scan_key'] in bc_par:
            continue 
        elif sum(mr[:b_size])>=1 and sum(mr[b_size:])==0:
            n_matrix_ind[0].append(querys[ind])
        elif sum(mr[:b_size])==0 and sum(mr[b_size:])>=0:
            n_matrix_ind[1].append(querys[ind])
        else: 
            n_matrix_ind[2].append(querys[ind])
    for x in n_matrix_ind:
        n_avg_sel.append(get_average_sel(x))
    return n_avg_sel

def vectorPlusOperatorUpdate(querys,workload_matrix,b_par,c_par):
    # print(workload_matrix[:,5])
    # 截取b和c分区矩阵
    bc_par=b_par+c_par
    b_size,c_size=len(b_par),len(c_par)
    bc_martix=mat(workload_matrix[:,bc_par])
    # n1_matrix_dict,n2_matrix_val,n3_matrix_ind,n3_matrix_val=[],[],[],[]
    n_matrix_dict=[]
    for i in range(5):
        n_matrix_dict.append({
            'ind':[],
            'val':[]
        })
    bc_array=np.asarray(bc_martix)
    for ind,mr in enumerate(bc_array):
        if sum(mr)==0:continue
        elif querys[ind]['scan_key'] in b_par:
            n_matrix_dict[3]['ind'].append(ind)
            n_matrix_dict[3]['val'].append(querys[ind]['freq'])
        elif querys[ind]['scan_key'] in c_par:
            n_matrix_dict[4]['ind'].append(ind)
            n_matrix_dict[4]['val'].append(querys[ind]['freq'])
        elif sum(mr[:b_size])>0 and sum(mr[b_size:])==0:
            n_matrix_dict[0]['val'].append(querys[ind]['freq'])
            n_matrix_dict[0]['ind'].append(ind)
        elif sum(mr[:b_size])==0 and sum(mr[b_size:])>0:
            n_matrix_dict[1]['val'].append(querys[ind]['freq'])
            n_matrix_dict[1]['ind'].append(ind)
        else:
            n_matrix_dict[2]['val'].append(querys[ind]['freq'])
            n_matrix_dict[2]['ind'].append(ind)
    return n_matrix_dict
def vectorPlusOperator(querys,workload_matrix,b_par,c_par):
    # print(workload_matrix[:,5])
    # 截取b和c分区矩阵
    b_matrix=mat(workload_matrix[:,[e for e in b_par]])
    c_matrix=mat(workload_matrix[:,[e for e in c_par]])
    b_size,c_size=len(b_par),len(c_par)
    bc_martix=np.hstack([b_matrix,c_matrix])
    n1_matrix_val,n2_matrix_val,n3_matrix_ind,n3_matrix_val=[],[],[],[]
    # n_feat_vector=mat(np.hstack([ones((1,b_size+c_size))]))
    # n_matrix_result=multiply(n_feat_vector,b_matrix)
    bc_array=np.asarray(bc_martix)
    for ind,mr in enumerate(bc_array):
        if sum(mr[:b_size])>0 and sum(mr[b_size:])==0:n1_matrix_val.append(querys[ind]['freq'])
        elif sum(mr[:b_size])==0 and sum(mr[b_size:])>0:n2_matrix_val.append(querys[ind]['freq'])
        elif sum(mr)==0:pass
        else:
            n3_matrix_ind.append(ind)
            n3_matrix_val.append(querys[ind]['freq'])

    # n2_matrix_result=multiply(mat(ones((1,len(b_par)))),b_matrix)
    # for ind,mr in enumerate(n2_matrix_result):
    #     if sum(mr)==0:n2_matrix_ind.append(ind)
    # n3_matrix_ind=[]
    # # n3_matrix_result=multiply(mat(ones((1,(len(b_par)+len(c_par))))),bc_martix)
    # for ind,mr in enumerate(n3_matrix_result):
    #     if sum(mr)==0:pass
    #     elif ind in n1_matrix_ind or ind in n2_matrix_ind:
    #         pass
    #     else:
    #         n3_matrix_ind.append(ind)   
    return sum(n1_matrix_val),sum(n2_matrix_val),sum(n3_matrix_val),n3_matrix_ind

def list_in_list(list1,list2):
    for x in list1:
        if x not in list2:
            return False
    return True

def list_solved_list(list1,list2):
    for x in list2:
        if x in list1:
            return True
    return False

def list_solved_list_content(list1,list2):
    solved_list=[]
    for x in list2:
        if list_solved_list(x,list1):
            solved_list.append(x)
    return solved_list

def get_average_sel(querys):
    if(len(querys)==0):return 0
    avg_sel=0
    sum_query=0
    for q in querys:
        avg_sel+=q['freq']*q['selectivity']
        sum_query+=q['freq']
    if sum_query==0:return 0
    return avg_sel/sum_query

def read_query_data(path,n):
    df=pd.read_csv(path)
    queries=df.to_dict('records')
    
    querys=[]
    affinity_matrix=np.array(([[0]*n])*n)
    for qid,q in enumerate(queries):
        value=[0]*n
        filter_attrs=eval(q['filter'])
        scan_attrs=eval(q['scan'])+filter_attrs
        for column_i in scan_attrs:
            value[column_i]=1
            affinity_matrix[column_i][column_i]+=1
            # 更新亲和度矩阵
            for i,v in enumerate(value[:column_i]):
                if v==1:
                    affinity_matrix[i][column_i]+=1
                    affinity_matrix[column_i][i]+=1
        gp_ob=eval(q['gp_ob'])
        querys.append({
            'value':value,
            'freq':1,
            # 'scan_key':df.iloc[row][2]-1,
            'scan_key':filter_attrs,
            'selectivity':q['sel'],
            'gp_ob':gp_ob
        })
    return affinity_matrix,querys

# def read_query_data(path,n):
#     df=pd.read_csv(path,header=None)
#     querys=[]
#     affinity_matrix=np.array(([[0]*n])*n)
#     for row in range(df.shape[0]):
#         value=[0]*n
#         for index in df.iloc[row][0].split(","):
#             column_i=int(index)
#             value[column_i]=1
#             affinity_matrix[column_i][column_i]+=df.iloc[row][1]
#             # 更新亲和度矩阵
#             for i,v in enumerate(value[:column_i]):
#                 if v==1:
#                     affinity_matrix[i][column_i]+=df.iloc[row][1]
#                     affinity_matrix[column_i][i]+=df.iloc[row][1]
#         # print(df.iloc[row][2])
#         # print(type(df.iloc[row][2]))
#         scan_keys=[]
#         gp_ob=[]
#         # if math.isnan(df.iloc[row][2]):
#         if (df.iloc[row][2])!=(df.iloc[row][2]):
#             pass
#         else:
#             scan_keys=[int(x) for x in str(df.iloc[row][2]).split(",")]
#         if (df.iloc[row][4])!=(df.iloc[row][4]):
#             pass
#         else:
#             if isinstance(df.iloc[row][4],float):
#                 gp_ob=[int(df.iloc[row][4])]
#             else:
#                 gp_ob=[int(x) for x in str(df.iloc[row][4]).split(",")]  
#         querys.append({
#             'value':value,
#             'freq':df.iloc[row][1],
#             # 'scan_key':df.iloc[row][2]-1,
#             'scan_key':scan_keys,
#             'selectivity':df.iloc[row][3],
#             'gp_ob':gp_ob
#         })
#     return affinity_matrix,querys

def read_query_data2(path,n):
    df=pd.read_csv(path,header=None)
    querys=[]
    affinity_matrix=np.array(([[0]*n])*n)
    for row in range(df.shape[0]):
        value=[0]*n
        for index in df.iloc[row][0].split(","):
            column_i=int(index)-1
            value[column_i]=1
            affinity_matrix[column_i][column_i]+=df.iloc[row][1]
            # 更新亲和度矩阵
            for i,v in enumerate(value[:column_i]):
                if v==1:
                    affinity_matrix[i][column_i]+=df.iloc[row][1]
                    affinity_matrix[column_i][i]+=df.iloc[row][1]
        querys.append({
            'value':value,
            'freq':df.iloc[row][1],
            'scan_key':df.iloc[row][2]-1,
            'selectivity':df.iloc[row][3]
        })
    return affinity_matrix,querys
def prune_affinity_matrix(affinity_matrix):
    affinity_matrix_copy=affinity_matrix.copy()
    accessedAttr=[]
    for i,row in enumerate(affinity_matrix):
        if sum(row)>0:
             accessedAttr.append(i)
    return affinity_matrix_copy[accessedAttr,:][:,accessedAttr],np.array(accessedAttr)

