# import globalvar as gl
import math
import numpy as np
from utils import list_in_list
from utils import list_solved_list
from Conf import col_inf
# from utils import get_average_sel
# from utils import list_solved_list_content
import math
tid_length=4
cardinality=1000000
page_size=5000
block_factor=10
attribute_index=0
attribute_uncluster_index=-1
# 索引页和缓冲区页
index_page_num=2
buffer_page_num=50

# new set parameters date(2023.03.08)
tab_name='lineitem'
index_list=[0]
table_rows=1000000
# gl.set_value("tid_length",tid_length)
# gl.set_value("cardinality",cardinality)
# gl.set_value("page_size",page_size)
# gl.set_value("block_factor",block_factor)
# gl.set_value("attribute_index",attribute_index)

# 一个分区经过一次切分后，变化后的成本（修改过的成本函数）
def cal_estimate_cost_update(querys,split_paritions,attrs_length):
    estimate_total_cost=0
    attrs_parition=[]
    for x in split_paritions: attrs_parition+=x
    for query in querys:
        primary_fragment=[]
        secondary_fragments=[]
        tuple_length_primary_fragment=0
        solved_attrs=[i for i,x in enumerate(query['value']) if x==1 and i in attrs_parition]
        # 如果query与split_paritions无关，则直接跳过
        if len(solved_attrs)==0:continue
        if query['scan_key'] in solved_attrs:
            for parition in split_paritions:
                if parition.count(query['scan_key'])>0:
                    primary_fragment=parition
                elif list_in_list(parition,solved_attrs):
                    secondary_fragments.append(parition)
        else:
            secondary_fragments=split_paritions
        if len(primary_fragment)!=0:
            tuple_length_primary_fragment=sum([attrs_length[x] for x in primary_fragment])+tid_length
        sequential_secondary_cost=0
        for secondary_fragment in secondary_fragments:
            tuple_length_secondary_fragment=sum([attrs_length[x] for x in secondary_fragment])+tid_length
            sequential_secondary_cost+=math.ceil((cardinality*tuple_length_secondary_fragment)/(page_size*block_factor))
        sequential=math.ceil((cardinality*(tuple_length_primary_fragment))/(page_size*block_factor))+sequential_secondary_cost
        estimate_total_cost+=sequential*query['freq']
    return estimate_total_cost


# 一个分区经过一次切分后，变化后的成本函数
def cal_estimate_cost(querys,split_paritions,attrs_length):
    estimate_total_cost=0
    attrs_parition=[]
    for x in split_paritions: attrs_parition+=x
    for query in querys:
        primary_fragment=[]
        secondary_fragment=[]
        solved_attrs=[i for i,x in enumerate(query['value']) if x==1 and i in attrs_parition]
        # 如果query与split_paritions无关，则直接跳过
        if len(solved_attrs)==0:continue
        if query['scan_key'] in solved_attrs:
            for parition in split_paritions:
                if parition.count(query['scan_key'])>0:
                    primary_fragment=parition
                elif list_in_list(parition,solved_attrs):
                    secondary_fragment.append(parition)
        else:
            secondary_fragment=split_paritions
        
        primary_cost,secondary_cost=0,0
        tuple_length_primary_fragment=sum([attrs_length[x] for x in primary_fragment])
        if query['scan_key']==attribute_index:
            primary_cost=index_page_num+(cardinality*query['selectivity']*tuple_length_primary_fragment)/page_size
        elif query['scan_key']==attribute_uncluster_index:
            primary_cost=index_page_num+cardinality*query['selectivity']/(2*(buffer_page_num-index_page_num))
        else:
            # compute Sequential
            primary_cost=math.ceil((cardinality*tuple_length_primary_fragment)/(page_size))
        for parition in secondary_fragment:
            tuple_length_secondary_fragment=sum([attrs_length[x] for x in parition])+tid_length
            secondary_cost+=math.ceil((cardinality*tuple_length_secondary_fragment*query['selectivity'])/(page_size))
        cost=primary_cost+secondary_cost
        estimate_total_cost+=cost
    return estimate_total_cost


# 16-Integrating frequent pattern clustering and branch-and-bound approaches for data partitioning
def cal_total_cost(querys,candidate_paritions,attrs_length):
    totol_cost=0
    for query in querys:
        primary_fragment=[]
        secondary_fragments=[]
        clustered_index=float('inf') #默认-1
        unclustered_index=0
        sequential=0
        solved_attrs=[i for i,x in enumerate(query['value']) if x==1]
        # primary_fragment/secondary_fragments的确定
        for parition in candidate_paritions:
            if parition.count(query['scan_key'])>0:
                primary_fragment=parition
            elif list_solved_list(parition,solved_attrs):
                secondary_fragments.append(parition)
        # 计算分区内元组长度
        tuple_length_primary_fragment=sum([attrs_length[x] for x in primary_fragment])+tid_length
        secondary_road=cardinality*query['selectivity']*len(secondary_fragments)
        # compute Clustered_index
        if query['scan_key']==attribute_index:
            clustered_index=math.ceil(cardinality*query['selectivity']*tuple_length_primary_fragment/page_size)+secondary_road
            # clustered_index=1+secondary_road
        # compute Unclustered_index
        elif secondary_road==0:
            # compute Sequential
            sequential=math.ceil((cardinality*tuple_length_primary_fragment)/(page_size*block_factor))+secondary_road
        else:
            unclustered_index=cardinality*query['selectivity']+secondary_road
            
        cost=min([clustered_index,unclustered_index,sequential])*query['freq']
        # print(cost)
        totol_cost+=cost
    # print("total:",totol_cost)
    return totol_cost

# 未考虑索引情况下
def main_fragment_cost_fun(n,primary_fragment):
    tuple_length_primary_fragment=primary_fragment+tid_length
    return n*math.ceil((cardinality*tuple_length_primary_fragment)/(page_size))
    # return n*(cardinality*tuple_length_primary_fragment)/(page_size)

# 尚未考虑索引
def secondary_fragment_cost_fun(n,secondary_fragment,avg_sel):
    tuple_length_secondary_fragments=secondary_fragment+tid_length
    return n*math.ceil((cardinality*tuple_length_secondary_fragments*avg_sel)/(page_size))
    # return n*(cardinality*tuple_length_secondary_fragments*avg_sel)/(page_size)

# 刘师兄修改的成本模型：第二分区的代价计算考虑selectivity
def cal_total_cost_update3(querys,candidate_paritions,attrs_length):
    totol_cost=0
    for query in querys:
        primary_fragment=[]
        secondary_fragments=[]
        sequential=0
        solved_attrs=[i for i,x in enumerate(query['value']) if x==1]
        # primary_fragment/secondary_fragments的确定
        for parition in candidate_paritions:
            if parition.count(query['scan_key'])>0:
                primary_fragment=parition
            elif list_solved_list(parition,solved_attrs):
                secondary_fragments.append(parition)
        # 计算主分区内元组长度
        tuple_length_primary_fragment=sum([attrs_length[x] for x in primary_fragment])+tid_length
        # 计算副分区元组长度
        secondary_cost=0
        for secondary_fragment in secondary_fragments:
            tuple_length_secondary_fragments=sum([attrs_length[x] for x in secondary_fragment])+tid_length
            secondary_cost+=math.ceil((cardinality*tuple_length_secondary_fragments*query['selectivity'])/(page_size))
            # secondary_cost+=(cardinality*tuple_length_secondary_fragments*query['selectivity'])/(page_size)
        primary_cost=0
        if query['scan_key']==attribute_index:
            primary_cost=index_page_num+math.ceil((cardinality*query['selectivity']*tuple_length_primary_fragment)/page_size)
        elif query['scan_key']==attribute_uncluster_index:
            primary_cost=index_page_num+math.ceil(cardinality*query['selectivity']/(2*(buffer_page_num-index_page_num)))
        else:
            # compute Sequential
            primary_cost=math.ceil((cardinality*tuple_length_primary_fragment)/(page_size))
            # primary_cost=(cardinality*tuple_length_primary_fragment)/(page_size)
        totol_cost+=(primary_cost+secondary_cost)*query['freq']
    return totol_cost  

# 增加连接成本的成本模型
def cal_total_cost_update(querys,candidate_paritions,attrs_length):
    # print(candidate_paritions)
    if(len(candidate_paritions)==0):return 0
    totol_cost=0
    for query in querys:
        primary_fragments=[]
        secondary_fragments=[]
        solved_attrs=[i for i,x in enumerate(query['value']) if x==1]
        # primary_fragment/secondary_fragments的确定
        for parition in candidate_paritions:
            if list_solved_list(parition,query['scan_key']):
                # print(query['scan_key'])
                primary_fragments.append({
                    'key':[key for key in query['scan_key'] if key in parition],
                    'val':parition
                })
            elif list_solved_list(parition,solved_attrs):
                secondary_fragments.append(parition)
        # 如果query没有scan key，则把表的cluster索引作为scan key.默认primary key,长度最短的分区
        # if len(query['scan_key'])==0:
        #     min_frag=[]
        #     min_frag_length=math.inf
        #     for parition in secondary_fragments:
        #         par_length=sum([attrs_length[x] for x in parition])+tid_length
        #         if par_length<min_frag_length:
        #             min_frag=parition
        #             min_frag_length=par_length
        #     secondary_fragments.remove(min_frag)
        #     primary_fragments.append({
        #     'key':[-1],
        #     'val':min_frag
        #     })
        # 计算主分区内元组长度
        primary_cost=0
        for primary_fragment in primary_fragments:
            tuple_length_primary_fragment=sum([attrs_length[x] for x in primary_fragment['val']])+tid_length
            # compute Sequential
            sequential_cost=math.ceil((cardinality*tuple_length_primary_fragment)/(page_size*block_factor))
            primary_cost+=sequential_cost
        # 计算副分区元组长度
        secondary_cost=0
        index_key_num=math.ceil(cardinality*query['selectivity'])
        for secondary_fragment in secondary_fragments:
            # 若索引文件放入主存缓冲区中，查找键值K只涉及到主存访问，而不执行IO操作。暂时不考虑这种情况
            tuple_length_secondary_fragments=sum([attrs_length[x] for x in secondary_fragment])+tid_length
            # 此时对主分区的键值，已建立hash表，遍历hash表,只需要把整个索引块进行查找
            join_cost=cardinality*tid_length/(page_size*block_factor)
            secondary_cost+=join_cost+math.ceil((cardinality*query['selectivity']*tuple_length_secondary_fragments)/(page_size*block_factor))
        totol_cost+=(primary_cost+secondary_cost)*query['freq']
    return totol_cost  

# An adaptable vertical partitioning method in distributed systems
# cost model
def cal_total_cost_update4(querys,candidate_paritions,attrs_length):
    # test data
    # candidate_paritions=[[0],[1,2],[3],[4,5]]
    # querys=[{'freq': 15, 'scan_key': [2], 'selectivity': 0.99, 'value': [0,1,1,1,0,0]}, 
    #         {'freq': 10, 'scan_key': [2], 'selectivity': 0.535, 'value': [1,1,0,0,1,1]}, 
    #         {'freq': 25, 'scan_key': [2], 'selectivity': 0.632, 'value': [0,0,0,1,1,1]},
    #         {'freq': 20, 'scan_key': [2], 'selectivity': 1.0, 'value': [0,1,1,0,0,0]}
    #         ]

    #refer : An adaptable vertical partitioning method in distributed systems q Jin Hyun Son a, Myoung Ho Kim
    #object function: [ c*avg(#DF)+avg(#IA)  ]
    if(len(candidate_paritions)==0):return 0
    c=2
    pars_querys=[]
    for par in candidate_paritions:
        par_querys=[]
        for q_i,query in enumerate(querys):
            q_idx=[i for i,x in enumerate(query['value']) if x==1]
            if list_solved_list(q_idx,par):
                par_querys.append(q_i)
        pars_querys.append(par_querys) 
    #compute avg(#DF)
    
    ki=[]
    for i in range(len(querys)):
        total=0
        for par_query in pars_querys:
            total+=par_query.count(i)
        ki.append(total)
    cardi_Qn=len(querys)
    avg_DF=sum([(k-1)*querys[i]['freq'] for i,k in enumerate(ki)])/cardi_Qn

    #compute avg(#IA)

    n_St=len(candidate_paritions)
    IA_n=[]
    for par_query in pars_querys:
        if len(par_query)>1:
            IA_n.append(sum([querys[x]['freq'] for x in par_query]))
        else:
            IA_n.append(0)

    avg_IA=sum(IA_n)/n_St

    return avg_DF*c+avg_IA

# paper:Integrating Vertical and Horizontal Partitioning into Automated Physical Database Design ( sigmod 2014)
# VPC: measure the effectiveness of a column-group for vertical partitioning
def cal_column_group_effectiveness(querys,candidate_paritions,attrs_length):
    # test data
    # attrs_length=[4,2,2,2,2,2]
    # candidate_paritions=[[0],[1,2],[3],[4,5]]
    # querys=[{'freq': 15, 'scan_key': [2], 'selectivity': 0.99, 'value': [0,1,1,1,0,0]},
    #         {'freq': 10, 'scan_key': [2], 'selectivity': 0.535, 'value': [1,1,0,0,1,1]},
    #         {'freq': 25, 'scan_key': [2], 'selectivity': 0.632, 'value': [0,0,0,1,1,1]},
    #         {'freq': 20, 'scan_key': [2], 'selectivity': 1.0, 'value': [0,1,1,0,0,0]}
    #         ]
    while([] in candidate_paritions):
        candidate_paritions.remove([])
    VPC_Matrix = np.array(([[0] * len(querys)]) * len(attrs_length),dtype=np.float)
    VPC_Matrix_Helper = np.array(([[0] * len(querys)]) * len(attrs_length),dtype=np.float)
    # VPC_Matrix=[[0]*len(querys)]*len(attrs_length)
    # VPC_Matrix_Helper=[[0]*len(querys)]*len(attrs_length)
    effectiveness=0
    for index,query in enumerate(querys):
        for rowIndex,item in enumerate(query['value']):
            VPC_Matrix[rowIndex][index]=item*query['freq']
            VPC_Matrix_Helper[rowIndex][index]=1*query['freq']
            if rowIndex in query['scan_key']:
                VPC_Matrix[rowIndex][index]*=query['selectivity']
                VPC_Matrix_Helper[rowIndex][index]*=query['selectivity']
    for par in candidate_paritions:
        scan_data,total_data=0,0
        for column in par:
            scan_data+=attrs_length[column]*sum(set(VPC_Matrix[column, :]))
            total_data+=attrs_length[column]*sum(set(VPC_Matrix_Helper[column, :]))
        effectiveness+=scan_data/(total_data)
    if effectiveness!=0:
        return effectiveness / len(candidate_paritions)
    else:
        return effectiveness


#  Tuple-reconstruction joins = #Vertical partitions accessed -1
def cal_avg_tuple_reconstruction_joins(querys,candidate_paritions,attrs_length):
    return len(candidate_paritions)-1

#  new cost model that includes complex clauses and tuple construction cost
# 2023/03/07
def is_index(attr_list):
    for attr in attr_list:
        if attr>=0:
            return False
    return True

def is_contain_vary_fields(par,vary_dict):
    for attr in par:
        if vary_dict[attr]==1:
            return True
    return False

BW=100*1024*1024 #M/s=> B/s
comp_factor,scan_factor=3,1
r_rand,r_seq=0.5,1
def cal_total_cost_improvement(querys,candidate_paritions,attrs_length,print_info=False):
    vary_dict=col_inf[tab_name]['vary']
    rows=table_rows

    total_cost=0
    for query in querys:
        proj_attrs=[i for i,x in enumerate(query['value']) if x==1]
        proj_width=sum([attrs_length[attr] for attr in proj_attrs])
        filter_attrs=query['scan_key']
        filter_width=sum([attrs_length[attr] for attr in filter_attrs])
        satisfy_rows=math.ceil(rows*query['selectivity'])
        pointer_width=4
        pk_width=4
        for par in candidate_paritions:
            par_width=sum([attrs_length[attr] for attr in par])
            # main partition
            if list_solved_list(par,filter_attrs):
                mp_cost=0
                if is_index(filter_attrs):
                    mp_cost+=comp_factor*(math.ceil(math.log(rows+1,3))+satisfy_rows)*filter_width/(r_seq*BW)+scan_factor*satisfy_rows*par_width/(r_rand*BW)
                else:
                    mp_cost+=comp_factor*rows*filter_width/(r_seq*BW)+scan_factor*rows*par_width/(r_seq*BW)
                total_cost+=mp_cost
                if print_info:
                    print(f'primary cost:{mp_cost}, partitions:{par}')
            # secondary partition
            elif list_solved_list(par,proj_attrs+query['gp_ob']):
                secondary_cost=0
                # scheme 1
                if is_contain_vary_fields(par,vary_dict):
                    secondary_cost+=comp_factor*rows*pointer_width/(r_seq*BW)
                else:
                    secondary_cost+=comp_factor*satisfy_rows*pointer_width/(r_rand*BW)
                secondary_cost+=satisfy_rows*par_width/(r_rand*BW)
                # scheme 2
                # secondary_cost+=comp_factor*(math.ceil(math.log(rows+1,3))+satisfy_rows)*pk_width/(r_seq*BW)+satisfy_rows*(par_width+pk_width)/(r_rand*BW)
                total_cost+=secondary_cost
                if print_info:
                    print(f'secondary cost:{secondary_cost}, partitions:{par}')
        if query['gp_ob']:
            total_cost+=comp_factor*rows*sum([attrs_length[attr] for attr in query['gp_ob']])/(r_seq*BW)
    # return total_cost**query['freq']
    return total_cost


def cal_total_cost_with_tree(QUERYS,querys, candidate_paritions, attrs_length, wood,print_info=False):
    vary_dict = col_inf[tab_name]['vary']
    tot_rows = len(wood.dataset)
    total_cost = 0
    for qid,query in enumerate(QUERYS):
        proj_attrs = [i for i, x in enumerate(query['value']) if x == 1]
        proj_width = sum([attrs_length[attr] for attr in proj_attrs])
        filter_attrs = query['scan_key']
        filter_width = sum([attrs_length[attr] for attr in filter_attrs])
        satisfy_rows = math.ceil(tot_rows * query['selectivity'])
        rows=wood.eval_wood_blocksize(qid,querys[qid])
        if satisfy_rows<rows: satisfy_rows=rows
        # rows=tot_rows
        # print(f'tot rows:{tot_rows}, estimated rows:{rows}')
        pointer_width = 4
        pk_width = 4
        for par in candidate_paritions:
            par_width = sum([attrs_length[attr] for attr in par])
            # main partition
            if list_solved_list(par, filter_attrs):
                mp_cost = 0
                if is_index(filter_attrs):
                    mp_cost += comp_factor * (math.ceil(math.log(rows + 1, 3)) + satisfy_rows) * filter_width / (
                                r_seq * BW) + scan_factor * satisfy_rows * par_width / (r_rand * BW)
                else:
                    mp_cost += comp_factor * rows * filter_width / (r_seq * BW) + scan_factor * rows * par_width / (
                                r_seq * BW)
                total_cost += mp_cost
                if print_info:
                    print(f'primary cost:{mp_cost}, partitions:{par}')
            # secondary partition
            elif list_solved_list(par, proj_attrs + query['gp_ob']):
                secondary_cost = 0
                # scheme 1
                if is_contain_vary_fields(par, vary_dict):
                    secondary_cost += comp_factor * rows * pointer_width / (r_seq * BW)
                else:
                    secondary_cost += comp_factor * satisfy_rows * pointer_width / (r_rand * BW)
                secondary_cost += satisfy_rows * par_width / (r_rand * BW)
                # scheme 2
                # secondary_cost+=comp_factor*(math.ceil(math.log(rows+1,3))+satisfy_rows)*pk_width/(r_seq*BW)+satisfy_rows*(par_width+pk_width)/(r_rand*BW)
                total_cost += secondary_cost
                if print_info:
                    print(f'secondary cost:{secondary_cost}, partitions:{par}')
        if query['gp_ob']:
            total_cost += comp_factor * rows * sum([attrs_length[attr] for attr in query['gp_ob']]) / (r_seq * BW)
    # return total_cost**query['freq']
    return total_cost

def cal_latency_scan_block(query,rows,satisfy_rows,pages,print_info=False):
    vary_dict=col_inf[tab_name]['vary']
    attrs_length=col_inf[tab_name]['length']
    total_cost=0
    proj_attrs=query['scan']
    proj_width=sum([attrs_length[attr] for attr in proj_attrs])
    filter_attrs=query['filter']
    filter_width=sum([attrs_length[attr] for attr in filter_attrs])
    pointer_width=4
    pk_width=4
    for par in pages:
        par_width=sum([attrs_length[attr] for attr in par])
        # main partition
        if list_solved_list(par,filter_attrs):
            mp_cost=0
            if is_index(filter_attrs):
                mp_cost+=comp_factor*(math.ceil(math.log(rows+1,3))+satisfy_rows)*filter_width/(r_seq*BW)+scan_factor*satisfy_rows*par_width/(r_rand*BW)
            else:
                mp_cost+=comp_factor*rows*filter_width/(r_seq*BW)+scan_factor*rows*par_width/(r_seq*BW)
            total_cost+=mp_cost
            if print_info:
                print(f'primary cost:{mp_cost}, partitions:{par}')
        # secondary partition
        elif list_solved_list(par,proj_attrs+query['gp_ob']):
            secondary_cost=0
            # scheme 1
            if is_contain_vary_fields(par,vary_dict):
                secondary_cost+=comp_factor*rows*pointer_width/(r_seq*BW)
            else:
                secondary_cost+=comp_factor*satisfy_rows*pointer_width/(r_rand*BW)
            secondary_cost+=satisfy_rows*par_width/(r_rand*BW)
            # scheme 2
            # secondary_cost+=comp_factor*(math.ceil(math.log(rows+1,3))+satisfy_rows)*pk_width/(r_seq*BW)+satisfy_rows*(par_width+pk_width)/(r_rand*BW)
            total_cost+=secondary_cost
            if print_info:
                print(f'secondary cost:{secondary_cost}, partitions:{par}')
    if query['gp_ob']:
        total_cost+=comp_factor*rows*sum([attrs_length[attr] for attr in query['gp_ob']])/(r_seq*BW)
    return total_cost