import numpy as np
import copy
from sklearn.cluster import SpectralClustering
import globalvar as gl
from fp_growth_plus import load_data,Fp_growth_plus
from utils import list_in_list, list_solved_list,prune_affinity_matrix,read_query_data
from DiskCost import cal_total_cost_improvement as cal_total_cost_update
import DiskCost as DC
import threading
import time
import multiprocessing as mul
import warnings
# 主要针对合并过程进行改进：解决上一版本一些已有的bug
# bug1：合并分区时，如果待合并的所有分区范围大于已有的频繁模式，则按照频繁模式的标准，
# bug2：合并分区时，如果选择奖励最大的频繁模式，那可属于其子集的频繁模式，也有正向奖励，而且是多个，
# 奖励总和是大于父频繁模式的，这样选择此频繁模式是不公平的
# eg： [1 2 3 4] 20  [5] 0
#  [1 2] 15
#  [3 4 5] 10
# 这样选择[1 2] [3 4 5]是较优的
lock = threading.Lock()
L_GLOBAL = []
QUERYS=[]
ATTRS_LENGTH=[]

def get_freq_set_by_range(complete_column):
    global L_GLOBAL
    part_L = []
    for item in reversed(L_GLOBAL):
        for key in item:
            if list_in_list([x for x in key], complete_column):
                part_L.append({key: item[key]})
    return part_L


def normalizePartition(candidate_paritions, accessed_attributes):
    new_candidate_paritions = []
    accessed_attributes = accessed_attributes.tolist()
    unaccessed_un_par = [[]]
    unaccessed_complete_par = []
    for i in range(len(ATTRS_LENGTH)):
        if (i not in accessed_attributes):
            unaccessed_un_par[0].append(i)
            unaccessed_complete_par.append([i])
    for par in candidate_paritions:
        new_par = []
        for attr in par:
            if attr in accessed_attributes: new_par.append(attr)
        if (len(new_par) > 0): new_candidate_paritions.append(new_par)
    cost1 = cal_total_cost_update(QUERYS, new_candidate_paritions + unaccessed_un_par, ATTRS_LENGTH)
    cost2 = cal_total_cost_update(QUERYS, new_candidate_paritions + unaccessed_complete_par, ATTRS_LENGTH)
    if cost1 < cost2:
        return new_candidate_paritions + unaccessed_un_par
    else:
        return new_candidate_paritions + unaccessed_complete_par

def compute_cost_by_spectal_cluster_R(file_input_info):
    warnings.filterwarnings('ignore')
    global L_GLOBAL
    global QUERYS
    global ATTRS_LENGTH
    affinity_matrix, QUERYS = read_query_data(file_input_info['path'], file_input_info['attr_num'])
    ATTRS_LENGTH = file_input_info['attrs_length']
    if 'tab' in file_input_info:
        DC.tab_name=file_input_info['tab']
    # print(affinity_matrix)
    X, accessedAttr = prune_affinity_matrix(affinity_matrix)
    # print(X)
    min_cost = float('inf')
    min_cluster = []
    # for index, k in enumerate(range(2,math.ceil(attr_num/2)+1)):
    # for index, k in enumerate(range(2,math.ceil(file_input_info['attr_num']/2)+1)):
    # with warnings.catch_warnings():
    #     warnings.filterwarnings(
    #         "ignore",
    #         message="Graph is not fully connected, spectral embedding may not work as expected.", category=UserWarning)
    for k in range(1, len(X) + 1):
        y_pred = SpectralClustering(n_clusters=k, affinity='precomputed',
                                    assign_labels="discretize",
                                    random_state=0).fit_predict(X)
        # print(y_pred)
        candidate_paritions = []
        for i in range(k):
            class_label = np.where(y_pred == i)[0]
            candidate_paritions.append(accessedAttr[class_label.tolist()].tolist())
        candidate_paritions_normalization = normalizePartition(candidate_paritions, accessedAttr)
        cost = cal_total_cost_update(QUERYS, candidate_paritions_normalization, ATTRS_LENGTH)
        # cost=cal_total_memory_cost(querys,candidate_paritions,attrs_length)
        if cost < min_cost:
            min_cost = cost
            min_cluster = candidate_paritions_normalization
        # print ("Calinski-Harabasz-me Score ：n_clusters=", k,"score:", calinski_harabasz_score_me(X, y_pred,k))
    # while([] in min_cluster):min_cluster.remove([])
    # print("原始分区:", min_cluster)
    data_set, _ = load_data(accessedAttr, QUERYS)
    min_support = 0
    L_GLOBAL, _ = Fp_growth_plus().generate_L(data_set, min_support)

    optimal_candidate_paritions = get_best_partition_res(min_cluster)
    # optimal_candidate_paritions=normalizePartition(optimal_candidate_paritions,accessedAttr,querys,attrs_length)
    # cost=cal_total_cost_update(querys,optimal_candidate_paritions,attrs_length)
    # min_cost=cal_total_memory_cost(querys,candidate_clusters,attrs_length)
    # execution_cost=cal_subtable_by_partition_time(querys,file_input_info['path'],optimal_candidate_paritions)
    # print("合并后分区:", optimal_candidate_paritions)
    # return normalizePartition(optimal_candidate_paritions,accessedAttr,querys,attrs_length)
    return optimal_candidate_paritions

# 递归函数
def split_candidate_parition_by_cut_reward(complete_column_range, temp_candidates2):
    # 如果要分割的聚簇只有一个元素，直接返回即可
    if (len(complete_column_range) == 1): return [complete_column_range]
    temp_candidates = temp_candidates2.copy()
    L = get_freq_set_by_range(complete_column_range)
    # 提前计算 将该聚簇作为主分区的查询 访问成本
    # chunk_freq_set = [processNumberFreqset.remote(itemset, complete_column_range, temp_candidates)
    #                   for itemset in reversed(L)]
    freq_item_dict = []
    for itemset in reversed(L):
        for key in itemset:
            temp_complete_column_range=complete_column_range.copy()
            [temp_complete_column_range.remove(x) for x in key]
            if len(temp_complete_column_range)==0:continue
            reward_res=cut_reward_fun_update([[x for x in key],temp_complete_column_range],temp_candidates)
            if(reward_res['val']>=0):
                freq_item_dict.append(reward_res)
    # res = ray.get(chunk_freq_set)
    # print(res)
    # freq_item_dict = [item for list in res for item in list if len(list) != 0]
    splited_paritions = []
    left_unsplited_par = copy.deepcopy(complete_column_range)
    while (True):
        # 如果频繁项集列表为空，表明候选项已经排列完毕
        if len(freq_item_dict) == 0:
            if len(left_unsplited_par) > 0: splited_paritions.append(left_unsplited_par)
            break
        # 选择第一个奖励最大的频繁项
        freq_item_dict.sort(key=lambda x: (x["val"]), reverse=True)
        current_cut_item = freq_item_dict[0]
        freq_item_dict.remove(current_cut_item)
        left_unsplited_par = list(set(left_unsplited_par) - set(current_cut_item['fre_item'][0]))
        # 被切割过的分区 也有可能会被二次切割
        if (len(left_unsplited_par) == 0):
            other_temp_candidates = temp_candidates.copy()
        else:
            other_temp_candidates = [left_unsplited_par] + temp_candidates.copy()
        splited_paritions += split_candidate_parition_by_cut_reward(current_cut_item['fre_item'][0],other_temp_candidates)
        temp_candidates.append(current_cut_item['fre_item'][0])
        if (len(left_unsplited_par) == 0):
            break
        elif (len(left_unsplited_par) == 1):
            splited_paritions.append(left_unsplited_par)
            break
        # i=0
        for i in range(len(freq_item_dict) - 1, -1, -1):
            freq_item = freq_item_dict[i]
            if list_in_list(freq_item['fre_item'][0], left_unsplited_par):
                # n_avg_sel=getUpdatedAvgSel(querys,gl.get_value('workload_matrix'),bc_par,b_size)
                update_freq_item = update_reward_fun_update(current_cut_item, freq_item,temp_candidates)
                if len(freq_item['fre_item'][0]) == len(left_unsplited_par):
                    update_freq_item['val'] = 0
                if (update_freq_item['val'] >= 0):
                    freq_item_dict[i] = update_freq_item
                else:
                    freq_item_dict.remove(freq_item)
            else:
                freq_item_dict.remove(freq_item)

    return splited_paritions

def get_atom_partition(split_cluster,candidate_clusters,queue):
    temp_clusters = candidate_clusters.copy()
    temp_clusters.remove(split_cluster)
    init_cost = cal_total_cost_update(QUERYS,[split_cluster], ATTRS_LENGTH)
    new_split_clusters = split_candidate_parition_by_cut_reward(split_cluster,temp_clusters)
    splited_update_cost = cal_total_cost_update(QUERYS, new_split_clusters, ATTRS_LENGTH)
    # print(split_cluster,init_cost,new_split_clusters,splited_update_cost)
    if splited_update_cost < init_cost:
       queue.put({'o_clusters': split_cluster,
                'u_clusters': new_split_clusters})
    else:
        queue.put({})

# parameter:candidate_paritions 代表clusters
def get_best_partition_res(candidate_clusters_orgin):
    candidate_clusters = candidate_clusters_orgin.copy()
    to_split_clusters = []
    split_schema = []
    for item in candidate_clusters_orgin:
        if len(item) > 1:
            to_split_clusters.append(item)
    queue = mul.Queue()
    jobs = []
    for split_cluster in to_split_clusters:
        process = mul.Process(target=get_atom_partition, args=(split_cluster, candidate_clusters, queue))
        process.start()
        jobs.append(process)
    # for job in jobs:job.join()
    chunk_splited_cluster = [queue.get() for _ in jobs]
    # print(chunk_splited_cluster)
    for idx, item in enumerate(chunk_splited_cluster):
        if item:
            candidate_clusters.remove(item['o_clusters'])
            candidate_clusters += item['u_clusters']
    # 寻找合并方案
    candidate_clusters = combine_candidate_parition_by_combine_reward(candidate_clusters)
    # for item in combine_schema:
    #     for par in item['o_clusters']:
    #         candidate_clusters.remove(par)
    #     candidate_clusters += item['u_clusters']
    return candidate_clusters


def rayProcessNumberFreqset(itemset, to_combined_clusters, temp_combined_cluster, freq_item_dict,init_cost):
    # freq_item_dict = []
    for key in itemset:
        if len(key) < 2: continue
        # 判断原簇方案是否可以组成该频繁项集
        temp_key = []
        for item_cluster in to_combined_clusters:
            if list_solved_list(item_cluster, key):
                temp_key.append(item_cluster)
        # 2种情况：
        # 1.如果tempkey>key 保留key的特性
        # 2.如果tempkey=key 加入tempkey作为新合并的数组
        keyset=set(key)
        tempkeyset=set([y for x in temp_key for y in x])
        if keyset <= tempkeyset:
            if temp_key in temp_combined_cluster['keys']:
                idx=temp_combined_cluster['keys'].index(temp_key)
                last_info=temp_combined_cluster['info'][idx]
                combined_items=[]
                if keyset < tempkeyset:
                    combined_items.append(list(keyset))
                    combined_items.append(list(tempkeyset-keyset))
                elif len(temp_key)!=1:
                    combined_items.append(list(tempkeyset))
                else:
                    continue
                current_reward_value=init_cost-cal_total_cost_update(QUERYS,last_info['restPars']+combined_items, ATTRS_LENGTH)
                if current_reward_value>last_info['item']['val']:
                    # print(combined_items,current_reward_value,"替换",last_info['item']['components'],last_info['item']['val'])
                    lock.acquire()
                    freq_item_dict[last_info['id']]['components']=combined_items
                    freq_item_dict[last_info['id']]['val']=current_reward_value
                    # freq_item_dict.remove(last_info['item'])
                    # freq_item_dict.append({'item': temp_key,'components':combined_items, 'val': current_reward_value})
                    lock.release()
                continue
            # 计算合并收益
            temp_combined_clusters = to_combined_clusters.copy()
            [temp_combined_clusters.remove(cluster) for cluster in temp_key]
            combined_items=[]
            if keyset < tempkeyset:
                combined_items.append(list(keyset))
                combined_items.append(list(tempkeyset-keyset))
            elif len(temp_key)!=1:
                combined_items.append(list(tempkeyset))
            else:
                continue
            new_combined_clusters=[combined_items,[list(tempkeyset)]]
            costs=[cal_total_cost_update(QUERYS, temp_combined_clusters+new_combined_clusters[0], ATTRS_LENGTH),cal_total_cost_update(QUERYS, temp_combined_clusters+new_combined_clusters[1], ATTRS_LENGTH)]
            if(costs[0]<costs[1]):
                min_index=0
            else:min_index=1
            if init_cost - costs[min_index] > 0:
                new_item={'item': temp_key,'components':new_combined_clusters[min_index], 'val': init_cost-costs[min_index]}
                lock.acquire()
                freq_item_dict.append(new_item)
                temp_combined_cluster['keys'].append(temp_key)
                temp_combined_cluster['info'].append({'id':len(freq_item_dict)-1,'restPars':temp_combined_clusters.copy(),'item':new_item})
                lock.release()
    return None


from MyThread import MyThread


def combine_candidate_parition_by_combine_reward(candidate_clusters_orgin):
    # combine the splitted clusters
    candidate_clusters=candidate_clusters_orgin.copy()
    i=1
    while(True):
        i-=1
        # to_combined_clusters表示上一合并状态后的全部方案
        time0=time.time()
        to_combined_clusters = candidate_clusters.copy()
        init_cost = cal_total_cost_update(QUERYS, to_combined_clusters, ATTRS_LENGTH)
        L = L_GLOBAL
        freq_item_dict = []
        temp_combined_cluster = {'keys':[],'info':[]}
        res = []
        # resize_L=[]
        # resize_L_chunk=[]
        # for L_dict_item in reversed(L):
        #     for pattern in L_dict_item:
        #         if(len(resize_L_chunk)<=100):resize_L_chunk.append(pattern)
        #         else: 
        #             resize_L.append(resize_L_chunk.copy())
        #             resize_L_chunk.clear()
        # for itemset in resize_L:
        for itemset in reversed(L):
            task = MyThread(rayProcessNumberFreqset,
                            (itemset, to_combined_clusters, temp_combined_cluster,freq_item_dict, init_cost))
            task.start()
            res.append(task.get_result())
            # output=rayProcessNumberFreqset(itemset, to_combined_clusters, temp_combined_cluster, init_cost, querys, attrs_length)
            # res.append(output)
        # freq_item_dict = [item for list in res for item in list if len(list) != 0]
        if(len(freq_item_dict)==0):break
        time1=time.time()
        combine_schema = []
        left_uncombined_par = to_combined_clusters.copy()
        while (True):
            if len(freq_item_dict) == 0: break
            # 选择第一个奖励最大的频繁项
            freq_item_dict.sort(key=lambda x: (x["val"]), reverse=True)
            
            # bug2：合并分区时，如果选择奖励最大的频繁模式，那可属于其子集的频繁模式，也有正向奖励，而且是多个，
            # 奖励总和是大于父频繁模式的，这样选择此频繁模式是不公平的
            # eg： [1 2 3 4] 20  [5] 0
            #  [1 2] 15
            #  [3 4 5] 10
            # 这样选择[1 2] [3 4 5]是较优的
            if(freq_item_dict[0]['val']<=0):break
            current_combined_item = freq_item_dict[0]['item']
            combine_schema.append({
                'o_clusters': current_combined_item,
                'u_clusters': freq_item_dict[0]['components'],
                'reward': freq_item_dict[0]['val']
            })
            [left_uncombined_par.remove(x) for x in current_combined_item]
            # left_uncombined_par+=freq_item_dict[0]['components']
            del freq_item_dict[0]
            # if (len(left_uncombined_par) == 1):
            #     break
            for i in range(len(freq_item_dict) - 1, -1, -1):
                freq_item = freq_item_dict[i]
                # temp_left_uncombined_par=left_uncombined_par.copy()
                # flag=True
                for par in freq_item['item']:
                    if par in current_combined_item:
                        freq_item_dict.remove(freq_item)
                        # flag = False
                        break
                    # else:
                    #     temp_left_uncombined_par.remove(par)
                # if(flag):
                #     have_update_par=[schema['u_clusters'] for schema in combine_schema]
                #     update_val=cal_total_cost_update(querys, left_uncombined_par+have_update_par, attrs_length)-cal_total_cost_update(querys, temp_left_uncombined_par+have_update_par+freq_item['components'], attrs_length)
                #     if(update_val<=0):
                #         freq_item_dict.remove(freq_item)
                #     else:
                #         freq_item['val']=update_val
        time2=time.time()
        # print("第一轮合并时间：",time1-time0," 第二轮合并时间：",time2-time1)
        for item in combine_schema:
            for par in item['o_clusters']:
                candidate_clusters.remove(par)
            candidate_clusters += item['u_clusters']
    return candidate_clusters

# 由于已经确定好切割的频繁项，所以avg_sel,n3_matrix_ind可以事先计算出来，不影响update
def update_reward_fun_update(last_cut_info, my_cut_info,  temp_candidates2):
    temp_candidates = temp_candidates2.copy()
    before_change_par = [last_cut_info['fre_item'][1]]
    after_change_par = [my_cut_info['fre_item'][0],
                        list(set(my_cut_info['fre_item'][1]) - set(last_cut_info['fre_item'][0]))]
    res = {
        'fre_item': [my_cut_info['fre_item'][0],
                     list(set(my_cut_info['fre_item'][1]) - set(last_cut_info['fre_item'][0]))],
        'val': cal_total_cost_update(QUERYS, temp_candidates + before_change_par, ATTRS_LENGTH) - cal_total_cost_update(
            QUERYS, temp_candidates + after_change_par, ATTRS_LENGTH)
    }
    return res

def cut_reward_fun_update(splited_column, temp_candidates2):
    temp_candidates = temp_candidates2.copy()
    # 只访问b段属性的查询数量为𝑛_1,只访问c段属性的查询数量为𝑛_2，既访问b又访问c段属性的查询数量为𝑛_3。
    b_parition = splited_column[0]
    c_parition = splited_column[1]
    res = {
        'fre_item': [b_parition, c_parition],
        # 考虑整体分区方案的，而不是原始簇内的子分区
        'val': cal_total_cost_update(QUERYS, temp_candidates + [b_parition + c_parition],
                                     ATTRS_LENGTH) - cal_total_cost_update(QUERYS, splited_column + temp_candidates,
                                                                           ATTRS_LENGTH),
    }
    return res

