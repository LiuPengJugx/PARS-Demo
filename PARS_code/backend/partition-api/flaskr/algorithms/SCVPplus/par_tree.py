import copy
import numpy as np
import sys
sys.path.append('/home/liupengju/pycharmProjects/partition-api/')
from flaskr.algorithms.SCVPplus.utils import *
import math
import pickle
class PartitionNode:
    def __init__(self, num_dims=0,vp=[], boundary=[], nid=None,num_children = 0,
                 pid=None, children_ids=[], is_leaf=True, node_size=0):
        self.num_dims = num_dims
        self.boundary = boundary # apply for multiple types
        self.vp=vp
        self.nid = nid  # node id
        self.pid = pid  # parent id
        self.children_ids = children_ids
        self.num_children = num_children
        self.is_leaf = is_leaf
        self.depth = 0
        self.node_size = node_size
        self.split_scheme=None
        self.dataset = None
        self.queryset = None

    def split_queryset(self, split_dim,split_op, split_value):
        '''
        split the queryset into 3 parts:
        the left part, the right part, and those cross the split value
        '''
        if self.queryset is not None:
            left_part = []
            right_part = []
            mid_part = []
            if isinstance(split_value,str):
                # median
                mid_part = self.queryset
            else:
                for query in self.queryset:
                    flag=True
                    LF,RF=0,0
                    for pred in query['preds']:
                        if pred[0] not in self.vp: continue
                        op=pred[1]
                        cur_dim=self.vp.index(pred[0])
                        if cur_dim==split_dim:
                            flag=False
                            # int or numerical text(e.g.,date)
                            if isinstance(split_value,list):
                                if isinstance(pred[2],list):
                                    if list_in_list(pred[2],split_value):
                                        LF=1
                                    elif list_solved_list(pred[2],split_value):
                                        RF,LF=1,1
                                    else: RF=1
                                else:
                                    if pred[1]=='=':
                                        if pred[2] in split_value: LF=1  # Then default pred[1] is = 
                                        else: RF=1
                                    else:
                                        if (pred[2]<min(split_value) and pred[1] in ['<','<=']) or (pred[2]==min(split_value) and pred[1]=='<'):
                                            RF=1
                                        elif (pred[2]>max(split_value) and pred[1] in ['>','>=']) or (pred[2]==max(split_value) and pred[1]=='>'):
                                            RF=1
                                        else: LF,RF=1,1
                            else:
                                # if pred[0].find('=')!=-1: RF,LF=1,1
                                if split_op=='<':
                                    if isinstance(pred[2],list):
                                        for sv in pred[2]:
                                            if sv<=split_value: LF=1
                                            else: RF=1
                                    else:
                                        if pred[2]<=split_value:
                                            if pred[1] in ['<','<=','=']: LF=1
                                            elif pred[1]=='>' and pred[2]==split_value: RF=1
                                            else: LF,RF=1,1
                                        else:
                                            if pred[1] in ['<','<=']: LF,RF=1,1
                                            else: RF=1
                                else:
                                    if isinstance(pred[2],list):
                                        if split_value in pred[2]: 
                                            if len(pred[2])==1: LF=1 
                                            else: LF,RF=1,1
                                        else: RF=1
                                    else:
                                        if pred[2]==split_value:
                                            if pred[1] in ['<','>']: RF=1
                                            elif pred[1]=='=':LF=1
                                            else: LF,RF=1,1
                                        elif pred[2]<split_value:
                                            if pred[1] in ['<','<=','=']:RF=1
                                            else: LF,RF=1,1
                                        elif pred[2]>split_value:
                                            if pred[1] in ['>','>=','=']:RF=1
                                            else: LF,RF=1,1
                        if RF+LF==2:break
                    if flag or (RF+LF)==2: mid_part.append(query)
                    else:
                        if LF==1: left_part.append(query)
                        elif RF==1: right_part.append(query)
            # print("[Split Queryset] left part:",len(left_part), "right part:",len(right_part),"mid part:",len(mid_part))
            return left_part, right_part, mid_part

    def get_candidate_cuts(self):
        
        # how to cut <type> field? build mapping dict 
        candidate_cut_pos = []
        ops=['<','=']
        for query in self.queryset:
            for pred in query['preds']:
                if pred[0] not in self.vp: continue
                else: dim=self.vp.index(pred[0])
                if pred[1] in ['>=','<=','>','<']: op=ops[0]
                else: op=ops[1]
                if not isinstance(pred[2],str):
                    candidate_cut_pos.append((dim,op,pred[2]))
        for dim in range(self.num_dims):
            if self.boundary[dim] is None:
                split_value='median'
            elif isinstance(self.boundary[dim],list) or isinstance(self.boundary[dim],np.ndarray):
                split_value = np.median(self.dataset[:, dim])
            candidate_cut_pos.append((dim,ops[0],split_value))
        return candidate_cut_pos
    
    def get_candidate_median_cuts(self):
        candidate_cut_pos=[]
        ops=['<','=']
        for dim in range(self.num_dims):
            if self.boundary[dim] is None:
                split_value='median'
            elif isinstance(self.boundary[dim],list) or isinstance(self.boundary[dim],np.ndarray):
                split_value = np.median(self.dataset[:, dim])
            candidate_cut_pos.append((dim,ops[0],split_value))
        return candidate_cut_pos

    def if_split(self, split_dim, split_op, split_value, data_threshold, test=False):  # rename: if_split_get_gain
        '''
        return the skip gain and children partition size if split a node from a given split dimension and split value
        '''
        # print("current_node.nid:", current_node.nid)
        # print("current_node.is_leaf:", current_node.is_leaf)
        # print("current_node.dataset is None:", current_node.dataset is None)
        if isinstance(split_value,int) or isinstance(split_value,float):
            if split_op=='=':
                sub_dataset1_size = np.count_nonzero(self.dataset[:, split_dim] == split_value)
            else:
                sub_dataset1_size = np.count_nonzero(self.dataset[:, split_dim] <= split_value)  # process time: 0.007
        # elif is_valid_date(split_value):
        #     sub_dataset1_size =len(np.where(self.dataset[:, split_dim] < split_value))
        elif isinstance(split_value,str):
            if split_value=='median':
                sub_dataset1_size=int(self.node_size/2)
            else: 
                print('invalid split value:',split_value)
                exit(-1)
            # else:
            #     sub_dataset1_size = np.count_nonzero(self.dataset[:, split_dim] == split_value)
        elif isinstance(split_value,list):
            sub_dataset1_size=sum([np.count_nonzero(self.dataset[:, split_dim]==value) for value in split_value])
        else:
            print('invaild split value type:',type(split_value))
        sub_dataset2_size = self.node_size - sub_dataset1_size

        if sub_dataset1_size < data_threshold or sub_dataset2_size < data_threshold:
            return False, 0, sub_dataset1_size, sub_dataset2_size

        left_part, right_part, mid_part = self.split_queryset(split_dim, split_op, split_value)
        num_overlap_child1 = len(left_part) + len(mid_part)
        num_overlap_child2 = len(right_part) + len(mid_part)

        if test:
            print("num left part:", len(left_part), "num right part:", len(right_part), "num mid part:", len(mid_part))
            print("left part:", left_part, "right part:", right_part, "mid part:", mid_part)

        # temp_child_node1, temp_child_node2 = self.__if_split_get_child(split_dim, split_value)
        skip_gain = len(
            self.queryset) * self.node_size - num_overlap_child1 * sub_dataset1_size - num_overlap_child2 * sub_dataset2_size
        return True, skip_gain, sub_dataset1_size, sub_dataset2_size

    def return_satisfy_rows(self, query):
        logical_ops=['or','and']
        
        def merge_idx(idx_list,op):
            if len(idx_list)==0: return np.array([])
            elif len(idx_list)==1: return idx_list[0]
            else:
                final_idx=idx_list[0]
                for no in range(1,len(idx_list)):
                    if op=='or':
                        final_idx=np.union1d(final_idx,idx_list[no])
                    else:
                        final_idx=np.intersect1d(final_idx,idx_list[no])
                return final_idx

        def seek_satisfy_dataset(pred):
            if not pred: return self.dataset
            if pred[0] in logical_ops:
                idx_list=[]
                for sub_pred in pred[1]:
                    idx_list.append(seek_satisfy_dataset(sub_pred))
                return merge_idx(idx_list,pred[0])
            else:
                if pred[0] not in self.vp:
                    return np.arange(self.dataset.shape[0])
                scan_dim=self.vp.index(pred[0])
                pred=pred[1:]
                if pred[0]=='=': op='=='
                else: op=pred[0]
                idx=[]
                if isinstance(pred[1],list):
                    for x in range(self.dataset.shape[0]):
                        if pred[0]=='=' and self.dataset[x,scan_dim] in pred[1]:
                            idx.append(x)
                        elif pred[0]=='!=' and self.dataset[x,scan_dim] not in pred[1]:
                            idx.append(x)
                else:
                    for x in range(self.dataset.shape[0]):
                        if eval(f'{self.dataset[x,scan_dim]}{op}{pred[1]}'):
                            idx.append(x)
                return np.array(idx)
        return len(seek_satisfy_dataset(query['lg_preds']))

    def is_overlap_by_row(self,row,comp_dim=None):
        for cid,col in enumerate(row):
            if isinstance(self.boundary[cid],list):
                if col not in self.boundary[cid]: return False
            elif self.boundary[cid] is None:
                continue
            else:    
                if col<self.boundary[cid][0] or col>self.boundary[cid][1]:
                    return False
            if comp_dim is not None and cid>=comp_dim: break
        return True    

    def is_overlap(self, query):
        logical_ops=['or','and']
        """Begin Define"""
        # Return: 1->overlap, 0->not overlap 
        def compare_perds(pred):
            if not pred: return 1
            # print(pred)
            if pred[0] in logical_ops:
                overlap_flag_list=[]
                for sub_pred in pred[1]:
                    return_overlap_flag=compare_perds(sub_pred)
                    if return_overlap_flag!=2:
                        overlap_flag_list.append(return_overlap_flag)
                if len(overlap_flag_list)==0:
                    return 2
                elif pred[0]==logical_ops[0]: #or
                    if sum(overlap_flag_list)>=1: return 1
                    else: return 0
                else: #and
                    if 0 in overlap_flag_list: return 0
                    else: return 1

            else:
                if pred[0] not in self.vp:
                    return 2
                scan_dim=self.vp.index(pred[0])
                pred=pred[1:]
                if self.boundary[scan_dim] is None:
                    return 1
                elif isinstance(self.boundary[scan_dim],list):
                    if isinstance(pred[1],list):
                        if list_solved_list(pred[1],self.boundary[scan_dim]) and pred[0]=='=':
                            return 1
                        elif not list_in_list(self.boundary[scan_dim],pred[1]) and pred[0]=='!=':
                            return 1
                        else: return 0
                    else:
                        if pred[0]=='=' and pred[1] in self.boundary[scan_dim]:
                            return 1
                        elif pred[0]=='!=' and (pred[1] not in self.boundary[scan_dim] or (pred[1] in self.boundary[scan_dim] and len(self.boundary[scan_dim])>1)):
                            return 1 
                        else: return 0
                else:
                    if isinstance(pred[1],list):
                        # '=' or '!='
                        if pred[0]=='!=': return 1
                        for pred_val in pred[1]:
                            if pred_val>=self.boundary[scan_dim][1] and pred_val<=self.boundary[scan_dim][0]:
                                return 1
                        return 0
                    else:
                        if (pred[1]>self.boundary[scan_dim][1] and pred[0] in ['<=','<']) or (pred[1]<self.boundary[scan_dim][0] and pred[0] in ['>=','>']):
                            return 1
                        if pred[1]>self.boundary[scan_dim][0] and pred[1]<self.boundary[scan_dim][1]:
                            return 1
                        elif (pred[1]==self.boundary[scan_dim][0] or pred[1]==self.boundary[scan_dim][1]) and pred[0] in ['<=','>=','=']:
                            return 1
                        else:
                            return 0
        """Finish Define"""
        overlap_flag=compare_perds(query['lg_preds'])
        return overlap_flag  

    def is_overlap_n(self, query):
        '''
        todo: Convert `in` `not in` `like` `not like` ——> `!=` `=` 
        '''
        overlap_flag = False
        # inside_flag = True
        query_bound={}
        for pred in eval(query['preds']): 
            if pred[0] not in self.vp: continue
            scan_dim=self.vp.index(pred[0])
            if scan_dim not in query_bound.keys():
                query_bound[scan_dim]=[(pred[1],pred[2])]
            else: query_bound[scan_dim].append((pred[1],pred[2]))

        for scan_dim in query_bound.keys():
            if self.boundary[scan_dim] is None:
                continue
            elif isinstance(self.boundary[scan_dim],list):
                preds=query_bound[scan_dim]
                for pred in preds:
                    if isinstance(pred[1],list):
                        if list_solved_list(pred[1],self.boundary[scan_dim]) and pred[0]=='=':
                            overlap_flag=True
                        elif not list_in_list(self.boundary[scan_dim],pred[1]) and pred[0]=='!=':
                            overlap_flag=True
                        else: overlap_flag=False
                    else:
                        if pred[0]=='=' and pred[1] in self.boundary[scan_dim]:
                            overlap_flag=True
                        elif pred[0]=='!=' and (pred[1] not in self.boundary[scan_dim] or (pred[1] in self.boundary[scan_dim] and len(self.boundary[scan_dim])>1)):
                            overlap_flag=True 
                        else: overlap_flag=False
            else:
                preds=query_bound[scan_dim]
                for pred in preds:
                    if (pred[1]>self.boundary[scan_dim][1] and pred[0] in ['<=','<']) or (pred[1]<self.boundary[scan_dim][0] and pred[0] in ['>=','>']):
                        overlap_flag=True
                    if pred[1]>self.boundary[scan_dim][0] and pred[1]<self.boundary[scan_dim][1]:
                        overlap_flag=True
                    elif (pred[1]==self.boundary[scan_dim][0] or pred[1]==self.boundary[scan_dim][1]) and pred[0] in ['<=','>=','=']:
                        overlap_flag=True
                    else:
                        overlap_flag=False
        if overlap_flag:
            return 1
        else:
            return 0

class PartitionTree:
    def __init__(self, boundary = [],vp=[]):
        self.vp=vp
        self.num_dims = len(vp)
        self.boundary=boundary
        # self.boundary=[domain[0] for domain in boundary]+[domain[1] for domain in boundary]
        self.pt_root=PartitionNode(self.num_dims,vp, self.boundary, nid = 0, num_children = 0, pid = -1, children_ids = [], is_leaf = True, node_size = 0)
        
        self.nid_node_dict = {0: self.pt_root} # node id to node dictionary
        self.node_count = 1
        

    def get_leaves(self):
        nodes = []
        for nid, node in self.nid_node_dict.items():
            if node.is_leaf:
                nodes.append(node)
        return nodes

    def apply_random_split(self, parent_nid, split_dim, block_limit, encode_dicts):
        parent_node = self.nid_node_dict[parent_nid]
        parent_node.dataset=parent_node.dataset[np.argsort(parent_node.dataset[:,split_dim])]
        parent_node.split_scheme=(split_dim,'avg_split',0)
        datasets=np.array_split(parent_node.dataset,math.ceil(parent_node.node_size/block_limit))
        row_id=0
        for dataset in datasets:
            child_node = PartitionNode(self.num_dims,self.vp, boundary=col_parser(dataset,encode_dicts,self.vp), nid = -1, pid = -1, num_children = 0, children_ids = [], is_leaf = True, node_size = len(dataset))
            child_node.dataset=dataset
            end_row_id=row_id+len(dataset)
            child_node.raw_row_ids=np.arange(row_id,end_row_id)
            self.add_node(parent_nid, child_node)
            row_id=end_row_id
        print('Random split has completed!')


    def apply_split(self, parent_nid, split_dim, split_op, split_value, encode_dicts,data_threshold):
        parent_node = self.nid_node_dict[parent_nid]
        
        # def 
        # create sub nodes
        child_node1 = copy.copy(parent_node)
        child_node1.children_ids = []

        # child_node2 = copy.deepcopy(parent_node)
        child_node2 = copy.copy(parent_node)
        child_node2.children_ids = []

        # cur_boundary=child_node1.boundary[split_dim]
        # if isinstance(cur_boundary,np.ndarray):
        #     if isinstance(split_value,list): 
        #         print('Unsolved split cond!')
        #         exit(-1)
        #     child_node1.boundary[split_dim][1]= split_value
        #     child_node2.boundary[split_dim][0]= split_value
        # elif isinstance(cur_boundary,list):
        #     if not isinstance(split_value,list): my_split_value=[split_value]
        #     else:  my_split_value=split_value
        #     child_node1.boundary[split_dim]=my_split_value
        #     child_node2.boundary[split_dim]=list(set(child_node2.boundary[split_dim])-set(my_split_value))

        if parent_node.dataset is not None:
            if isinstance(split_value,str):
                parent_node.split_scheme=(split_dim,'median_split',0)
                split_set=np.split(parent_node.dataset, 2, axis=0)
                child_node1.dataset,child_node2.dataset=split_set[0],split_set[1]
            else:
                if split_op=='<':
                    op_bool1=parent_node.dataset[:,split_dim] <= split_value
                    op_bool2=parent_node.dataset[:,split_dim] > split_value
                    child_node1.dataset = parent_node.dataset[op_bool1]
                    child_node2.dataset = parent_node.dataset[op_bool2]
                    child_node1.raw_row_ids = parent_node.raw_row_ids[op_bool1]
                    child_node2.raw_row_ids = parent_node.raw_row_ids[op_bool2]
                else:
                    if isinstance(split_value,list):
                        true_pos,false_pos=[],[]
                        for pos in range(parent_node.dataset.shape[0]):
                            if parent_node.dataset[pos][split_dim] in split_value: true_pos.append(pos)
                            else: false_pos.append(pos)
                        child_node1.dataset=parent_node.dataset[true_pos,:]
                        child_node2.dataset=parent_node.dataset[false_pos,:]
                        child_node1.raw_row_ids = parent_node.raw_row_ids[true_pos]
                        child_node2.raw_row_ids = parent_node.raw_row_ids[false_pos]
                    else:
                        op_bool1=parent_node.dataset[:,split_dim] == split_value
                        op_bool2=parent_node.dataset[:,split_dim] != split_value
                        child_node1.dataset = parent_node.dataset[op_bool1]
                        child_node2.dataset = parent_node.dataset[op_bool2]
                        child_node1.raw_row_ids = parent_node.raw_row_ids[op_bool1]
                        child_node2.raw_row_ids = parent_node.raw_row_ids[op_bool2]

            child_node1.node_size = len(child_node1.dataset)
            child_node2.node_size = len(child_node2.dataset)
            if child_node1.node_size< data_threshold or child_node2.node_size<data_threshold:
                print("invalid split!!!!!!!!!!!")
            child_node1.boundary=col_parser(child_node1.dataset,encode_dicts,self.vp)
            child_node2.boundary=col_parser(child_node2.dataset,encode_dicts,self.vp)
            child_node1.is_leaf=True
            child_node2.is_leaf=True
        if parent_node.queryset is not None:
            left_part, right_part, mid_part = parent_node.split_queryset(split_dim, split_op, split_value)
            child_node1.queryset = left_part + mid_part
            child_node2.queryset = right_part + mid_part
            
        #print("[Apply Split] After split node", parent_nid, "left child queryset:", child_node1.queryset, "MBRs:", child_node1.query_MBRs)
        #print("[Apply Split] After split node", parent_nid, "right child queryset:", child_node2.queryset, "MBRs:", child_node2.query_MBRs)

        # update current node
        self.add_node(parent_nid, child_node1)
        self.add_node(parent_nid, child_node2)
        self.nid_node_dict[parent_nid].split_type = "candidate cut"
        return child_node1,child_node2
            
    def add_node(self, parent_id, child_node):
        child_node.nid = self.node_count
        self.node_count += 1
        
        child_node.pid = parent_id
        self.nid_node_dict[child_node.nid] = child_node
        
        child_node.depth = self.nid_node_dict[parent_id].depth + 1
        
        self.nid_node_dict[parent_id].children_ids.append(child_node.nid)
        self.nid_node_dict[parent_id].num_children += 1
        self.nid_node_dict[parent_id].is_leaf = False
    
    def query_single(self, query, print_info = False, consider_scan=False):
        if consider_scan:
            return list(self.nid_node_dict.keys())
        partition_ids = self.__find_overlapped_partition(self.pt_root, query, print_info)
        return partition_ids

    def __find_overlapped_partition(self, node, query, print_info = False):
        if print_info:
            print("Enter node", node.nid)
            
        if node.is_leaf:
            if print_info:
                print("node", node.nid, "is leaf")
            
            if print_info and node.is_overlap(query) > 0:
                print("node", node.nid, "is added as result")
            return [node.nid] if node.is_overlap(query) > 0 else []
            
        node_id_list = []
        if node.is_overlap(query) <= 0:
            if print_info:
                print("node", node.nid, "is not overlap with the query")
            pass
        else:
            if print_info:
                print("searching childrens for node", node.nid)
            for nid in node.children_ids:
                node_id_list += self.__find_overlapped_partition(self.nid_node_dict[nid], query, print_info)
        return list(set(node_id_list))
    
    def evaluate_query_cost(self, queries, print_result = False):
        # if len(queries)==0: return 0
        '''
        get the logical IOs of the queris
        return the average query cost
        '''
        total_cost = 0
        for count,query in enumerate(queries):
            cost = 0
            overlapped_leaf_ids = self.query_single(query)
            print(f"~~~~~~~~~Query#{count}: Blocks: {overlapped_leaf_ids}")
            
            actual_data_size=[]
            for nid in overlapped_leaf_ids:
                if nid >= 0:
                    cur_node=self.nid_node_dict[nid]
                    cost += cur_node.node_size
                    actual_data_size.append(cur_node.node_size)
                else:
                    cost += (-nid) # redundant partition cost
            print(f"query #{count}: {actual_data_size}")
            # print(f"query #{count}: cost:{sum(actual_data_size)} num:{len(actual_data_size)}")
            total_cost += cost
        
        if print_result:
            print("Total logical IOs:", total_cost)
            print("Average logical IOs:", total_cost // len(queries))
        return total_cost // len(queries)

    def save_tree(self, path):
        node_list = self.__generate_node_list(self.pt_root) # do we really need this step?
        # serialized_node_list = self.__serialize(node_list)
        serialized_node_list = self.__serialize_by_pickle(node_list)
        #print(serialized_node_list)
        # np.savetxt(path, serialized_node_list, delimiter=',')
        with open(path, "wb") as f:
            pickle.dump(serialized_node_list, f, True)
        return serialized_node_list
            
    def load_tree(self, path):
        # serialized_node_list = genfromtxt(path, delimiter=',')
        with open(path, "rb") as f:
            serialized_node_list=pickle.load(f)
        self.__build_tree_from_serialized_node_list(serialized_node_list)

    def __generate_node_list(self, node):
        '''
        recursively add childrens into the list
        '''
        node_list = [node]
        for nid in node.children_ids:
            node_list += self.__generate_node_list(self.nid_node_dict[nid])
        return node_list

    def __serialize_by_pickle(self,node_list):
        serialized_node_list = []
        for node in node_list:
            # follow the same order of attributes in partition class
            attributes = [node.num_dims,node.boundary,node.nid,node.pid]
            attributes.append(node.num_children) # number of children
            #attributes += node.children_ids
            attributes.append(1 if node.is_leaf else 0)
            attributes.append(node.node_size)
            attributes.append(node.depth)
            if node.is_leaf: attributes.append(node.raw_row_ids)
            else: attributes.append(np.array([]))
            attributes.append(node.split_scheme)
            attributes.append(node.children_ids)
            serialized_node_list.append(attributes)
        return serialized_node_list


    def __build_tree_from_serialized_node_list(self, serialized_node_list):
        self.node_count=len(serialized_node_list)
        self.pt_root = None
        self.nid_node_dict.clear()
        pid_children_ids_dict = {}
        for serialized_node in serialized_node_list:
            num_dims = serialized_node[0]
            boundary = serialized_node[1]
            nid = serialized_node[2]  # node id
            pid = serialized_node[3]  # parent id
            num_children = serialized_node[4]
            is_leaf = False if serialized_node[5] == 0 else True
            node_size = serialized_node[6]
            node = PartitionNode(num_dims,self.vp, boundary, nid, num_children, pid, [], is_leaf, node_size)  # let the children_ids empty
            node.depth=serialized_node[7]
            if is_leaf: node.raw_row_ids=serialized_node[8]
            node.dataset=np.array([])
            node.split_scheme=serialized_node[9]
            node.children_ids=serialized_node[10]
            self.nid_node_dict[nid] = node  # update dict

        #     if node.pid in pid_children_ids_dict:
        #         pid_children_ids_dict[node.pid].append(node.nid)
        #     else:
        #         pid_children_ids_dict[node.pid] = [node.nid]

        # # make sure the irregular shape partition is placed at the end of the child list
        # for pid, children_ids in pid_children_ids_dict.items():
        #     if pid == -1:
        #         continue
        #     self.nid_node_dict[pid].children_ids = children_ids
        
        self.pt_root = self.nid_node_dict[0]