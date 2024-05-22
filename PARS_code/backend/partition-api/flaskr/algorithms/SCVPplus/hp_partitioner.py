import sys
import numpy as np
import math
import ray
import os
sys.path.append('D:/PycharmProjects/partition-api/')
from flaskr.db import Postgres
import flaskr.algorithms.SCVP.DiskCost as DC
from flaskr.algorithms.SCVP.MyThread import  MyThread
from flaskr.algorithms.SCVPplus.par_tree import PartitionTree
from flaskr.algorithms.SCVPplus.Conf import col_inf
from flaskr.algorithms.SCVPplus.query_encoder import Q_Encoder
import pandas as pd
import pickle
from flaskr.algorithms.SCVPplus.utils import *
from line_profiler import LineProfiler

def do_profile(func):
    def profiled_func(*args, **kwargs):
        profiler = LineProfiler()
        profiler.add_function(func)
        profiler.enable_by_count()
        try:
            return func(*args, **kwargs)
        finally:
            profiler.disable_by_count()
            profiler.print_stats()
            sys.stdout.flush()
    return profiled_func

# last python version 3.6.2
class Wood:
    def __init__(self,vpars,dataset,queries,col_inf_per_tab,data_threshold,encode_dicts,mini_pages=None):
        self.schema=col_inf_per_tab
        self.dataset=dataset
        self.queries=queries
        self.vpars=vpars
        if mini_pages: self.pages=mini_pages
        else: self.pages=vpars
        self.data_threshold=data_threshold
        self.encode_dicts=encode_dicts
        self.boundary=col_parser(dataset,encode_dicts)
        self.child_trees={}

    def gen_trees(self,strategy=1):
        print(f'total q: {len(self.queries)}')
        for vp in self.vpars: 
            vp_width=sum([self.schema['length'][attr_id] for attr_id in vp])
            start_time=time.time()
            partition_tree=PartitionTree(self.boundary[vp],vp)
            partition_tree.pt_root.dataset = self.dataset[:,vp]
            partition_tree.pt_root.raw_row_ids=np.arange(0,len(self.dataset[:,vp]))
            partition_tree.pt_root.node_size = len(self.dataset)
            partition_tree.pt_root.queryset =[q for q in self.queries if list_solved_list(q['scan']+q['filter']+q['gp_ob'],vp)]
            # print(f'{vp}: {len(partition_tree.pt_root.queryset)}') 
            self.child_trees[str(vp)]=partition_tree
            if strategy==1:
                self.__PMT(partition_tree,math.ceil(self.data_threshold/vp_width))
            else: 
                self._BST(partition_tree,math.ceil(self.data_threshold/vp_width))
            end_time=time.time()
            print(f"{vp} Tree Build Time (s):", end_time-start_time)

    def load_trees(self,base_path,strategy=1,raw_data=False):
        time0=time.time()
        for vid,vp in enumerate(self.vpars):
            partition_tree=PartitionTree(self.boundary[vp],vp)
            if base_path.find('tpch')!=-1:
                if strategy==0: load_path=base_path+"_"+str(vp)
                else: load_path=base_path
            else:
                if strategy==0: load_path=base_path+"_"+str(sum(vp))
                else: load_path=base_path
            # if base_path.find('tpch')!=-1:
            #     if strategy==0: load_path=base_path+"_"+str(vp)
            #     else: load_path=base_path+"_"+str(vp)+"_"+str(self.pages)
            # else:
            #     if strategy==0: load_path=base_path+"_"+str(sum(vp))
            #     else:
            #         # load_path=base_path+"_"+str(vp)+"_"+str(self.pages)
            #         end_idx=math.ceil(len(self.pages)/3)
            #         load_path=base_path+"_"+str([sum(page) for page in self.pages[:end_idx]])
            partition_tree.load_tree(load_path)
            if raw_data:
                sub_dataset = self.dataset[:,vp]
                for node in partition_tree.get_leaves():
                    node.dataset=sub_dataset[node.raw_row_ids,:]
            self.child_trees[str(vp)]=partition_tree
        print(f'Tree Route Time(s) :{time.time()-time0}')
        

    def route_data(self,dataset,comp_dim=None):
        time0=time.time()
        for vp in self.vpars:
            child_tree=self.child_trees[str(vp)]
            sub_dataset=dataset[:,vp]
            # total_rows=sub_dataset.shape[0]
            # for nid in child_tree.nid_node_dict.keys():
            #     child_tree.nid_node_dict[nid].dataset=[[] for _ in range(split_size)]
            @ray.remote
            def route_row_ray(chunk_id,row_start_idx,rows,child_tree,comp_dim):
                add_dict={}
                for id,row in enumerate(rows):
                    row_id=row_start_idx+id
                    target_node=child_tree.pt_root
                    while not target_node.is_leaf:
                        # flag=False
                        for nid in target_node.children_ids:
                            child_node=child_tree.nid_node_dict[nid]
                            if child_node.is_overlap_by_row(row,comp_dim):
                                target_node=child_node
                                # flag=True
                                break
                    #     if not flag: add_dict[row_id]=-1 
                    # if not target_node.is_leaf:
                    #     add_dict[row_id]=-1 
                    # else:
                    #     add_dict[row_id]=target_node.nid
                    add_dict[row_id]=target_node.nid
                print(f'Chunk {chunk_id} has completed!')
                return add_dict

            def route_chunk_rows(chunk_id,rows,child_tree):
                for row in rows:
                    target_node=child_tree.pt_root
                    while not target_node.is_leaf:
                        target_node.dataset[chunk_id].append(row)
                        flag=False
                        for nid in target_node.children_ids:
                            child_node=child_tree.nid_node_dict[nid]
                            if child_node.is_overlap_by_row(row):
                                target_node=child_node
                                flag=True
                                break
                        if not flag: return 0
                    if not target_node.is_leaf:
                            return 0
                    else:
                        target_node.dataset[chunk_id].append(row)
                        # 
                return 1
            def route_row_2_node(add_dict,child_tree):
                for row_id in add_dict.keys():
                    # print(f'Process Row {row_id}')
                    nid=add_dict[row_id]
                    node=child_tree.nid_node_dict[nid]
                    if len(node.dataset)==0:
                        node.node_size=1
                        node.dataset=np.array([sub_dataset[row_id]])
                    else:
                        node.node_size+=1
                        node.dataset=np.row_stack((node.dataset,sub_dataset[row_id]))
            result_ids=[]
            chunk_size,num_process=100,20
            if len(sub_dataset)>=chunk_size: split_size=chunk_size
            else: split_size=len(sub_dataset)
            row_start_idx=0
            
            first_loop=True
            child_tree_id=ray.put(child_tree)
            for chunk_id,chunk in enumerate(np.array_split(sub_dataset,split_size)):
                chunk_ = ray.put(chunk)
                result_id=route_row_ray.remote(chunk_id,row_start_idx,chunk_,child_tree_id,comp_dim)
                del chunk_
                result_ids.append(result_id)
                del result_id
                row_start_idx+=len(chunk)
                if chunk_id % num_process == num_process - 1:
                    if first_loop:
                        first_loop = False
                        last_batch_ids = result_ids.copy()
                        result_ids.clear()
                        continue
                    else:
                        print("= = = Process Dump For Chunk", chunk_id - 2 * num_process + 1, "to",
                            chunk_id - num_process, "= = =")
                        while len(last_batch_ids):
                            done_id, last_batch_ids = ray.wait(last_batch_ids)
                            add_dict = ray.get(done_id[0])
                            route_row_2_node(add_dict,child_tree)
                        last_batch_ids = result_ids.copy()
                        result_ids.clear()
                # task=MyThread(route_chunk_rows,(chunk_id,chunk,child_tree))
                # task.start()
                # result_ids.append(task.get_result())
            del child_tree_id
            while len(last_batch_ids):
                done_id, last_batch_ids = ray.wait(last_batch_ids)
                add_dict = ray.get(done_id[0])
                route_row_2_node(add_dict,child_tree)
            last_batch_ids.clear()
            # dict_results=ray.get(result_ids)
            # print(f'Ray Time(s) :{time.time()-time0}')
            # for add_dict in dict_results:
            #     for row_id in add_dict.keys():
            #         # print(f'Process Row {row_id}')
            #         nid=add_dict[row_id]
            #         if nid==-1: exit(-1)
            #         else: 
            #             node=child_tree.nid_node_dict[nid]
            #             if node.dataset is None:
            #                 node.node_size=1
            #                 node.dataset=np.array([sub_dataset[row_id]])
            #             else:
            #                 node.node_size+=1
            #                 node.dataset=np.row_stack((node.dataset,sub_dataset[row_id]))
            # for nid in child_tree.nid_node_dict.keys():
            #     node=child_tree.nid_node_dict[nid]
                # node.dataset=np.array(sum(node.dataset,[]))
                # node.node_size=len(node.dataset)
        print(f'Tree Route Time(s) :{time.time()-time0}')
    
    def save_trees(self,base_path,strategy=0):
        for tid,par_k in enumerate(self.child_trees.keys()):
            
            if strategy==0: 
                self.child_trees[par_k].save_tree(base_path+"_"+par_k)
            elif strategy==1: 
                self.child_trees[par_k].save_tree(base_path)
            
            # if base_path.find('tpch')!=-1:
            #     if strategy==0: self.child_trees[par_k].save_tree(base_path+"_"+par_k)
            #     else: self.child_trees[par_k].save_tree(base_path+"_"+par_k+"_"+str(self.pages))
            # else:
            #     if strategy==0:
            #         self.child_trees[par_k].save_tree(base_path+"_"+str(sum(eval(par_k))))
            #     else:
            #         # self.child_trees[par_k].save_tree(base_path+"_"+par_k+"_"+str(self.pages))
            #         end_idx=math.ceil(len(self.pages)/3)
            #         self.child_trees[par_k].save_tree(base_path+"_"+str([sum(page) for page in self.pages[:end_idx]]))
        
    def eval_wood_blocksize(self,qid,q):
        # if not isinstance(queries,list) or not isinstance(queries,np.ndarray): queries=[queries]
        total_cost=0
        # solved_cols=list(set(q['scan']+q['filter']+q['gp_ob']))
        # print(self.child_trees.keys())
        # for vp in self.child_trees.keys():
        #     if list_solved_list(solved_cols,eval(vp)):
        tree=list(self.child_trees.values())[0]
        overlapped_leaf_ids=tree.query_single(q)
        print(f"~~~~~~~~~Query#{qid}: Blocks: {len(overlapped_leaf_ids)} / {len(tree.get_leaves())}")
        for nid in overlapped_leaf_ids: total_cost+=tree.nid_node_dict[nid].node_size
        return total_cost
    
    def eval_wood_latency(self,qid,q):
        solved_cols=list(set(q['scan']+q['filter']+q['gp_ob']))
        total_lantecy=0
        DC.tab_name=q['tab']
        @ray.remote
        def process_chunk_nodes(chunk_ref,q_ref,tree_ref):
            node_sel_dict={}
            for nid in chunk_ref:
                leaf_node=tree_ref.nid_node_dict[nid]
                satisfy_rows=leaf_node.return_satisfy_rows(q_ref)
                node_sel_dict[nid]=satisfy_rows
            return node_sel_dict
        
        def process_chunk_nodes2(chunk_ref,q_ref,tree_ref):
            node_sel_dict={}
            for nid in chunk_ref:
                leaf_node=tree_ref.nid_node_dict[nid]
                satisfy_rows=leaf_node.return_satisfy_rows(q_ref)
                node_sel_dict[nid]=satisfy_rows
            return node_sel_dict
        
        for vp in self.child_trees.keys():
            if list_solved_list(solved_cols,eval(vp)):
                child_tree=self.child_trees[vp]
                cur_pages=None
                e_vp=eval(vp)
                if e_vp in self.pages:
                    cur_pages=[e_vp]
                else:
                    cur_pages=self.pages
                if not hasattr(child_tree,'q_dict'): child_tree.q_dict={}
                if qid in child_tree.q_dict:
                    node_sel_dict=child_tree.q_dict[qid]
                else:
                    overlapped_leaf_ids=child_tree.query_single(q)
                    print('leaf_ids: ',len(overlapped_leaf_ids))
                    # split multiple chunks
                    node_sel_dict={}
                    if len(overlapped_leaf_ids)==0: pass
                    else:
                        if len(overlapped_leaf_ids)>=30: split_size=30
                        else: split_size=len(overlapped_leaf_ids)
                        result_ids=[]
                        tree_ref=ray.put(child_tree)
                        q_ref=ray.put(q)
                        for chunk in np.array_split(np.array(overlapped_leaf_ids),split_size):
                            # process_chunk_nodes2(chunk,q,child_tree)
                            _chunk_ref=ray.put(chunk)
                            result_ids.append(process_chunk_nodes.remote(_chunk_ref,q_ref,tree_ref))
                            del _chunk_ref
                        del tree_ref
                        del q_ref
                        for sel_dict in ray.get(result_ids): 
                            node_sel_dict.update(sel_dict)
                    child_tree.q_dict[qid]=node_sel_dict
                for nid in node_sel_dict.keys():
                    rows=child_tree.nid_node_dict[nid].dataset.shape[0]
                    satisfy_rows=node_sel_dict[nid]
                    # print(f'node {nid}, rows:{rows}, satisfy_rows:{satisfy_rows}')
                    total_lantecy+=DC.cal_latency_scan_block(q,rows,satisfy_rows,cur_pages)
        return total_lantecy   



    # base tree
    @do_profile
    def _BST(self,partition_tree,block_limit):
        # root_node: nid=0
        root_nid=0
        split_dim=0
        partition_tree.apply_random_split(root_nid,split_dim,block_limit,self.encode_dicts)

        
    # predicate-median tree
    @do_profile
    def __PMT(self,partition_tree,data_threshold):
        CanSplit = True
        print_s=True
        while CanSplit:
            CanSplit = False           
            leaves = partition_tree.get_leaves()
            for leaf in leaves:
                # print("current leaf node id:",leaf.nid, "leaf node dataset size:",len(leaf.dataset))
                if leaf.node_size < 2 * data_threshold:
                    continue
                # print(f'Recut leaf {leaf.nid}: {leaf.node_size}')
                candidate_cuts = leaf.get_candidate_cuts()
                # get best candidate cut position
                skip, max_skip, max_skip_split_dim, max_skip_split_op, max_skip_split_value = 0, -1, 0, '<', 0
                for split_dim,split_op,split_value in candidate_cuts:
                    valid,skip,_,_ = leaf.if_split(split_dim, split_op,split_value, data_threshold)
                    if valid and skip > max_skip:
                        max_skip = skip
                        max_skip_split_dim = split_dim
                        max_skip_split_op = split_op
                        max_skip_split_value = split_value
                        
                # if max_skip<0:
                #     # print('Try median cuts!!!')
                #     candidate_cuts2 = leaf.get_candidate_median_cuts()
                #     # get best candidate cut position
                #     for split_dim,split_op,split_value in candidate_cuts2:
                #         valid,skip,_,_ = leaf.if_split(split_dim, split_op,split_value, data_threshold)
                #         if valid and skip > max_skip:
                #             max_skip = skip
                #             max_skip_split_dim = split_dim
                #             max_skip_split_op = split_op
                #             max_skip_split_value = split_value
                if max_skip >=0:
                    # if the cost become smaller, apply the cut
                    leaf.split_scheme=(max_skip_split_dim,f'{max_skip_split_op}{max_skip_split_value}',max_skip)
                    child_node1, child_node2 = partition_tree.apply_split(leaf.nid, max_skip_split_dim, max_skip_split_op, max_skip_split_value,self.encode_dicts,data_threshold)
                    CanSplit = True


class Scvp_Plus:
    def __init__(self) -> None:
        self.benchmark_base_path='D:/PycharmProjects/partition-api/queries/'    

    def load_pre_encode_queryset(self,path,tab):
        df=pd.read_csv(path)
        queries=df[df['tab']==tab].to_dict('records')
        for qid,q in enumerate(queries):
            queries[qid]['scan']=eval(q['scan'])
            queries[qid]['filter']=eval(q['filter'])
            queries[qid]['gp_ob']=eval(q['gp_ob'])
            queries[qid]['preds']=eval(q['preds'])
            queries[qid]['lg_preds']=eval(q['lg_preds'])
        return queries

    def convert_queryset(self,queries,benchmark,tab):
        encode_dicts=load_dict(benchmark,tab)
        for qid,q in enumerate(queries):
            convert_preds=[]
            # print('Before: ',queries.loc[qid,'preds'])
            for pred in q['preds']:
                # transfer != to =; >= > to <= <
                if pred[1]=='!=':
                    if pred[0] in encode_dicts.keys():
                        if not isinstance(pred[2],list): pred[2]=[pred[2]]
                        pred[2]=list(set(list(encode_dicts[pred[0]].values()))-set(pred[2]))
                        pred[1]='='
                # elif pred[1] in ['>=','>']:
                convert_preds.append(pred)
            # print('After: ',str(encode_preds))
            queries[qid]['preds']=convert_preds
        return queries

    def load_encode_queryset(self,path,benchmark,tab):
        encode_dicts=load_dict(benchmark,tab)
        df=pd.read_csv(path)
        queries=df[df['tab']==tab].to_dict('records')
        for qid,q in enumerate(queries):
            print(q)
            encode_preds=[]
            # print('Before: ',queries.loc[qid,'preds'])
            for pred in eval(q['preds']):
                pred[2]=encode_pred_val(pred[0],pred[2],encode_dicts)
                # transfer != to =; >= > to <= <
                if pred[1]=='!=':
                    if pred[0] in encode_dicts.keys():
                        if not isinstance(pred[2],list): pred[2]=[pred[2]]
                        pred[2]=list(set(list(encode_dicts[pred[0]].values()))-set(pred[2]))
                        pred[1]='='
                # elif pred[1] in ['>=','>']:
                encode_preds.append(pred)
            # print('After: ',str(encode_preds))

            queries[qid]['preds']=encode_preds
            queries[qid]['scan']=eval(q['scan'])
            queries[qid]['filter']=eval(q['filter'])
            queries[qid]['gp_ob']=eval(q['gp_ob'])
            queries[qid]['lg_preds']=eval(q['lg_preds'])
        return queries

    def clean_data(self,benchmark,tab):
        def transfer_date_array(arr):
            for aid,item in enumerate(arr): arr[aid]=transfer_date_timestamp(item)
            print(arr)
            return arr
        cleaned_col_ids=[]
        transfer_col_ids=[]
        attnull_col_ids=[[],[]]
        save_base_path=f'flaskr/algorithms/SCVPplus/data/{benchmark}/'
        dataset=pd.read_csv(f'{save_base_path}{tab}.csv',delimiter=',').values
        #todo: can be automatic 
        pg=Postgres()
        pg.getPgConnCur()
        rows,_=pg.executeQuery(f"SELECT a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull FROM pg_class as c,pg_attribute as a where a.attrelid = c.oid and a.attnum>0 and c.relname = '{tab}';")
        for row in rows:
            if not row[2]:
                if row[1].find('integer')!=-1: attnull_col_ids[0].append(col_inf[tab]['name'].index(row[0]))
                else: attnull_col_ids[1].append(col_inf[tab]['name'].index(row[0]))
            if row[0] in col_inf[tab]['name'] and row[1].find('character(')!=-1 and row[1]!='character(1)':
                print(row)
                cleaned_col_ids.append(col_inf[tab]['name'].index(row[0]))
            # transfer date format
            if row[0] in col_inf[tab]['name'] and row[1].find('date')!=-1:
                print(row)
                transfer_col_ids.append(col_inf[tab]['name'].index(row[0]))
        # -->clean-->delete space
        if cleaned_col_ids:
            dataset[:,cleaned_col_ids]=np.char.strip(np.array(dataset[:,cleaned_col_ids],dtype=str))
        # -->transfer--> date
        print('transfer_col_ids:',transfer_col_ids)
        if transfer_col_ids:
            arr=np.apply_along_axis(transfer_date_array,0,np.array(dataset[:,transfer_col_ids],dtype=str))
            dataset[:,transfer_col_ids]=arr.astype("float32")
        print('date transform completes!')
        # encode type format
        encode_dicts={}
        # -->encode--> type/status fields
        for col_id in range(dataset.shape[1]):
            cell=dataset[0][col_id]
            if isinstance(cell,str): 
                distinct_data=set(dataset[:,col_id])
                if len(distinct_data)/dataset.shape[0]<=0.3:
                    encode_dicts[col_id]={}
                    for code,cell_key in enumerate(distinct_data): encode_dicts[col_id][cell_key]=code
                    print(encode_dicts[col_id])
                    # clear
                    if col_id in attnull_col_ids[1]: attnull_col_ids[1].remove(col_id)
        encode_col_ids=list(encode_dicts.keys())
        def encode_col(row):
            for rid,item in enumerate(row):
                row[rid]=encode_dicts[encode_col_ids[rid]][item]
            return row
        if encode_col_ids:
            arr=np.apply_along_axis(encode_col,1,dataset[:,encode_col_ids])
            dataset[:,encode_col_ids]=arr.astype(np.int)
        # encode nan value in numerical/ string columns
        def nan_numerical_col(row):
            for rid,item in enumerate(row):
                if math.isnan(float(item)): row[rid]=-1
            return row
        def nan_string_col(row):
            for rid,item in enumerate(row):
                if not isinstance(item,str): 
                    if math.isnan(float(item)): row[rid]='None'
            return row
        if attnull_col_ids[0]:
            arr=np.apply_along_axis(nan_numerical_col,1,dataset[:,attnull_col_ids[0]])
            dataset[:,attnull_col_ids[0]]=arr.astype(np.int)
        if attnull_col_ids[1]:
            arr=np.apply_along_axis(nan_string_col,1,dataset[:,attnull_col_ids[1]])
            dataset[:,attnull_col_ids[1]]=arr.astype(np.str)
        print('types encode completes!')

        pd.DataFrame(dataset).to_csv(f'{save_base_path}{tab}_c.csv',header=None)
        with open(f'{save_base_path}{tab}_dict.pkl', "wb") as tf:
            pickle.dump(encode_dicts,tf)
        print(f'Table {tab} has been cleaned!')
        return dataset,encode_dicts


    def gen_trees_by_vp(self,benchmark,tab,vp_pars,load_new=False,load_old=False,data_threshold=10000,strategy=1,mini_pages=None,queries=None):
        # data clean
        # dataset,encode_dicts=self.clean_data(benchmark,tab)
        dataset=load_data(benchmark,tab)
        encode_dicts=load_dict(benchmark,tab)
        # print(encode_dicts)
        if queries is None:
            default_workload_path=f'{self.benchmark_base_path}{benchmark}-queries/encoding_tpch_qs_complete.csv'
            queries=self.load_pre_encode_queryset(default_workload_path,tab)
        queries=self.convert_queryset(queries,benchmark,tab)
        wood=Wood(vp_pars,dataset,queries,col_inf[tab],data_threshold,encode_dicts,mini_pages=mini_pages)
        tree_type='scvp' if strategy==1 else 'base'
        save_tree_path=f'D:/PycharmProjects/partition-api/flaskr/algorithms/SCVPplus/tree/{benchmark}/{tab}_{tree_type}_tree'
        # what is the difference between load_new and load_old?
        if load_new: # assign new data for leaf nodes by the routing way
            wood.load_trees(save_tree_path,strategy=strategy)
            # if is_load_data:
            if strategy==0:
                wood.route_data(dataset,comp_dim=0)
            else: wood.route_data(dataset)
        elif load_old:  # assign sample data for leaf nodes
            wood.load_trees(save_tree_path,strategy=strategy,raw_data=True)
        else:
            wood.gen_trees(strategy)
            wood.save_trees(save_tree_path,strategy=strategy)
        return wood

    def test_tree_access(self,woods:dict):
        total_cost=0
        queries=self.input_new_sqls(tabs=list(woods.keys()))
        for query in queries:
            total_cost+=woods[query['tab']].eval_wood_blocksize(query)
        print('Total access cost: ',total_cost)

    def input_new_sqls(self,benchmark='tpch',tabs=None,sqls=None):
        parsed_queries=[]
        if sqls is None:
            base_dir=f'queries/{benchmark}-queries'
            for file_name in sorted(os.listdir(base_dir)):
                with open(f'{base_dir}/{file_name}','r') as reader:
                    if file_name in ['d1-8.sql','d1-9.sql','d1-15.sql','d1-22.sql'] or file_name[-3:]!='sql':
                        continue
                    queries=Q_Encoder(benchmark).encode(reader.read())
                    if tabs:
                        for query in queries:
                            if query['tab'] in tabs:parsed_queries.append(query)
                    else: parsed_queries+=queries
            # for no in list(set(range(1,23))-set((8,9,15,22))):
            #     with open(f'{base_dir}/d1-{no}.sql','r') as reader:
            #         queries=Q_Encoder(benchmark).encode(reader.read())
            #         if tabs:
            #             for query in queries:
            #                 if query['tab'] in tabs:parsed_queries.append(query)
            #         else: parsed_queries+=queries
        else:
            if isinstance(sqls,str): sqls=[sqls]
            for sql in sqls:
                queries=Q_Encoder(benchmark).encode(sql)
                if tabs:
                    for query in queries:
                        if query['tab'] in tabs:parsed_queries.append(query)
                else: parsed_queries+=queries
        return parsed_queries

    def eval_tree_latency(self,wood:dict,queries):
        total_latency=0
        for qid,query in enumerate(queries):
            if query['tab'] in wood.keys():
                latency=wood[query['tab']].eval_wood_latency(qid,query)
                total_latency+=latency
                print(f'qid: {qid}, latency: {latency}')
        return total_latency

    def eval_tree_blocks(self,wood:dict,queries):
        total_latency=0
        for qid,query in enumerate(queries):
            if query['tab'] in wood.keys():
                latency=wood[query['tab']].eval_wood_blocksize(qid,query)
                total_latency+=latency
                print(f'qid: {qid}, block size: {latency}')
        return total_latency
                    

if __name__=='__main__':
    scvp=Scvp_Plus()
    # scvp.clean_data('synthetic','widetable30')
    woods={}
    vps={}
    for tab in col_inf.keys():
        col_count=len(col_inf[tab]['name'])
        mid_point=int((col_count-1)/2)
        vps[tab]=[list(range(0,mid_point+1)),[0]+list(range(mid_point+1,col_count))]
    # vps={'lineitem':[list(range(8,16)),list(range(8))],
    # 'part':[list(range(4)),list(range(4,8))]}
    for tab in col_inf.keys():
    # for tab in ['lineitem','part']:
        woods[tab]=scvp.gen_trees_by_vp('tpch',tab,vps[tab],load_old=True,strategy=1)
    scvp.test_tree_access(woods)
