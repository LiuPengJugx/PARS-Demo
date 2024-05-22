from sql_metadata import Parser
from moz_sql_parser import parse
# from sqlglot import parse_one, exp
import sys
sys.path.append('/home/liupengju/pycharmProjects/partition-api/')
from flaskr.db import Postgres
from flaskr.algorithms.SCVPplus.Conf import col_inf,schema_inf
from flaskr.algorithms.SCVPplus.utils import *
import re
import os
pg=Postgres()
pg.getPgConnCur()
pg.cur.execute("SET search_path=\"ADPS\";")
def get_all_tab_rows(tab=None):
    card_dict={}
    if tab:
        return pg.executeQuery(f'SELECT COUNT(*) FROM {tab};')[0][0][0]
    else:
        for tab in col_inf.keys():
            card_dict[tab]=pg.executeQuery(f'SELECT COUNT(*) FROM {tab};')[0][0][0]
    return card_dict


def get_tab_from_conf(col):
    for tab in col_inf.keys():
        if col in col_inf[tab]['name']:
            return tab,col_inf[tab]['name'].index(col)
    return None,None

def get_all_cols_from_tab(tab):
    if tab in col_inf.keys():
        return list(range(0,len(col_inf[tab]['name'])))
    else:
        return None


class Logical_Node:
    def __init__(self,text=''):
        self.text=text
        self.parent=None
        self.childs=[]
# class Logical_Tree:
#     def __init__(self):
#         self.root=Logical_Node()
#         self.node_dict_list={}

def add_node(parent_node,child_node):
    parent_node.childs.append(child_node)
    child_node.parent=parent_node
def del_node(parent_node,child_node):
    if child_node in parent_node.childs:
        parent_node.childs.remove(child_node)


def get_predicates_from_where(where_dts,tabs,col_dict,keep_colnames=False):
    # domains ==> predicates
    # level: logical_op->op
    logical_ops=['or','and']
    ops={'eq':'=','neq':'!=','in':'=','nin':'!=','between':['>=','<='],'not_like':'!=','like':'=',
        'gte':'>=','lte':'<=','lt':'<','gt':'>'}
    preds={}
    lg_tree_str={}
    logical_trees=[]
    for where_dt in where_dts:
        logical_tree=Logical_Node()
        logical_trees.append(logical_tree)
        nodes=[(where_dt,logical_tree)]
        lg_op_list=[]
        op_list=[]
        pred_list=[] # store string node
        temp_lg_nodes=[] #store temporary 
        depth=0
        while nodes:
            depth+=1
            new_nodes=[]
            for nid,node_cell in enumerate(nodes):
                node,lg_node=node_cell[0],nodes[nid][1]
                if isinstance(node,dict):
                    # logical_op/ op
                    for key in node.keys():
                        # error keys
                        if key in ['not','exists']:continue
                        if key in ops.keys(): 
                            op_list.append(key)
                            new_nodes.append((node[key],lg_node))
                        if key in logical_ops: 
                            lg_op_list.append(key)
                            lg_node.text=key
                            new_lg_node=Logical_Node()
                            add_node(lg_node,new_lg_node)
                            new_nodes.append((node[key],new_lg_node))
                elif isinstance(node,list):
                    # items: [str (value)/ dict (op)]
                    if isinstance(node[0],str):
                        sub_query_flag=False
                        for im_k,item in enumerate(node):
                            if isinstance(item,dict):
                                if 'date' in item.keys(): node[im_k]=item['date']['literal']
                                elif 'literal' in item.keys(): 
                                    node[im_k]=str(item['literal'])
                                    # if isinstance(item['literal'],list): node[im_k]=str(item['literal']) else: 
                                elif 'select' in item.keys(): sub_query_flag=True
                                elif 'add' in item.keys(): 
                                    node[im_k]=str(sum(item['add']))
                                elif 'sub' in item.keys(): 
                                    node[im_k]=str(item['sub'][0]-item['sub'][1])
                            elif isinstance(item,list): node[im_k]=str(item)
                            elif isinstance(item,int) or isinstance(item,float): node[im_k]=str(item)
                        if sub_query_flag:
                            continue
                        new_nodes.append(('————'.join(node),lg_node))
                    else:
                        for item in node:
                            new_lg_node=Logical_Node()
                            parent_lg_node=lg_node.parent
                            del_node(parent_lg_node,lg_node)
                            add_node(parent_lg_node,new_lg_node)
                            new_nodes.append((item,new_lg_node))
                elif isinstance(node,str):
                    pred_list.append(node)
                    lg_node.text=node
                    temp_lg_nodes.append(lg_node)
            # nodes=copy.deepcopy(new_nodes)
            nodes=new_nodes
            # print('depth:',depth,'; nodes:',nodes)
        # generate predicates
        # print(lg_op_list)
        # print(op_list)
        new_pred_list=[]
        for nid,leaf_node in enumerate(pred_list):
            strs=leaf_node.split('————')
            # clear table aliases
            for sid,s in enumerate(strs):
                dot_pos=s.find('.')
                if dot_pos>=0 and get_tab_from_conf(s[dot_pos+1:])[0]: strs[sid]=s[dot_pos+1:]
            if op_list[nid]=='between':
                # pred=[strs[1],ops[op_list[nid]][0],strs[0],ops[op_list[nid]][1],strs[2]]
                pred=[strs[0],ops[op_list[nid]][0],strs[1],ops[op_list[nid]][1],strs[2]]
            else:
                pred=[strs[0],ops[op_list[nid]],strs[1]]
            new_pred_list.append(pred)   

        for nid,leaf_node in enumerate(new_pred_list):
            lg_node=temp_lg_nodes[nid]
            tab1=get_tab_from_conf(leaf_node[0])[0]
            if tab1 and tab1 not in preds: preds[tab1]=[]
            if len(leaf_node)==5:
                preds[tab1].append([leaf_node[0],leaf_node[1],leaf_node[2]])
                preds[tab1].append([leaf_node[0],leaf_node[3],leaf_node[4]])
                # preds[tab1].append([strs[0],ops[op_list[nid]][0],strs[1]])
                # preds[tab1].append([strs[0],ops[op_list[nid]][1],strs[2]])
                lg_node.text='and'
                add_node(lg_node,Logical_Node(preds[tab1][-2]))
                add_node(lg_node,Logical_Node(preds[tab1][-1]))
            else:
                tab2=get_tab_from_conf(leaf_node[2])[0]
                if not tab2 and tab1:
                # if not tab2:
                    preds[tab1].append(leaf_node)
                    lg_node.text=leaf_node
                elif tab1 and tab2:
                    if tab2 not in preds: preds[tab2]=[]
                    if leaf_node[1]=='=':
                        for leaf_node2 in new_pred_list:
                            if not get_tab_from_conf(leaf_node2[2])[0]:
                                if leaf_node2[0]==leaf_node[0]: 
                                    pred=[leaf_node[2],leaf_node2[1],leaf_node2[2]]
                                    preds[tab2].append(pred)
                                    add_node(lg_node.parent,Logical_Node(pred))
                                elif leaf_node2[0]==leaf_node[2]:
                                    pred=[leaf_node[0],leaf_node2[1],leaf_node2[2]]
                                    preds[tab1].append(pred)
                                    add_node(lg_node.parent,Logical_Node(pred))
                    del_node(lg_node.parent,lg_node)
            # print(pred_strs)
        # for pred in preds:
        #     if len(pred)==3:
        #         tab1=get_tab_from_conf(pred[0])[0]
        #         tab2=get_tab_from_conf(pred[2])[0]
        #         if tab1 and not tab2: sql_embs[tab1]['preds'].append([col_inf[tab1]['name'].index(pred[0]),pred[1],pred[2]])
        #         # tansfer more join predicates 
        #         elif tab1 and tab2:
        #             for pred2 in preds:
        #                 if not get_tab_from_conf(pred2[2])[0]:
        #                     if pred2[0]==pred[0]: sql_embs[tab2]['preds'].append([col_inf[tab2]['name'].index(pred[2]),pred2[1],pred2[2]])
        #                     elif pred2[0]==pred[2]: sql_embs[tab1]['preds'].append([col_inf[tab1]['name'].index(pred[0]),pred2[1],pred2[2]])
        #     else:
        #         print('unkonwn pred length:',len(pred))


    """Define print fun"""
    def print_lg_tree(node):
        tree_preds={}
        for tab in tabs: tree_preds[tab]=[]
        if node.text in logical_ops:
            if len(node.childs)==1: 
                sub_result=print_lg_tree(node.childs[0])
                for tab in sub_result.keys():
                    tree_preds[tab]=sub_result[tab]
            else:
                for tab in tabs: tree_preds[tab]=[node.text,[]]
                for child_node in node.childs:
                    sub_result=print_lg_tree(child_node)
                    for tab in sub_result.keys():
                        if sub_result[tab]!=[]:
                            tree_preds[tab][1].append(sub_result[tab])
                for tab in tabs:
                    if len(tree_preds[tab][1])==1: tree_preds[tab]=tree_preds[tab][1][0]
                    elif len(tree_preds[tab][1])==0: tree_preds[tab]=tree_preds[tab][1]
        else:
            if node.text!='':
                if keep_colnames:
                    tab,_=get_tab_from_conf(node.text[0])
                    tree_preds[tab]=node.text
                else:
                    tab,col_id=get_tab_from_conf(node.text[0])
                    tree_preds[tab]=[col_id,node.text[1],encode_pred_val(col_id,node.text[2],col_dict[tab])]
        return tree_preds
    """Function define finished"""
    
    if logical_trees:
        new_logical_tree=Logical_Node('or')
        if len(logical_trees)>1:
            for logical_tree in logical_trees:
                add_node(new_logical_tree,logical_tree)
        else:
            new_logical_tree=logical_trees[0]
        lg_tree_str=print_lg_tree(new_logical_tree)
    # print(lg_tree_str)
    return preds,lg_tree_str
                    
def get_sel_presql(sql,card_dict):
    # print(sql)
    query_plan=pg.executeQuery('EXPLAIN '+sql)[0]
    q_sel_dict={}
    for opt in query_plan:
        # print(opt)
        plan_str=opt[0]
        scan_tab=''
        if plan_str.find('Seq Scan')>=0:
            scan_tab=re.findall(r'Scan on(.*?)\(cost',plan_str)[0].strip().split(' ')[0]
            est_rows=int(re.findall(r'rows=(.*?)width',plan_str)[0].strip())
            # print(scan_tab,est_rows)
            # if scan_tab in q_sel_dict.keys(): q_sel_dict[scan_tab].append(est_rows)
            # else: q_sel_dict[scan_tab]=[est_rows]
        elif plan_str.find('Index Scan')>=0:
            scan_tab=re.findall(r'on(.*?)\(cost',plan_str)[0].strip().split(' ')[0]
            est_rows=int(re.findall(r'rows=(.*?)width',plan_str)[0].strip())
            # print(scan_tab,est_rows)
        elif plan_str.find('Index Only Scan')>=0:
            scan_tab=re.findall(r'on(.*?)\(cost',plan_str)[0].strip().split(' ')[-2]
            est_rows=int(re.findall(r'rows=(.*?)width',plan_str)[0].strip())
        if scan_tab in card_dict.keys():
            if scan_tab in q_sel_dict.keys(): q_sel_dict[scan_tab].append(est_rows)
            else: q_sel_dict[scan_tab]=[est_rows]

    for k in q_sel_dict.keys(): 
        q_sel_dict[k]=max(q_sel_dict[k])/card_dict[k]
    return q_sel_dict

def encode_one_query(sql,has_logical=False,col_dict=None,card_dict=None,keep_colnames=False):
    sql_embs={}
    # print(sql)
    metadata=Parser(sql)
    q_sel_dict=get_sel_presql(sql,card_dict)
    print('q_sel_dict: ',q_sel_dict)
    for tab in metadata.tables:
        if tab in card_dict.keys():
            sql_embs[tab]={'tab':tab,'scan':[],'filter':[],'sel':q_sel_dict[tab],'preds':[],'gp_ob':[]}
    clauses=['select','where','group_by','order_by']
    for clause in clauses:
        if clause not in metadata.columns_dict.keys(): metadata.columns_dict[clause]=[]
    for col in metadata.columns_dict['select']:
        # # consider *
        # if col=='*':
        #     for tab in sql_embs.keys(): sql_embs[tab]['scan']=get_all_cols_from_tab(tab)
        #     break
        # elif col.find('*')>0:
        #     tab=col.split('.*')[0]
        #     sql_embs[tab]['scan']=get_all_cols_from_tab(tab)
        # else:
        tab,col_id=get_tab_from_conf(col)
        if tab:
            if keep_colnames:
                sql_embs[tab]['scan'].append(col)
            else:
                sql_embs[tab]['scan'].append(col_id)
    
    for col in metadata.columns_dict['where']:
        tab,col_id=get_tab_from_conf(col)
        if tab:
            if keep_colnames:
                sql_embs[tab]['filter'].append(col)
            else:
                sql_embs[tab]['filter'].append(col_id)
            # sql_embs[tab]['filter_domains'].append([0,0])
    for col in set(metadata.columns_dict['group_by']+metadata.columns_dict['order_by']):
        tab,col_id=get_tab_from_conf(col)
        if tab:
            if keep_colnames:
                sql_embs[tab]['gp_ob'].append(col)
            else:
                sql_embs[tab]['gp_ob'].append(col_id)
    # print(metadata.columns_dict)
    sql_json=parse(sql)
    # print(json.dumps(sql_json))
    # extract where predictes
    where_info=[]
    if 'where' in sql_json.keys(): where_info.append(sql_json['where'])
    if isinstance(sql_json['from'],dict):
        from_value_obj=sql_json['from']['value']
        if isinstance(from_value_obj,dict):
            if 'where' in from_value_obj.keys(): where_info.append(sql_json['from']['value']['where'])
        # elif isinstance(from_value_obj,str):

    # print(where_info)
    preds,lg_tree_str=get_predicates_from_where(where_info,metadata.tables,col_dict,keep_colnames=keep_colnames)
    # print('preds:',preds)
    # divide (join) preds to different tables
    for tab in preds.keys():
        for pred in preds[tab]:
            if keep_colnames:
                sql_embs[tab]['preds'].append([pred[0],pred[1],pred[2]])
            else:
                col_id=col_inf[tab]['name'].index(pred[0])
                sql_embs[tab]['preds'].append([col_id,pred[1],encode_pred_val(col_id,pred[2],col_dict[tab])])
    # from sqloxide import parse_sql
    # output = parse_sql(sql=sql, dialect='ansi')
    # print(output)
    # print(sql_embs)
    if has_logical:
        for sql_emb in sql_embs.values():
            tab=sql_emb['tab']
            if tab in lg_tree_str:
                sql_emb['lg_preds']=lg_tree_str[tab]
            else:
                sql_emb['lg_preds']=[]
    return sql_embs


if __name__=='__main__':
    benchmark='synthetic'
    base_dir=f'queries/{benchmark}-queries'
    sqls=[]
    card_dict=get_all_tab_rows()
    col_dict={}
    for tab in col_inf.keys():
        if benchmark=='synthetic' and tab.find('widetable')==-1:
            continue
        col_dict[tab]=load_dict(benchmark,tab)
    total_sql_embs=[]
    for file_name in sorted(os.listdir(base_dir)):
        PATH=f'{base_dir}/{file_name}'
        if os.path.isdir(PATH): continue
        with open(PATH,'r') as reader:
            if file_name in ['d1-8.sql','d1-9.sql','d1-15.sql','d1-22.sql'] or file_name[-3:]!='sql':
                continue
            # if file_name not in ['d1-19.sql']:
            #     continue
            sql=reader.read()
            print(file_name)
            sql_embs=encode_one_query(sql,has_logical=True,card_dict=card_dict,col_dict=col_dict,keep_colnames=False)
            total_sql_embs+=list(sql_embs.values())
    # print(total_sql_embs)

    # write simple query emb
    # tpch_q_dict={}
    # for id,sql_emb in enumerate(total_sql_embs):
    #     if not sql_emb['scan'] and not sql_emb['filter']:
    #         continue
    #     smp_emb={'scan':','.join([str(item) for item in set(sql_emb['scan']+sql_emb['filter'])]),
    #         'freq':1,
    #         'filter':','.join([str(item) for item in sql_emb['filter']]),
    #         'sel':round(sql_emb['sel'],3),
    #         'gp_ob':','.join([str(item) for item in sql_emb['gp_ob']]),
    #     }
    #     if sql_emb['tab'] not in tpch_q_dict.keys():
    #         tpch_q_dict[sql_emb['tab']]=[smp_emb]
    #     else:
    #         tpch_q_dict[sql_emb['tab']].append(smp_emb)
    # for tab in tpch_q_dict.keys():
    #     write_csv(f'/home/liupengju/pycharmProjects/partition-api/flaskr/algorithms/SCVP/data/tpchplus/{tab}.csv',tpch_q_dict[tab],tpch_q_dict[tab][0].keys(),header=False)

    # write complete embedding
    tpch_q_dict={}
    for sql_emb in total_sql_embs:
        if sql_emb['tab'] not in tpch_q_dict.keys():
            tpch_q_dict[sql_emb['tab']]=[sql_emb]
        else:
            tpch_q_dict[sql_emb['tab']].append(sql_emb)

    for tab in tpch_q_dict.keys():
        # if tab=='widetable100':
        write_csv(f'/home/liupengju/pycharmProjects/partition-api/flaskr/algorithms/SCVP/data/tpchpro/{tab}.csv',tpch_q_dict[tab],total_sql_embs[0].keys())
