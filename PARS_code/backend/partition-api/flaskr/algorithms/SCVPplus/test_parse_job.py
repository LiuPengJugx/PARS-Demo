import os
import sys
sys.path.append('/home/liupengju/pycharmProjects/partition-api/')
from flaskr.algorithms.SCVPplus.Conf import col_inf,schema_inf
from flaskr.algorithms.SCVPplus.utils import *
from flaskr.algorithms.SCVPplus.q_parser import get_all_tab_rows,encode_one_query
from flaskr.algorithms.SCVPplus.hp_partitioner import Scvp_Plus
scvp=Scvp_Plus()
benchmark='job'
# for tab in schema_inf[benchmark]:
#     scvp.clean_data(benchmark,tab)
# dataset=load_data(benchmark,tab)
# encode_dicts=load_dict(benchmark,tab)
base_dir=f'queries/{benchmark}-queries'
sqls=[]
card_dict=get_all_tab_rows()
col_dict={}
for tab in schema_inf[benchmark]:
    col_dict[tab]=load_dict(benchmark,tab)
total_sql_embs=[]
for file_name in sorted(os.listdir(base_dir)):
    print(file_name)
    path=f'{base_dir}/{file_name}'
    if os.path.isfile(path):
        with open(path,'r') as reader:
            sql_str=reader.read()
            sqls=sql_str.split(';\n')
            for id,sql in enumerate(sqls):
                # if id!=3: continue 
                sql_embs=encode_one_query(sql,has_logical=True,card_dict=card_dict,col_dict=col_dict,keep_colnames=False)
                total_sql_embs+=list(sql_embs.values())

tpch_q_dict={}
for sql_emb in total_sql_embs:
    if sql_emb['tab'] not in tpch_q_dict.keys():
        tpch_q_dict[sql_emb['tab']]=[sql_emb]
    else:
        tpch_q_dict[sql_emb['tab']].append(sql_emb)

for tab in tpch_q_dict.keys():
    write_csv(f'/home/liupengju/pycharmProjects/partition-api/flaskr/algorithms/SCVP/data/{benchmark}/{tab}.csv',tpch_q_dict[tab],total_sql_embs[0].keys())
