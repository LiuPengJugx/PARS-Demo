import sys
sys.path.append('/home/liupengju/pycharmProjects/partition-api/')
from flaskr.db import Postgres
from flaskr.algorithms.SCVPplus.q_parser import encode_one_query
from flaskr.algorithms.SCVPplus.utils import *
from flaskr.algorithms.SCVPplus.Conf import col_inf,schema_inf

class Q_Encoder:
    def __init__(self,benchmark) -> None:
        self.col_dict={}
        for tab in schema_inf[benchmark]:
            self.col_dict[tab]=load_dict(benchmark,tab)
        pg=Postgres()
        pg.getPgConnCur()
        pg.cur.execute("SET search_path=\"ADPS\";")
        self.card_dict={}
        for tab in col_inf.keys():
            self.card_dict[tab]=pg.executeQuery(f'SELECT COUNT(*) FROM {tab};')[0][0][0]
    def encode(self,q,keep_colnames=False):
        sql_embs=encode_one_query(q,has_logical=True,col_dict=self.col_dict,card_dict=self.card_dict,keep_colnames=keep_colnames)
        # return sql_embs
        return list(sql_embs.values())