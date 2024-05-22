import random
class Q_Generator:
    def __init__(self) -> None:
        self.tab='widetable'
        self.syn_suffix_dict={'widetable30':'a_','widetable50':'b_','widetable100':'c_'}

    def gen_attrs(self,tab,attr_nums,bound):
        select_str=[]
        select_attr_num=random.randint(bound[0],bound[1])
        for _ in range(select_attr_num):
            rand_attr=self.syn_suffix_dict[tab]+str(random.randint(0,attr_nums-1))
            if rand_attr not in select_str: select_str.append(rand_attr)
        return select_str

    def gen_queries(self,attr_nums,q_amount):
        cur_tab=self.tab+str(attr_nums)
        sqls=[]
        lg_ops=['and','and','or']
        ops=['>','<','>=','<=']
        for qid in range(q_amount):
            select_str=self.gen_attrs(cur_tab,attr_nums,(1,4))
            where_str=self.gen_attrs(cur_tab,attr_nums,(1,2))
            where_preds_str=[]
            for attr in where_str:
                where_preds_str.append(attr+random.sample(ops,1)[0]+str(random.randint(1,9999)))
                where_preds_str.append(random.sample(lg_ops,1)[0])
            where_preds_str=where_preds_str[:-1]
            if random.randint(0,1)==0: gp_str=[]
            else: gp_str=select_str
            if len(gp_str)==0:
                ob_str=random.sample(where_str,random.randint(0,1))
            else:
                ob_str=random.sample(gp_str,random.randint(0,1))
            sql_template=f"SELECT {','.join(select_str)} FROM {cur_tab} WHERE {' '.join(where_preds_str)}"
            if gp_str:
                sql_template+=f" GROUP BY {','.join(gp_str)}"
            elif ob_str:
                sql_template+=f" ORDER BY {','.join(ob_str)}"
            print(sql_template)
            # if qid!=q_amount-1: sqls.append(sql_template+";\n")
            # else:
            sqls.append(sql_template+";")
        return sqls

if __name__=='__main__':
    q_gen=Q_Generator()
    cnt=1
    attr_q_num_mappers={30:5,50:10,100:20}
    # for attr_num in [30,50,100]:
    for attr_num in [30,50,100]:
        sqls=q_gen.gen_queries(attr_num,attr_q_num_mappers[attr_num])
        for sql in sqls:
            if attr_num==100:
                with open(f'/home/liupengju/pycharmProjects/partition-api/queries/synthetic-queries/{cnt}.sql','w') as f:
                    f.write(sql)
            cnt+=1