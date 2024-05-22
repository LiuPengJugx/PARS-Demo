import gym
from gym.spaces import *
import numpy as np
from sql_metadata import Parser
import sys
import os
import copy
import random
sys.path.append('D:/PycharmProjects/partition-api/')
from flaskr.algorithms.SCVPplus.q_parser import get_tab_from_conf,get_all_tab_rows
from flaskr.algorithms.SCVPplus.Conf import col_inf,schema_inf
from flaskr.algorithms.SCVP.ClimbHill import generate_vps
from flaskr.algorithms.SCVPplus.query_encoder import Q_Encoder
from flaskr.algorithms.SCVP.utils import read_query_data
import flaskr.algorithms.SCVP.DiskCost as DC
from flaskr.algorithms.SCVP.DiskCost import cal_total_cost_improvement as cal_total_cost_update
import multiprocessing as mul

class Env(gym.Env):
    def __init__(self,tab):
        self.tab=tab
        DC.tab_name=tab
        if self.tab.find('widetable30')!=-1:
            self.dims=(6,30)
        elif self.tab.find('widetable50')!=-1:
            self.dims=(6,50)
        elif self.tab.find('widetable100')!=-1:
            self.dims=(6,100)
        elif self.tab in schema_inf['job']:
            self.dims=(31,12)
        else:
            self.dims=(23,16)
        self.action_dict=[]
        for c1 in range(self.dims[1]-1):
            for c2 in range(c1+1,self.dims[1]):
                self.action_dict.append([c1,c2])
        self.action_dict.append([-1,-1]) # terminal action and obtained reward is zero!
        self.action_space=Discrete(len(self.action_dict))
        self.observation_space=Box(shape=(self.dims[0],self.dims[1]), low=0, high=1e7, dtype=np.int)
        self.attr_num=len(col_inf[self.tab]['length'])
        
        self.querys=self.generate_historical_queries()
        # if self.tab=='widetable100':
        #     self.best_pars=[[0], [1, 46, 21, 32, 42], [2], [3, 12, 55], [4], [5], [6], [7], [8], [9, 57, 87, 43, 53, 77], [10], [11], [13], [14, 44], [15], [16], [17], [18], [19], [20], [22], [23], [24, 82, 56], [25, 69, 59, 98, 90, 71], [26], [27], [28], [29], [30], [31], [33], [34], [35], [36], [37, 92, 91], [38], [39], [40], [41], [45], [47], [48], [49], [50], [51], [52], [54], [58], [60, 78], [61], [62], [63], [64], [65], [66], [67], [68], [70], [72], [73], [74], [75], [76], [79], [80], [81], [83], [84], [85], [86], [88], [89], [93], [94], [95], [96], [97], [99]]
        # else:
        self.best_pars=generate_vps(self.querys,col_inf[self.tab]['length'],self.tab)
        print('Hillclimb:',self.best_pars)
        self.V_best=1/cal_total_cost_update(self.querys,self.best_pars,col_inf[self.tab]['length'])
        self.V_0=1/cal_total_cost_update(self.querys,[[i] for i in range(self.attr_num)],col_inf[self.tab]['length'])
        print(f'V_0 {self.V_0}')
        # parameters to be initialized
        self.state=self._get_init_state()
        self.cur_pars=[[i] for i in range(self.attr_num)]
        self.V_last=self.V_0
        self.steps=0
        self.done=False

    def generate_historical_queries(self):
        if self.tab in schema_inf['job']:
            benchmark='job'
        else: benchmark='tpchpro'
        base_dir=f'D:/PycharmProjects/partition-api/flaskr/algorithms/SCVP/data/{benchmark}'
        _,querys=read_query_data(f'{base_dir}/{self.tab}.csv',len(col_inf[self.tab]['length']))
        print('End, q:',len(querys))
        return querys

    def step(self, action):
        self.steps+=1
        act_scheme=self.action_dict[action]
        m1,m2=act_scheme[0],act_scheme[1]
        m1_par,m2_par=[],[]
        for par in self.cur_pars:
            if m1 in par: m1_par=par
            if m2 in par: m2_par=par
            if m1_par and m2_par: break 
        # merged attributes are in the same partition 
        skip_flag,end_flag=False,False
        if m1==-1:
            end_flag=True
            self.done=True
        elif m1_par==m2_par:
            skip_flag=True
        # merged attributes not belong to the partitioned table
        elif (not m1_par) or (not m2_par):
            self.done=True # it refers to a column that does not exist in the current table
            skip_flag=True
        else:
            m1=self.cur_pars.index(m1_par)
            m2=self.cur_pars.index(m2_par)
            self.cur_pars[m1]+=m2_par
            self.cur_pars.remove(m2_par)
            self.state[0][m1]+=self.state[0][m2]
            for row in range(1,self.dims[0]):
                if self.state[row][m1]+self.state[row][m2]>=1:
                    self.state[row][m1]=1
                else: self.state[row][m1]=0
            self.state[:,(m2):-1]=self.state[:,(m2+1):]
            self.state[:,-1]=0

        V_c=1/cal_total_cost_update(self.querys,self.cur_pars,col_inf[self.tab]['length'])
        print(f'V_c: {V_c}')
        # end: just one partition / V{state}<V{state-1} / invalid action
        if self.state[0,1]==0:
            self.done=True #only one partition left
        elif self.steps>=self.attr_num-1:
            self.done=True
        # elif V_c<self.V_last:
        #     self.done=True
        self.V_last=V_c
        if end_flag: 
            # reward=-5 # design for tpch
            reward=-50*50  # design for widetable100
        elif skip_flag: reward=-0.1
        else: reward=self._compute_reward(V_c)
        # print(f'Step {self.steps}: Action {act_scheme} Pos {m1}- {m2} Pars:{self.cur_pars}')
        print(f'Step {self.steps}: Action {act_scheme} Pos {m1}- {m2}')
        print(f'Reward: {reward}')
        return self.state, reward, self.done, {}

    def _compute_reward(self,V_c):
        return 100*(V_c-self.V_0)/(self.V_best-self.V_0)


    def _get_init_state(self):
        obs=np.array(([[0]*self.dims[1]])*self.dims[0])
        if self.tab.find('widetable')!=-1:
            dir='synthetic'
        elif self.tab in schema_inf['job']:
            dir='job'
        else: dir='tpch'
        base_dir=f'D:/PycharmProjects/partition-api/queries/{dir}-queries'
        tab_rows=get_all_tab_rows(self.tab)
        for col_idx,size in enumerate(col_inf[self.tab]['length']):
            obs[0][col_idx]=size*tab_rows
        
        if dir=='synthetic':
            start_idx_dict={'widetable30':1,'widetable50':6,'widetable100':11}
            for no in range(start_idx_dict[self.tab],start_idx_dict[self.tab]+self.dims[0]-1):
                with open(f'{base_dir}/{no}.sql','r') as reader:
                    sql=reader.read()
                    access_cols=Parser(sql).columns
                    for col in access_cols:
                        tab,col_idx=get_tab_from_conf(col)
                        if tab==self.tab:
                            obs[no+1-start_idx_dict[self.tab]][col_idx]=1
        elif dir=='tpch':
            for no in range(1,self.dims[0]):
                with open(f'{base_dir}/d1-{no}.sql','r') as reader:
                    if no in [8,9]: 
                        looked_sql_cols={
                            8:'l_extendedprice,l_discount,l_partkey,l_suppkey,l_orderkey,o_orderdate,o_orderkey,o_custkey,c_custkey,c_nationkey,n_name,n_nationkey,n_regionkey,,p_partkey,p_type,s_suppkey,s_nationkey,r_regionkey,r_name',
                            9:'n_name,n_nationkey,o_orderdate,o_orderkey,l_extendedprice,l_discount,l_quantity,l_suppkey,l_partkey,l_orderkey,ps_supplycost,ps_suppkey,ps_partkey,s_suppkey,s_nationkey,p_partkey,p_name'}
                        access_cols=looked_sql_cols[no].split(',')
                    else:
                        sql=reader.read()
                        access_cols=Parser(sql).columns
                    for col in access_cols:
                        tab,col_idx=get_tab_from_conf(col)
                        if tab==self.tab:
                            obs[no][col_idx]=1
        else:
            for file_name in sorted(os.listdir(base_dir)):
                path=f'{base_dir}/{file_name}'
                if os.path.isfile(path):
                    with open(path,'r') as reader:
                        sql_str=reader.read()
                        sqls=sql_str.split(';\n')
                        for no,sql in enumerate(sqls):
                            for col in Parser(sql).columns:
                                tab,col_idx=get_tab_from_conf(col)
                                if tab==self.tab:
                                    obs[no+1][col_idx]=1
        return obs

    def reset(self):
        self.steps=0
        self.done=False
        self.V_last=self.V_0
        self.cur_pars=[[i] for i in range(self.attr_num)]
        self.state=self._get_init_state()
        return self.state
       

from stable_baselines3 import PPO
import numpy as np
import time
class Drl_Partitioner:
    def __init__(self) -> None:
        self.base_path='D:/PycharmProjects/partition-api/flaskr/algorithms/SCVPplus/baseline'

    def load(self,table):
        env=Env(table)
        saved_model_path=f'{self.base_path}/pre_model/drl_partition_1e5_{table}'
        time0=time.time()
        model=PPO.load(saved_model_path,device='cuda:4')
        print('Model load time:',time.time()-time0)
        obs=env.reset()
        for step in range(5000):
            action, _states=model.predict(obs, deterministic=True)
            state,reward,done,info=env.step(action)
            # print(f'step {step}, action {action}, reward {reward}')
            if done: break
        return env.cur_pars

    def train(self,table):
        env=Env(table)
        # best pars: [0,1] [0,2] [0,4] [0,8] [0,10] [7,9] [11,12] [11,13] [11,14]==> actions: 0 1 3 7 9 85 110 111 112
        # for action in [1, 3, 7, 9, 85, 110, 111, 112,0,120]:
        #     state,reward,done,_=env.step(action)
        #     # if done: break
        # env.reset()
        saved_model_path=f'{self.base_path}/pre_model/drl_partition_1e5_{table}'
        # # Run Command: tensorboard --logdir ./_tensorboard_env/env/  --bind
        model = PPO("MlpPolicy", env, verbose=1,device='cuda:1',tensorboard_log=f"{self.base_path}/_tensorboard_env/env/")
        model.learn(total_timesteps=2e4)
        model.save(saved_model_path)
        obs=env.reset()
        for step in range(5000):
            action, _states=model.predict(obs, deterministic=True)
            state,reward,done,info=env.step(action)
            # print(f'step {step}, action {action}, reward {reward}')
            if done: break
        return (table,env.cur_pars)

if __name__=='__main__':
    # trained_tabs=['lineitem','nation','part','supplier','orders','customer','partsupp','region','supplier','widetable30','widetable50','widetable100']
    trained_tabs=["cast_info"]
    # "title","movie_info_idx","cast_info","movie_info","movie_keyword","movie_companies"
    # ,,'widetable50'
    # Drl_Partitioner().load('supplier')
    queue = mul.Queue()
    jobs,chunk_result=[],[]
    for tab in trained_tabs:
        print(tab)
        print(Drl_Partitioner().load(tab))
        # print(Drl_Partitioner().train(tab))
    #     process = mul.Process(target=Drl_Partitioner().train, args=(tab,))
    #     process.start()
    #     jobs.append(process)
    # for _ in jobs:
    #     chunk_result.append(queue.get())
# lineitem:[[2, 0, 1, 4, 10, 14], [3, 5], [6, 8], [7], [9], [11, 12], [13], [15]]
# customer: [[0], [1], [2], [3, 5], [4], [6], [7]]
# title: [[0, 3, 4], [1], [2], [5], [6], [7], [8], [9], [10], [11]]