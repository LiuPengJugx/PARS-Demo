import sys
sys.path.append('/home/liupengju/pycharmProjects/partition-api/flaskr/algorithms/SCVP')

from utils import read_query_data
# from DiskCost import cal_total_cost_update
from DiskCost import cal_total_cost_improvement as cal_total_cost_update
import DiskCost as DC
def generate_vps(querys,attrs_length,tab):
    DC.tab_name=tab
    # do partition
    attr_num=len(attrs_length)
    cand=[[] for i in range(attr_num)]
    for i in range(attr_num):
        cand[i].append(i)
    candCost=cal_total_cost_update(querys, cand, attrs_length)
    minCost=float("inf")
    candList=[]
    R=[]
    while True:
        R=cand
        minCost=candCost
        candList.clear()
        for i in range(len(R)):
            for j in range(i+1,len(R)):
                cand=[[] for i in range(len(R)-1)]
                s=R[i]+R[j]
                for k in range(len(R)):
                    if k==i:
                        cand[k]=s
                    elif k<j:
                        cand[k]=R[k]
                    elif k>j:
                        cand[k-1]=R[k]
                candList.append(cand)
        if len(candList)>0:
            cand,candCost=getLowerCostCand(candList,querys,attrs_length)
        if candCost>=minCost:
            break
    #end loop
    print(R)
    return R


def compute_cost_by_climb_hill(file_input_info):
    affinity_matrix,querys=read_query_data(file_input_info['path'],file_input_info['attr_num'])
    attrs_length=file_input_info['attrs_length']
    if 'tab' in file_input_info:
        DC.tab_name=file_input_info['tab']
    return generate_vps(querys,attrs_length,file_input_info['tab'])
    
        
def getLowerCostCand(candList,querys,attrs_length):
    
    min_cost=float("inf")
    min_cand=[]
    for cand in candList:
        candCost=cal_total_cost_update(querys, cand, attrs_length)
        if candCost<min_cost:
            min_cost=candCost
            min_cand=cand
    return min_cand,min_cost