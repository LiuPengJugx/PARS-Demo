import pandas as pd
import random
import math
import pandas as pd
from pandas.core.frame import DataFrame
def component_query(access_attr,freq,scan_attr,selectivity):
    attr_list_str=[str(attr) for attr in access_attr]
    return [",".join(attr_list_str),freq,scan_attr,selectivity]


def generate_random_query(attr_num_complete,query_num):
    iter=0
    # 每个查询所有属性的方差波动范围都是一样的
    # 将属性范围分成5份,随机选中某一份
    cut_num=5
    attr_split_list=[]
    start_index=0
    split_length=round(attr_num_complete/cut_num)
    while(True):
        end_index=start_index+split_length-1
        if start_index>=attr_num_complete or end_index>=attr_num_complete:
            break
        attr_split_list.append([start_index,end_index])
        start_index=end_index+1

    maximum_sigma_percentage=0.2
    querys=[]
    is_generate_perform=0
    while(True):
        access_attr=[]
        range_integer=random.randint(0,cut_num-1)
        # 0~99
        range_lower,range_upper=attr_split_list[range_integer][0],attr_split_list[range_integer][1]
        mid_point=round((range_lower+range_upper)/2)
        # 控制sql访问属性的数量
        access_attr_sum=random.randint(1,10)
        for i in range(range_lower,range_upper+1):
            sigma=random.uniform(0,(range_upper-mid_point)*maximum_sigma_percentage)
            attr_value=math.ceil(random.gauss(mid_point,sigma))
            if attr_value>range_upper:attr_value=range_upper
            elif attr_value<range_lower:attr_value=range_lower
            if attr_value in access_attr:
                continue
            else: access_attr.append(attr_value)
            if len(access_attr)==access_attr_sum:break
        
        if is_generate_perform<=5:
            freq=random.randint(round(query_num/10),round(query_num/8))
            is_generate_perform+=1
        else:
            # 干扰query ：1~20之间的整数
            freq=random.randint(1,20)

        # access_attr范围内之间的a个整数,其中0<=a<=2
        scan_key_num=min(random.randint(0,2),len(access_attr))
        scan_attr=[]
        for i in random.sample(access_attr,scan_key_num):
            scan_attr.append(str(i))
        scan_attr=",".join(scan_attr)
        selectivity=0.02
        if iter+freq>query_num:
            freq=query_num-iter
            querys.append(component_query(access_attr,freq,scan_attr,selectivity))
            break
        else:
            querys.append(component_query(access_attr,freq,scan_attr,selectivity))
            iter+=freq

    return querys


def main():
    # attr_num_list=[30,50,100,200]
    attr_num_list=[100,200]
    for attr_num in attr_num_list:
        # attr_num=100
        query_num=1000
        querys=generate_random_query(attr_num,query_num)
        df=DataFrame(querys)
        print(querys)
        df.to_csv("/home/liupengju/pycharmProjects/SCVP-V2/data/random/%dattr.csv"%(attr_num),header=0,index=0)
    
    # query_num_list=[200,500,1000,2000,5000]
    # for query_num in query_num_list:
    #     attr_num=50
    #     # query_num=1000
    #     querys=generate_random_query(attr_num,query_num)
    #     df=DataFrame(querys)
    #     print(querys)
    #     df.to_csv("/home/liupengju/pycharmProjects/SCVP-V2/data/random/%dquery.csv"%(query_num),header=0,index=0)

    
if __name__=="__main__":
    main()