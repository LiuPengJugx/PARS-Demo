from dateutil.parser import parse as parseDate
import numpy as np
import time
import pandas as pd
import pickle
import math
import re
import csv
def list_in_list(list1,list2):
    for x in list1:
        if x not in list2:
            return False
    return True

def list_solved_list(list1,list2):
    for x in list2:
        if x in list1:
            return True
    return False

def is_valid_date(strdate):
        try:
            parseDate(strdate)
            return True
        except:
            return False

def transfer_date_timestamp(date_str):
    return time.mktime(parseDate(date_str).timetuple())

def transfer_timestamp_date(timestamps):
    return time.strftime("%Y-%m-%d",time.localtime(timestamps))

def col_parser(dataset,encode_dicts,vp=None):
    boundary=[]
    for col_id in range(dataset.shape[1]):
        if vp: raw_col_id=vp[col_id]
        else:
            raw_col_id=col_id
        if raw_col_id in encode_dicts.keys():
            domain=sorted(np.unique(dataset[:,col_id]))
            # domain=list(encode_dicts[raw_col_id].values())
        else:
            cell=dataset[0,col_id]
            if isinstance(cell,int) or isinstance(cell,float):
                if(math.isnan(float(cell))):
                    domain=None # nan
                else:
                    domain=np.array([min(dataset[:,col_id]),max(dataset[:,col_id])])
            elif isinstance(cell,str):
                domain=None # unsorted strs
            else:
                domain=np.array([min(dataset[:,col_id]),max(dataset[:,col_id])])
            # # date
            # if is_valid_date(cell):
            #     domain=[min(dataset[:,col_id]),max(dataset[:,col_id])]
            # # less types/status
            # elif len(set(dataset[:,col_id]))/dataset.shape[0]<=0.3:
            # # comments
            #     domain=['T1','T'+str(len(set(dataset[:,col_id])))]
            # else:
            #     domain=[None,None]
        boundary.append(domain)
    # print(boundary)
    return np.array(boundary)

def sql_like(target,search_dict):
    pred_val=[]
    sub_strs=re.split("%|_",target)
    for domain in search_dict.keys():
        flag=True
        for sub_str in sub_strs:
            if domain.find(sub_str)==-1:
                flag=False
                break
        if flag: pred_val.append(search_dict[domain])
    if pred_val:
        if len(pred_val)==1: return pred_val[0]
        return pred_val
    return -1




def encode_pred_val(pred_col,pred_val,encode_dicts):
    cond=pred_val
    try:
        cond=int(cond)
    except:
        pass
    if not is_valid_date(cond):
        try:
            cond=eval(cond)
        except:
            pass
    if pred_col in encode_dicts.keys():
        if isinstance(cond,list):
            temp_cond=[]
            for __cond in cond:
                if __cond in encode_dicts[pred_col].keys():
                    temp_cond.append(encode_dicts[pred_col][__cond])
                else: temp_cond.append(-1)
            pred_val=temp_cond
        else:
            if cond.find('%')!=-1 or cond.find('_')!=-1:
                pred_val=sql_like(cond,encode_dicts[pred_col])
            else:
                if cond in encode_dicts[pred_col].keys():
                    pred_val=encode_dicts[pred_col][cond]
                else: pred_val=-1
    elif isinstance(cond,int) or isinstance(cond,float) or isinstance(cond,list):
        pred_val=cond
    elif is_valid_date(cond):
        pred_val=transfer_date_timestamp(cond)
    return pred_val

def load_data(benchmark,tab):
        save_base_path=f'D:/PycharmProjects/partition-api/flaskr/algorithms/SCVPplus/data/{benchmark}/'
        dataset=pd.read_csv(f'{save_base_path}{tab}_c.csv',header=None).values
        if benchmark=='synthetic':
            dataset=dataset[1:,1:]
        else:
            dataset=dataset[:,1:]
        return dataset

def load_dict(benchmark,tab):
    save_base_path=f'D:/PycharmProjects/partition-api/flaskr/algorithms/SCVPplus/data/{benchmark}/'
    encode_dicts=None
    with open(f'{save_base_path}{tab}_dict.pkl', "rb") as tf:
        encode_dicts = pickle.load(tf)
    return encode_dicts

def write_csv(path,data,cols,header=True):
    with open(path, 'w', encoding='utf-8', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=cols)
        if header:
            writer.writeheader()
        writer.writerows(data)