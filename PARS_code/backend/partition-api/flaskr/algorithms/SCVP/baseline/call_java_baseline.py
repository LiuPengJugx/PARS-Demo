import os
import time
import numpy as np
from jpype import *
def init_jvm(jarName):
    if not isJVMStarted():
        jar_path = os.path.abspath('.')
        # startJVM('/home/liupengju/java/jdk1.8.0_281/jre/lib/amd64/server/libjvm.so', "-ea", "-Djava.class.path=%s" % (jar_path+"/baseline/"+jarName), convertStrings=False)
        startJVM(getDefaultJVMPath(), "-ea", "-Djava.class.path=%s" % ("baseline/"+jarName), convertStrings=False)

def solve(algorithmResults):
    result={'partitions':{},'runTimes':{}}
    # mp=java.util.HashMap()
    for table in algorithmResults.partitions.keys():
        result['partitions'][table]={}
        mp=algorithmResults.partitions.get(table)
        for algo in mp.keys():
            partitionMap=mp.get(algo)
            partition=[]
            for k in partitionMap.keys():
                partition.append([item for item in partitionMap.get(k).toArray()])
            result['partitions'][table][algo.name()]=partition
    time_list=[]
    for table in algorithmResults.runTimes.keys():
        result['runTimes'][table]={}
        mp=algorithmResults.runTimes.get(table)
        for algo in mp.keys():
            result['runTimes'][table][algo.name()]=mp.get(algo)
            time_list.append(mp.get(algo))
    return result,sum(time_list)
def runBaseLine(dataset,costmodel):
    # init_jvm()
    AlgorithmRunner = JClass('experiments.AlgorithmRunner')
    # if dataset=="tpch":
    algorithmResults= AlgorithmRunner.runBaselineOnDiffDatasets(dataset,costmodel)
    return solve(algorithmResults)

def runBaseLineDemo(datasetDict,cost_type,algorithm_indexs,baseDir):
    time0=time.time()
    AlgorithmRunner = JClass('experiments.AlgorithmRunner')
    dt=java.util.HashMap()
    for key in datasetDict:
        lens=JArray(JInt)(datasetDict[key])
        dt.put(key,lens)
    algorithmResults= AlgorithmRunner.runBaselineBySelfConfig(dt,cost_type,JArray(JInt)(algorithm_indexs),baseDir)
    
    result,total_time=solve(algorithmResults)
    consume_time=time.time()-time0
    for table_k in result['runTimes'].keys():
        for algo_k in result['runTimes'][table_k].keys():
            # raw_times=np.array(result['runTimes'][table_k][algo_k])
            # result['runTimes'][table_k][algo_k]=(raw_times*consume_time/total_time).tolist()
            result['runTimes'][table_k][algo_k]*=consume_time/total_time
    print(result)
    return result

def shutdowm():
    shutdownJVM()

if __name__=="__main__":
    # init_jvm('vertical-partition-demo.jar')
    init_jvm('test.jar')
    datasetDict={
        'customer.csv':[4,25,40,4,15,4,10,117],
        # 'customer1621443954439.csv':[4,25,40,4,15,4,10,117]
    }
    cost_type='Huang'
    algorithm_indexs=[0,1,3,4,5,6,7]
    # baseDir="http://10.77.110.152:5000/static/tempParsedSqls/"
    baseDir="/home/liupengju/pycharmProjects/partition-api/flaskr/static/tempParsedSqls/"
    runBaseLineDemo(datasetDict,cost_type,algorithm_indexs,baseDir)
    shutdowm()