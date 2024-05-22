import globalvar as gl
import math
# 预定义参数
Cn,Cw,Tw,Lw,Co=0,0,0,64,0
attrs_length_dict=gl.get_value("attrs_length_dict")
# Lw如何set?
cardinality=gl.get_value("cardinality")
Cn=cardinality
def mod(a, b):    
    c = a // b
    r = a - c * b
    return r

# 取余 
def rem(a, b):
    c = int(a / b) 
    r = a - c * b
    return r

def cal_total_memory_cost2(querys,paritions,attrs_length):
    total_Miss=0
    Co=0
    for query in querys:
        for partition in paritions:
            # 出现partition里的属性无序排列的情况
            partition.sort()
            solved_attrs=[i for i,x in enumerate(query['value']) if x==1 and i in partition]
            
            if not solved_attrs:continue
            # 判断是否为连续属性
            is_sequence_attrs_flag=True
            s_begin_index=partition.index(solved_attrs[0])
            for idx,s_attr in enumerate(solved_attrs):
                if s_attr!=partition[s_begin_index+idx]:is_sequence_attrs_flag=False
            # 容器宽度
            Cw=sum([attrs_length[x-1] for x in partition])
            
            #------------------------------------------------------------
            # 连续属性
            if is_sequence_attrs_flag:
                #To表示连续属性与分区开始处的偏移量
                To=sum([attrs_length[x-1] for x in solved_attrs])
                Tw=sum([attrs_length[x-1] for x in solved_attrs])
                Co=0
                if Cw-Tw<Lw:
                    Miss_i=math.ceil((Cn*Cw+Co)/Tw)
                    totalCachedRows=1+query['selectivity']*(Lw/Cw-1)
                    Miss_i*=(query['selectivity']/totalCachedRows)*Miss_i
                else: 
                    v=Lw/math.gcd(Cw,Lw)
                    total_Miss_ir=0
                    for r in range(int(v)):
                        ro=Cw*r
                        lineoffset=mod(ro+Co+To,Lw)
                        Miss_ir=math.ceil((lineoffset+Tw)/Lw)
                        total_Miss_ir+=Miss_ir*query['selectivity']
                    Miss_i=(Cn/v)*total_Miss_ir
                total_Miss+=Miss_i
            #---------------------------------------------------------------
            #非连续属性
            else:
                # 设置一个pos array，记录属性的位置
                pos_par=[0]*len(partition)
                for x in solved_attrs:
                    pos_par[partition.index(x)]=1
                # for i,x in enumerate(partition):
                #      if x in solved_attrs:
                #         pos_par[i]=1
                # 可计算 间隔gap=r和 projection T
                gaps=0 #存储下标
                Ts=0 #存储下标
                gap_Ts=[]
                
                # elem={
                #     'offset':0,
                #     'width':0,
                #     'tag':0 # 0代表gap，1代表projection
                # }
                # if(len(pos_par)==1):
                #     elem={
                #         'offset':0,
                #         'width':attrs_length[partition[0]],
                #         'tag':0 }
                #     if pos_par[0]==0:
                #         elem['tag']=0
                #     else:
                #         elem['tag']=1
                #     gap_Ts.append(elem)
                # else:
                begin=0
                for ind,val in enumerate(pos_par):
                    if ind==len(pos_par)-1:
                        elem={
                            'offset':sum([attrs_length[x] for x in partition[:begin]]),
                            'width':sum([attrs_length[x] for x in partition[begin:ind+1]]),
                            'tag':val }
                        gap_Ts.append(elem)
                        if val==0:gaps+=1
                        else: Ts+=1
                    elif val==0 and pos_par[ind+1]==1:
                        elem={
                            'offset':sum([attrs_length[x] for x in partition[:begin]]),
                            'width':sum([attrs_length[x] for x in partition[begin:ind+1]]),
                            'tag':0 }
                        gap_Ts.append(elem)
                        gaps+=1
                        begin=ind+1
                    elif val==1 and pos_par[ind+1]==0:
                        elem={
                            'offset':sum([attrs_length[x] for x in partition[:begin]]),
                            'width':sum([attrs_length[x] for x in partition[begin:ind+1]]),
                            'tag':1 }
                        gap_Ts.append(elem)
                        Ts+=1
                        begin=ind+1
                    else:
                        continue
                # 根据gaps、Ts、计算
                # gap_Ts=[
                #     {'offset': 0, 'tag': 0, 'width': 8}, 
                #     {'offset': 8, 'tag': 1, 'width': 8}, 
                #     {'offset': 16, 'tag': 0, 'width': 84}, 
                #     {'offset': 100, 'tag': 1, 'width': 8},
                #     {'offset': 108, 'tag': 0, 'width': 72},
                #     {'offset': 180, 'tag': 1, 'width': 8}
                # ]
                # Cw=8+8+84+8+72+8
                # for it in gap_Ts:
                # full scan
                if sum([1 for x in gap_Ts if x['width']<Lw and x['tag']==0])==gaps:
                    Miss_i=math.ceil((Cn*Cw+Co)/Lw)
                    totalCachedRows=1+query['selectivity']*(Lw/Cw-1)
                    Miss_i*=(query['selectivity']/totalCachedRows)*Miss_i
                    total_Miss+=Miss_i
                # a partial projection
                else:
                    # 第一步,合并行连接处间隔较小的projections
                    last_T_i=0
                    first_T_i=0
                    if gap_Ts[-1]['tag']==1:
                        last_T_i=-1
                    else: last_T_i=-2
                    if gap_Ts[0]['tag']==0:
                        first_T_i=1
                    else: first_T_i=0
                    T_first_o=gap_Ts[first_T_i]['offset']
                    T_first_w=gap_Ts[first_T_i]['width']
                    T_last_o=gap_Ts[last_T_i]['offset']
                    T_last_w=gap_Ts[last_T_i]['width']
                    T_row=(T_first_o+Cw)-(T_last_o+T_last_w)
                    if(T_row<Lw):
                        # 更新每行last projection的宽度
                        gap_Ts[last_T_i]['offset']=T_last_o
                        gap_Ts[last_T_i]['width']=T_first_w+T_last_w+T_row

                        # 更新每行所有片段的offset
                        if first_T_i==1:
                            del gap_Ts[0]
                            del gap_Ts[0]
                        else:
                            del gap_Ts[first_T_i]
                        for gt_elem in gap_Ts:
                            gt_elem['offset']-=T_row+T_first_w

                    # for i,v in enumerate(gap_Ts):
                    #     if v['width']<Lw and i!=1 and v['tag']==0:
                    #         T_first_o=gap_Ts[i-1]['offset']
                    #         T_first_w=gap_Ts[i-1]['width']
                    #         # ii=i+1
                    #         # while(True):
                    #         #     ii+=1
                    #         #     if gap_Ts[ii]['width']<Lw and i!=1 and gap_Ts[ii]['tag']==0:
                    #         T_last_o=gap_Ts[i+1]['offset']
                    #         T_last_w=gap_Ts[i-1]['width']
                    #         T_row=(T_first_o+Cw)-(T_last_o+T_last_w)
                    #         gap_Ts[i-1]['offset']=T_last_o
                    #         gap_Ts[i-1]['width']=T_first_w+T_last_w+T_row
                    #         del gap_Ts[i]
                    #         del gap_Ts[i+1]
                    #         i-=1

                    # 第二步，进行部分投影的成本计算
                    v=Lw/math.gcd(Cw,Lw)
                    total_Miss_ir=0
                    for r in range(int(v)):
                        ro=Cw*r
                        # 每一行要计算多个projection的cache line
                        is_first_consider=True
                        for i,val_item in enumerate(gap_Ts):
                            if val_item['tag']==0 and val_item['width']>Lw:
                                # 若第一个间隔前存在projection
                                if gap_Ts[i]['offset']>0 and is_first_consider:
                                    Tw_temp=gap_Ts[i]['offset']
                                    To_temp=0
                                    lineoffset=mod(ro+Tw_temp+To_temp,Lw)
                                    Miss_ir=math.ceil((lineoffset+Tw_temp)/Lw)
                                    total_Miss_ir+=Miss_ir*query['selectivity']

                                is_first_consider=False
                                To_temp=gap_Ts[i]['offset']+gap_Ts[i]['width']

                                ii=i
                                flag=False
                                while(ii<len(gap_Ts)-1):
                                    ii+=1
                                    if gap_Ts[ii]['width']>Lw and gap_Ts[ii]['tag']==0:
                                        flag=True
                                        break    
                                if(flag):
                                    Tw_temp=gap_Ts[ii]['offset']-(gap_Ts[i]['offset']+gap_Ts[i]['width'])
                                    lineoffset=mod(ro+Tw_temp+To_temp,Lw)
                                    Miss_ir=math.ceil((lineoffset+Tw_temp)/Lw)
                                    total_Miss_ir+=Miss_ir*query['selectivity']
                                #后续没有间隔较大的gap
                                else:
                                    if(i==len(gap_Ts)-1):continue
                                    else:
                                        Tw_temp=sum([x['width'] for x in gap_Ts[i+1:]])
                                        To_temp=gap_Ts[i]['offset']
                                        lineoffset=mod(ro+Tw_temp+To_temp,Lw)
                                        Miss_ir=math.ceil((lineoffset+Tw_temp)/Lw)
                                        total_Miss_ir+=Miss_ir*query['selectivity']

                    Miss_i=(Cn/v)*total_Miss_ir
                    total_Miss+=Miss_i
    return int(total_Miss)


def cal_total_memory_cost1(querys,paritions,attrs_length):
    l1CacheLineWidth=64
    l2CacheLineWidth=64
    total_cache_misses=0
    for query in querys:
        for partition in paritions:
            partition.sort()
            accessMask=[query['value'][i] for i in partition]
            if sum(accessMask)==0:continue
            partitionSize = 0
            gapOffset = 0
            gapWidth = 0
            gap = False
            gapOffsets = []
            gapWidths = []
            numRows=cardinality
            for idx,accmask in enumerate(accessMask):
                if accmask==0:
                    if not gap:
                        gapWidth=0
                        gap=True
                    gapWidth+=attrs_length[partition[idx]]
                else:
                    if gap:
                        gapOffsets.append(gapOffset)
                        gapWidths.append(gapWidth)
                        gapOffset += gapWidth
                        gap = False
                    gapOffset+=attrs_length[partition[idx]]
                partitionSize+=attrs_length[partition[idx]]
            if gap:
                gapOffsets.append(gapOffset)
                gapWidths.append(gapWidth)

            # Make sure there is a dummy zero-width gap at the begining/end, if the first/last attribute is projected.
            if len(gapOffsets) == 0 or gapOffsets[0] > 0:
                newGapOffsets=[]
                newGapOffsets.append(0)
                newGapOffsets+=gapOffsets
                gapOffsets = newGapOffsets

                newGapWidths = []
                newGapWidths.append(0)
                newGapWidths+=gapWidths
                gapWidths = newGapWidths
            
            if gapOffset == partitionSize:
                gapOffsets.append(partitionSize)
                gapWidths.append(0)
            total_cache_misses+= getCacheMisses(gapOffsets, gapWidths, partitionSize, numRows, 0, l2CacheLineWidth)*query['freq']
    return total_cache_misses

def getCacheMisses(gapOffsets,gapWidths, containerWidth, containerRows, containerOffset, cacheLineWidth):
    misses = 0
    	
    #point to the first partial projection offset (should be 0,
    #if there exists a placeholder first gap even if it is zero in width)
    partialProjectionOffset = gapOffsets[0]
    
    #flag to indicate whether it is the first (gap) skip or not
    firstSkip = True

    #flag to indicate whether or not we can merge the first and the last partial projectedColumns
    mergeFirstLast = False
    if (gapWidths[0] + gapWidths[len(gapWidths)-1]) < cacheLineWidth:
        mergeFirstLast = True	# first and last partial projectedColumns can be merged, and the gap between them cannot be skipped
    else:
        partialProjectionOffset += gapWidths[0]
        firstSkip = False 
        #since the first and last partial projectedColumns cannot be merged,
        #we skip the leftmost gap, so the next one won't be the first (gap) skip 
    

    #if there is only one series of contiguous attributes projected we skip the rest of this method
    if len(gapOffsets) == 2:
        partialProjectionWidth = gapOffsets[1] - gapWidths[0]
        return calCacheMisses(partialProjectionWidth, gapWidths[0], containerWidth,containerRows, containerOffset, cacheLineWidth)

        
    #width of first-last merged partial projection
    firstLastWidth = 0

    #some gaps between the first and last one were skipped
    skippedInBetween = False
    for i in range(1,len(gapOffsets)-1):
        #can skip this gap
        if gapWidths[i] >= cacheLineWidth:
            skippedInBetween = True
            partialProjectionWidth = gapOffsets[i] - partialProjectionOffset
            if mergeFirstLast and firstSkip:
                firstLastWidth += partialProjectionWidth 
            else:
                misses += calCacheMisses(partialProjectionWidth, partialProjectionOffset, containerWidth, containerRows, containerOffset, cacheLineWidth)
            partialProjectionOffset = gapOffsets[i] + gapWidths[i]
            firstSkip = False
        
    if mergeFirstLast:
        firstLastWidth += (containerWidth - partialProjectionOffset)
        misses += calCacheMisses(firstLastWidth, partialProjectionOffset, containerWidth, containerRows, containerOffset, cacheLineWidth)
    elif not skippedInBetween:
        misses += calCacheMisses(gapOffsets[len(gapOffsets) - 1] - gapWidths[0], gapWidths[0], containerWidth,containerRows, containerOffset, cacheLineWidth)
    
    return misses


def calCacheMisses(projectionWidth,projectionOffset,containerWidth, containerRows, containerOffset, cacheLineWidth):
    misses = 0
    if (containerWidth-projectionWidth) < cacheLineWidth:
        #non-projected segments of the container cannot be skipped
        misses = math.ceil((containerWidth*containerRows + containerOffset)/cacheLineWidth)
    else:
        #parts of the container can be skipped
        v = cacheLineWidth / GCD(containerWidth, cacheLineWidth)
        for r in range(int(v)):
            rowOffset = containerWidth * r
            lineOffset = (containerOffset + rowOffset + projectionOffset) % cacheLineWidth
            misses += math.ceil((lineOffset + projectionWidth)/cacheLineWidth)
        misses = (misses * containerRows / v)
    return misses


def GCD(a,b):
    if b==0:
        return a
    else:
        return GCD(b,a%b)





import math
from utils import list_solved_list_content
# The block size of the DBMS.
DEFAULT_BLOCK_SIZE = 8 * 1024
#The buffer size of the DBMS.
DEFAULT_BUFFER_SIZE = 1024 * DEFAULT_BLOCK_SIZE
#Seek time.
seekTime = 0.008
#The read bandwidth of the disk.
readDiskBW = 92 * 1024 * 1024
#The write bandwidth of the disk.
writeDiskBW = 70 * 1024 * 1024
#The block size of the DBMS.
blockSize=DEFAULT_BLOCK_SIZE
#The I/O buffer size of the DBMS.
bufferSize=DEFAULT_BUFFER_SIZE
#cardinity
import globalvar as gl
numRows=gl.get_value("cardinality")
def cal_total_memory_cost(querys,paritions,attrs_length):
    total_cost=0
    for query in querys:
        q_cost=0
        solved_attr_index=[i for i,x in enumerate(query['value']) if x==1]
        solved_paritions=list_solved_list_content(solved_attr_index,paritions)
        referencedPartitionsRowSize=0
        for parition in solved_paritions:
            referencedPartitionsRowSize+=sum([attrs_length[x] for x in parition])

        for parition in solved_paritions:
            partitionRowSize=sum([attrs_length[x] for x in parition])
            # * We assume that all referenced partitions share the same buffer. The following is
            # * the memory size occupied by the current partition in the buffer.
            partitionBufferSize = max(math.floor(bufferSize * partitionRowSize / referencedPartitionsRowSize),blockSize)
            #we have to read at least one block from the disk
            #This is the number of blocks that fit into the buffer for the current partition.
            blocksReadPerBuffer = math.floor(partitionBufferSize / blockSize)
            #This is the total number of blocks of the current partition.
            numberOfBlocks = math.ceil(partitionRowSize * numRows / blockSize)* query['selectivity']
            #Time spent on seeking and scanning.
            seekCost = seekTime * math.ceil(numberOfBlocks / blocksReadPerBuffer)
            scanCost = numberOfBlocks * blockSize / readDiskBW
            q_cost+=seekCost + scanCost
        total_cost+=q_cost*query['freq']
    return total_cost