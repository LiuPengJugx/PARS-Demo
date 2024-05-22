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
def calHDDCost(querys,paritions,attrs_length):
    for query in querys:
        solved_paritions=list_solved_list_content(query['value'],paritions)
        referencedPartitionsRowSize=sum([attrs_length[x] for x in solved_paritions])
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
    return seekCost + scanCost

