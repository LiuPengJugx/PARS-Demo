def cal_total_memory_cost(querys,paritions,attrs_length):
    l1CacheLineWidth=64
    for query in querys:
        for parition in paritions:
            accessMask=[i for i,x in enumerate(query['value']) if x==1 and i in partition]
            if sum(accessMask)==0:continue
            partitionSize = 0
            gapOffset = 0
            gapWidth = 0
            gap = False
            gapOffsets = []
            gapWidths = []
            for idx,accmask in enumerate(accessMask):
                if accmask==0:
                    if not gap:
                        gapWidth=0
                        gap=True
                    gapWidth+=attrs_length[parition[idx]]
                else:
                    if gap:
                        gapOffsets.append(gapOffset)
                        gapWidths.append(gapWidth)
                        gapOffset += gapWidth
                        gap = False
                    gapOffset+=attrs_length[parition[idx]]
                partitionSize+=attrs_length[parition[idx]]
            if gap:
                gapOffsets.append(gapOffset)
                gapWidths.append(gapWidth)

            # Make sure there is a dummy zero-width gap at the begining/end, if the first/last attribute is projected.
            if len(gapOffsets) == 0 and gapOffsets[0] > 0:
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
            return getCacheMisses(gapOffsets, gapWidths, partitionSize, numRows, 0, l2CacheLineWidth)
            
def getCacheMisses(gapOffsets,gapWidths, containerWidth, containerRows, containerOffset, cacheLineWidth):
    misses = 0;
    	
    #point to the first partial projection offset (should be 0,
    #if there exists a placeholder first gap even if it is zero in width)
    partialProjectionOffset = gapOffsets[0];
    
    #flag to indicate whether it is the first (gap) skip or not
    firstSkip = True

    #flag to indicate whether or not we can merge the first and the last partial projectedColumns
    mergeFirstLast = False
    if (gapWidths[0] + gapWidths[len(gapWidths)-1]) < cacheLineWidth:
        mergeFirstLast = True;	# first and last partial projectedColumns can be merged, and the gap between them cannot be skipped
    else:
        partialProjectionOffset += gapWidths[0];
        firstSkip = False 
        #since the first and last partial projectedColumns cannot be merged,
        #we skip the leftmost gap, so the next one won't be the first (gap) skip 
    

    #if there is only one series of contiguous attributes projected we skip the rest of this method
    if len(gapOffsets) == 2:
        partialProjectionWidth = gapOffsets[1] - gapWidths[0];
        return calCacheMisses(partialProjectionWidth, gapWidths[0], containerWidth,containerRows, containerOffset, cacheLineWidth)

        
    #width of first-last merged partial projection
    firstLastWidth = 0;

    #some gaps between the first and last one were skipped
    skippedInBetween = False;
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
        misses += calCacheMisses(gapOffsets[gapOffsets.length - 1] - gapWidths[0], gapWidths[0], containerWidth,containerRows, containerOffset, cacheLineWidth)
    
    return misses


def calCacheMisses(projectionWidth,projectionOffset,containerWidth, containerRows, containerOffset, cacheLineWidth):
    misses = 0
    if (containerWidth-projectionWidth) < cacheLineWidth:
        #non-projected segments of the container cannot be skipped
        misses = Math.ceil((containerWidth*containerRows + containerOffset)/cacheLineWidth)
    else:
        #parts of the container can be skipped
        v = cacheLineWidth / GCD(containerWidth, cacheLineWidth)
        for r in range(v):
            rowOffset = containerWidth * r
            lineOffset = (containerOffset + rowOffset + projectionOffset) % cacheLineWidth
            misses += Math.ceil((lineOffset + projectionWidth)/cacheLineWidth)
        misses = (misses * containerRows / v)
    return misses;


def GCD(a,b):
    if b==0:
        return a
    else:
        return GCD(b,a%b)