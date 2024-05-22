import request from '@/utils/request'
export function fetchStudentList(query){
    return request({
        url: '/aidb/storage/tableInfo/'+query['tabName'],
        method: 'get',
        params: query
    })
}

export function fetchBlocks(data){
    return request({
        url: '/aidb/storage/vertical/partitions/V2/blocks',
        method: 'post',
        data
    })
}

export function fetchAllBlocks(query){
    return request({
        url: '/aidb/storage/vertical/partitions/V2/blockList',
        method: 'get',
        params: query
    })
}

export function fetchDeploymentCode(query){
    return request({
        url: '/aidb/storage/vertical/partitions/V2/genSql',
        method: 'get',
        params: query
    })
}

export function fetchNodeInfo(query){
    return request({
        url: '/aidb/storage/vertical/partitions/V2/node/'+query.nid,
        method: 'get',
        params: query
    })
}

export function fetchTreeStructure(query){
    return request({
        url: '/aidb/storage/vertical/partitions/V2/tree',
        method: 'get',
        params: query
    })
}

export function fetchAllTables(){
    return request({
        url: '/aidb/storage/tabList',
        method: 'get'
    })
}
export function fetchTableCount(tableName){
    return request({
        url: '/aidb/storage/tableCount/'+tableName,
        method: 'get'
    })
}


export function fetchTableColumns(tableName){
    return request({
        url: '/aidb/storage/tableInfo/columns/'+tableName,
        method: 'get'
    })
}

export function fetchWorkload(path){
    return request({
        url: '/aidb/storage/workload/'+path,
        method: 'get'
    })
}

export function fetchWorkloadV2(path){
    return request({
        url: '/aidb/storage/workload/V2/'+path,
        method: 'get'
    })
}

export function addWorkload(data){
    return request({
        url: '/aidb/storage/workload/add',
        method: 'post',
        data
    })
}

export function addWorkloadV2(data){
    return request({
        url: '/aidb/storage/workload/V2/add',
        method: 'post',
        data
    })
}

export function parseSql(sql){
    return request({
        url: '/aidb/storage/vertical/partitions/parse',
        method: 'get',
        params: sql
    })
}

export function genSql(){
    return request({
        url: '/aidb/storage/vertical/partitions/V2/random/sql',
        method: 'get',
    })
}

export function executeAnalysis(data){
    return request({
        url:'/aidb/storage/vertical/partitions/analysis',
        method:'post',
        data
    })
}

export function executeAnalysisV2(data){
    return request({
        url:'/aidb/storage/vertical/partitions/V2/analysis',
        method:'post',
        data
    })
}