<template>
  <div style="padding-top: 40px;" class="main-panel">
      <el-row type="flex" justify="center" style="background-color: white;margin-top: -38px;min-height: 500px">
        <el-col :span="12" :offset="0" >
          <el-tabs @tab-click="tabClick"  tab-position="left" :stretch="true" style="margin-top: 40px;margin-right: 5px">
            <el-tab-pane label="Manual Input Query">
              <el-row>
                <el-col>
                  <p style="text-align: right"><a href="#" @click="onGenSql"><i class="el-icon-refresh-left"></i>Generate random query</a></p>

                  <el-form id="defineQueryForm"  :model="formV2" ref="form" label-position="left"	label-width="100px" :inline="false" size="normal">
                    <el-form-item >
                  <span slot="label">
                     <span class="el-form-item-label" >Raw SQL</span>
                  </span>
                      <el-input
                          type="textarea"
                          :autosize="{ minRows: 3, maxRows:10}"
                          placeholder="Please input your query"
                          v-model="formV2.sql">
                      </el-input>

                    </el-form-item>
                    <el-form-item style="text-align: center">
                      <el-button type="primary" plain size="small" @click="onParseSql">Parse Query</el-button>
                      <el-button type="success" plain size="small" @click="onAddOneQuery">Add Query</el-button>
                    </el-form-item>
                  </el-form>
                </el-col>
              </el-row>

            </el-tab-pane>
            <el-tab-pane label="Pre-saved Queries">

              <el-row type="flex" justify="center" align="middle" style="margin: 20px">
                <el-col :span="14" :offset="0">
                  <h3 style="text-align: center">Generate queries for table <i class="el-icon-edit"></i> :
                    <el-select
                        v-model="formV2.curViewedTablename"
                        placeholder="Please select table"
                        @change="setColumnOption(formV2.curViewedTablename)"
                    >
                      <el-option
                          v-for="item in tabledata"
                          :key="item"
                          :label="item"
                          :value="item">
                      </el-option>
                    </el-select>
                  </h3>
                </el-col>
              </el-row>
              <el-row type="flex" justify="center" align="middle">

                <el-col :span="8" :offset="0">
                  <el-tag type="info">Workload: {{formV2.curViewedTablename}}</el-tag>
                  <!-- <el-select size="medium"  v-model="selectedWorkloadName"  placeholder="文件" clearable filterable >
                    <el-option v-for="item in tabledata"
                      :key="item"
                      :label="item"
                      :value="item">
                    </el-option>
                  </el-select> -->
                </el-col>
                <el-col :span="2" :offset="1">
                  <el-button type="primary" size="small" plain @click="readData">View</el-button>
                </el-col>
              </el-row>
            </el-tab-pane>
          </el-tabs>

          <el-row v-if="parsePanelFlag" type="flex" justify="center">
            <el-col :span="22">
              <el-tabs type="border-card"  >
                <el-tab-pane v-for="(emb,index) in parsedSqlRes.rawEmbs" :key="'q_emb'+index">
                  <span slot="label"><i v-if="index%2==0" style="margin-right: 3px"  class="el-icon-d-arrow-right"></i>{{emb.tab}}</span>
                  <a-descriptions title="SQL MetaData" bordered style="font-size: 40px" :column="2">
                    <a-descriptions-item label="Projection Columns">
                      <!--                          <p v-for="col in emb.scan">col</p>-->
                      {{emb.scan}}
                    </a-descriptions-item>
                    <a-descriptions-item label="Filter Columns">
                      {{emb.filter}}
                    </a-descriptions-item>
                    <a-descriptions-item label="Selectivity">
                      {{emb.sel}}
                    </a-descriptions-item>
                    <a-descriptions-item label="Sorting/Grouping Columns">
                      {{emb.gp_ob}}
                    </a-descriptions-item>
                    <a-descriptions-item label="Predicates">
                      <a-tag size="small" style="margin-bottom: 4px" v-for="pred in emb.preds" color="blue">
                        {{pred}}}
                      </a-tag>
                    </a-descriptions-item>
                    <a-descriptions-item label="Logical Predicate Tree">
                      <a-tree :tree-data="emb.lg_preds" show-icon default-expand-all>
                        <a-icon slot="switcherIcon" type="down" />
                        <a-icon slot="drag" type="drag" />
                        <a-icon slot="italic" type="italic" />
                        <template slot="custom" slot-scope="{ selected }">
                          <a-icon :type="selected ? 'frown' : 'frown-o'" />
                        </template>
                      </a-tree>
                    </a-descriptions-item>
                  </a-descriptions>

                </el-tab-pane>
              </el-tabs>


            </el-col>
          </el-row>
        </el-col>
        <el-col :span="1" :offset="0">
          <div class="my-divider" style="width: 3px;min-height: 100%;background-color: #a5a9a5"></div>
        </el-col>
        <el-col :span="11" :offset="0">

<!--          <el-table-->
<!--            :data="tableData"-->
<!--            empty-text="No data"-->
<!--            max-height="600">-->
<!--              <el-table-column -->
<!--                 min-width="250"-->
<!--                label="Accessed columns"-->
<!--                >-->
<!--                <template slot-scope="scope">-->
<!--                  {{ scope.row.value }}-->
<!--                </template>-->
<!--              </el-table-column>-->
<!--              <el-table-column-->
<!--                label="Frequency"-->
<!--                prop="freq"-->
<!--                align="center"-->
<!--                width="110"-->
<!--                >-->
<!--              </el-table-column>-->
<!--              <el-table-column-->
<!--                label="Scan keys"-->
<!--                min-width="140"-->
<!--                >-->
<!--                <template slot-scope="scope">-->
<!--                  <el-tag size="medium">{{ scope.row.scan_key }}</el-tag>-->
<!--                </template>-->
<!--              </el-table-column>-->
<!--              <el-table-column-->
<!--                label="Selectivity"-->
<!--                align="center"-->
<!--                prop="selectivity"-->
<!--                >-->
<!--              </el-table-column>-->
<!--              <el-table-column label="Operation">-->
<!--                <template slot-scope="scope">-->
<!--                  &lt;!&ndash; <el-button-->
<!--                    size="mini"-->
<!--                    @click="handleEdit(scope.$index, scope.row)">编辑</el-button> &ndash;&gt;-->
<!--                  <el-button-->
<!--                    size="mini"-->
<!--                    plain-->
<!--                    type="danger"-->
<!--                    @click="handleDelete(scope.$index, tableData)">Delete</el-button>-->
<!--                </template>-->
<!--              </el-table-column>-->
<!--            </el-table> -->
          <!--workload v2-->
          <template v-if="queryPanelFlag" >
            <el-tabs  v-model="this.formV2.tablename" style="margin-left: -30px;margin-right: 15px;margin-top: 5px">
              <el-tab-pane v-for="(workloadData,tablename,index) in tableData" :name="tablename" :key="'load'+index" :label="tablename">
                <el-table
                    :data="workloadData"
                    empty-text="No data"
                    :header-cell-style="{background:'#f0f9eb'}"
                    max-height="600">
                  <el-table-column
                      min-width="220"
                      label="Projection columns"
                  >
                    <template slot-scope="scope">
                      {{ scope.row.scan }}
                    </template>
                  </el-table-column>

                  <el-table-column
                      label="Filter columns"
                      min-width="140"
                  >
                    <template slot-scope="scope">
                      <el-tag size="medium">{{ scope.row.filter }}</el-tag>
                    </template>
                  </el-table-column>
                  <el-table-column
                      label="GP_OB columns"
                      min-width="140">
                    <template slot-scope="scope">
                      {{ scope.row.gp_ob }}
                    </template>
                  </el-table-column>
                  <el-table-column
                      label="Selectivity"
                      align="center"
                  >
                    <template slot-scope="scope">
                      {{ scope.row.sel.toFixed(3) }}
                    </template>
                    <!--              prop="sel"-->
                  </el-table-column>
                  <el-table-column label="Operation">
                    <template slot-scope="scope">
                      <!-- <el-button
                        size="mini"
                        @click="handleEdit(scope.$index, scope.row)">编辑</el-button> -->
                      <el-button
                          size="mini"
                          plain
                          type="danger"
                          @click="handleDelete(scope.$index, workloadData)">Delete</el-button>
                    </template>
                  </el-table-column>
                </el-table>

              </el-tab-pane>
            </el-tabs>


          </template>

        </el-col>
      </el-row>

      <el-row type="flex" justify="center" >
        <el-col :span="12" >
          <el-row type="flex" justify="center" style="margin-top:50px;padding-top: 30px;background-color: white;height:700px;max-height: 1000px;border-radius: 50% 20% / 10% 40%;">
            <el-col :span="10" style="text-align: center">
              <!--          <el-row type="flex" justify="center" >-->
              <!--            <el-col :span="4" >-->
              <el-button type="primary" size="small" @click="submitData" plain>Save Workload</el-button>
              <!--            </el-col>-->
              <!--          </el-row>-->
              <!--          <el-row type="flex" justify="center">-->
              <div class="block" >
                <div class="radio">
                  Sort：
                  <el-radio-group v-model="reverse">
                    <el-radio :label="true">Ascending</el-radio>
                    <el-radio :label="false">Descending</el-radio>
                  </el-radio-group>
                </div>
                <el-timeline :reverse="reverse">
                  <el-timeline-item v-for="(activity, index) in activities"  :key="index" :timestamp="activity.timestamp" placement="top">
                    <el-card>
                      <h4>Saved workload file: <span style="font-style: italic">{{activity.filename}}</span></h4>
                      <p>Submitted at {{activity.timestamp}}</p>
                    </el-card>
                  </el-timeline-item>
                </el-timeline>
                <!--              <el-timeline >-->
                <!--                <el-timeline-item-->
                <!--                    size="large"-->
                <!--                    v-for="(activity, index) in activities"-->
                <!--                    :key="index"-->
                <!--                    :timestamp="activity.timestamp">-->
                <!--                  {{activity.filename}}-->
                <!--                </el-timeline-item>-->
                <!--              </el-timeline>-->
              </div>
              <!--          </el-row>-->
            </el-col>
            <el-col :span="2">
              <el-button type="success" size="small" plain @click="commitWorkload">Upload</el-button>
            </el-col>
          </el-row>
        </el-col>


      </el-row>

  </div>
</template>

<script>
import {addWorkloadV2, fetchTableColumns, fetchWorkloadV2, parseSql,genSql} from '@/api/storageVp'

import { mapGetters } from 'vuex'
export default {
    data() {
      return {
        tableData:{},
        curStoredWorkloadData:[],
        form:{
          tablename:'',
          value: '',
          freq: 1,
          scan_key: '',
          selectivity: 0.01,
        },

        formV2:{
          tablename:'',
          curViewedTablename:'',
          sql:"select l_partkey,l_linenumber,l_extendedprice from lineitem where l_partkey=2 and l_quantity>5 or l_shipdate < '1998/02/05' order by l_partkey;"
        },
        parsePanelFlag:false,
        queryPanelFlag:false,
        parsedSqlRes:{
          rawEmbs:[],
          encodeEmbs:[],
          // treeData:[{
          //   title: 'parent 1',
          //   key: '0-0',
          //   slots: {
          //     icon: 'smile',
          //   },
          //   children: [
          //     { title: 'leaf', key: '0-0-0', slots: { icon: 'meh' } },
          //     { title: 'leaf', key: '0-0-1', scopedSlots: { icon: 'custom' } },
          //   ],
          // }],
        },
        columnsOption:{},
        // workloadFiles:['customer','lineitem','nation','orders','part','partsupp','region','supplier'],
        selectedWorkloadName:'',
        reverse: true,
        activities: []
      }
    },
    created(){
      console.log("workload 没有被复用")
      this.formV2.curViewedTablename=this.tabledata[0]
      this.setColumnOption(this.formV2.curViewedTablename)
    },
    computed:{
      ...mapGetters([
        'tabledata',
        'workloads',
        'methods',
        'costModels',
        'tableScale'
      ])
    },
    methods: {
      setColumnOption(tablename){
        return new Promise((resolve, reject) => {
          fetchTableColumns(tablename).then(response=>{
            this.columnsOption[tablename]=response.data.columns.map((v,i)=>{
              return v
            })
          }).catch(e =>{
            reject(e)
          })
        });
        // fetchTableColumns(tablename).then(response=>{
        //   this.columnsOption[tablename]=response.data.columns.map((v,i)=>{
        //     return v
        //   })
        // })
        // this.form.scan_key=[]
        // this.form.value=[]
      },
      handleDelete(index, rows) {
        // 删除表数据
        rows.splice(index,1)
      },
      onAddRow() {
        const formData = JSON.parse(JSON.stringify(this.form))
        this.tableData.push(formData)
      },
      onAddOneQuery() {
        this.queryPanelFlag=false
        for(let emb of this.parsedSqlRes.encodeEmbs){
          console.log(emb.tab)
          let new_emb=JSON.parse(JSON.stringify(emb));
          if(!(emb.tab in this.tableData))
            this.tableData[emb.tab]=[]
          // if(emb.tab==this.formV2.tablename){
          for(let k in emb){
            if(k=='scan'||k=='filter'||k=='gp_ob'){
              let new_col=emb[k].map(vv=>{
                if(vv>=0)return this.columnsOption[emb.tab][vv]
              })
              new_emb[k]=new_col
            }
          }
          this.tableData[emb.tab].unshift(new_emb)
          // }
        }
        this.formV2.tablename=this.parsedSqlRes.encodeEmbs[0].tab
        this.queryPanelFlag=true
      },
      onParseSql(){
        parseSql({'sql':this.formV2.sql}).then(res=>{
          this.parsedSqlRes.rawEmbs=res.data[0]
          this.parsedSqlRes.encodeEmbs=res.data[1]
          this.parsePanelFlag=true
          for(let emb of this.parsedSqlRes.encodeEmbs){
            if(!(emb.tab in this.columnsOption))
              this.setColumnOption(emb.tab)
          }
        })


      },
      onGenSql(){
        genSql().then(res=>{
          this.formV2.sql=res.data
        })
      },
      readData(){
        // 将编号转化为列名
        //fetch query embeddings
        this.queryPanelFlag=false
        fetchWorkloadV2(this.formV2.curViewedTablename).then(response=>{
          this.tableData[this.formV2.curViewedTablename]=response.data.map((row)=>{
            for(let k in row){
              if(k=='scan'||k=='filter'||k=='gp_ob'){
                let newRow=row[k].map(vv=>{
                  if(vv>=0)return this.columnsOption[this.formV2.curViewedTablename][vv]
                })
                row[k]=newRow
              }
            }
            return row
          })
          this.queryPanelFlag=true
          this.formV2.tablename=this.formV2.curViewedTablename
        })

      },
      tabClick(tab){
        if(tab.label!='Input query'){
          this.parsePanelFlag=false
        }
      },
      submitData(){
        for(let tablename in this.tableData){
            const tableDataCopy=JSON.parse(JSON.stringify(this.tableData[tablename]))
            // 将列名转换成编号
            let updateTableData=tableDataCopy.map((row)=>{
              for(let k in row){
                if(k=='value'||k=='scan_key'){
                  let newRow=[]
                  row[k].map(vv=>{
                    return newRow.push(this.columnsOption[tablename].findIndex((n)=>n==vv))
                  })
                  row[k]=newRow
                }
              }
              return row
            })
            let myDate=new Date()
            this.activities.push({
              filename: tablename+'_'+myDate.getTime()+".csv",
              timestamp: myDate.toLocaleTimeString(),
              tabname: tablename,
              data:updateTableData
            })
        }
      },
      commitWorkload(){
        let fileList=[]
        let tableList=this.tabledata,scaleList=this.tableScale
        for(let activity of this.activities){
          fileList.push(activity['filename'])
          if(!(tableList.includes(activity['tabname']))){
            tableList.push(activity['tabname'])
            scaleList.push(100000)
          }
          if(activity['tabname'])
          addWorkloadV2({fname:activity['filename'],data:activity['data']}).then(response=>{
            this.encodeMessageOption(response.data.msg,'success')
          })
        }
        this.$store.commit("SET_WORKLOAD",fileList)
        this.$store.commit("SET_TABLE",[tableList,scaleList])
        this.encodeMessageOption('Submit successfully','success')
        this.activities=[]
      },
      encodeMessageOption(text,stype){
        this.$message({
          dangerouslyUseHTMLString: true,
          message: '<h2>'+text+'</h2>',
          type: stype,
        });
      }
    }
}
</script>

<style lang="scss" scoped>
*{
  font-size: 18px;
}
.main-panel{
  background-color: #f3f5f2;
}

.block{
  margin-top: 20px;
  max-height: 1000px;
}
.el-form{
  margin-left: 30px;
  margin-top: 10px;
}
.el-tabs{
  max-height:1000px;
  // width:50%;
  margin-bottom: 40px;
}
#defineQueryForm{
  .el-form-item-label{
    font-size: 18px;
  }
}

</style>

<style lang="scss">
.el-tabs__nav-scroll .el-tabs__item{
  font-weight: bolder !important;
  font-size: 18px;
}
.ant-descriptions-bordered .ant-descriptions-item-label{
  font-size: 18px;
}
.ant-descriptions-item-content{
  font-size: 17px;
}
</style>