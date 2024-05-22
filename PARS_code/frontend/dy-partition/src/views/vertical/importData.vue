<template>
<div class="main-panel">
<el-row type="flex" class="row-bg input-panel" justify="center">
    <el-col :span="12" :offset="1" id="importTableForm">
      <a-card style="height: 221px;border-radius: 5px">
<!--        <p style="text-align: center"><i class="el-icon-tickets"></i> Form</p>-->
        <a-divider style="font-style: italic;margin-top: -5px">Input Form</a-divider>
        <el-row type="flex"  align="middle" style="margin-top: 5px" justify="center">
          <el-col :span="11" :offset="0" style="text-align: center">
            <span style="margin-right: 8px;">Select Table:  </span>

<!--            <a-select style="width: 150px"  @change="initTableConfig" >-->
<!--              <a-select-option v-for="group in table_options" :value="group.label">-->
<!--                {{group.label}}-->
<!--              </a-select-option>-->
<!--            </a-select>-->

            <el-select size="small" style="width: 50%" v-model="listQuery.tabName" @change="initTableConfig" placeholder="Select table">
              <el-option-group
                  v-for="group in table_options"
                  :key="group.label"
                  :label="group.label">
                <el-option
                    v-for="item in group.options"
                    :key="item.value"
                    :label="item.label"
                    :value="item.value">
                </el-option>
              </el-option-group>
            </el-select>

          </el-col>
          <el-col :span="13" :offset="0" style="text-align: center">
            <span style="margin-right: 8px">Set Viewable Table Size:</span>
            <a-input-number style="width: 150px" :autofocus="true"  :min="1" :max="10000000" :default-value="3" v-model="listQuery.rowNum"/>
<!--            <el-input-number v-model="listQuery.rowNum" size="medium"  :min="1" :max="100000" ></el-input-number>-->
          </el-col>
        </el-row>
        <el-row type="flex" justify="center" style="margin-top: 25px">
          <el-col :span="10" style="text-align: center">
            <!--          <el-button type="primary" icon="el-icon-search" @click="getTableData"   plain>View</el-button>-->
            <el-button type="primary" icon="el-icon-circle-plus-outline" @click="saveTableInfo" plain>Add</el-button>
            <!--            <el-button type="primary" size="default" >Add table</el-button>-->
          </el-col>

        </el-row>
      </a-card>
    </el-col>

    <el-col :span="11" :offset="2">
      <a-card style="height: 221px;border-radius: 5px">
<!--      <p style="margin-bottom: 10px">Partitioned Tables:</p>-->
        <a-divider style="font-style: italic;margin-top: -5px">Partitioned Tables:</a-divider>
        <div>
          <template v-if="selectedTableList.length>0" v-for="(tab,index) in selectedTableList" >
            <a-tag :color="preSetColor[index]"  closable @close="()=>deleteSavedTable(tab)">
              {{tab}}
            </a-tag>
          </template>
          <template v-if="selectedTableList.length==0">
            <p>No Data.</p>
          </template>
        </div>
      </a-card>
    </el-col>
</el-row>
<div v-loading="listLoading" class="result-panel" v-show="tableVisablePanel">
    <el-row type="flex" class="row-bg" justify="center">
      <el-col :span="15" :offset="0">
        <!-- Table component for table data      -->
        <el-table :row-class-name="tableRowClassName" :header-cell-style="{background:'#f0f9eb'}"  :data="studentInfo" empty-text="No data" max-height="1000" border stripe>
          <el-table-column v-for="col in columns"
                           :prop="col.val"
                           :key="col.id"
                           :label="col.label"
                           align="center"
          >
          </el-table-column>
        </el-table>

        <el-row type="flex" justify="center" >
          <!-- <el-col :span="24" :offset="0"> -->
          <el-pagination
              style="margin-top: 20px"
              background
              layout="prev, pager, next"
              @current-change="handleCurrentChange"
              :page-count="listQuery.pageCount"
              :current-page.sync="listQuery.currentPage">
          </el-pagination>
          <!-- </el-col> -->
        </el-row>

      </el-col>
      <el-col :span="8" :offset="1">
        <el-descriptions  title="" :column="3" size="medium" border>
          <el-descriptions-item>
            <template slot="label">
              <i class="el-icon-user"></i>
              Table name
            </template>
            {{listQuery.tabName}}
          </el-descriptions-item>
          <el-descriptions-item>
            <template slot="label">
              <i class="el-icon-mobile-phone"></i>
              Columns Count
            </template>
            {{tableProfile.columnCount}}
          </el-descriptions-item>
          <el-descriptions-item>
            <template slot="label">
              <i class="el-icon-location-outline"></i>
              Primary Key
            </template>
            {{tableProfile.primarykey}}
          </el-descriptions-item>
        </el-descriptions>

        <el-table
            :data="tableProfile.profile"
            :header-cell-style="{background:'#f0f9eb'}"
            border
            max-height="1000"
            style="width: 100%">
          <el-table-column
              prop="name"
              label="Column Name"
              align="center"
              min-width="180">
          </el-table-column>
          <el-table-column
              prop="type"
              label="Type"
              align="center"
              min-width="180">
          </el-table-column>
          <el-table-column
              prop="isnull"
              align="center"
              min-width="180"
              label="Not Null">
          </el-table-column>
        </el-table>

      </el-col>
    </el-row>
</div>

<el-button size="small" type="success" @click="openDrawer" style="position: absolute;right: 0px;top: 38px" plain>Define table</el-button>
<el-row type="flex" justify="center">

  <el-drawer
      title="Add New Table"
      :visible.sync="drawer"
      size="75%"
      direction="btt">
    <el-row type="flex" justify="center">
      <el-col :span="12">
        <Form :model="formInline" :label-width="250" class="formRegister">
          <FormItem prop="tabName">
            <span slot="label">
               <span class="el-form-item-label" >Table Name</span>
            </span>
            <Input type="text" v-model="formInline.tabName"></Input>
          </FormItem>
          <FormItem prop="rowCount" >
            <span slot="label">
               <span class="el-form-item-label" >Row Number</span>
            </span>
            <el-input-number v-model="formInline.rowCount" size="medium"  :min="1" :max="100000" ></el-input-number>
          </FormItem>
          <FormItem prop="dataGen">
            <span slot="label">
               <span class="el-form-item-label" >Data Generate method</span>
            </span>
            <el-radio v-model="formInline.dataGen" :label="0" border >Random</el-radio>
            <el-radio v-model="formInline.dataGen" :label="1" border >Upload</el-radio>
          </FormItem>
          <FormItem prop="dataSource">
            <span slot="label">
               <span class="el-form-item-label" >Table Structure</span>
            </span>
            <div>
              <a-button type="dashed" @click="handleAdd" size="small" >Add Row</a-button>
              <a-table bordered :data-source="formInline.dataSource" :columns="formInline.columns">
                <template slot="name" slot-scope="text, record">
                  <Input type="text" v-model="record.name"></Input>
                </template>
                <template slot="length" slot-scope="text, record">
                  <Input type="text" v-model="record.length"></Input>
                </template>
                <template slot="type" slot-scope="text, record">
                  <el-select v-model="record.type">
                    <el-option
                        v-for="(item,tid) in formInline.typeList"
                        :key="tid"
                        :label="item"
                        :value="item">
                    </el-option>
                  </el-select>
                </template>
                <template slot="operation" slot-scope="text, record">
                  <el-popconfirm
                      v-if="formInline.dataSource.length"
                      title="Sure to delete?"
                      @confirm="() => onDelete(record.key)"
                  >
                    <a href="#" slot="reference">Delete</a>
                  </el-popconfirm>
                </template>
              </a-table>
            </div>
          </FormItem>
          <FormItem>
            <el-button type="primary" @click="handleTabCreate(formInline)">Create Table</el-button>
          </FormItem>
        </Form>

      </el-col>

    </el-row>
    <a-divider><i class="el-icon-crop"></i></a-divider>
    <el-row type="flex" justify="center">
     <el-col :span="12">
       <Form :model="uploadTabForm" :label-width="250" class="formRegister">
         <FormItem prop="tabName">
            <span slot="label">
               <span class="el-form-item-label" >Upload Table Name</span>
            </span>
           <Input type="text" v-model="uploadTabForm.tabName"></Input>
         </FormItem>
         <FormItem prop="fileList">
            <span slot="label">
               <span class="el-form-item-label" >Upload Data</span>
            </span>
           <el-upload
               class="upload-demo"
               drag
               :data="uploadTabForm"
               :file-list="uploadTabForm.fileList"
               :limit="10"
               :on-error="insertDataFailure"
               :on-success="insertDataSuccess"
               action="http://10.77.110.133:4001/aidb/storage/insertTab"
               multiple>
             <i class="el-icon-upload"></i>
             <div class="el-upload__text">Drag the file here, or click Upload</div>
             <div slot="tip" class="el-upload__tip">Only <span style="color: red;font-style: italic;font-weight: bold">.csv</span> format is allowed</div>
           </el-upload>
         </FormItem>
       </Form>
     </el-col>
    </el-row>
  </el-drawer>


</el-row>
</div>
</template>

<script>
import axios from "axios";
import {fetchStudentList,fetchAllTables,fetchTableCount} from '@/api/storageVp'
export default {
  data() {
    return {
      table_options:[
        {
          value:'orders',
          label:'orders'
        },
        {
          value:'customer',
          label:'customer'
        },
        {
          value:'lineitem',
          label:'lineitem'
        }
      ],
      columns:[],
      studentInfo: [],
      tableVisablePanel:false,
      listQuery:{
        tabName:'customer',
        rowNum:100000,
        actRowNum:0,
        pageSize:8,
        pageCount:5,
        currentPage:1,
      },
      tableProfile:{
        tableName:'test',
        columnCount:0,
        primarykey:'a1',
        profile:[]
      },
      listLoading:false,
      selectedTableList:[],
      selectedTableScale:[],
      selectedBenchmark:[],
      preSetColor:['pink','red','orange','green','cyan','blue','purple'],
      drawer:false,
      formInline: {
        tabName: 'new table name',
        rowCount: 1000,
        dataGen:1,
        dataSource:[
          {
            key: '0',
            name: 'id',
            length: '4',
            type: 'Integer',
          }
        ],
        count: 1,
        columns: [
          {
            title: 'name',
            dataIndex: 'name',
            width: '30%',
            scopedSlots: { customRender: 'name' },
          },
          {
            title: 'length',
            dataIndex: 'length',
            scopedSlots: { customRender: 'length' },
          },
          {
            title: 'type',
            dataIndex: 'type',
            scopedSlots: { customRender: 'type' },
          },
          {
            title: 'operation',
            dataIndex: 'operation',
            scopedSlots: { customRender: 'operation' },
          },
        ],
        typeList:['Varchar','Int','Integer','Tinyint','Decimal','Numeric','Date','Text']
      },
      uploadTabForm:{
        tabName:'new table name',
        // fileList:[{name: 'table_data.csv', url: 'http://10.77.110.133:5000/static/tempTabData'}]
      }

    }
  },
  // computed:{
  //   // pageCount(){
  //   //   return this.listQuery.rowNum/this.listQuery.pageSize
  //   // }
  // },
  created() {
    this.updateTableOptions()
    this.getTableData()
  },
  methods: {
      updateTableOptions(){
        this.table_options=[]
        fetchAllTables().then(res=>{
          for(var benchmark of res.data['bm_keys']){
            let option={'label':benchmark,'options':[]}
            for(var tab of res.data['tab_data'][benchmark]){
              option['options'].push({'value':tab,'label':tab})
            }
            this.table_options.push(option)
          }

        })
      },
      fetchTableProfile(workloadName){
        axios.get('/aidb/dypartition/tableProfile/'+workloadName+'.csv').then((response)=>{
          this.tableProfile.tableName=response.data.name
          this.tableProfile.columnCount=response.data.column_count
          this.tableProfile.primarykey=response.data.primary_key
          this.tableProfile.profile=response.data.profile.map((row)=>{
            if(row[3]) return {name:row[0],type:row[1],isnull:'False'}
            else  return {name:row[0],type:row[1],isnull:'True'}
          })
          console.log(this.tableProfile)
        })
      },
      handleCurrentChange(val) {
        this.listQuery.currentPage=val
        this.changeTableDataPage()
      },
      saveTableInfo(){
        if(this.selectedTableList.indexOf(this.listQuery.tabName)==-1){
          this.selectedTableList.push(this.listQuery.tabName)
          this.selectedTableScale.push(this.listQuery.actRowNum)
          for(let item of this.table_options){
            // if(this.selectedBenchmark.contains(item['label']))
            //   continue
            if(this.selectedBenchmark.indexOf(item['label']) !== -1){
              continue
            }
            for(let option of item['options']){
              if(option['label']==this.listQuery.tabName){
                this.selectedBenchmark.push(item['label'])
                break
              }

            }
            // if(item['options'].includes({'label':this.listQuery.tabName,'value':this.listQuery.tabName}))

          }
          this.$store.commit('SET_TABLE',[this.selectedTableList,this.selectedTableScale])
          this.$store.commit('SET_BENCHMARK',this.selectedBenchmark)
        }
      },
      deleteSavedTable(e){
        let deleteIndex=this.selectedTableList.indexOf(e)
        this.selectedTableList.splice(deleteIndex,1)
        this.selectedTableScale.splice(deleteIndex,1)
        this.$store.commit('SET_TABLE',[this.selectedTableList,this.selectedTableScale])
      },
      async changeTableDataPage(){
        this.listLoading=true
        this.tableVisablePanel=true
        await fetchStudentList(this.listQuery).then(response=>{
          let data=response.data
          this.columns=data.columns.map((v,i)=>{
            return {
              id:i,
              val:v,
              label:v
            }
          })
          this.studentInfo=data.student_info.map(v=>{
            let tupleInfo={}
            for(let i=0;i<v.length;i++){
              tupleInfo[data.columns[i]]=v[i]
            }
            return tupleInfo
          })
          // 存到store中
          // let columnNames=this.columns.map(item=>{
          //   return item.label
          // })
          // this.$store.commit('SET_TABLE',{name:this.listQuery.tabName,num:this.listQuery.rowNum,columns:columnNames})

          this.listLoading=false

        })
      },
      getTableData(){
        fetchTableCount(this.listQuery.tabName).then(response=>{
          this.listQuery.actRowNum=response.count
          if(response.count<this.listQuery.rowNum)
            this.listQuery.pageCount=Math.ceil(this.listQuery.actRowNum/this.listQuery.pageSize)
          else
            this.listQuery.pageCount=Math.ceil(this.listQuery.rowNum/this.listQuery.pageSize)
            // this.listQuery.rowNum=response.count

          this.fetchTableProfile(this.listQuery.tabName)
          this.changeTableDataPage()
        })
        // this.listQuery['pageCount']=this.listQuery['']
        // parameters={
        //   tabName:this.listQuery.tabName,
        //   limit:this.currentPage*this.listQuery.pageSize
        // }
      },
      tableRowClassName({row, rowIndex}) {
        if (rowIndex === 1) {
          console.log('warning-row')
          return 'warning-row';
        } else if (rowIndex === 3) {
          return 'success-row';
        }
        return '';
      },
      initTableConfig(){
        this.listQuery.currentPage=1
        // this.listQuery.rowNum=100000
        this.getTableData()
      },
      openDrawer(){
        this.drawer=true
      },
      encodeMessageOption(text,stype){
        this.$message({
          dangerouslyUseHTMLString: true,
          message: '<h2>'+text+'</h2>',
          type: stype,
        });
      },
      handleTabCreate(data){
        axios.post('/aidb/storage/createTab',data).then((response)=>{
          console.log(response.data.status)
          if(response.data.status===1){
            this.encodeMessageOption(response.data.message,'success')
            this.updateTableOptions()
          }else{
            this.encodeMessageOption(response.data.message,'warning')
          }
        })
      },

      onCellChange(key, dataIndex, value) {
        const dataSource = [...this.formInline.dataSource];
        const target = dataSource.find(item => item.key === key);
        if (target) {
          target[dataIndex] = value;
          this.formInline.dataSource = dataSource;
        }
      },
      onDelete(key) {
        const dataSource = [...this.formInline.dataSource];
        this.formInline.dataSource = dataSource.filter(item => item.key !== key);
      },
      handleAdd() {
        const { count, dataSource } = this.formInline;
        const newData = {
          key: count,
          name: `column${count}`,
          length: 10,
          type: `Varchar`,
        };
        this.formInline.dataSource = [...dataSource, newData];
        this.formInline.count = count + 1;
      },
      insertDataSuccess(response, file, fileList){
        if(response.status===0){
          this.encodeMessageOption(response.message,'success')
        }else{
          this.encodeMessageOption(response.message,'warning')
        }
      },
      insertDataFailure(err, file, fileList){
        console.log(err)
        this.encodeMessageOption('Upload data error!','warning')
      }
  }
}
</script>

<style lang="scss" scoped>

.main-panel{
  padding-top: 40px;
  height: 100%;
  background-color: #f3f5f2;
}
#importTableForm{
  //padding:10px 0;
  //border: solid 2px #959898;
}
*{
  font-size: 20px;
}
.input-panel{
  //font-weight: bolder;
  //background-color: white;
}
.result-panel{
  margin: 50px 0px;
}
.result-panel .row-bg{
  margin-bottom: 30px;
}
.formRegister  .el-form-item-label {
  font-size: 18px;
}

</style>

<style lang="scss">
.ant-input-number-handler-wrap{
  opacity: 1;
}
header#el-drawer__title{
  color: #67C23A;
  font-weight: bold;
}

input.el-input__inner{
  font-size: 16px;
}

.ivu-form-item-content{
  span.el-radio__label{
    font-size: 15px;

  }
  input.ivu-input{
    font-size: 17px;
    color:blue;
    font-style: italic;
  }
  button.ant-btn{
    font-size: 16px;
  }
  table{
    font-size: 18px;
  }
  tr.ant-table-row{
    td{
      input.ivu-input{
        font-size: 18px;
      }
    }
  }

}


</style>