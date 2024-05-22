<template>
  <div id="dy-page">
    <el-row justify="space-between" type="flex">
      <el-col :span="9">
        <el-row type="flex" align="middle">
          <el-col>
            <el-row
              type="flex"
              justify="space-between"
              style="margin-bottom: 20px"
            >
              <!--            可用功能区2-->
              <el-col :span="11">
                <a-card
                  title="Select the algorithm to be compared"
                  style="width: 100%;"
                >
<!--                  <a slot="extra" href="#">...</a>-->
                  <el-checkbox
                    :indeterminate="isIndeterminate"
                    v-model="checkAll"

                    @change="handleCheckAllChange"
                    ><h3>Check all</h3>
                  </el-checkbox>
                  <div style="margin: 15px 0"></div>
                  <el-checkbox-group
                    v-model="selectedAlgos"
                    @change="handleCheckedAlgosChange"
                  >
                    <el-checkbox
                      v-for="(algo, idx) in algos"
                      :label="idx"
                      :key="algo"
                      size="medium"
                      ><span style="font-size: 17.2px">{{ algo }}</span>
                    </el-checkbox>
                  </el-checkbox-group>
                </a-card>
              </el-col>
              <!--            可用功能区3-->
              <el-col :span="12">
                <a-card  title="Set workload" style="width: 100%;">
<!--                  <a slot="extra" href="#">more</a>-->
                  <el-select
                    v-model="selectedDatasets"
                    multiple
                    style="margin-bottom: 20px; width: 100%;"
                    size="large"
                    placeholder="Please select"
                  >
                    <el-option
                      v-for="(dataset, idx) in datasets"
                      :key="dataset"
                      :label="dataset"
                      :value="idx"
                    >
                    </el-option>
                  </el-select>
                  <el-button type="primary" size="small" plain @click="openDrawer" style="font-size: 13px">View</el-button>
                  <el-button type="success" size="small" plain @click="exportCsv" style="font-size: 13px">Export CSV</el-button>
                <!--   show the workload profile in detail        -->
                  <el-drawer
                      title="Table and workload profile"
                      :visible.sync="drawer"
                      size="65%"
                      direction="btt">
                    <h3 style="text-align: center">
                      <span style="margin-right:10px">Viewing Worklaod</span>
                      <el-select v-model="viewingWorkload" @change="handleWorkloadChange">
                        <el-option  v-for="(value, idx) in selectedDatasets"
                                    :key="idx"
                                    :value="datasets[value]">

                        </el-option>
                      </el-select>
                    </h3>
                    <a-divider><i class="el-icon-crop"></i></a-divider>
                    <el-row justify="space-around" type="flex">
                      <el-col :span="14" >
                        <p style="font-size: 16px;font-weight: 700">Workload Profile</p>
                        <el-table
                            :data="workloadData"
                            border
                            :header-cell-style="{background:'#f0f9eb'}"
                            empty-text="No data"
                            min-height="450">
                          <el-table-column
                              min-width="480"
                              label="Accessed columns"
                          >
                            <template slot-scope="scope">
                              {{ scope.row.value }}
                            </template>
                          </el-table-column>
                          <el-table-column
                              label="Frequency"
                              prop="freq"
                              align="center"
                              width="110"
                          >
                          </el-table-column>
                          <el-table-column
                              label="Scan keys"
                              min-width="280"
                          >
                            <template slot-scope="scope">
                              <el-tag size="medium">{{ scope.row.scan_key }}</el-tag>
                            </template>
                          </el-table-column>
                          <el-table-column
                              label="Selectivity"
                              prop="selectivity"
                              width="120"
                              align="center"
                          >
                          </el-table-column>
                          <el-table-column
                              label="Arrival Time"
                              prop="arrive_time"
                              width="120"
                              align="center"
                          >
                          </el-table-column>
                        </el-table>

                        <el-row type="flex" justify="center" >
                          <!-- <el-col :span="24" :offset="0"> -->
                          <el-pagination
                              background
                              layout="prev, pager, next"
                              @current-change="handlePageChange"
                              :page-count="listQuery.pageCount"
                              :current-page.sync="listQuery.currentPage">
                          </el-pagination>
                          <!-- </el-col> -->
                        </el-row>
                      </el-col>
                      <el-col :span="8" :offset="1">
                        <el-descriptions class="margin-top" title="Table Profile" :column="3" size="medium" border>
                          <el-descriptions-item>
                            <template slot="label">
                              <i class="el-icon-user"></i>
                              Table name
                            </template>
                            {{tableProfile.tableName}}
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
                            max-height="490"
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

                  </el-drawer>
                </a-card>
              </el-col>
            </el-row>
            <!--            可用功能区1-->
            <el-row id="expSet">
              <template>
                <a-card title="Experiment Setup">
                  <a-card-grid style="width: 100%" :hoverable="false">
                    <h4>
                      <i class="el-icon-timer" style="font-size: 17px"
                        >Step 1:Select time range: [{{timeRange[0]}},{{timeRange[1]}}]
                      </i>
                    </h4>
                    <div class="block">
                      <el-slider
                        v-model="timeRange"
                        range
                        :marks="timeMarks"
                        :max="500"
                        :min="0"
                      >
                      </el-slider>
                    </div>
                  </a-card-grid>
                  <a-card-grid style="width: 100%; text-align: left">
                    <h4>
                      <i class="el-icon-pie-chart" style="font-size: 17px"
                        >Step 2: Evaluation indicators (denoted as M1, M2):
                      </i>
                    </h4>
                    <div style="margin-top: 20px">
                      <el-radio
                        v-model="selectedMetric"
                        :label="0"
                        border
                        size="medium"
                        >I/O Cost (Per query)
                      </el-radio>
                      <el-radio
                        v-model="selectedMetric2"
                        :label="1"
                        border
                        size="medium"
                        >Repartition Cost
                      </el-radio>
                    </div>
                  </a-card-grid>
                  <a-card-grid style="width: 100%; text-align: left">
                    <h4>
                      <i class="el-icon-video-camera" style="font-size: 17px"
                        >Step 3:Select visual components:</i
                      >
                    </h4>
                  </a-card-grid>

                  <a-card-grid class="plot-option">
                    <el-checkbox-group v-model="plots.staticComps">
                      <el-checkbox :label="0">Line Chart (M1)</el-checkbox>
                      <el-checkbox :label="1">Histogram (M2)</el-checkbox>
                    </el-checkbox-group>
                  </a-card-grid>
                  <a-card-grid class="plot-option">
                    <el-checkbox-group v-model="plots.dyComps">
                      <el-checkbox :label="0">Line Race (M1)</el-checkbox>
                      <el-checkbox :label="1">Bar Race (M2)</el-checkbox>
                    </el-checkbox-group>
                  </a-card-grid>
                  <a-card-grid class="plot-option">
                    <el-checkbox-group v-model="plots.complexComps">
                      <el-checkbox :label="0">Heat Map</el-checkbox>
                    </el-checkbox-group>
                  </a-card-grid>

                  <a-card-grid style="width: 100%; text-align: left">
                    <h4>
                      <i class="el-icon-video-play" style="font-size: 16px"
                        >Step 4:Start:</i>
                    </h4>
                    <el-row type="flex" justify="center" style="margin-bottom: 20px;margin-top: 40px">
                      <el-button
                        type="primary"
                        size="small"
                        style="font-size: 13px"
                        @click="controlRunning"
                        >Start/Pause
                      </el-button>
                      <el-button
                        type="warning"
                        size="small"
                        style="font-size: 13px"
                        @click="abortRunning"
                        >Abort
                      </el-button>
                      <el-button v-show="expProgress.percentage==100"
                          type="success"
                          size="small" style="font-size: 13px"
                          @click="exportJSON"
                      >Export JSON
                      </el-button>
                    </el-row>
                    <div style="text-align: right;margin-bottom: -35px">
                      <img src="/public/experiment.svg" width="9%" alt="">
                    </div>
                    <h3>Experiment progress:</h3>
                    <el-progress
                        style="margin-top: 20px"
                      :text-inside="true"
                      :stroke-width="16"
                      :percentage="expProgress.percentage"
                      :color="expProgress.customColors"
                    ></el-progress>

                  </a-card-grid>
                </a-card>
              </template>
            </el-row>
          </el-col>
        </el-row>
      </el-col>

      <el-col :span="14">
        <div
          v-loading="plotLoading"
          element-loading-text="loading"

          element-loading-background="rgba(256, 256, 256, 1)"
          style="
            width: 100%;
            background-color: white;
            min-height: 1000px;
            padding: 20px;
          "
        >
          <!--          demo绘图：Echarts库-->
          <a-tabs
              v-model:activeKey="activeKey"

            size="large"
            tab-position="top"
            @change="changeTabPage"
          >
            <a-tab-pane
              key="1"
              :forceRender="true"
              v-bind:disabled="disabledTabs.includes(0)"
            >
              <span slot="tab">
                <a-icon type="dot-chart" />
<!--                staticComps-->
                Static Charts
              </span>
              <div v-show="chartPanelShow">
                <el-row class="chart-panel">
                  <h3><i class="el-icon-pie-chart"></i> Line Chart(M1): Compare the I/O cost (per query) on different partitions.</h3>
                  <div
                      id="static-line-example"
                      style="width: 100%; min-height:400px"
                  ></div>
                </el-row>
                <el-row class="chart-panel">
                  <h3><i class="el-icon-pie-chart"></i> Histogram(M2): Compare the total repartitioning cost of different dynamic partitioning algorithm.</h3>
                  <div
                      id="static-bar-example"
                      style="width: 95%; min-height: 400px"
                  ></div>
                </el-row>
              </div>
              <div v-show="!chartPanelShow" style="text-align: center">
                <img style="width:35%;margin-top: 250px;opacity: 0.7" src="/public/statistic2.svg" alt="">
              </div>
            </a-tab-pane>

            <a-tab-pane
              key="2"
              :forceRender="true"
              v-bind:disabled="disabledTabs.includes(1)"
            >
              <span slot="tab">
                <a-icon type="dot-chart" />
<!--                dyComps-->
                Dynamic Charts
              </span>
              <div v-show="chartPanelShow">
                <el-row type="flex" justify="end" style="margin:10px 0">
                  <el-col :span="6" class="change-box" >
                    <span>Change Workload</span>
                    <el-select
                        v-model="dySelector.showedDataset"
                        @change="changeRacePlot"
                        size="mini"
                        placeholder="Select DataSet to show"
                    >
                      <el-option
                          v-for="no in dySelector.confirmSelectedDatasets"
                          :key="no"
                          :label="datasets[no]"
                          :value="no"
                      >
                      </el-option>
                    </el-select>
                  </el-col>
                </el-row>
                <el-row class="chart-panel">
                  <h3><i class="el-icon-pie-chart"></i> Line Race(M1): Dynamically see changes in average I/O cost for workloads as partitions are updated.</h3>
                  <div id="line-example" style="width: 100%; min-height: 380px"></div>
                </el-row>
                <el-row class="chart-panel">
                  <h3><i class="el-icon-pie-chart"></i> Bar Race(M2): Dynamically see changes in total repartitioning cost of different algorithms as partitions are updated.</h3>
                  <div id="bar-example" style="width: 100%; min-height: 380px"></div>
                </el-row>
              </div>

            </a-tab-pane>
            <a-tab-pane
              key="3"
              :forceRender="true"
              v-bind:disabled="disabledTabs.includes(2)"
            >
              <span slot="tab">
                <a-icon type="border-horizontal" />
                Heatmap Chart
              </span>
              <div v-show="chartPanelShow">
                <el-row type="flex" justify="space-between" style="margin:10px 0">

                  <el-col :span="4" :offset="2">
                    <el-button type="danger" size="small" v-if="heatmapParas.status==='run'" @click="controlHeatmap(0)" plain>Pause</el-button>
                    <el-button type="primary" size="small" v-else @click="controlHeatmap(1)" plain>Run</el-button>

                  </el-col>
                  <el-col :span="6"  :offset="5"  class="change-box">
                    <span>Change Workload</span>
                    <el-select
                        v-model="dySelector.showedDatasetComplex"
                        @change="changeHeatmapPlot"
                        size="mini"
                        placeholder="Select workload to show"
                    >
                      <el-option
                          v-for="no in dySelector.confirmSelectedDatasets"
                          :key="no"
                          :label="datasets[no]"
                          :value="no"
                      >
                      </el-option>
                    </el-select>
                  </el-col>

                  <el-col :span="6" class="change-box">
                    <span>Change Algorithm</span>
                    <el-select
                        v-model="dySelector.showedAlgoComplex"
                        @change="changeHeatmapPlot"
                        size="mini"
                        placeholder="Select algorithm to show"
                    >
                      <el-option
                          v-for="no in dySelector.confirmSelectedAlgos"
                          :key="no"
                          :label="algos[no]"
                          :value="no"
                      >
                      </el-option>
                    </el-select>
                  </el-col>
                </el-row>
              </div>

              <el-row class="chart-panel">
                <h3><i class="el-icon-pie-chart"></i> Heat Map: Observe the changes in the partitioning scheme as the hotness of the different data blocks in the table being accessed changes.</h3>
                <div
                  id="heatmap-example"
                  style="width: 100%; height: 700px"
                ></div>
              </el-row>
              <el-row type="flex" justify="center">
                <div id="relation-example" style="width: 100%;height: 350px"></div>
<!--                <h3><span class="tag-group__title">Current Partitions  </span><el-tag  effect="plain">{{heatCurPar}}</el-tag></h3>-->
              </el-row>

            </a-tab-pane>
          </a-tabs>
        </div>
      </el-col>
    </el-row>
  </div>
</template>

<script>
import axios from "axios";
import Papa from 'papaparse'
import FileSaver from 'file-saver'
const algoOptions = ["AutoStore", "Smopdc", "Feedback", "PPO-Controller"];
const dataOptions = [
  "lineitem0000",
  "orders0000",
  "SYN1200",
  "SYN1600",
  "SYN3000",
  "SYN4000"
];
export default {
  data() {
    return {
      activeKey:'1',
      checkAll: false,
      selectedAlgos: [0],
      algos: algoOptions,
      isIndeterminate: true,
      selectedDatasets: [0],
      datasets: dataOptions,
      viewingWorkload:'',
      drawer:false,
      workloadData:[],
      workloadColumns:[],
      heatAxisMap:{},
      heatAxisFreqMap:{},
      heatCurPar:'',
      listQuery:{
        tabName:'',
        rowNum:0,
        pageSize:10,
        pageCount:0,
        currentPage:1,
      },
      tableProfile:{
        tableName:'test',
        columnCount:0,
        primarykey:'a1',
        profile:[]
      },
      fileList: [
        {
          name: "example.csv",
          url: "",
        },
      ],
      timeRange: [1, 72],
      timeMarks: {
        0: "0",
        24: "1 day",
        168: "1 week",
        500: "500h",
      },
      parData:{},
      selectedMetric: 0,
      selectedMetric2: 1,
      //禁用的标签页
      disabledTabs: [0, 1, 2],
      //动态图需要展示的数据集
      dySelector: {
        confirmSelectedDatasets: [],
        confirmSelectedAlgos: [],
        showedDataset: "",
        showedDatasetComplex: "",
        showedAlgoComplex: "",
      },
      plots: {
        staticComps: [0, 1],
        dyComps: [0, 1],
        complexComps: [0],
      },
      plotLoading:false,
      chartPanelShow:false,
      echartsHandlers: {
        lineHandler: "",
        barHandler: "",
        lineRaceHandler: "",
        barRaceHandler: "",
        heatmapHandler: "",
        relationHandler:""
      },
      heatmapParas:{
        timer:"",
        myChart:'',
        par_time_list:[],
        reparCnt:0,
        runningTime:-1,
        par_time_dict:{},
        relationChart:'',
        status:'pause'
      },
      runningStatus: false,
      expProgress: {
        percentage: 0,
        customColors: [
          { color: "#f56c6c", percentage: 20 },
          { color: "#e6a23c", percentage: 40 },
          { color: "#5cb87a", percentage: 60 },
          { color: "#1989fa", percentage: 80 },
          { color: "#6f7ad3", percentage: 100 },
        ],
      },
      algoResults: [],
    };
  },
  // mounted() {
  //   this.plotLineRaceExample();
  //   this.plotBarRaceExample();
  //   this.plotLineExample();
  //   this.plotBarExample();
  //   this.plotHeatMapExample();
  // },
  methods: {
    handleCheckAllChange(val) {
      this.selectedAlgos = val? algoOptions.map((v, i) => {
            return i;
          })
        : [];
      this.isIndeterminate = false;
    },
    handleCheckedAlgosChange(value) {
      let checkedCount = value.length;
      this.checkAll = checkedCount === this.algos.length;
      this.isIndeterminate =
        checkedCount > 0 && checkedCount < this.algos.length;
    },
    openDrawer(){
      this.drawer=true
      this.viewingWorkload=dataOptions[this.selectedDatasets[0]]
      this.handleWorkloadChange(this.viewingWorkload)
    },
    handleWorkloadChange(workloadName){
      axios.get('/aidb/dypartition/columns/'+workloadName).then((response)=>{
        this.workloadColumns=response.data.columns
        axios.get('/aidb/dypartition/workloadSize/'+workloadName).then((response)=>{
          this.listQuery.rowNum=response.data.count
          this.listQuery.tabName=workloadName
          this.listQuery.pageCount=Math.ceil(this.listQuery.rowNum/this.listQuery.pageSize)
          this.fetchWorkloadDataByPage()
        })
      })
      this.fetchTableProfile(workloadName)
    },
    handlePageChange(val) {
      this.listQuery.currentPage=val
      this.fetchWorkloadDataByPage()
    },
    fetchWorkloadDataByPage(){
      axios.get('/aidb/dypartition/viewWorkload',{params:this.listQuery}).then((response)=>{
        // console.log(response.data)
        this.workloadData=response.data.queries.map((row)=>{
          for(let k in row){
            if(k=='value'||k=='scan_key'){
              let newRow=row[k].map(vv=>{
                if(vv>=0)return this.workloadColumns[vv]
              })
              row[k]=newRow
            }
          }
          return row
        })
      })
    },
    fetchTableProfile(workloadName){
      axios.get('/aidb/dypartition/tableProfile/'+workloadName).then((response)=>{
        this.tableProfile.tableName=response.data.name
        this.tableProfile.columnCount=response.data.column_count
        this.tableProfile.primarykey=response.data.primary_key
        this.tableProfile.profile=response.data.profile.map((row)=>{
          if(row[3]) return {name:row[0],type:row[1],isnull:'False'}
          else  return {name:row[0],type:row[1],isnull:'True'}
        })
        // columnOption=response.data.columns.map((v,i)=>{
        //   return v
        // })
      })
    },
    exportCsv(){
      for (let selectedIdx of this.selectedDatasets) {
        let selWorkload=dataOptions[selectedIdx]
        axios.get('/aidb/dypartition/workload/export/'+selWorkload).then((response)=>{
          var csv = Papa.unparse(response.data.data);
          //定义文件内容，类型必须为Blob 否则createObjectURL会报错
          let content = new Blob([csv]);
          //生成url对象
          let urlObject = window.URL || window.webkitURL || window;
          let url = urlObject.createObjectURL(content);
          //生成<a></a>DOM元素
          let el = document.createElement("a");
          //链接赋值
          el.href = url;
          el.download = selWorkload+".csv";
          //必须点击否则不会下载
          el.click();
          //移除链接释放资源
          urlObject.revokeObjectURL(url);
        })
      }
    },
    controlRunning() {
      // this.runningStatus = !this.runningStatus;
      if (!this.runningStatus) {
        //遮挡全屏
        // const loading = this.$loading({
        //   lock: true,
        //   text: "loading",
        //   spinner: "el-icon-loading",
        //   background: "rgba(0, 0, 0, 0.7)",
        // });

        this.plotLoading=true
        //清空已经展示的图表

        if(this.chartPanelShow){
          this.clearAllPlots()
          this.chartPanelShow=false
          this.activeKey='1'
        }

        //为定时获取进度信息
        var timer = setInterval(()=>{
          axios.get('/aidb/dypartition/progress').then((response)=>{
            console.log(response.data.progress)
            this.expProgress.percentage=response.data.progress
          })
        },1500*(this.selectedDatasets.length))
        this.chartPanelShow=true
        axios
          .post("/aidb/dypartition/start", {
            selectedAlgos: this.selectedAlgos,
            selectedDatasets: this.selectedDatasets,
            timeRange: this.timeRange,
            selectedMetric: this.selectedMetric,
            plots: this.plots,
          },{timeout:1000*60*7})
          .then((res) => {
            console.log(res.data);
            //接触某些选择的标签页
            let cnt = 0;
            for (var key in this.plots) {
              if (this.plots[key].length > 0) {
                this.disabledTabs[this.disabledTabs.indexOf(cnt)] = -1;
                this.disabledTabs.push(-2);
              }
              cnt++;
            }
            //将返回结果赋值给指定数据
            this.algoResults = res.data.plot;
            this.parData=res.data.par_data
            //  修改进度条
            this.expProgress.percentage = 100;
            //重置动态图选择框  基本元素,浅拷贝
            this.dySelector.confirmSelectedDatasets = this.selectedDatasets;
            this.dySelector.showedDataset = this.selectedDatasets[0];
            this.dySelector.showedDatasetComplex = this.selectedDatasets[0];
            this.dySelector.confirmSelectedAlgos = this.selectedAlgos;
            this.dySelector.showedAlgoComplex = this.selectedAlgos[0];
            //取消进度获取
            clearInterval(timer)
            // 取消遮挡
            // loading.close();
            this.plotLoading=false

            //  初始展示静态图
            this.changeTabPage(1)
            // this.changeTabPage(3);
          });
      } else {
      }
    },
    abortRunning() {
      //  清空状态
      this.algoResults = [];
      this.disabledTabs = [0, 1, 2];
      this.chartPanelShow=false
      this.activeKey='1'
      this.clearAllPlots()
      this.expProgress.percentage=0

    },
    exportJSON(){
      let jsonRes={},fixed_scale=100
      for(let workloadName in this.parData){
        let jsonItem={table_name:this.parData[workloadName]['tableProfile']['name'],workload_file:workloadName,methods:{}}
        let columnNames=this.parData[workloadName]['tableProfile']['columns']
        for(let methodName in this.parData[workloadName]){
          if(methodName==='tableProfile') continue
          let jsonMethod={dynamic_partitioning:{}}
          for(let time in this.parData[workloadName][methodName]){
            let scheme={partitions:[]}
            for(let par of this.parData[workloadName][methodName][time]){
              let jsonPar={profile:par.map(columnIndex=>{return columnNames[columnIndex]}),blocks:[]}
              let block_no=1
              for(let z=0;z<fixed_scale;z+=20){
                let end_z=z+19
                if(end_z>=fixed_scale)
                  end_z=fixed_scale-1
                jsonPar.blocks.push({no:block_no,row_index:z+':'+end_z})
                block_no++
              }
              scheme.partitions.push(jsonPar)
            }
            jsonMethod['dynamic_partitioning'][time]=scheme
          }
          jsonItem.methods[methodName]=jsonMethod
        }
        jsonRes[workloadName]=jsonItem
      }
      //  export file
      const blob=new Blob([JSON.stringify(jsonRes)],{type:"application/json"})
      FileSaver.saveAs(blob,'dynamic_partition_result.json')
    },
    changeTabPage(activeKey) {
      console.log(activeKey);
      //当切换标签页且该图形未被渲染时，重新画图
      if (activeKey == 1) {
        if (
          this.plots.staticComps.includes(0) &&
          this.echartsHandlers.lineHandler == ""
        )
          this.plotLineExample();
        if (
          this.plots.staticComps.includes(1) &&
          this.echartsHandlers.barHandler == ""
        )
          this.plotBarExample();
      } else if (activeKey == 2) {
        if (
          this.plots.dyComps.includes(0) &&
          this.echartsHandlers.lineRaceHandler == ""
        )
          this.plotLineRaceExample();
        if (
          this.plots.dyComps.includes(1) &&
          this.echartsHandlers.barRaceHandler == ""
        )
          this.plotBarRaceExample();
      } else {
        if (
          this.plots.complexComps.includes(0) &&
          this.echartsHandlers.heatmapHandler == ""
        )
          this.plotHeatMapExample2();
      }
    },
    plotLineRaceExample() {
      var ROOT_PATH =
        "https://cdn.jsdelivr.net/gh/apache/echarts-website@asf-site/examples";
      var myChart;
      if (this.echartsHandlers.lineRaceHandler == "") {
        console.log("第一次创建");
        var chartDom = document.getElementById("line-example");
        myChart = this.$echarts.init(chartDom);
        this.echartsHandlers.lineRaceHandler = myChart;
      } else myChart = this.echartsHandlers.lineRaceHandler;
      var option;
      console.log(dataOptions[this.dySelector.showedDataset]);
      //获取当前查看的数据集
      run(
        this.algoResults.lineRaceData[
          dataOptions[this.dySelector.showedDataset]
        ]
      );
      // axios
      //   // .get(ROOT_PATH + "/data/asset/data/life-expectancy-table.json")
      //   .get("/life-expectancy-table.json")
      //   .then(function (response) {
      //     let _rawData = response.data;
      //     console.log(_rawData);
      //     run(_rawData);
      //   })
      //   .catch(function (error) {
      //     console.log(error);
      //   });
      function run(_rawData) {
        // const countries = ["Smopdc", "Feedback", "PPO Controller"];
        const countries = _rawData.label;
        const datasetWithFilters = [];
        const seriesList = [];
        countries.forEach(function (country) {
          var datasetId = "dataset_" + country;
          datasetWithFilters.push({
            id: datasetId,
            fromDatasetId: "dataset_raw",
            transform: {
              type: "filter",
              config: {
                and: [
                  { dimension: "Time", gte: 1 },
                  { dimension: "Algorithm", "=": country },
                ],
              },
            },
          });
          seriesList.push({
            type: "line",
            datasetId: datasetId,
            showSymbol: false,
            name: country,
            endLabel: {
              show: true,
              formatter: function (params) {
                return params.value[1] + ": " + params.value[0];
              },
            },
            labelLayout: {
              moveOverlap: "shiftY",
            },
            emphasis: {
              focus: "series",
            },
            encode: {
              x: "Time",
              y: "Querycost",
              label: ["Algorithm", "QueryCost"],
              itemName: "Time",
              tooltip: ["QueryCost"],
            },
          });
        });
        option = {
          animationDuration: 10000,
          dataset: [
            {
              id: "dataset_raw",
              source: _rawData.data,
            },
            ...datasetWithFilters,
          ],
          // title: {
          //   text: "Query cost of compared dynamic algorithms since time 0",
          // },
          tooltip: {
            order: "valueDesc",
            trigger: "axis",
          },
          xAxis: {
            type: "category",
            nameLocation: "middle",
            name:"Time"
          },
          yAxis: {
            name: "Avg. Query Cost (Blocks)",
            type: "value",
            min: function (value) {
              return value.min - 20;
            },
          },
          grid: {
            right: 140,
          },
          series: seriesList,
        };
        myChart.setOption(option);
      }

      option && myChart.setOption(option);
    },
    plotBarRaceExample() {
      var ROOT_PATH =
        "https://cdn.jsdelivr.net/gh/apache/echarts-website@asf-site/examples";
      var myChart;
      if (this.echartsHandlers.barRaceHandler == "") {
        var chartDom = document.getElementById("bar-example");
        myChart = this.$echarts.init(chartDom);
        this.echartsHandlers.barRaceHandler = myChart;
      } else myChart = this.echartsHandlers.barRaceHandler;
      var option;
      const updateFrequency = 2000;
      const dimension = 0;
      const algorithmColors = {
        AutoStore: "rgb(84 112 198)",
        Smopdc: "rgb(145 204 117)",
        Feedback: "rgb(250 200 88)",
        "PPO-Controller": "rgb(238 102 102)",
      };

      axios.all([axios.get("/life-expectancy-table.json")]).then(
        axios.spread((res) => {
          // const data = res.data;
          const data =
            this.algoResults.barRaceData[
              dataOptions[this.dySelector.showedDataset]
            ].data;
          const times = [];
          for (let i = 0; i < data.length; ++i) {
            if (times.length === 0 || times[times.length - 1] !== data[i][2]) {
              times.push(data[i][2]);
            }
          }
          let startIndex = 0;
          let startTime = times[startIndex];
          option = {
            grid: {
              top: 10,
              bottom: 30,
              left: 150,
              right: 80,
            },
            xAxis: {
              max: "dataMax",
              name:"Avg. Repartition Cost (Blocks)",
              axisLabel: {
                formatter: function (n) {
                  return Math.round(n) + "";
                },
              },
            },
            dataset: {
              source: data.slice(1).filter(function (d) {
                return d[2] === startTime;
              }),
            },
            yAxis: {
              name:"Algorithm",
              type: "category",
              inverse: true,
              max: 5,
              axisLabel: {
                show: true,
                fontSize: 14,
                formatter: function (value) {
                  // return value + "{flag|" + getFlag(value) + "}";
                  return value;
                },
                rich: {
                  flag: {
                    fontSize: 25,
                    padding: 5,
                  },
                },
              },
              animationDuration: 5000,
              // animationDurationUpdate: 300,
            },
            series: [
              {
                realtimeSort: true,
                seriesLayoutBy: "column",
                type: "bar",
                itemStyle: {
                  color: function (param) {
                    return algorithmColors[param.value[1]] || "#5470c6";
                  },
                },
                encode: {
                  x: dimension,
                  y: 3,
                },
                label: {
                  show: true,
                  precision: 1,
                  position: "right",
                  valueAnimation: true,
                  fontFamily: "monospace",
                },
              },
            ],
            // Disable init animation.
            animationDuration: 0,
            animationDurationUpdate: updateFrequency,
            animationEasing: "linear",
            animationEasingUpdate: "linear",
            graphic: {
              elements: [
                {
                  type: "text",
                  right: 160,
                  bottom: 60,
                  style: {
                    text: startTime,
                    font: "bolder 80px monospace",
                    fill: "rgba(100, 100, 100, 0.25)",
                  },
                  z: 100,
                },
              ],
            },
          };
          // console.log(option);
          myChart.setOption(option);
          for (let i = startIndex; i < times.length - 1; ++i) {
            (function (i) {
              setTimeout(function () {
                updateTime(times[i + 1]);
              }, (i - startIndex) * updateFrequency);
            })(i);
          }

          function updateTime(time) {
            let source = data.slice(1).filter(function (d) {
              return d[2] === time;
            });
            option.series[0].data = source;
            option.graphic.elements[0].style.text = time;
            myChart.setOption(option);
          }
        })
      );
      option && myChart.setOption(option);
    },
    plotLineExample: function () {
      var myChart;
      if (this.echartsHandlers.lineHandler == "") {
        var chartDom = document.getElementById("static-line-example");
        myChart = this.$echarts.init(chartDom);
        this.echartsHandlers.lineHandler = myChart;
      } else myChart = this.echartsHandlers.lineHandler;
      var option;
      var seriesData = [];
      for (var key in this.algoResults.lineData.data) {
        seriesData.push({
          name: key,
          data: this.algoResults.lineData.data[key],
          type: "line",
          smooth: true,
        });
      }
      option = {
        // title: {
        //   text: "Comparison of total query latency of workload datasets",
        // },
        xAxis: {
          name: "Workload",
          type: "category",
          data: this.algoResults.lineData.xlabel,
        },
        legend: {
          data: Object.keys(this.algoResults.lineData.data),
        },
        tooltip: {
          order: "valueDesc",
          trigger: "axis",
        },
        yAxis: {
          name: "Avg. Query Cost (Blocks)",
          type: "value",
          min: function (value) {
            return value.min - 20;
          },
        },
        series: seriesData,
        //     [
        //   {
        //     name: "Smopdc",
        //     data: [820, 932, 901, 934, 1290, 1330, 1320],
        //     type: "line",
        //     smooth: true,
        //   },
        //   {
        //     name: "Feedback",
        //     data: [80, 932, 901, 934, 1290, 1230, 1310],
        //     type: "line",
        //     smooth: true,
        //   },
        //   {
        //     name: "PPO Controller",
        //     data: [820, 912, 901, 934, 1290, 1130, 1300],
        //     type: "line",
        //     smooth: true,
        //   },
        // ],
      };

      option && myChart.setOption(option);
    },
    plotBarExample() {
      var myChart;
      if (this.echartsHandlers.barHandler == "") {
        var chartDom = document.getElementById("static-bar-example");
        myChart = this.$echarts.init(chartDom);
        this.echartsHandlers.barHandler = myChart;
      } else myChart = this.echartsHandlers.barHandler;
      var option;
      var seriesData = [];
      for (var key in this.algoResults.barData.data) {
        seriesData.push({
          name: key,
          data: this.algoResults.barData.data[key],
          type: "bar",
          smooth: true,
        });
      }
      option = {
        tooltip: {
          trigger: "axis",
          axisPointer: {
            type: "shadow",
          },
        },
        legend: {},
        grid: {
          left: "3%",
          right: "4%",
          bottom: "3%",
          containLabel: true,
        },
        xAxis: {
          type: "value",
          name: "Repartition Cost (Blocks)",
          boundaryGap: [0, 0.01],
        },
        yAxis: {
          type: "category",
          name: "Workload",
          data: this.algoResults.barData.ylabel,
        },
        series: seriesData,
      };
      option && myChart.setOption(option);
    },
    //查找某个属性的下标
    searchAttrIndex(attr, pars) {
      for (var i = 0; i < pars.length; i++)
        if (attr.toString() == pars[i]) return i;
      return -1;
    },
    async plotHeatMapExample() {
      var myChart;
      if (this.echartsHandlers.heatmapHandler == "") {
        var chartDom = document.getElementById("heatmap-example");
        myChart = this.$echarts.init(chartDom);
        this.echartsHandlers.heatmapHandler = myChart;
      } else myChart = this.echartsHandlers.heatmapHandler;
      let heatmapData =this.algoResults.heatmapData[
              dataOptions[this.dySelector.showedDatasetComplex]
              ][algoOptions[this.dySelector.showedAlgoComplex]];
      // let pars,data = [];
      // var par_time_list = Object.keys(heatmapData.pars);
      let par_time_dict=this.parData[dataOptions[this.dySelector.showedDatasetComplex]][algoOptions[this.dySelector.showedAlgoComplex]]
      let columnIndexes=this.parData[dataOptions[this.dySelector.showedDatasetComplex]]['tableProfile']['columns'].map((k,v)=>{return v})
      let columnNames=this.parData[dataOptions[this.dySelector.showedDatasetComplex]]['tableProfile']['columns']
      let rowNum=30
      var option;
      option = {
        // grid: {
        //   height: "60%",
        //   top: "5%",
        // },
        xAxis: {
          type: "category",
          data: [],
          name: "Attributes",
          "axisLabel": {
            interval: 0
          }
        },
        yAxis: {
          type: "category",
          name: "Block No",
          data:Object.keys(Array.apply(null, {length:rowNum})).map(item=>+item),
        },
        visualMap: {
          type:"continuous",
          min: -50,
          max: 50,
          // range: [-5, 10],
          calculable: true,
          orient: "horizontal",
          left: "center",
          // bottom: "5%",
          inRange: {
            color: ["#313695", "#4575b4", "#74add1", "#abd9e9", "#e0f3f8", "#ffffbf", "#fee090", "#fdae61", "#f46d43", "#d73027", "#a50026"]
          }
        },
        animationDuration:function (idx){
          return idx*100
        },
        animationDurationUpdate: function (idx){
          return idx*100
        },
        animationEasingUpdate:"cubicInOut",
        series: [
          {
            name: "Partition Block",
            type: "heatmap",
            data: [],
            // label: {
            //   show: true,
            // },
            emphasis: {
              itemStyle: {
                shadowBlur: 10,
                shadowColor: "rgba(0, 0, 0, 0.5)",
              },
            },

          },
        ],
        graphic: {
          elements: [
            {
              type: "text",
              right: 120,
              bottom: 0,
              style: {
                text: '0',
                font: "bolder 80px monospace",
                fill: "rgba(100, 100, 100, 0.25)",
              },
              z: 100,
            },
          ],
        }
      };
      option && myChart.setOption(option);
      function aa(text,size){
        return {graphic: {
          elements: [
            {
              type: "text",
              right: 130,
              bottom: 0,
              style: {
                text: text,
                font: "bolder "+size+"px monospace",
                fill: "rgba(100, 100, 100, 0.25)",
              },
              z: 100,
            },
          ],
        }}
      }
      //initialize  axis
      for(let col of columnNames){
        this.heatAxisMap[col]={}
        this.heatAxisFreqMap[col]={}
        for(let row=0;row<rowNum;row++){
          this.heatAxisMap[col][row]=0
          this.heatAxisFreqMap[col][row]=0
        }
      }
      myChart.setOption({xAxis: { data: columnNames }})
      let par_time_list=Object.keys(par_time_dict)
      for (var i = 0; i < par_time_list.length; i++) {
        var time0 = Number(par_time_list[i]);

        var time1;
        if (i == par_time_list.length - 1) time1 = 1000;
        else time1 = Number(par_time_list[i + 1]);
        let data = [];
        // for (var ith = 0; ith < pars.length; ith++)
        //   if(pars[ith]==""){
        //     for(var jth=0;jth<=9;jth++)
        //       data.push([ith,jth,-5])
        //   }
        if(i>0)
          //改变新分区时，进行重分区提示
          myChart.setOption(aa('Repartitioning',50))
        // myChart.setOption({xAxis: { data: pars },series: [{ data: data }]})
        await this.sleep(2000)
        myChart.setOption(aa(time0.toString(),80))
        let pars = par_time_dict[par_time_list[i]]
        this.heatCurPar=""
        for(var pid=0;pid<pars.length;pid++){
          for(let col of pars[pid]){
            let colName=columnNames[col]
            this.heatCurPar+=colName+" "
          }
          if(pid>0 || pid<pars.length-1) this.heatCurPar+=" || "
        }
        for (var time2_str in heatmapData.data) {
          var time2=Number(time2_str)
          if (time2 < time1 && time2 >= time0) {
            heatmapData.data[time2_str].forEach((q) => {
              for(var aid=0;aid<q["cols"].length;aid++){
                let colName=columnNames[q["cols"][aid]]
                this.heatAxisMap[colName][q["rows"][aid]]+=q["wg"]
                this.heatAxisMap[colName][q["rows"][aid]]+=1
                // var x = this.searchAttrIndex(attr, pars);
                // var y = q["rows"];
                // var z = q["wg"];
                // var flag=true
                // data.forEach((point,idx,arr) => {
                //   if (point[0] == x && point[1] == y) {arr[idx][2] += z;flag=false;return true}
                // });
                // if(flag) data.push([x,y,z])
              }
            });
            if(myChart.isDisposed()) return
            //load data
            data=[]
            for(let col in this.heatAxisMap)
              for(let row in this.heatAxisMap[col]){
                //self-declining
                this.heatAxisMap[col][row]-=0.5
                if(this.heatAxisMap[col][row]>50)
                  this.heatAxisMap[col][row]=50
                data.push([col,Number(row),this.heatAxisMap[col][row]])
              }
            myChart.setOption({series: [{ data: data }] });
            myChart.setOption(aa(time2.toString(),80))
            await this.sleep(1000)
          }else if(time2>=time1) break
        }
      }

    },
    plotHeatMapExample2() {
      if (this.echartsHandlers.heatmapHandler == "") {
        var chartDom = document.getElementById("heatmap-example");
        this.heatmapParas.myChart = this.$echarts.init(chartDom);
        this.echartsHandlers.heatmapHandler = this.heatmapParas.myChart;
      } else this.heatmapParas.myChart = this.echartsHandlers.heatmapHandler;
      // let pars,data = [];
      // var par_time_list = Object.keys(heatmapData.pars);
      let par_time_dict=this.parData[dataOptions[this.dySelector.showedDatasetComplex]][algoOptions[this.dySelector.showedAlgoComplex]]
      let columnNames=this.parData[dataOptions[this.dySelector.showedDatasetComplex]]['tableProfile']['columns']
      let rowNum=30
      let option
      option = {
        xAxis: {
          type: "category",
          data: [],
          name: "Attributes",
          "axisLabel": {
            interval: 0
          }
        },
        yAxis: {
          type: "category",
          name: "Block No",
          data:Object.keys(Array.apply(null, {length:rowNum})).map(item=>+item),
        },
        visualMap: {
          type:"continuous",
          min: -50,
          max: 50,
          // range: [-5, 10],
          calculable: true,
          orient: "horizontal",
          left: "center",
          // bottom: "5%",
          inRange: {
            color: ["#313695", "#4575b4", "#74add1", "#abd9e9", "#e0f3f8", "#ffffbf", "#fee090", "#fdae61", "#f46d43", "#d73027", "#a50026"]
          }
        },
        animationDuration:function (idx){
          return idx*100
        },
        animationDurationUpdate: function (idx){
          return idx*100
        },
        animationEasingUpdate:"cubicInOut",
        series: [
          {
            name: "Partition Block",
            type: "heatmap",
            data: [],
            // label: {
            //   show: true,
            // },
            emphasis: {
              itemStyle: {
                shadowBlur: 10,
                shadowColor: "rgba(0, 0, 0, 0.5)",
              },
            },

          },
        ],
        graphic: {
          elements: [
            {
              type: "text",
              right: 120,
              bottom: 0,
              style: {
                text: '0',
                font: "bolder 80px monospace",
                fill: "rgba(100, 100, 100, 0.25)",
              },
              z: 100,
            },
          ],
        }
      };
      option && this.heatmapParas.myChart.setOption(option);
      //initialize relation chart


      //initialize  axis
      this.heatAxisMap={}
      this.heatAxisFreqMap={}
      this.heatmapParas.reparCnt=0
      this.heatmapParas.runningTime=-1
      for(let col of columnNames){
        this.heatAxisMap[col]={}
        this.heatAxisFreqMap[col]={}
        for(let row=0;row<rowNum;row++){
          this.heatAxisMap[col][row]=0
          this.heatAxisFreqMap[col][row]=0
        }
      }
      this.heatmapParas.myChart.setOption({xAxis: { data: columnNames }})
      this.heatmapParas.par_time_list=Object.keys(par_time_dict)
      this.heatmapParas.par_time_dict=par_time_dict
      this.initializeRelationChart(columnNames)
      this.controlHeatmap(1)
    },
    initializeRelationChart(columnNames){
      // if (this.echartsHandlers.heatmapHandler == "") {
      var chartDom = document.getElementById("relation-example");
      this.heatmapParas.relationChart = this.$echarts.init(chartDom);
      this.echartsHandlers.heatmapHandler=this.heatmapParas.relationChart
      // }else this.heatmapParas.relationChart=this.echartsHandlers.heatmapHandler
      let option = {
        tooltip: {},
        xAxis: {
          type: 'category',
          name: "Attribute No",
          boundaryGap: false,
          data: columnNames.map((item,i)=>{return i})
        },
        yAxis: {
          type: 'value',
          name: "ith partition"
        },
        series: [
          {
            type: 'graph',
            layout: 'none',
            coordinateSystem: 'cartesian2d',
            symbolSize: 25,
            label: {
              show: true
            },
            edgeSymbol: ['circle', 'arrow'],
            edgeSymbolSize: [4, 10],
            data: [],
            links: [],
            itemStyle:{
              color:'#05c091'  //#73c0de, #fc8452
            },
            lineStyle: {
              color: '#2f4554'
            }
          }
        ]
      };
      this.heatmapParas.relationChart.setOption(option);
    },
    changeRelationChart(pars,colNames){
      let i=0,data=new Array(colNames.length).fill(0),links=[]
      pars.sort((x,y)=>{return x[0]-y[0]})
      // pars.sort((x,y)=>{return Math.max.apply(null,x)-Math.max.apply(null,y)})
      for(let par of pars){
        i++
        for(let colIdx of par) data[colIdx]=i
        if(par.length>1){
          for(let j=0;j<par.length-1;j++){
            links.push({
              source: par[j],
              target: par[j+1]
            })
          }
        }
      }
      this.heatmapParas.relationChart.setOption({series:[{data:data,links:links}]})
    },
    sleep(time){
      return new Promise((resolve) =>setTimeout(resolve,time) )
    },
    sleep2(time){
      var timeOut = new Date().getTime() + parseInt(time, 10);
      while(new Date().getTime() <= timeOut) {}
    },
    controlHeatmap(command){
      function aa(text,size){
        return {graphic: {
            elements: [
              {
                type: "text",
                right: 130,
                bottom: 0,
                style: {
                  text: text,
                  font: "bolder "+size+"px monospace",
                  fill: "rgba(100, 100, 100, 0.25)",
                },
                z: 100,
              },
            ],
          }}
      }
      if(command===1){
        this.heatmapParas.status='run'
        this.heatmapParas.timer=setInterval( ()=>{
          if(this.heatmapParas.reparCnt>=this.heatmapParas.par_time_list.length){
            clearInterval(this.heatmapParas.timer)
            return
          }
          let i=this.heatmapParas.reparCnt
          var time0 = Number(this.heatmapParas.par_time_list[i]);
          if(time0>this.timeRange[1]) clearInterval(this.heatmapParas.timer)
          if(this.heatmapParas.runningTime<0) this.heatmapParas.runningTime=time0
          console.log('time0:',time0)
          if(time0>this.heatmapParas.runningTime) return
          console.log('reparCnt:'+this.heatmapParas.reparCnt)
          let heatmapData =this.algoResults.heatmapData[
              dataOptions[this.dySelector.showedDatasetComplex]
              ][algoOptions[this.dySelector.showedAlgoComplex]];
          let columnNames=this.parData[dataOptions[this.dySelector.showedDatasetComplex]]['tableProfile']['columns']

          var time1;
          if (i == this.heatmapParas.par_time_list.length - 1) time1 = 1000;
          else time1 = Number(this.heatmapParas.par_time_list[i + 1]);
          let data = [];
          // for (var ith = 0; ith < pars.length; ith++)
          //   if(pars[ith]==""){
          //     for(var jth=0;jth<=9;jth++)
          //       data.push([ith,jth,-5])
          //   }
          if(i>0){
            //改变新分区时，进行重分区提示
            this.heatmapParas.myChart.setOption(aa('Repartitioning',50))
            // this.sleep2(2000)
          }
          // this.heatmapParas.myChart.setOption(aa(time0.toString(),80))
          let pars = this.heatmapParas.par_time_dict[this.heatmapParas.par_time_list[i]]
          // this.heatCurPar=""
          // for(var pid=0;pid<pars.length;pid++){
          //   for(let col of pars[pid]){
          //     let colName=columnNames[col]
          //     this.heatCurPar+=colName+" "
          //   }
          //   if(pid>0 || pid<pars.length-1) this.heatCurPar+=" || "
          // }
          this.changeRelationChart(pars,columnNames)
          for (var time2_str in heatmapData.data) {
            var time2=Number(time2_str)
            if (time2 < time1 && time2 >= this.heatmapParas.runningTime) {
              heatmapData.data[time2_str].forEach((q) => {
                for(var aid=0;aid<q["cols"].length;aid++){
                  let colName=columnNames[q["cols"][aid]]
                  this.heatAxisMap[colName][q["rows"][aid]]+=q["wg"]
                  this.heatAxisMap[colName][q["rows"][aid]]+=1
                }
              });
              if(this.heatmapParas.myChart.isDisposed()) return
              //load data
              data=[]
              for(let col in this.heatAxisMap)
                for(let row in this.heatAxisMap[col]){
                  //self-declining
                  this.heatAxisMap[col][row]-=0.5
                  if(this.heatAxisMap[col][row]>50)
                    this.heatAxisMap[col][row]=50
                  data.push([col,Number(row),this.heatAxisMap[col][row]])
                }
              this.heatmapParas.myChart.setOption({series: [{ data: data }] });
              this.heatmapParas.myChart.setOption(aa(time2.toString(),80))
              this.heatmapParas.runningTime=time2
              console.log('runningTime:'+this.heatmapParas.runningTime)
              // console.log('wait 5s!!!!')
              // this.sleep2(5000)

            }else if(time2>=time1){
              this.heatmapParas.runningTime=time2
              break
            }
          }
          ++this.heatmapParas.reparCnt
        },1500)
      }else{
        this.heatmapParas.status='pause'
        clearInterval(this.heatmapParas.timer)
      }
    },
    changeRacePlot() {
      //在dynamic 标签页使用不同的数据集时。重新渲染图形
      this.echartsHandlers.lineRaceHandler.clear()
      // this.echartsHandlers.barRaceHandler.clear()
      this.echartsHandlers.barRaceHandler.dispose()
      this.echartsHandlers.barRaceHandler=""
      this.plotLineRaceExample();
      this.plotBarRaceExample();
    },
    changeHeatmapPlot() {
      this.echartsHandlers.heatmapHandler.dispose()
      this.echartsHandlers.heatmapHandler=""
      if(this.echartsHandlers.relationHandler!=""){
        this.echartsHandlers.relationHandler.dispose()
        this.echartsHandlers.relationHandler=""
      }
      // default configuration
      this.clearHeatmapConf()
      this.plotHeatMapExample2();

    },
    clearHeatmapConf(){
      clearInterval(this.heatmapParas.timer)
      this.heatmapParas={
        timer:"",
        myChart:'',
        par_time_list:[],
        reparCnt:0,
        runningTime:-1,
        par_time_dict:{},
        relationChart:'',
        status:'pause'
      }
    },
    clearAllPlots(){
      //清空所有的echart配置项option
      for (var handler in this.echartsHandlers)
        if (this.echartsHandlers[handler] != "") {
          // console.log(this.echartsHandlers[handler])
          this.echartsHandlers[handler].dispose();
          this.echartsHandlers[handler] = "";
        }
      this.clearHeatmapConf()
    },
    tabRowStyle({row,rowIndex}){
      let styleDict={background:"red"}
      if(rowIndex==0) return styleDict
    }
  },
};
</script>

<style scoped>
#dy-page {
  background-color: #f3f5f2;
  min-height: 1000px;
  padding: 30px 20px;
}
#expSet{
  margin-top: 60px;
  /*padding-bottom: 100px;*/
}
/deep/.ant-card-head{
  font-size: 18px;
}
/deep/.el-drawer__header  {
  font-size: 20px;
  font-weight: bold;
  background: white;
  /*padding-bottom: 20px;*/

  /*border-bottom: 2px solid #918f8f;*/
  /*color: #67C23A;*/
}

.plot-option{
  width: 33%;
  min-height: 90px;
  text-align: center;

}
/*.plot-option .el-checkbox{*/
/*  font-size: 25px;*/
/*}*/

/deep/ .el-checkbox__label {
  font-size: 16.2px;
  /*color: #fff !important;*/
}
.ant-tabs-tab span{
  font-size: 17px;
}
.change-box span{
  font-size: 15px;
  margin-right: 5px;
}
.chart-panel h3{
  text-align: center;
  /*margin-bottom: 10px;*/
}
</style>
