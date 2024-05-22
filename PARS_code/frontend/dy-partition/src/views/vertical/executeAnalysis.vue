<template>
  <a-spin :spinning="spinning">
  <div style="background-color: #f3f5f2;height: 100%">
    <!--实验执行页面（新）-->
    <el-row type="flex" justify="center">
      <el-col :span="10" :offset="1" id="expSet">
            <a-card style="margin-top: 18px;height: 550px;" title="DBConfig">
              <el-row type="flex" justify="center">
                <el-col :span="12">
                  <a-card-grid style="width: 100%" :hoverable="false">
                    <h4>
                      <i class="el-icon-timer" style="font-size: 17px">
                        Configuration 1: Partitioned Tables
                      </i>
                    </h4>
                    <el-checkbox-group
                        v-model="configuration.tables"
                    >
                      <el-checkbox
                          v-for="(tablename, idx) in tabledata"
                          :label="idx"
                          checked
                          :key="tablename+idx"
                          size="medium"
                      ><span style="font-size: 17.2px">{{ tablename }}</span>
                      </el-checkbox>
                    </el-checkbox-group>

                  </a-card-grid>

                </el-col>
                <el-col :span="12">
                  <a-card-grid style="width: 100%; text-align: left">
                    <h4>
                      <i class="el-icon-pie-chart" style="font-size: 17px">
                        Configuration 2: Collected Workloads
                      </i>
                    </h4>
                    <el-select
                        v-model="configuration.workloads"
                        multiple
                        style="margin-bottom: 20px; width: 100%;"
                        size="large"
                        placeholder="Please select"
                    >
                      <el-option
                          v-for="(file, idx) in workloads"
                          :key="file"
                          :label="file"
                          checked
                          :value="idx"
                      >
                      </el-option>
                    </el-select>
                    <!--                  <el-button type="primary" size="small" plain @click="openDrawer" style="font-size: 13px">View</el-button>-->
                  </a-card-grid>

                </el-col>


              </el-row>

              <el-row type="flex" justify="center">
                <a-card-grid style="width: 100%; text-align: left">
                  <h4>
                    <i class="el-icon-video-camera" style="font-size: 17px"
                    >Configuration 3: Optimization Objectives</i>
                  </h4>
                  <div style="margin-top: 18px">
                    <el-radio
                        v-for="(objective, idx) in optObjectives"
                        style="margin-right: -2px"
                        :label="idx"
                        border
                        v-model="configuration.objectives[idx]"
                        size="medium"
                    ><span class="el-times" style="font-size: 15px;font-style: italic">{{objWeights[idx]}}&times;</span>{{objective}}
                    </el-radio>
                  </div>
                </a-card-grid>
              </el-row>

              <el-row type="flex" justify="center">
                <el-col :span="24">
                  <a-card-grid style="width: 100%; text-align: left">
                    <h4>
                      <i class="el-icon-video-camera" style="font-size: 17px"
                      >Configuration 4: Baseline Methods and Evaluation Models</i
                      >
                    </h4>
                  </a-card-grid>
                </el-col>
              </el-row>

              <!--          <div class="block">-->
              <!--            <el-slider-->
              <!--                v-model="timeRange"-->
              <!--                range-->
              <!--                :marks="timeMarks"-->
              <!--                :max="500"-->
              <!--                :min="0"-->
              <!--            >-->
              <!--            </el-slider>-->
              <!--          </div>-->

              <a-card-grid class="plot-option">
                <el-checkbox-group v-model="configuration.baselines1">
                  <el-checkbox v-for="(method, idx) in methods" v-if="method.key==11||method.key==6" :key="idx" :label="idx" checked>{{method.label}}</el-checkbox>
                  <!--                -->
                  <!--              <el-checkbox :label="1">Histogram (M2)</el-checkbox>-->
                </el-checkbox-group>
              </a-card-grid>

              <a-card-grid class="plot-option">
                <el-checkbox-group v-model="configuration.baselines2">
<!--                  <el-checkbox :label="0" checked>ROW</el-checkbox>-->
                  <el-checkbox :label="1" checked>SCVP</el-checkbox>
                  <el-checkbox :label="2" checked>SCVP-RV</el-checkbox>
                </el-checkbox-group>
              </a-card-grid>

              <a-card-grid class="plot-option">
                <el-checkbox-group v-model="configuration.costmodels">
                  <el-checkbox :label="0" checked>HDD</el-checkbox>
                </el-checkbox-group>
              </a-card-grid>

              <!--            <a-card-grid style="width: 100%; text-align: left">-->
              <!--              <h4>-->
              <!--                <i class="el-icon-video-play" style="font-size: 16px"-->
              <!--                >Step 4:Start:</i>-->
              <!--              </h4>-->
              <!--              <el-row type="flex" justify="center" style="margin-bottom: 20px;margin-top: 40px">-->
              <!--                <el-button-->
              <!--                    type="primary"-->
              <!--                    size="small"-->
              <!--                    style="font-size: 13px"-->

              <!--                >Start/Pause-->
              <!--                </el-button>-->
              <!--                <el-button-->
              <!--                    type="warning"-->
              <!--                    size="small"-->
              <!--                    style="font-size: 13px"-->

              <!--                >Abort-->
              <!--                </el-button>-->

              <!--              </el-row>-->
              <!--            </a-card-grid>-->
            </a-card>
      </el-col>
      <el-col :span="11"  :offset="1">
        <!--实验执行页面（旧）-->
        <el-row type="flex" align="bottom" justify="center" class="algo-configuration">
          <el-col :span="24" >
            <a-card title="Environment Setup" style="height: 550px;font-size: 17px;margin-top: 18px">
              <a-card-grid style="width: 100%; margin:0 0; height: 150px; text-align: left">
                <h4>
                  <i class="el-icon-wind-power" style="font-size: 17px"
                  > Benchmarks: <el-tag size="medium"  type="primary" v-for="(item,idx) in benchmarks" :key="'bh'+idx">{{item}}</el-tag></i>
                </h4>
<!--                <h4>-->
<!--                  <i class="el-icon-wind-power" style="font-size: 17px"-->
<!--                  > Workload Count: <el-tag size="medium"  type="primary">{{workloads.length}}</el-tag></i>-->
<!--                </h4>-->
                <h4>
                  <i class="el-icon-document" style="font-size: 17px"
                  > Workload Files: </i> <el-tag size="medium" type="primary" v-for="(item,idx) in workloads" :key="'wd'+idx">{{item}}</el-tag>
                </h4>
              </a-card-grid>
              <a-card-grid style="width: 100%;margin: 70px 0;"  :hoverable="false">

                <h4>
                  <i class="el-icon-s-grid" style="font-size: 17px"
                  > Partitioning Algorithms: </i><el-tag  v-for="item in methods" :key="'method'+item.key" size="small" type="primary" >{{item.label}}</el-tag>
                </h4>
                <h4>
                  <i class="el-icon-s-data" style="font-size: 17px"
                  > Cost Model: </i><el-tag style="margin-left:2px" v-for="item in costModels" :key="'cost'+item.key" size="small" type="primary">{{item}}</el-tag>
                </h4>

              </a-card-grid>

              <a-card-grid style="width: 100%;margin-top: -3px" :hoverable="false">
                <div >
                  <el-button @click="getAnalysisResult" type="success" style="float: right; "  round>Start</el-button>
                  <el-button @click="changePanelInfo('tpch')" type="primary" style="float: right; margin-right: 5px" plain round>TPC-H</el-button>
                  <el-button @click="changePanelInfo('job')" type="primary" style="float: right; margin-right: 5px" plain round>JOB</el-button>
                  <el-button-group style="float: right; margin-right: 5px">
                    <el-button type="primary" @click="changePanelInfo('varyAttribute')" round plain>WDT 1<i class="el-icon-arrow-down" ></i></el-button>
                    <!--              <el-button type="primary" round plain>1<i class="el-icon-arrow-down" ></i></el-button>-->
                    <el-button type="primary" @click="changePanelInfo('varyWorkload')" round plain>WDT 2<i class="el-icon-arrow-down" ></i></el-button>
                  </el-button-group>
                </div>
              </a-card-grid>
            </a-card>

<!--            <el-card class="box-card select-panel">-->
<!--              <div slot="header" class="clearfix">-->
<!--                <span>User-defined Experiment Panel</span>-->
<!--                <el-button @click="getAnalysisResult" type="primary" style="float: right; "  round>Start</el-button>-->
<!--                <el-button @click="changePanelInfo('tpch')" type="primary" style="float: right; margin-right: 5px" plain round>TPC-H</el-button>-->
<!--                <el-button @click="changePanelInfo('job')" type="primary" style="float: right; margin-right: 5px" plain round>JOB</el-button>-->
<!--                <el-button-group style="float: right; margin-right: 5px">-->
<!--                  <el-button type="primary" @click="changePanelInfo('varyAttribute')" round plain>WideTable 1<i class="el-icon-arrow-down" ></i></el-button>-->
<!--                  &lt;!&ndash;              <el-button type="primary" round plain>1<i class="el-icon-arrow-down" ></i></el-button>&ndash;&gt;-->
<!--                  <el-button type="primary" @click="changePanelInfo('varyWorkload')" round plain>2<i class="el-icon-arrow-down" ></i></el-button>-->
<!--                </el-button-group>-->
<!--                -->
<!--              </div>-->
<!--              <div class="text-item">-->
<!--                <span><i class="el-icon-wind-power"></i>Workload count:</span>-->
<!--                <el-tag size="small" type="warning">{{workloads.length}}</el-tag>-->
<!--              </div>-->
<!--              <div class="text-item">-->
<!--                <span><i class="el-icon-document"></i>Workload files:</span>-->
<!--                <el-tag size="small" type="warning" v-for="(item,idx) in workloads" :key="'wd'+idx">{{item}}</el-tag>-->
<!--              </div>-->
<!--              <div class="text-item">-->
<!--                <span><i class="el-icon-s-grid"></i>Partitioning algorithms:</span>-->
<!--                <el-tag  v-for="item in methods" :key="'method'+item.key" size="small" type="warning" >{{item.label}}</el-tag>-->
<!--              </div>-->
<!--              <div class="text-item">-->
<!--                <span><i class="el-icon-s-data"></i>Cost model:</span>-->
<!--                <el-tag style="margin-left:2px" v-for="item in costModels" :key="'cost'+item.key" size="small" type="warning">{{item}}</el-tag>-->
<!--              </div>-->
<!--            </el-card>-->
          </el-col>
        </el-row>

      </el-col>
    </el-row>


    <el-row v-if="listVisable" class="result-page"  :key="1">
      <a-divider style="font-style: italic"> Configuration Analysis Overview</a-divider>
      <el-row type="flex" justify="center">
<!--            <el-col :span="8">-->
<!--              <div id="pieHist1" style="width: 100%;height: 450px"></div>-->
<!--            </el-col>-->
<!--        <span style="color: red">{{benchmarks[selectedBenchmark]}}</span>-->

        <el-col :span="8">
          <div id="pieHist1" style="width: 100%;height: 350px"></div>
        </el-col>
        <el-col :span="8">
          <div id="pieHist2" style="width: 100%;height: 350px"></div>
        </el-col>
        <el-col :span="8">
          <div id="pieHist3" style="width: 100%;height: 350px"></div>
        </el-col>
      </el-row>
    </el-row>

    <el-row v-if="listVisable" class="result-page" style="margin-top: -15px" :key="7">
      <el-col :span="24" :offset="0" >
        <!--        <h2>Performance analysis</h2>-->
        <a-divider style="font-style: italic;"><i class="el-icon-data-analysis"></i>Performance analysis</a-divider>
        <el-row type="flex" justify="center">
          <el-col :span="6" :offset="0">
            <h3 class="h3-span">(a) Overall Performance Analysis</h3>
<!--            of <span >{{benchmarks[selectedBenchmark]}}</span> Benchmark-->
            <a-table style="margin-top: 50px;margin-left: 4px" :columns="expColumns" :data-source="expData" :pagination="false">
              <a slot="model" slot-scope="text">{{ text }}</a>
              <span slot="customTitle"> <a-icon type="smile-o"/>Model</span>
              <span slot="costTitle" >
                Cost<br/>Estimate
              </span>
              <span slot="latencyTitle" >
                Query<br/>Latency
              </span>
              <span slot="timeTitle" >
                Time<br/>Overhead
              </span>
              <span slot="rank" slot-scope="rank">
                <a-tag
                    v-for="tag in rank"
                    :key="tag"
                    :color="tag === 'Good'||tag==='Average' ? 'geekblue': 'green'"
                >
                  {{ tag.toUpperCase() }}
                </a-tag>
              </span>
            </a-table>
          </el-col>
<!--          <el-col :span="8" :offset="0">-->
<!--            <div id="barHist"></div>-->
<!--          </el-col>-->
          <el-col :span="6" :offset="0">
            <div id="lineHist"></div>
          </el-col>
          <el-col :span="6" :offset="0">
            <div id="linePlot"></div>
          </el-col>
          <el-col :span="6" :offset="0">
            <div id="barHist"></div>
          </el-col>
        </el-row>

        <!--        <el-row  style="text-align: center;margin-bottom: 5px">-->
        <!--          &lt;!&ndash;          <el-col :span="15">&ndash;&gt;-->
        <!--          <span style="font-size: 16px;font-weight: bold">Compare  </span>-->
        <!--          <el-select v-model="curPerfOption" @change="changePlot">-->
        <!--            <el-option-->
        <!--                v-for="(item,index) in this.perfOptions"-->
        <!--                :key="'perf'+item.value"-->
        <!--                :label="item.label"-->
        <!--                :value="item.value">-->
        <!--            </el-option>-->
        <!--          </el-select>-->
        <!--          &lt;!&ndash;          </el-col>&ndash;&gt;-->
        <!--        </el-row>-->
        <!--        <el-row type="flex" justify="center">-->
        <!--          <el-col :span="23" >-->
        <!--            <div id="lineHist" v-if="this.curPerfOption==='lineHist'"></div>-->
        <!--            <div id="linePlot" v-else-if="this.curPerfOption==='linePlot'"></div>-->
        <!--            <div id="barHist" v-else></div>-->
        <!--          </el-col>-->
        <!--          &lt;!&ndash;          <el-col :span="23" v-else>&ndash;&gt;-->
        <!--          &lt;!&ndash;            <div id="barHist" ></div>&ndash;&gt;-->
        <!--          &lt;!&ndash;          </el-col>&ndash;&gt;-->
        <!--          &lt;!&ndash;          style="height: 700px;width: 700px"&ndash;&gt;-->
        <!--        </el-row>-->
      </el-col>
    </el-row>



    <!-- 展示页 -->
    <el-row  v-if="listVisable" class="result-page" style="margin-top: 18px" :key="2">

      <!-- partition decision panel -->
      <el-row type="flex" justify="center">
        <el-col :span="11" :offset="1">
          <a-divider style="font-style: italic">Partitioning Plan</a-divider>
          <el-row type="flex" justify="center">
            <el-col :span="8" :offset="0">
              <i class="el-icon-document" style="margin-right: 5px">Benchmark</i>
              <el-select @change="showBenchmark"  v-model="selectedBenchmark" style="width: 40%" placeholder="Please select">
                <el-option
                    v-for="(item,index) in this.benchmarks"
                    :key="'BCK'+index"
                    :label="item"
                    :value="index">
                </el-option>
              </el-select>

            </el-col>
            <!--        <el-col :span="6" :offset="0">-->
            <!--          <span><i class="el-icon-s-grid">Algorithm</i> </span>-->
            <!--          <el-select @change="showPartitions"  v-model="selectedMethod" placeholder="Please select algorithm">-->
            <!--            <el-option-->
            <!--                v-for="(item,index) in this.resData.methods"-->
            <!--                :key="'method'+index"-->
            <!--                :label="item"-->
            <!--                :value="index">-->
            <!--            </el-option>-->
            <!--          </el-select>-->
            <!--        </el-col>-->

          </el-row>
          <p style="text-align: center;font-size: 15px;margin-top: 5px">Note: You can view the desired partitioning plan by changing checkbox option.</p>

          <a-card-grid style="width: 100%;margin-top: 30px;" :hoverable="false">
            <div style="text-align: center">
              <a-steps :current="2" direction="horizontal" size="medium" style="margin-bottom: 40px">
                <a-step title="Waiting for Configuration" />
                <a-step title="In Analyzing Progress" />
                <a-step title="Finishing Partitioning Recommendation" />
              </a-steps>
              <h3 style="margin-bottom: 10px;font-size: 18.5px">The optimal partitioning plan is generated by <el-link style="scale: 130%;margin-bottom: 8px;margin-right: 3px;margin-left: 3px" type="danger">SCVP-RV</el-link> Partitioner</h3>
<!--              <h4>-->
<!--                <i class="el-icon-timer">-->
<!--                  Layout Basic Information:-->
<!--                </i>-->
<!--              </h4>-->
              <el-row type="flex" justify="center">
                <el-col :span="4" :offset="1">
                  <span style="position: absolute;left: 30px;top: 25px;color: #1890ff;font-weight: bold;font-style: italic;font-size: 18px">Scores→</span>
                  <a-progress type="circle" :percent="resData['score'][methodIndexs[0]]" :showInfo="false" :width="80" />
                  <p style="font-weight: bold">ROW</p>
                  <p style="margin-top: -82px;font-weight: bold;font-style: italic">{{resData['score'][methodIndexs[0]]}}</p>
                </el-col>
                <el-col :span="4" :offset="1">
                  <a-progress type="circle" :percent="resData['score'][methodIndexs[2]]" :showInfo="false" :width="80" />
                  <p style="font-weight: bold">AVP-RL</p>
                  <p style="margin-top: -82px;font-weight: bold;font-style: italic">{{num2Filter(resData['score'][methodIndexs[2]])}}</p>
                </el-col>
                <el-col :span="4" :offset="1">
                  <a-progress type="circle" :percent="resData['score'][methodIndexs[1]]" :showInfo="false" :width="80" />
                  <p style="font-weight: bold">SCVP</p>
                  <p style="margin-top: -82px;font-weight: bold;font-style: italic">{{num2Filter(resData['score'][methodIndexs[1]])}}</p>
                </el-col>
                <el-col :span="4" :offset="1">
                  <a-progress type="circle" :percent="resData['score'][methodIndexs[3]]" :showInfo="false" :width="80" />
                  <p style="font-weight: bold">SCVP-RV</p>
                  <p style="margin-top: -82px;font-weight: bold;font-style: italic">{{num2Filter(resData['score'][methodIndexs[3]])}}</p>
                </el-col>
<!--                *100/Math.max(...resData['score_list'])-->
              </el-row>
              <h3 style="margin-top: 60px;" class="recommend-title">
                <i class="el-icon-timer">

                </i>
                Recommend Reasons
              </h3>
            </div>

            <h5 class="recommend-text" style="padding: 10px;">
              Compared to <span class="highlight-text">ROW (random partitioning)</span>, <span class="highlight-text">AVP-RL (SOTA)</span> and <span class="highlight-text">SCVP (SOTA)</span>, the <span class="highlight-text">SCVP-RV</span> layout has reduced data scanning by at least <span class="highlight-text">{{numFilter(Math.min(...this.resData['pref']['cost'][1])*100)}}%</span>, up to <span class="highlight-text">{{numFilter(Math.max(...this.resData['pref']['cost'][1])*100)}}%</span>  of data scanning; it has reduced latency by at least <span class="highlight-text">{{numFilter(Math.min(...this.resData['pref']['latency'][1])*100)}}%</span>, and up to <span class="highlight-text">{{numFilter(Math.max(...this.resData['pref']['latency'][1])*100)}}%</span>  of query latency;
              Their algorithm time overheads respectively are AVP-RL(<span class="highlight-text">{{num2Filter(this.resData['pref']['time3'][0])}}s</span>), SCVP(<span class="highlight-text">{{num2Filter(this.resData['pref']['time3'][1])}}s</span>), SCVP-RV(<span class="highlight-text">{{num2Filter(this.resData['pref']['time3'][2])}}s</span>).
            </h5>
<!--            it is <span class="highlight-text">{{num2Filter(this.resData['pref']['time2'])}}</span> times faster than the SOTA, reducing the model execution time for each table layout by <span class="highlight-text">{{num2Filter(this.resData['pref']['time'][0][1])}}s</span>. But it is slightly slower than SCVP, increasing the model execution time for each table layout by <span class="highlight-text">0.01s</span>。-->
            <!--              <a-divider style="font-style: italic;"></a-divider>-->
              <el-row type="flex" justify="center" style="margin-top: 10px;margin-bottom: 5px">
                <el-col :span="10">
                  <i class="el-icon-document" style="margin-right: 5px;font-size: 16px;font-weight: bold;font-style: italic">Table</i>
                  <el-select @change="showPartitions('test')"  v-model="selectedWorkload" placeholder="Please select workload">
                    <el-option
                        v-for="(item,index) in this.resData.result"
                        :key="'workload'+index"
                        :label="item.workload.slice(0,-4)"
                        :value="index">
                    </el-option>
                  </el-select>
                </el-col>
                <el-col :span="2" :offset="0">
                  <el-button size="mini" type="primary" plain @click="exportJSON">Export</el-button>
                </el-col>
              </el-row>

              <el-row>
                <el-descriptions title="Layout Basic Information:" id="nodeDesp" style="padding: 10px">
                  <span slot="title" style="font-size: 17.5px;font-weight: 700;color: black">Layout Basic Information (<span class="cur-tab-span" style="font-size: 18px;font-style: italic">~{{tabledata[selectedWorkload]}}</span>):</span>
                  <el-descriptions-item label="Block Number"><span>{{this.resData['vp_plan'][tabledata[this.selectedWorkload]]['leaf_num']}}</span></el-descriptions-item>
                  <el-descriptions-item label="Tree Depth"><span>{{this.resData['vp_plan'][tabledata[this.selectedWorkload]]['max_tree_depth']}}</span></el-descriptions-item>
                  <el-descriptions-item label="Leaf Node Number"><span>{{this.resData['vp_plan'][tabledata[this.selectedWorkload]]['leaf_num']}}</span></el-descriptions-item>
                  <el-descriptions-item label="Page Number per Block"><span>{{this.resData['vp_plan'][tabledata[this.selectedWorkload]]['page_nums']}}</span> </el-descriptions-item>
<!--                  <el-descriptions-item label="Boundary"><span>{{selectedNodeProfile.boundary}}</span></el-descriptions-item>-->
                </el-descriptions>
              </el-row>

          </a-card-grid>
        </el-col>
        <el-col :span="11" :offset="1">
          <a-divider style="font-style: italic"><i class="el-icon-rank"></i><span class="cur-tab-span">{{tabledata[selectedWorkload]}}~</span> Partition Tree Construction Process</a-divider>
          <TreeComp :json="testTreeData" :isDetail="{default:true}" :class="{landscape: 1}"/>
        </el-col>

      </el-row>

      <el-row type="flex" justify="center">
        <el-col :span="14">
          <a-divider style="font-style: italic"><i class="el-icon-share"></i><span class="cur-tab-span">{{tabledata[selectedWorkload]}}~</span> Partition Tree Structure</a-divider>
          <el-row type="flex" justify="space-between" id="tree-option">
            <el-col :span="7">
              <span>Default Tree Expansion Depth</span><br/>
              <el-select @change="showTree" v-model="treeOption.extendDepth" placeholder="">
                <el-option
                    v-for="(item,index) in [2,3,4,5]"
                    :key="'extDep'+index"
                    :label="item"
                    :value="item">
                </el-option>
              </el-select>
            </el-col>
            <el-col :span="7">
              <span>Maximum Displayed Tree Depth</span><br/>
              <el-select @change="showTree"  v-model="treeOption.maxDepth" placeholder="">
                <el-option
                    v-for="(item,index) in [5,10,15,20,30,50]"
                    :key="'maxdep'+index"
                    :label="item"
                    :value="item">
                </el-option>
              </el-select>
            </el-col>
            <!--          <el-col :span="5">-->
            <!--            <span>Max node num (same level):</span><br/>-->
            <!--            <el-select @change="showTree"  v-model="treeOption.maxNodeNum" placeholder="">-->
            <!--              <el-option-->
            <!--                  v-for="(item,index) in [5,6,8,10]"-->
            <!--                  :key="'maxNode'+index"-->
            <!--                  :label="item"-->
            <!--                  :value="item">-->
            <!--              </el-option>-->
            <!--            </el-select>-->
            <!--          </el-col>-->
            <el-col :span="7">
              <span>Maximum Displayed Column Number</span><br/>
              <el-select @change="showTree"  v-model="treeOption.maxAttrNum" placeholder="">
                <el-option
                    v-for="(item,index) in [0,1,2,3,4,5]"
                    :key="'maxNode'+index"
                    :label="item"
                    :value="item">
                </el-option>
              </el-select>
            </el-col>
          </el-row>

          <el-row type="flex" justify="center">
            <el-col :span="23">
              <TreeChart :json="treeData"  @click-node="clickNode"/>
            </el-col>

            <!--          <el-tabs type="border-card" style="width: 90%;margin-top: 20px">-->
            <!--            <el-tab-pane label="Partition Tree">-->
            <!--              <el-row type="flex" justify="center">-->
            <!--                <TreeChart :json="treeData" @click-node="clickNode"/>-->
            <!--              </el-row>-->
            <!--              <el-row type="flex" justify="center" >-->
            <!--                <el-col>-->
            <!--                  <el-descriptions title="Node Profile" id="nodeDesp" >-->
            <!--                    <el-descriptions-item label="Node No"><span>{{selectedNodeProfile.no}}</span></el-descriptions-item>-->
            <!--                    <el-descriptions-item label="Depth"><span>{{selectedNodeProfile.depth}}</span></el-descriptions-item>-->
            <!--                    <el-descriptions-item label="Size"><span>{{selectedNodeProfile.node_size}}</span> </el-descriptions-item>-->
            <!--                    <el-descriptions-item label="Is Leaf Node"><span>{{selectedNodeProfile.is_leaf}}</span></el-descriptions-item>-->
            <!--                    <el-descriptions-item label="Boundary"><span>{{selectedNodeProfile.boundary}}</span></el-descriptions-item>-->
            <!--                  </el-descriptions>-->
            <!--                </el-col>-->
            <!--              </el-row>-->
            <!--            </el-tab-pane>-->
            <!--            <el-tab-pane label="Tree Construction Process">-->
            <!--              <el-row type="flex" justify="center">-->
            <!--                <TreeChart  :json="splitTreeData" @click-node="clickSplitNode"/>-->
            <!--              </el-row>-->
            <!--            </el-tab-pane>-->
            <!--          </el-tabs>-->
          </el-row>
        </el-col>
        <el-col :span="11" :offset="0">
          <a-divider style="font-style: italic"><i class="el-icon-c-scale-to-original"></i><span class="cur-tab-span">{{tabledata[selectedWorkload]}}~</span> Page Layout within a block</a-divider>
          <el-row type="flex" align="middle" >
            <el-col :span="22" :offset="1">
                <div id="parHist"></div>
            </el-col>

          </el-row>

          <el-row type="flex" align="center" >
            <el-col :span="22":push="1">
              <el-descriptions title="Node Profile" id="nodeDesp" >
                <el-descriptions-item label="Node No"><span>{{selectedNodeProfile.no+1}}</span></el-descriptions-item>
                <el-descriptions-item label="Depth"><span>{{selectedNodeProfile.depth}}</span></el-descriptions-item>
                <el-descriptions-item label="Size"><span>{{selectedNodeProfile.node_size}}</span> </el-descriptions-item>
                <el-descriptions-item label="Is Leaf Node"><span>{{selectedNodeProfile.is_leaf}}</span></el-descriptions-item>
                <el-descriptions-item label="Boundary"><span>{{selectedNodeProfile.boundary}}</span></el-descriptions-item>
              </el-descriptions>
            </el-col>

          </el-row>
        </el-col>
      </el-row>

    </el-row>


    <el-row v-if="listVisable" class="result-page block-page" style="margin-top: 20px" :key="4" >
      <a-divider style="font-style: italic"><i class="el-icon-menu"></i><span class="cur-tab-span">{{tabledata[selectedWorkload]}}~</span> Data Blocks</a-divider>
      <!--      :span="Math.ceil(18/curParLayoutDict.length)"-->
      <!--      <el-row type="flex" justify="center" >-->
      <!--        <el-col v-for="(partition,pid) in curParLayoutDict" class="block-panel" :offset="1" :key="pid" :span="partition.length*2" style="text-align: center">-->
      <!--          <template v-for="n in 2" v-if="curPageData[pid][n-1].length>0" >-->
      <!--            <Table border :columns="partition" height="200" :data="curPageData[pid][n-1]" ></Table>-->
      <!--            <Tag v-if="partition.length===1" color="geekblue" style="width: 100%;font-size: 16px;white-space: normal;height: 63px;overflow:hidden;">Column range: {{partition.map((v, k)=>{return curTableDict.columnNames.indexOf(v['key'])})}}, Rows range: ({{listBlock.pageSize*(listBlock.currentPage-1)+(n-1)*20}}:{{listBlock.pageSize*(listBlock.currentPage-1)+n*20-1}})</Tag>-->
      <!--            <Tag v-else color="geekblue" style="width: 100%;font-size: 16px;white-space: normal;height: 63px;overflow:hidden;">METADATA<br>Column range: {{partition.map((v, k)=>{return curTableDict.columnNames.indexOf(v['key'])})}}, Rows range: ({{listBlock.pageSize*(listBlock.currentPage-1)+(n-1)*20}}:{{listBlock.pageSize*(listBlock.currentPage-1)+n*20-1}})</Tag>-->
      <!--            <Tag color="cyan" style="margin: 0 0 20px 0;width: 100%;font-size: 16px;font-weight: bold">Block {{listBlock.pageSize*(listBlock.currentPage-1)+n+n*pid}}</Tag>-->
      <!--            &lt;!&ndash;          <Tag type="border" closable color="primary">Block {{n*(pid+1)}}</Tag>&ndash;&gt;-->
      <!--          </template>-->
      <!--        </el-col>-->
      <!--      </el-row>-->
      <el-row class="data-block" type="flex" justify="center" v-for="n in listBlock.pageSize" :key="'row_'+n">
          <el-col v-for="(partition,pid) in curParLayoutDict.slice(0,show_par_num)" class="block-panel" :offset="1" :key="'col_'+pid" :span="partition.length>12?24:partition.length*2" style="text-align: center">
            <template>
              <Table border :columns="partition" height="200" :data="curBlockDict[n-1].dataset[pid]" ></Table>
              <Tag v-if="partition.length===1" color="geekblue" style="width: 100%;font-size: 16px;white-space: normal;height: 63px;overflow:hidden;">Column range: {{partition.map((v, k)=>{return curTableDict.columnNames.indexOf(v['key'])})}}, Row numbers: ({{Math.min(...curBlockDict[n-1].row_ids)}}:{{Math.max(...curBlockDict[n-1].row_ids)}})</Tag>
              <Tag v-else color="geekblue" style="width: 100%;font-size: 16px;white-space: normal;height: 63px;overflow:hidden;">METADATA<br>Column range: {{partition.map((v, k)=>{return curTableDict.columnNames.indexOf(v['key'])})}}, Row numbers: ({{Math.min(...curBlockDict[n-1].row_ids)}}:{{Math.max(...curBlockDict[n-1].row_ids)}})</Tag>
              <Tag color="cyan" style="margin: 0 0 20px 0;width: 100%;font-size: 16px;font-weight: bold">Page {{listBlock.pageSize*(listBlock.currentPage-1)+n}}-{{pid+1}}</Tag>
            </template>
          </el-col>
          <h2 style="position: absolute;right:13px;bottom: -55px;font-style: italic">Block {{listBlock.pageSize*(listBlock.currentPage-1)+n}}</h2>
      </el-row>

      <el-row type="flex" justify="center" >
        <!-- <el-col :span="24" :offset="0"> -->
        <el-pagination
            background
            layout="prev, pager, next"
            @current-change="changeBlockData"
            :page-count="listBlock.pageCount"
            :current-page.sync="listBlock.currentPage">
        </el-pagination>
        <!-- </el-col> -->
      </el-row>
    </el-row>



    <!-- <p>{{tabledata}}</p>
    <p>{{workload}}</p>
    <p>{{methods}}</p>
    <p>{{costModels}}</p> -->
  </div>
  </a-spin>
</template>

<script>
import { mapGetters } from 'vuex'
import {executeAnalysisV2,fetchBlocks,fetchTreeStructure,fetchAllBlocks,fetchNodeInfo,fetchDeploymentCode} from '@/api/storageVp'
import FileSaver from 'file-saver'
import { Loading } from 'element-ui';
import TreeChart from "vue-tree-chart";
import  TreeComp  from './treechar.vue'

export default {
  components: {
    TreeChart,TreeComp
  },
  data() {
    return {
      configuration:{
        tables:[],
        workloads: [],
        objectives:[0,1,2],
        baselines1:[6,11],
        baselines2:[],
        costmodels:[]
      },
      selectedBenchmark:0,
      selectedWorkload:0,
      selectedMethod:1,
      methodIndexs:[],
      objWeights:[0.7,0.1,0.2,0.5],
      curPartitions:[],
      curParLayoutDict:[],
      // methodColors:['#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de', '#3ba272', '#fc8452', '#9a60b4', '#ea7ccc'],
      methodColors:['#5470c6', '#3ba272', '#fac858', '#ee6666', '#73c0de', '#3ba272', '#fc8452', '#9a60b4', '#ea7ccc'],
      ymaxDict:{"TPC-H":50,"JOB":50,"WDT":100},
      show_par_num:0,
      curTableDict:{},
      curBlockDict:[{dataset:[],row_ids:[]},{dataset:[],row_ids:[]}],
      curPageData:[],
      perfOptions:[{label:'Query Latency(s)',value:'linePlot'},{label:'Partition Generation Time',value:'barHist'},{label:'Estimated Block Cost',value:'lineHist'}],
      curPerfOption:'linePlot',
      options:{
        fillOpacity:0.6,
        padding:2,
        axisXLabel:"string"
      },
      barCategoryDict:{
        default:['ROW','COLUMN','OPTIMAL','SCVP'],
        java:['NAVATHE','O2P','TROJAN','AUTOPART','HYF','Rodriguez','SCVP'],
        highPerf:['HILLCLIMB','SCVP-RV'],
        lowPerf:['AVP-RL']
      },
      barCategoryDictFormatted:{},
      resData:{methods:['aa','bb'],result:[{workload:'customer.csv'}]},
      listVisable:false,
      // listVisable:true,
      listBlock:{
        tabName:'',
        // pageSize:40,
        pageSize:0,
        pageCount:0,
        rowNum:0,
        // currentPage:1
        currentPage:0
      },
      splitTreeData:  {
        name: 'root',
        image_url: "/public/tree.svg",
        class: ["rootNode"],
        extend:false,
        children: [
          {
            name: 'children1',
            image_url: "/public/leaf.svg"
          },
          {
            name: 'children2',
            image_url: "/public/leaf.svg",
            mate: [{
              name: 'mate',
              image_url: "/public/leaf.svg"
            }],
            children: [
              {
                name: 'grandchild',
                image_url: "/public/leaf.svg"
              },
              {
                name: 'grandchild2',
                image_url: "/public/leaf.svg"
              },
              {
                name: 'grandchild3',
                image_url: "/public/leaf.svg"
              }
            ],
            extend:false
          },
          {
            name: 'children3',
            image_url: "/public/leaf.svg"
          }
        ]
      },
      spinning:false,
      treeData:{},
      testTreeData:  {
        name: 'Split Condition: a=2',
        image_url: "#",
        class: ["rootNode"],
        children: [
          {
            name: 'Predicate split: a<2',
            image_url: "#"
          },
          {
            name: 'Predicate split: a>2',
            image_url: "#",
            mate: [{
              name: 'mate',
              image_url: "#"
            }],
            children: [
              {
                name: 'Predicate split: b>2',
                image_url: "#"
              },
              {
                name: 'Predicate split: c>2',
                image_url: "#"
              },
              {
                name: 'Predicate split: d>2',
                image_url: "#"
              }
            ]
          }
        ]
      },
      treeOption:{
        maxDepth:15,
        extendDepth:4,
        maxNodeNum:6,
        maxAttrNum:2
      },
      selectedNodeProfile:{
        no:'',
        boundary:'',
        depth:'',
        is_leaf:'',
        node_size:'',
      },
      expColumns:[
        {
          // title: 'Model',
          dataIndex: 'model',
          align:'center',
          key: 'model',
          slots: { title: 'customTitle' },
          scopedSlots: { customRender: 'model' }
        },
        {
          // title:'Cost Estimate',
          dataIndex: 'cost',
          key: 'cost',

          ellipsis: true,
          slots:{title:"costTitle"},
          sorter: (a, b) => a.cost - b.cost,
          sortDirections: ['descend', 'ascend']
        },
        {
          // title: 'Query Latency',
          dataIndex: 'latency',
          key: 'latency',
          ellipsis: true,
          slots:{title:"latencyTitle"},
          sorter: (a, b) => a.latency - b.latency,
          sortDirections: ['descend', 'ascend']
        },
        {
          // title: 'Time Overhead',
          dataIndex: 'time',
          key: 'time',

          ellipsis: true,
          slots:{title:"timeTitle"},
          sorter: (a, b) => a.time - b.time,
          sortDirections: ['descend', 'ascend']
        },{
          title: 'Rank',
          key: 'rank',
          align:'center',
          dataIndex: 'rank',
          scopedSlots: { customRender: 'rank' },
        }],

      expData:[
          {
        key: '1',
        model: 'ROW',
        cost: 32,
        latency: 1.2,
        time: 25,
        tags: ['nice', 'developer']
        },
        {
        key: '2',
        model: 'SCVP',
        cost: 31,
        latency: 1.2,
        time: 25,
        tags: ['loser'],
        },
        {
        key: '3',
        model: 'AVP-RL',
        cost: 30,
        latency: 1.2,
        time: 25,
        tags: ['nice'],
        },
        {
        key: '4',
        model: 'Improvement',
        cost: 29,
        latency: 1.2,
        time: 25,
        tags: ['nice'],
      }
      ]
    }
  },
  computed:{
    ...mapGetters([
      'tabledata',
      'tableScale',
      'benchmarks',
      'workloads',
      'methods',
      'costModels',
      'optObjectives'
    ])
  },
  created(){
    for(let key in this.barCategoryDict){
      for(let algo of this.barCategoryDict[key]){
        this.barCategoryDictFormatted[algo]=key
      }
    }
    this.configuration.workloads=Array.from({ length: this.workloads.length}, (_, index) => index)
    // this.changePanelInfo("tpch")
  },
  methods: {
    sleep(millisecond){
      var time =1;
      var timer = setInterval(function() {
        // 判断剩余秒数
        if (time == 0) {
          //清除定时器
          clearInterval(timer);
        } else {
          time--;
          console.log(1)
        }
      }, millisecond);
    },
    num2Filter(value) {
      let realVal = value.toFixed(2);
      return realVal;
    },
    numFilter(value) {
      let realVal = parseInt(value);
      return realVal;
    },
    getAnalysisResult(){
      // let loadingInstance = Loading.service({ fullscreen: true, text:'Running...' });
      this.methodIndexs=[]
      for(let method of this.methods){
        this.methodIndexs.push(method['key'])
      }
      this.methodIndexs=this.methodIndexs.sort((a,b)=>{return a-b})
      this.listVisable=true
      // this.sleep(3000)
      console.log(this.benchmarks[this.selectedBenchmark])
      executeAnalysisV2({workloads:this.workloads,benchmark:this.benchmarks[this.selectedBenchmark],methods:this.methodIndexs,costModels:this.costModels}).then(response=>{
        this.resData=response.data
        this.showPartitions()
        // this.changePlot(this.curPerfOption)
        this.showPlot()
        this.drawPieChart(1)
        this.drawPieChart(2)
        this.drawPieChart(3)
        console.log('close!')
        // loadingInstance.close()
      })
    },
    showBenchmark(){

    },

    changePanelInfo(key){
      this.$store.commit('SET_COSTMODELS',["HDD"])
      this.listVisable=false
      // this.configuration.workloads=[]
      // this.configuration={
      //   tables:[],
      //   workloads: [],
      //   objectives:[0,1,2],
      //   baselines1:[],
      //   baselines2:[],
      //   costmodels:[]
      // }
      if(key=="varyWorkload"){
        // this.$store.commit('SET_METHODS',[{"key":0,"label":"AUTOPART"},{"key":1,"label":"HILLCLIMB"},{"key":10,"label":"SCVP"},{"key":3,"label":"O2P"},{"key":7,"label":"COLUMN"},{"key":9,"label":"HYF"}])
        this.$store.commit('SET_METHODS',[{"key":10,"label":"SCVP"},{"key":6,"label":"ROW"},{"key":11,"label":"AVP-RL"},{"key":12,"label":"SCVP-RV"}])
        this.$store.commit("SET_WORKLOAD",["widetable500.csv","widetable1000.csv","widetable2000.csv"])
        this.$store.commit('SET_TABLE',[["widetable50","widetable50","widetable50"],[1000000,1000000,1000000]])
        this.$store.commit('SET_BENCHMARK',['WDT'])

      }else if(key=="varyAttribute"){
        // this.$store.commit('SET_METHODS',[{"key":10,"label":"SCVP"},{"key":11,"label":"AVP-RL"},{"key":6,"label":"Row"},{"key":7,"label":"Column"}])
        this.$store.commit('SET_METHODS',[{"key":10,"label":"SCVP"},{"key":6,"label":"ROW"},{"key":11,"label":"AVP-RL"},{"key":12,"label":"SCVP-RV"}])
        this.$store.commit("SET_WORKLOAD",["widetable30.csv","widetable50.csv","widetable100.csv"])
        this.$store.commit('SET_TABLE',[["widetable30","widetable50","widetable100"],[1000000,1000000,1000000]])
        this.$store.commit('SET_BENCHMARK',['WDT'])
        // this.$store.commit('SET_METHODS',[{"key":1,"label":"HILLCLIMB"},{"key":10,"label":"SCVP"},{"key":7,"label":"COLUMN"},{"key":8,"label":"Rodriguez"},{"key":9,"label":"HYF"}])
        // this.$store.commit("SET_WORKLOAD",["widetable30attr.csv","widetable50attr.csv","widetable80attr.csv","widetable100attr.csv","widetable150attr.csv"])
        // this.$store.commit('SET_TABLE',[["widetable","widetable","widetable","widetable","widetable"],[300,300,300,300,300]])
      }else if(key=="tpch"){
        this.$store.commit('SET_TABLE',[["customer","orders","part","supplier","lineitem"],[100,100000,100000,100000,1000000]])
        this.$store.commit("SET_WORKLOAD",["customer.csv","orders.csv","part.csv","supplier.csv","lineitem.csv"])
        // this.$store.commit('SET_METHODS',[{"key":10,"label":"SCVP"},{"key":11,"label":"AVP-RL"},{"key":0,"label":"AUTOPART"},{"key":3,"label":"O2P"},{"key":9,"label":"HYF"}])
        this.$store.commit('SET_METHODS',[{"key":10,"label":"SCVP"},{"key":6,"label":"ROW"},{"key":11,"label":"AVP-RL"},{"key":12,"label":"SCVP-RV"}])
        this.$store.commit('SET_BENCHMARK',['TPC-H'])
      }else if(key=="job"){
        this.$store.commit('SET_TABLE',[["title","cast_info","movie_info_idx","movie_keyword","movie_companies","movie_info"],[100000,100000,100000,100000]])
        this.$store.commit("SET_WORKLOAD",["title.csv","cast_info.csv","movie_info_idx.csv","movie_keyword.csv","movie_companies.csv","movie_info.csv"])
        // this.$store.commit('SET_METHODS',[{"key":11,"label":"AVP-RL"},{"key":10,"label":"SCVP"},{"key":0,"label":"AUTOPART"},{"key":3,"label":"O2P"},{"key":9,"label":"HYF"}])
        this.$store.commit('SET_METHODS',[{"key":10,"label":"SCVP"},{"key":6,"label":"ROW"},{"key":11,"label":"AVP-RL"},{"key":12,"label":"SCVP-RV"}])
        this.$store.commit('SET_BENCHMARK',['JOB'])
      }
      this.objWeights=[0.7,0.1,0.2]

    },
    changePlot(curOption){
      if(curOption==='lineHist'){
        console.log(curOption)
        this.drawStackLine()
      }else if(curOption==='linePlot'){
        console.log(curOption)
        this.drawLine()
      }else{
        console.log(curOption)
        this.drawStackBar()
      }
    },
    showPlot(){
      this.drawStackLine()
      this.drawLine()
      this.drawStackBar()
      this.drawTableChart()
    },
    showPartitions(key){
      // console.log("problem exist in showPartitions")
      if(key=='test'){
        this.spinning=true
        console.log('spining')
      }
      this.curPartitions=[]
      this.curParLayoutDict=[]
      let has_shown_par_length=0,flag=false
      this.resData.result[this.selectedWorkload].partitions[this.selectedMethod].map((arr,i)=>{
        let par_w=this.resData.result[this.selectedWorkload].parLengths[this.selectedMethod][i]
        let col_avg_w=par_w/arr.length
        let par={name:'partition'+i,value:par_w,children:[]}
        let parHeader=[]
        for(let col of arr){
          // let col_w=colInf['length'][colInf['name'].indexOf(col)]
          par.children.push({name:col,value:col_avg_w})
          parHeader.push({title:col,key:col})
          // par_w+=col_w
        }
        this.curPartitions.push(par)
        this.curParLayoutDict.push(parHeader)
        has_shown_par_length+=arr.length
        if((!flag) && has_shown_par_length>=8){
          this.show_par_num=i+1
          flag=true
        }
      })
      let totBlock=this.resData.result[this.selectedWorkload].blocks[this.selectedMethod]
      var chartDom = document.getElementById('parHist');
      this.$echarts.dispose(chartDom)
      var myChart = this.$echarts.init(chartDom);
      let option = {
        series: [{
            type: 'treemap',
            data: this.curPartitions,
            label:{
              normal:{
                show:true,
                textStyle:{
                  fontSize:18
                }
              }
            }
        }]
      };
      myChart.setOption(option)
      //(initialize) update page information
      this.listBlock.tabName=this.tabledata[this.selectedWorkload]
      this.listBlock.rowNum=this.tableScale[this.selectedWorkload]
      this.listBlock.pageSize=2
      // this.listBlock.pageCount=Math.ceil(this.listBlock.rowNum/this.listBlock.pageSize)
      this.listBlock.pageCount=Math.ceil(totBlock/this.listBlock.pageSize)
      //show block information
      this.changeBlockData()
      //show tree structure
      this.showTree()
      this.spinning=false
    },
    changeBlockData(){
      console.log("problem exist in changeBlockData")
      // fetchStudentList(this.listBlock).then(response=> {
      //   this.curTableDict={columnNames:response.data.columns,tableDataWholePage:response.data.student_info}
      //   this.curPageData=[]
      //   for(let i = 0; i < this.curParLayoutDict.length; i++){
      //     this.curPageData.push([])
      //     for(let j of [1,2]){
      //       this.curPageData[i].push(this.generateBlockData(this.curParLayoutDict[i],j))
      //     }
      //   }
      // })

      fetchBlocks({tabName:this.listBlock.tabName,
        benchmark:this.benchmarks[this.selectedBenchmark],
        method:this.methodIndexs[this.selectedMethod],
        pars:this.resData.result[this.selectedWorkload].partitions[this.selectedMethod],
        pageSize:this.listBlock.pageSize,
        currentPage:this.listBlock.currentPage,
        pageCount:this.listBlock.pageCount}).then(res=>{
        this.curBlockDict=res.data.blocks
        this.curTableDict={columnNames:res.data.columns}
      })
    },
    generateBlockData(partition,j){
      // console.log("problem exist in generateBlockData")
      let data=[],baseStart=0
      baseStart=(j-1)*this.listBlock.pageSize/2
      if(baseStart>=this.curTableDict['tableDataWholePage'].length) return data
      for(let i=0;i<this.listBlock.pageSize/2;i++){
        if(baseStart+i>=this.curTableDict['tableDataWholePage'].length) return data
        let row={}
        for(let col of partition){
          row[col['key']]=this.curTableDict['tableDataWholePage'][i+baseStart][this.curTableDict['columnNames'].indexOf(col['key'])]
        }
        data.push(row)
      }
      return data
    },
    exportJSON(){
      let jsonRes
      // V1
      // for(let i=0;i<this.resData.result.length;i++){
      //   let table=this.resData.result[i]
      //   let jsonItem={workload_file:table.workload,methods:{}}
      //   let m_i=0
      //   for(let methodPars of table.partitions){
      //     let jsonMethod={partitions:[]}
      //     for(let j=0;j<methodPars.length;j++){
      //       let par=methodPars[j]
      //       let jsonPar={profile:par,blocks:[]}
      //
      //       let block_no=1
      //       for(let z=0;z<this.tableScale[i];z+=20){
      //         let end_z=z+19
      //         if(end_z>=this.tableScale[i])
      //           end_z=this.tableScale[i]-1
      //         jsonPar.blocks.push({no:block_no,row_index:z+':'+end_z})
      //         block_no++
      //       }
      //       jsonMethod.partitions.push(jsonPar)
      //     }
      //     jsonItem.methods[this.resData.methods[m_i]]=jsonMethod
      //     m_i++
      //   }
      //   jsonRes[this.tabledata[i]]=jsonItem
      // }

      let workloadData=this.resData.result[this.selectedWorkload]
      let tabName=this.tabledata[this.selectedWorkload]
      jsonRes={
        table:tabName,
        workload_file:workloadData.workload,
        partitions:{profile:workloadData.partitions[this.selectedMethod], blocks:[]}
      }
      let method_name=this.resData.methods[this.selectedMethod]
      // V2
      fetchAllBlocks({tabName:tabName,benchmark:this.benchmarks[this.selectedBenchmark],method:this.methodIndexs[this.selectedMethod]}).then(res=>{
        jsonRes.partitions.blocks=res.data
        //  export file
        const blob=new Blob([JSON.stringify(jsonRes)],{type:"application/json"})
        FileSaver.saveAs(blob,tabName+'_data_routing_result.json')
      })
      let par_arr=workloadData.partitions[this.selectedMethod]
      let par_str=''
      for(let j=0;j<par_arr.length;j++){
        par_str+="["+(par_arr[j]+"")+"];"
      }
      fetchDeploymentCode({tabName:tabName,method:method_name,partitions:par_str}).then(res=>{
        let sqlData=res.data
        const blob=new Blob([sqlData],{type:"text/plain"})
        FileSaver.saveAs(blob,tabName+'_deployment_code.sql')
      })

    },
    drawPieChart(type){
      let titleDict={1:'Comparison of the Number of Queries Involved in Each Table',2:'Proportions of Visited and Unvisited Columns',3:'Proportions of Numeric Predicates vs. Textual Predicates'}
      let pieType={1:'tab_query_ratio',2:'access_unaccess_ratio',3:'numeric_text_ratio',}
      let pieData=this.resData['pie_statistic'][pieType[type]]
      // console.log(pieData)
      let data=[]
      if(type==1){
        for(var i=0;i<pieData.length;i++){
          data.push({ value: pieData[i], name: this.tabledata[i]})
        }
      }else if(type==2){
        data=[{ value: pieData[0], name: 'Visited Columns' },{ value: pieData[1], name: 'Unvisited Columns' }]
      }else{
        data=[{ value: pieData[0], name: 'Textual Predicates' },{ value: pieData[1], name: 'Numeric Predicates' }]
      }
      // let dataDict={
      //   1:[{ value: 1048, name: 'Customer' },{ value: 524, name: 'Orders' }],
      //   2:[{ value: 200, name: 'Unvisited Columns' },{ value: 300, name: 'Visited Columns' }],
      //   3:[{ value: 250, name: 'Numeric Predicates' },{ value: 250, name: 'Textual Predicates' }]
      // }
      let chartIdName='pieHist'+type
      this.$nextTick(function() {
        console.log("Pie DOM is now updated")
        var chartDom = document.getElementById(chartIdName);
        this.$echarts.dispose(chartDom)
        var myChart = this.$echarts.init(chartDom);
        // let methodNames=this.resData.result.keys
        let option = {
          title: {
            text: titleDict[type],
            textStyle:{
              fontSize:15
              // fontSize:18
            },
            left: 'center'
          },
          tooltip: {
            trigger: 'item'
          },
          // toolBox:{
          //   feature:{
          //     saveAsImage:{
          //       type:'svg'
          //     }
          //   }
          // },
          // toolbox: {
          //   feature: {
          //     dataZoom: {
          //       yAxisIndex: 'none'
          //     },
          //     restore: {},
          //     saveAsImage: {type:'svg'}
          //   }
          // },
          legend: {
            orient: 'vertical',
            top:'10%',
            // right: '10%',
            right: '3%',
            textStyle:{
              // fontSize:16,
              fontSize:14,
            }
          },

          series: [
            {
              name: 'Proportion',
              type: 'pie',
              radius: '50%',
              data: data,
              label:{
                // fontSize:17,
                fontSize:15,
                // fontWeight:'bold'
              },
              emphasis: {
                itemStyle: {
                  shadowBlur: 10,
                  shadowOffsetX: 0,
                  shadowColor: 'rgba(0, 0, 0, 0.5)'
                }
              }
            }
          ]
        };
        console.log("PIE Option type: ",type)
        console.log(option)
        myChart.setOption(option)
      })
    },
    drawTableChart(){
      let latency_list=this.resData.methods.map((method, i) => {
        let tot_latency=0
        for(let item of this.resData.result){
          tot_latency+=item.latency[i]
        }
        return this.num2Filter(tot_latency/this.resData.result.length)
      })
      let cost_list=this.resData.methods.map((method, i) => {
        let tot_cost=0
        for(let item of this.resData.result){
          tot_cost+=item.costs[i]
        }
        return this.num2Filter(tot_cost/this.resData.result.length)
      })
      let time_list=this.resData.methods.map((method, i) => {
        let tot_time=0
        for(let item of this.resData.result){
          tot_time+=item.overhead[i]
        }
        return this.num2Filter(tot_time/this.resData.result.length)
      })
      // let latency_sort_indexes=this.getSortedIndexes(...latency_list)
      // console.log('sort indexes',latency_sort_indexes)
      let tag_dict={0:'Average',1:'Good',2:'Good',3:'Outstanding'}
      this.expData=this.resData.methods.map((method, i) => {
        let item={
          key: i,
          model: method,
          cost: cost_list[i],
          latency: latency_list[i],
          time: time_list[i],
          rank: [tag_dict[i]]
        }
        return item
      })
    },
    getSortedIndexes(list) {
        // 创建一个辅助数组，存储原始下标
        let indexes = list.map((item, index) => index);
        // 对原始数组进行排序以保持下标数组的同步
        let sortedList = list.slice().sort((a, b) => a - b);
        // 使用map()函数将排序后的项目映射回它们的原始下标
        let sortedIndexes = sortedList.map(item => indexes.indexOf(item));
        return sortedIndexes;
    },
    drawStackLine(){
      this.$nextTick(function() {
        console.log("Line DOM is now updated")
        var chartDom = document.getElementById('lineHist');
        this.$echarts.dispose(chartDom)
        var myChart = this.$echarts.init(chartDom);
        let methodNames=this.resData.result.keys
        let option = {
          title: {
            text: '(b) Comparison of Scan Blocks per Table Layout',
            textStyle:{
              fontSize:15
              // fontSize:18
            },
            left: 'center'
          },
          tooltip: {
            trigger: 'axis',
            axisPointer: {
              type: 'cross',
              label: {
                backgroundColor: '#6a7985'
              }
            }
          },
          legend: {
            data:this.resData.methods,
            top:'14%',
            left:'20%',
            orient:'horizontal',
            textStyle:{
              // fontSize:16,
              fontSize:14,
            }
          },

          grid: {
            // left: '10%',
            right: '4%',
            // right: '15%',
            bottom: '3%',
            containLabel: true
          },
          xAxis: [
            {
              type: 'category',
              boundaryGap: false,
              data: this.resData.result.map((item,i)=>{
                return item['workload'].slice(0,-4)
              }),
              name:'workload',
              axisLabel:{
                fontSize:16,
                // color:'black',
                rotate: 20
              },
              nameTextStyle:{
                fontSize:14,

              },
            }
          ],
          yAxis: [
            {
              type: 'value',
              name:'Accumulated Scan Blocks',
              axisLabel:{
                fontSize:15,
                color:'black'
              },
              nameTextStyle:{
                fontSize:14,
                padding: [0, 0, 0, 30]
              }
            }
          ],
          series: this.resData.methods.map((method,i)=>{
            let series={
              name: method,
              type: 'line',
              stack: '总量',
              areaStyle: {},
              emphasis: {
                focus: 'series'
              },
              data: this.resData.result.map((item)=>{
                return item.costs[i]
              }),
              itemStyle:{
                color:this.methodColors[i]
              },
            }
            return series
          })
        };
        console.log('StackLine Option:')
        console.log(option)
        myChart.setOption(option)
      })
    },
    clearPlot(id){
      console.log(document.getElementById(id))
      this.$echarts.dispose(document.getElementById(id))
    },
    drawLine(){
      this.$nextTick(function() {
        var chartDom = document.getElementById('linePlot');
        this.$echarts.dispose(chartDom)
        var myChart = this.$echarts.init(chartDom);
        let option={
          title: {
            text: '(c) Comparison of Query Latency per Table Layout',
            textStyle:{
              fontSize:15
              // fontSize:18
            },
            left: 'center'
          },
          xAxis: {
            type: 'category',
            data: this.resData.result.map((item, i) => {
              return item['workload'].slice(0,-4)
            }),
            name: 'workload',
            axisLabel:{
              fontSize:16,
              // color:'black',
              rotate:20
            },
            nameTextStyle:{
              fontSize:14
            }
          },
          grid: {
            // left: '10%',
            right: '4%',
            bottom: '3%',
            containLabel: true
          },
          yAxis: {
            type: 'value',
            name:'Avg. Query Latency(s)',
            axisLabel:{
              fontSize:16,
              color:'black',

            },
            nameTextStyle:{
              fontSize:14,
              padding: [0, 0, 0, 30]
            }
          },
          tooltip: {
            trigger: 'axis',
            axisPointer: {
              type: 'cross',
              label: {
                backgroundColor: '#6a7985'
              }
            }
          },
          legend: {
            data:this.resData.methods,
            top:'14%',
            left:'20%',
            orient:'horizontal',
            textStyle:{
              // fontSize:16,
              fontSize:14,
            }
          },
          series:this.resData.methods.map((method, i) => {
            let series = {
              name: method,
              type: 'line',
              data:this.resData.result.map((item)=>{
                return item.latency[i]
              }),
              itemStyle:{
                color:this.methodColors[i]
              },
            }
            return series
          })
        };
        console.log('Line Option:')
        console.log(option)
        myChart.setOption(option)
      })
    },
    drawStackBar(){
      this.$nextTick(function() {
        console.log("Bar DOM is now updated")
        var chartDom = document.getElementById('barHist');
        // if (this.$echarts.getInstanceByDom(chartDom) == null || 1 == 1) {
        this.$echarts.dispose(chartDom)
        var myChart = this.$echarts.init(chartDom);
        let methodNames = this.resData.result.keys
        let option = {
          title: {
            text: '(d) Comparison of Time Overhead per Table Layout',
            textStyle:{
              fontSize:15
              // fontSize:18
            },
            left: 'center'
          },
          tooltip: {
            trigger: 'axis',
            axisPointer: {
              type: 'shadow'
            }
          },
          legend: {
            data:this.resData.methods,
            top:'14%',
            left:'20%',
            orient:'horizontal',
            textStyle:{
              // fontSize:16,
              fontSize:14,
            }
          },
          grid: {
            // left: '14%',
            right: '4%',
            bottom: '3%',
            containLabel: true
          },
          xAxis: [
            {
              type: 'category',
              data: this.resData.result.map((item, i) => {
                return item['workload'].slice(0,-4)
              }),
              name: 'workload',
              axisLabel:{
                fontSize:16,
                // color:'black',
                rotate:20
              },
              nameTextStyle:{
                fontSize:14
              }
            }
          ],
          yAxis: [
            {
              type: 'value',
              name: 'Avg. Model Time(s)',
              axisLabel:{
                fontSize:16,
                color:'black'
              },
              max:this.ymaxDict[this.benchmarks[this.selectedBenchmark]],
              nameTextStyle:{
                fontSize:14
              }
            }
          ],
          series: this.resData.methods.map((method, i) => {
            let series = {
              name: method,
              type: 'bar',
              stack: this.barCategoryDictFormatted[method],
              areaStyle: {},
              emphasis: {
                focus: 'series'
              },
              data: this.resData.result.map((item) => {
                return item.overhead[i]
              }),
              itemStyle:{
                color:this.methodColors[i]
              },
            }

            return series
          })
        };
        console.log('StackBar Option:')
        console.log(option)
        myChart.setOption(option)
        // }
      })
    },
    showTree(){
      fetchTreeStructure({method:this.methodIndexs[this.selectedMethod],benchmark:this.benchmarks[this.selectedBenchmark],workload:this.workloads[this.selectedWorkload],rootUrl:'/public/tree.svg',leafUrl:'/public/leaf.svg',
        maxDepth:this.treeOption.maxDepth,extendDepth:this.treeOption.extendDepth,maxNodeNum:this.treeOption.maxNodeNum,maxAttrNum:this.treeOption.maxAttrNum}).then((res)=>{
        this.testTreeData=res.data.split_scheme
        this.treeData=res.data.tree_structure
        let root={'nid':0}
        this.clickNode(root)
        // this.selectedNodeProfile={
        //   no:'',
        //   boundary:'',
        //   depth:'',
        //   is_leaf:'',
        //   node_size:'',
        // }
      })
    },
    clickNode(node){
      node.extend=!node.extend
      fetchNodeInfo({nid:node.nid,tabName:this.tabledata[this.selectedWorkload],benchmark:this.benchmarks[this.selectedBenchmark],method:this.methodIndexs[this.selectedMethod]}).then(res=>{
        this.selectedNodeProfile=res.data
      })
    },
    clickSplitNode(node){
      node.extend=!node.extend
    }
  },
}
</script>

<style lang="scss" scoped>
  //*{
  //  font-size: 20px;
  //}
  .plot-option{
    width: 33.33%;
    min-height: 90px;
    text-align: center;
  }
  .select-panel{
    margin-top: 40px;
    font-weight: bold;
    background-color: white;
  }
  .result-page{
    h2{
      font-size: 22px;
      i{
        margin-right: 15px;
      }
      color:black;
      text-align:center;
      margin: 50px auto;
    }

    background-color: white;
  }
  .algo-configuration{
    span{
      margin: auto 8px;
    }
    div.item{
      margin: 10px auto;
    }

    padding-bottom: 15px;
    // margin-left: 10px;
  }
  .text-item{
    margin-top: 10px;
  }

  #lineHist{
    width: 100%;
    height: 400px;
  }
  #linePlot{
    width: 100%;
    height: 400px;
  }
  #barHist{
    width: 100%;
    height: 400px;
  }
  #parHist{
    width: 100%;
    height: 550px;
  }
  .ant-divider{
    margin-bottom:50px;
  }

  .highlight-text{
    color: red;
  }
  #tree-option{
    text-align: center;
    span{
      margin-right: 5px;
      font-size: 15px;
      //font-weight: bold;
    }
  }
  .data-block{
    width: 95%;
    margin: 10px auto;
    background-color: #f8f8f9;
    padding: 10px 0px 20px 0px;
    //border: 1px solid #f8f8f9;
    border-radius: 10px 100px / 120px;
    border: dotted;
    border-color: #c7c7c9;
    border-width: 10px 4px;
  }
  #nodeDesp{
    background-color: #f5f7fa;
    span{
      font-size: 17.5px;
      color:#108ee9
    }
  }

  .recommend-text{
    font-size: 16.5px;
    font-style: italic;
    font-weight: normal;
    background-color: #f5f7fa;
    //color:#108ee9
  }
  .recommend-title{
    color: black;font-style: italic;
    font-size: 18px;
  }

  //.myTree{
  //  text-align: center;
  //}
  //.block-panel{
  //  span{
  //    margin-bottom: 50px;
  //  }
  //}
</style>

<style>
/*global style (temporary used)*/
.node .person .name{
  color: black;
  height: auto!important;
  font-size: 15px;
  //width: 30em!important;
}
.node .person .avat{
  border: none!important;
}
.el-tag{
  //scale: 120%;
  font-size: 17px;
}
.el-button{
  font-size: 17px;
}
.el-tabs__nav-scroll .el-tabs__item{
  font-size: 18px;
}

.el-descriptions__body{
  background-color: #f5f7fa;
}

#nodeDesp span.el-descriptions-item__label{
    font-size: 17px;
    font-style: italic;

}
.ant-divider-inner-text{
  font-size:20px;
}
.ant-divider{
  i{
    margin-right:8px;

  }
}


.h3-span{
  font-weight: bolder;text-align: center;color: #333;margin-top: -3px;
  span{
    color: red!important;
    font-style: italic;
  }
}
span.cur-tab-span{
  color: #1890ff;
}
</style>