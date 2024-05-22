<template>
  <div id="methodBody">
      <el-row type="flex" style="background-color: white;padding:15px 10px">
<!--        <el-radio size="medium" border  @change="onCostModelsChanged" v-model="selectedCostModels" v-for="model in costModels" :key="model.key" :label="model.key">{{model.label}}</el-radio>-->
      </el-row>
      <el-row type="flex" style="background-color: white;padding:15px 10px">
        <el-col class="selectedCard" :span="22" :offset="2">
          <div style="margin-top:10px;">
            <span><i class="el-icon-s-marketing"></i>Optimization Objectives:</span>
            <el-checkbox-group @change="onObjectiveChange" v-model="selectedObjList" size="medium" style="display:inline-block;margin-left: 10px">
              <el-checkbox key="1" label="Query Latency" checked></el-checkbox>
              <el-checkbox key="2" label="Scanning Cost"></el-checkbox>
              <el-checkbox key="3" label="Model Time Overhead" checked></el-checkbox>
              <el-checkbox key="4" label="System Throughout" ></el-checkbox>
              <el-checkbox key="5" label="Partition Manageability" ></el-checkbox>
            </el-checkbox-group>
          </div>
          <div style="margin-top:8px;">
<!--            <span><i class="el-icon-s-marketing"></i>Optimization Objectives:</span>-->

            <span style="margin-left: 170px">Weights:</span>
            <a-input-number :class="objMargins" v-for="obj_key in objMethods"  v-bind:disabled="selectedObjList.indexOf(obj_key) == -1" :min="0" :max="1" :default-value="0.5" :precision="2" :step="0.05" :key="obj_key"/>
          </div>
          <br/>
          <span><i class="el-icon-s-grid"></i>PARS's Partitioner:</span>
          <el-tag size="small">SCVP-RV<i class="el-icon-check"></i></el-tag>
          <br/>
          <h3 style="font-size: 18px;color: gray;margin-left: 330px;font-style: italic;font-weight: normal;">The Refined Version of Spectral Clustering-based Vertical Partitioning (SCVP-RV) is a multi-stages strategy for solving the vertical partitioning problem. Leveraging the cost independence property between partitions, it designs an estimation function for quickly calculating column group (CG)
            division gains, making it suitable for large tables and heavy loads. SCVP-RV first calls spectral clustering to create attribute clusters from attribute affinity matrixs as initial CGs, and then uses a greedy search strategy to top-down split the CGs into atomic CGs based on mined frequent patterns. Subsequently,
            it merges these atomic CGs bottom-up, also based on frequent patterns, to obtain the final column partitions. Next, SCVP-RV utilizes predicate and median conditions involving all numerical and non-numerical columns, rather than simple primary keys, as the expansion criteria for leaf nodes to greedily build a binary partition tree for allocating tuples.</h3>
          <br/>
          <div style="margin-top:-15px">
          <span><i class="el-icon-s-grid"></i>Baseline Models:</span>
            <el-tag v-for="(item,index) in accessedMethods" v-if="item.key!=12" :key="'method'+index" size="small"><template>{{item.label}}<i class="el-icon-check"></i></template></el-tag>
          </div>
          <div style="margin-top:20px">
            <span><i class="el-icon-s-data"></i>Cost Models:</span>
            <el-tag v-for="(item,index) in accessedCostModels" :key="'method'+index" size="small">{{item.label}}</el-tag> <el-link href="https://www.sciencedirect.com/science/article/pii/S002002551500643X?via%3Dihub" type="success" target="_blank" style="margin-left: -20px;margin-top: -20px">pdf</el-link>
            <br/>
            <h3 style="font-size: 18px;font-style: italic;font-weight: normal;color: gray;margin-left: 210px">The Hard Disk (HDD) cost model is employed to approximate the quantity of blocks that need to be accessed in order to fulfill a specific query within a database system that incorporates a particular vertical partitioning strategy.</h3>
          </div>
        </el-col>
      </el-row>

      <el-row type="flex" justify="space-between">
        <el-col :span="10" :offset="1">
          <el-row  class="select-panel">
              <h2>Select baselines to be a performance reference</h2>
              <div>
                <el-transfer style="zoom: 110%" v-model="selectedMethods" :data="methods" :titles="['Candidate Models','Selected Models']" @change="onMethodsChanged"></el-transfer>
              </div>
          </el-row>
<!--          <el-row  class="select-panel" >-->
<!--            <h3>Select cost model to be compared</h3>-->
<!--            <el-radio size="medium" border  @change="onCostModelsChanged" v-model="selectedCostModels" v-for="model in costModels" :key="model.key" :label="model.key">{{model.label}}</el-radio>-->
<!--          </el-row>-->
        </el-col>
        <el-col :span="10" :pull="1" :offset="1" id="introduce-panel">
<!--          <h2 id="intro_title">-->
<!--            <el-divider direction="vertical" width="5px"></el-divider>-->
<!--          </h2>-->
          <el-row type="flex" class="introduce-card" justify="center">
            <el-col :span="24">
              <h2>A brief introduction for selected baseline algorithms</h2>
<!-- <h3 style="text-align: center"><el-tag size="mini">Intro</el-tag>Selected algorithms</h3> -->
              <a-collapse class="page-content"  default-active-key="1" :bordered="false">
                <template #expandIcon="props">
                  <a-icon type="caret-right" :rotate="props.isActive ? 90 : 0" />
                </template>
                <a-collapse-panel :name="index+''" :key="index" v-for="(item,index) in accessedMethods"  :style="collapseCustomStyle">
                  <span slot="header" >
                    {{item.label}}<el-link v-if="item.link!='#'" :href="item.link" type="success" target="_blank">pdf</el-link>
<!--                     <span class="ant-collapse-header"></span>-->
                  </span>
                  <p style="font-style: italic;font-weight: normal;">{{ item.content }}</p>
                </a-collapse-panel>
              </a-collapse>
            </el-col>
<!--            <el-col :span="10" :offset="2" >-->
<!--              <h3 style="text-align: center"><el-tag size="mini">Intro</el-tag>Cost models</h3>-->
<!--              <a-collapse default-active-key="1" :bordered="false">-->
<!--                <template #expandIcon="props">-->
<!--                  <a-icon type="caret-right" :rotate="props.isActive ? 90 : 0" />-->
<!--                </template>-->
<!--                <a-collapse-panel :name="index+''" :key="index" v-for="(item,index) in accessedCostModels"  :style="collapseCustomStyle">-->
<!--                  <span slot="header">-->
<!--                    {{item.label}} <el-link :href="item.link" type="success" target="_blank">pdf</el-link>-->
<!--                  </span>-->
<!--                  <p>{{ item.content }}</p>-->
<!--                </a-collapse-panel>-->
<!--              </a-collapse>-->
<!--            </el-col>-->
          </el-row>
        </el-col>
      </el-row>
      <div style="background-color: white;border-radius:30px;">
        <div style="margin: 20px 0;padding: 20px 0;text-align: center;background-color: #f3f5f2">
          <div>
            <h2><i class="el-icon-top-right"></i> Upload Your Partitioner and Evaluator</h2><br>
          </div>
          <div class="add-button">
            <el-button @click="addBaseline('algo')"  type="primary"  v-bind:plain="addKey!='algo'">Create New Algorithm</el-button>
            <el-button @click="addBaseline('costmodel')" type="primary" v-bind:plain="addKey!='costmodel'" style="margin-left: 40px" >Create New Cost Model</el-button>
          </div>
        </div>

        <el-row justify="center" type="flex"  v-show="addPanelVisible">
          <el-col class="add-panel" :span="24" >
            <el-row type="flex" justify="center" >
              <el-col :span="11" :offset="1" >
                <h2 style="font-style: italic">——Algorithm Template Instruction——</h2>
                <template v-if="addKey=='algo'" >
                  <h3 class="title-description">
                    <span style="color: #F56C6C"><i class="el-icon-star-on"></i>Partitioner Method Name</span> ( Workload <span style="color: #909399">[parameter 1: List]</span>, Evaluator <span style="color: #909399">[parameter 2: Class]</span>, Fields <span style="color: #909399">[parameter 3: List]</span> ):
                  </h3>
                  <h3 style="font-weight: normal">(Note that: add your static partitioning algorithm to test its performance over given benchmarks.)</h3>
                  <h3 class="subtitle-description">
                    <i class="el-icon-star-on"></i>Introduction of Parameters:
                  </h3>
                  <ul type="circle" class="plain-description">
                    <li> <b>Workload:</b> a list of queries. Every query is a python class with accessed attribute indexes (type: list&lt;int&gt;), scan keys (type: list&lt;int&gt;), frequency (type: int), selectivity (type:float).<br>
                      Example: [ [[1,2,3],[1],10,0.01], [[0,1,2,3,4,5],[3,5],50,0.12]], ...]
                    </li>
                    <li><b>Evaluator:</b> a python class with the I/O cost estimate function <i>F</i> that receives two parameters: (a) workload (type: list&lt;query&gt;) (b) partition scheme (type: 2-dim list&lt;int&gt;). <br>
                      Example: <i>Evaluator.estimateIO</i>(workload ,[[0],[1,2,3],[4,5]])=> 102
                    </li>
                    <li><b>Fields:</b> A list of attribute lengths of partitioned table (type: list&lt;int&gt;) <br>
                      Example: [4,5,10,10,10,50]
                    </li>
                  </ul>
                  <h3 class="subtitle-description"><i class="el-icon-star-on"></i>Return:</h3>
                  <p class="plain-description">A complete vertical partition scheme (type: 2-dim list&lt;int&gt;). e.g., [[0],[1,2,3],[4,5]]</p>
                </template>
                <template v-else>
                  <h3 class="title-description">
                    <span style="color: #F56C6C"><i class="el-icon-star-on"></i>Evaluator Method Name</span> ( Workload <span style="color: #909399">[parameter 1: List]</span>, Partitions <span style="color: #909399">[parameter 2: 2-dim List]</span>, TabProfile <span style="color: #909399">[parameter 3: Dict]</span> ):
                  </h3>
                  <h3 style="font-weight: normal">(Note that: add your cost model to evaluate the quality of different partitioning algorithms over given benchmarks.)</h3>
                  <h3 class="subtitle-description">
                    <i class="el-icon-star-on"></i>Introduction of parameters:
                  </h3>
                  <ul type="circle" class="plain-description">
                    <li><b>Workload:</b> a list of queries. Every query is a python class with accessed attribute indexes (type: list&lt;int&gt;), scan keys (type: list&lt;int&gt;), frequency (type: int), selectivity (type:float).<br>
                      Example: [ [[1,2,3],[1],10,0.01], [[0,1,2,3,4,5],[3,5],50,0.12]], ...]
                    </li>
                    <li><b>Partitions:</b> a set of vertical partitions of the table. Every partition consists a column group. <br>
                      Example: the table R(a0,a1,a2,a3,a4,a5) is split into three partitions=> [[0],[1,2,3],[4,5]]
                    </li>
                    <li><b>TabProfile:</b> A DICT class that records some key feature of partitioned table, e.g., row Number, primary key, block size, fields, etc. <br>
                      Example: <i>TabProfile['rowNumber']</i>=132323; <i>TabProfile['primaryKey']</i>=[0]; <i>TabProfile['blockSize']</i>=5(kb); <i>TabProfile['fields']</i>=[4,5,10,10,10,50];
                    </li>
                  </ul>
                  <h3 class="subtitle-description"><i class="el-icon-star-on"></i>Return:</h3>
                  <p>Total I/O cost (block) of given workload (type: int). e.g., 10000.</p>
                </template>
              </el-col>
              <el-col :span="10" >
                <div>
                  <el-form  :model="addForm" label-width="150px">
                    <el-form-item label="Abbreviation">
                      <el-input v-model="addForm.modelName" placeholder="Please Input your model Name:"></el-input>
                    </el-form-item>
                    <el-form-item label="Method Name">
                      <el-input v-model="addForm.methodName" placeholder="Please Input method Name:"></el-input>
                    </el-form-item>
                    <el-form-item label="Your Code">
                      <el-input type="textarea"  :autosize="{ minRows: 15, maxRows: 500}" v-model="addForm.methodContent" placeholder="Please write your code:"></el-input>
                    </el-form-item>
                    <el-form-item>
                      <el-button type="primary" @click="checkFunValid">Verify</el-button> <el-button  type="success" @click="uploadCodeFun" v-if="codeValid">Upload</el-button> <h3 style="display: inline">the new <template v-if="addKey=='algo'"> partitioning algorithm</template> <template v-else>cost model</template></h3>
                    </el-form-item>
                  </el-form>
                </div>
              </el-col>
            </el-row>
          </el-col>
        </el-row>

      </div>

  </div>
</template>

<script>
export default {

  data() {
    // const generateMethodsData = _ => {
    //   const data = [];
    //   for (let i = 0; i <= 8; i++) {
    //     data.push({
    //       key: i,
    //       label: `垂直分区算法 ${ i }`,
    //       content:`这是垂直分区算法 ${i}的介绍......这是成本模型 ${i}的介绍......这是成本模型 ${i}的介绍......这是成本模型 ${i}的介绍......`
    //     });
    //   }
    //   return data;
    // };
    // const generateCostModelsData = _ => {
    //   const data = [];
    //   for (let i = 0; i <= 3; i++) {
    //     data.push({
    //       key: i,
    //       label: `成本模型 ${ i }`,
    //       content:`这是成本模型 ${i}的介绍......这是成本模型 ${i}的介绍......这是成本模型 ${i}的介绍......这是成本模型 ${i}的介绍......`
    //     });
    //   }
    //   return data;
    // };
    return {
      // 方法选择组件数据
      methods: [
        {key:0,label:'AUTOPART',link:'https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=1311234',content:'AutoPart first generates a set of atomic partitions. A vertical partition is atomic if every query that accesses it in the load accesses all the attributes in that partition. In other words, there are no queries that access a subset of the attributes of the atomic partition. Thereafter, in each iteration, the partition is extended by combining the atomic partition with the new partition obtained in the previous iteration between or among atomic partitions. Repeat this process until the estimated cost of the workload does not improve.'},
        {key:1,label:'HILLCLIMB',link:'https://www.sciencedirect.com/science/article/pii/B9780127224428500446?via%3Dihub',content:'A bottom-up algorithm starts with the column layout (each attribute is located in a different vertical partition). In each iteration, HILLCLIMB finds and merges two partitions which have the best improvement on the expected query cost after merging. This means that the number of vertical partitions is reduced by one in each iteration. The algorithm stops iterating when there is no improvement in the expected query cost.'},
        {key:2,label:'OPTIMAL',link:'#',content:'Go through all the cases to get the optimal partitioning scheme.',disabled:true},
        {key:3,label:'NAVATHE',link:'https://dl.acm.org/doi/pdf/10.1145/66926.66966',content:'NAVATHE is a top-down algorithm. Given a set of attributes and a set of queries referencing those attributes, it will constructs an attribute-affinity matrix. aff(i, j) represents the number of occurrences of attribute i and attribute j, which also known as their affinity. Then, NAVATHE clusters elements of the matrix so that these attribute pairs with high affinity are closely linked together, then recursively splits the aggregated attributes into vertical partitions.'},
        {key:4,label:'O2P',link:'http://alekh.org/papers/JD11.pdf',content:'O2P is a top-down algorithm for online vertical partitioning. It starts with NAVATHE\'s partitions. To obtain the best vertical partitions, O2P uses dynamic programming to record the cost of the non-optimal vertical partitions in the previous step, and then uses a greedy approach to create a locally best partitioning scheme in each step'},
        {key:5,label:'TROJAN',link:'http://people.csail.mit.edu/alekh/papers/JQD11.pdf',content:'Trojan is a threshold-based pruning algorithm proposed in 2011. In the first step, it enumerates all possible column groups, uses a measure of interest of column groups, keeps only those with high interest, prunes those with interest below a certain threshold, and then transforms the partitioning problem into a 0-1 backpacking problem, i.e., the pruned column groups are combined into a number of complete (i.e., containing all attributes) and disjoint sets of partitions.',disabled:true},
        {key:6,label:'ROW',link:'#',content:'There is no vertical partitioning involved; all table attributes are grouped within a single column group; tuples are allocated into blocks using simple rules or random assignment. '},
        {key:7,label:'COLUMN',link:'#',content:'Each table attribute is assigned to a separate column group.'},
        {key:8,label:'Rodriguez',link:'https://link.springer.com/chapter/10.1007/978-3-642-23091-2_48',content:'This paper proposes CBPA algorithm and extends the concept of frequent itemsets based on affinity matrix. It regards all affinity value in the matrix as a set, and keeps the top 30% of the set as frequent affinity. Then CBPA traverses each attribute in turn, and all edges which contains the attribute and satisfies frequent affinity are regarded as a partition.'},
        {key:9,label:'HYF',link:'https://linkinghub.elsevier.com/retrieve/pii/S002002551500643X',content:'HYF uses the apriori algorithm and applies cosine similarity of attributes to replace occurrence frequency when generating frequent itemsets. Then uses greedy strategy to combine candidate frequent itemsets to get the final vertical partitions'},
        {key:12,label:'SCVP-RV',link:'#',content:'The Refined Version of Spectral Clustering-based Vertical Partitioning (SCVP-RV) is a multi-stages strategy for solving the vertical partitioning problem. Leveraging the cost independence property between partitions, it designs an estimation function for quickly calculating column group (CG)\n' +
              '            division gains, making it suitable for large tables and heavy loads. SCVP-RV first calls spectral clustering to create attribute clusters from attribute affinity matrixs as initial CGs, and then uses a greedy search strategy to top-down split the CGs into atomic CGs based on mined frequent patterns. Subsequently,\n' +
              '            it merges these atomic CGs bottom-up, also based on frequent patterns, to obtain the final column partitions. Next, SCVP-RV utilizes predicate and median conditions involving all numerical and non-numerical columns, rather than simple primary keys, as the expansion criteria for leaf nodes to greedily build a binary partition tree for allocating tuples.'},
        {key:11,label:'AVP-RL',link:'https://link.springer.com/chapter/10.1007/978-3-030-30278-8_16',content:'Automatic Vertical Partitioning with Reinforcement Learning (AVP-RL) mappings the partitioning problem to a markov decision task. The core network of agent is a distributional deep Q-Network enhanced with prioritized experience replay. Moreover, AVP-RL introduces a novel partitioning environment for agent, encompassing an observation space, action space, and reward function. Within this environment, a greedy HillClimb policy is adopted as expert rules to assess the rewards of agent\'s actions. A main constraint of AVP-RL is that each table requires the allocation of a separately trained agent.'},
        {key:10,label:'SCVP',link:'https://www.jos.org.cn/jos/article/abstract/6496?st=search',content:'Spectral Clustering-based Vertical Partitioning (SCVP) is a hybrid model and multi-stage strategy designed to address the vertical partitioning problem. It leverages spectral clustering, FP-growth, and a bottom-up greedy search algorithm to enhance the quality of partitions. Additionally, SCVP incorporates a split-gain update function, which is aimed at reducing the execution time when dealing with large-scale data sets. Finally, SCVP takes primary key columns as the candidate predicate set to construct the partition tree, ensuring the even allocation of tuples to different blocks.'},
      ],
      selectedMethods: [6],
      costModels:[
        {key:0,label:'HDD',link:'https://www.sciencedirect.com/science/article/pii/S002002551500643X?via%3Dihub',content:'The Hard Disk (HDD) cost model is employed to approximate the quantity of blocks that need to be accessed in order to fulfill a specific query within a database system that incorporates a particular vertical partitioning strategy.'},
        // {key:1,label:'Son',content:'In the distributed database environment, the goal of vertical partitioning is to improve the system throughput and execution performance of workload on each node. There two relevant indicators DF and IA. DF refers to the total frequency of all queries which access different partitions, related to the query execution performance. IA refers to the total frequency of interference when queries are accessing, related to the system\'s throughput. The indicator for evaluating the performance of a partition scheme is the weighted sum of DF and IA.'}
      ],
      selectedCostModels:[],
      // 方法介绍组件数据
      accessedMethods:[],
      selectedObjList:[],
      objMethods:["Query Latency","Scanning Cost","Model Time Overhead","System Throughout","Partition Manageability"],
      objMargins:['input_w1','input_w2','input_w3','input_w4','input_w5'],
      activeMethodNames: [0],
      accessedCostModels:[],
      activeCostModelNames: [0],
      collapseCustomStyle:'background: #f7f7f7;border-radius: 4px;margin-bottom: 24px;border: 0;overflow: hidden',
      addKey:'algo',
      addPanelVisible:true,
      addForm:{
        modelName:'SAMPLE',
        methodName:'',
        methodContent:'',
      },
      codeValid:false
    };
  },
  created(){
    this.onCostModelsChanged(0)
    this.onMethodsChanged(this.selectedMethods,null,null)
    this.addBaseline('algo')
    this.$store.commit('SET_OBJECTIVES',this.selectedObjList)
  },
  methods: {
    onMethodsChanged(cur_value,direction,keys){
      // console.log("%o %s %o",cur_value,direction,keys)
      this.accessedMethods=[]
      // 展示cur_value的所有方法
      cur_value.sort()
      cur_value.map(k=>{
          this.accessedMethods.push(this.methods[k])
      })
      this.$store.commit('SET_METHODS',this.accessedMethods)
    },
    onObjectiveChange(obj_list){
      this.$store.commit('SET_OBJECTIVES',obj_list)
    },
    onCostModelsChanged(key){
      // console.log("%o %s %o",cur_value,direction,keys)
      this.accessedCostModels=[]
      this.accessedCostModels.push(this.costModels[key])
      // 展示cur_value的所有方法
      // cur_value.sort()
      // cur_value.map(k=>{
      //     this.accessedCostModels.push(this.costModels[k])
      // })
      let costModelIndexs=[]
      for(let costmodel of this.accessedCostModels){
        costModelIndexs.push(costmodel['label'])
      }
      this.$store.commit('SET_COSTMODELS',costModelIndexs)
    },
    addBaseline(key){
      this.addKey=key
      this.addPanelVisible=true
      if(key=='algo'){
        this.addForm.methodName='my_partitioner_method'
        this.addForm.methodContent='#This is sample code.\n' +
            'my_partition_scheme=[]\n' +
            'for attr_id in range(len(Fields)):\n' +
            '   my_partition_scheme.append([attr_id])\n' +
            'return my_partition_scheme'
      }else{
        this.addForm.methodName='my_evaluator_method'
        this.addForm.methodContent="#This is sample code.\n" +
            "total_IO=0f\n" +
            "for query in Workload:\n" +
            "   for par in Partitions:\n" +
            "      if access(query,par): \n" +
            "           total_IO+=TabProfile['rowNumber']*par_len(TabProfile['fields'],par)*query.freq\n" +
            "                              /TabProfile['blockSize']\n" +
            "return total_IO\n"

      }
    },
    encodeMessageOption(text,stype){
      this.$message({
        dangerouslyUseHTMLString: true,
        message: '<h2>'+text+'</h2>',
        type: stype,
        center:true
      });
    },
    checkFunValid(){
      if(true){
        this.encodeMessageOption('Congratulations, the code is valid!','success')
        this.codeValid=true
      }else{
        this.encodeMessageOption('The code needs to be modified!','warning')
      }
    },
    uploadCodeFun(){
      this.encodeMessageOption('Upload success!','success')
      this.codeValid=false
    }
  },
}
</script>

<style lang="scss" scoped>
  #methodBody{
    background-color: #f3f5f2;
    height: 100%;
  }
  .costmodel-panel{
    margin-top: 50px;
  }
  .selectedCard span{
    font-weight: bolder;
    font-size: 20px;
  }
  .el-checkbox{
    zoom: 145%;
    //font-weight: bolder!important;
    //font-size: 28px!important;
  }
  .selectedCard .el-tag{
    margin: auto 10px;
  }
  .select-panel{
    margin-top: 20px;
    padding: 15px 20px;
    background-color: white;
    h3{
      font-size: 17px;
      margin-bottom: 15px;
    }
  }
  .el-col h2{
    text-align: center;
    font-weight: bolder;
  }

  .introduce-card{
    margin: 18px auto;
  }

  .page-content{
    font-size: 25px;
  }

  .add-panel{
    h3{
      font-style: italic;
    }
    .el-button{
      font-size: 18px;
    }
  }

  .add-button{
    .el-button{
      font-size: 18px;
    }
  }
  #introduce-panel{
    margin-top: 20px;
    padding: 0 20px 0 20px;
    background-color: white;
  }
  .el-link{
    margin-bottom: 4px;
  }
  .ant-collapse{
    span{
      font-size: 20px;
      font-weight: bold;
    }
    p{
      font-size: 20px;
    }
  }
  .el-form-item .el-form-item__label{
    font-size: 50px;
  }



  .title-description{
    font-size: 22px;
  }
  .subtitle-description{
    font-size: 19px;
  }
  .plain-description{
    font-size: 17px;
  }
  .input_w1{
    margin-left: 120px;
  }

</style>
<style lang="scss" >
.el-form-item__label{
  font-size: 16px;
}
.el-textarea__inner{
  font-size: 16px;
}
.ant-input-number-input{
  font-size: 17px;
}
</style>
