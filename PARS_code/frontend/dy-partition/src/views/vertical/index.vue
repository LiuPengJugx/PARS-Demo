<template>
  <div id="static-page">
    <div id="main-panel">
      <el-row
          class="inner-header"
          type="flex"
          justify="space-between"
          align="bottom"
      >
<!--        <el-col :span="2" ></el-col>-->
        <el-col  :span="4" :offset="1">
          <Progress
              :text-inside="true"
              :percent="percentage"
              :stroke-width="30"
              :stroke-color="['#108ee9', '#87d068']"
          />
          <h2 style="color: white;font-style: italic;text-align: center"><i class="el-icon-top-right"></i> Configuration Progress</h2>
          <!-- <el-progress :text-inside="true" :width="120" :stroke-width="30" :percentage="percentage" status="success"></el-progress> -->
          <!-- <el-progress :stroke-width="10" :width="120" type="circle" :percentage="percentage"></el-progress>   -->
          <!-- <span style="font-size:20px;margin:auto 20px;">步骤{{(Number(this.activeIndex)+1)}}/{{this.activePage.length}}</span> -->
        </el-col>
        <!-- <el-col :span="4" :offset="4">
            <el-button type="primary" size="default">上一步</el-button>

            <el-button type="primary" size="default">下一步</el-button>
        </el-col> -->
        <el-col :span="8" :offset="0">
          <el-menu
              :default-active="activeIndex"
              class="el-menu-demo"
              mode="horizontal"
              background-color="rgba(0,0,0,0.02)"

              active-text-color="#1890ff"
              @select="handleSelect"
          >
<!--            #37D7DF-->
            <el-menu-item
                v-for="(page, idx) in activePage"
                :index="idx.toString()"
                :key="page.path"
            >
              <span><i :class="page.icon" ></i>{{ page.name }}</span></el-menu-item>
          </el-menu>
          <div class="line"></div>
        </el-col>
      </el-row>
<!--      <el-row type="flex">-->
<!--         <el-divider><i class="el-icon-edit"></i></el-divider>-->
<!--      </el-row>-->
      <!-- <fixed-menu /> -->
      <el-row type="flex" justify="center">
        <!-- <app-main /> -->
        <fixed-panel id="main"/>
      </el-row>
    </div>
  </div>
</template>

<script>
import FixedPanel from "./components/FixedPanel.vue";

export default {
  components: { FixedPanel },
  data() {
    return {
      activeIndex: "0",
      basePath: "/vertical",
      activePage: [
        { name: "Specify Partitioned Table", path: "/importData", icon: "el-icon-edit" },
        {name: "Add Query Log",path: "/importWorkload",icon: "el-icon-upload",},
        {
          name: "Select Baseline Algorithms",
          path: "/selectMethod",
          icon: "el-icon-connection",
        },
        { name: "Conduct Partitioning Decision", path: "/executeAnalysis", icon: "el-icon-s-data" },
      ],
      percentage: 0,
    };
  },
  created() {
    this.percentage = 100 / this.activePage.length;
    this.$router.push({ path: this.basePath + this.activePage[0]["path"] });
    // 默认填写算法配置
    // this.$store.commit('SET_TABLE',[["customer","lineitem","supplier","part"],[100,400,200,200]]) // tablename -> selected table rows
    // this.$store.commit("SET_WORKLOAD",["customer.csv","supplier.csv","lineitem.csv","part.csv"])
    this.$store.commit('SET_TABLE',[["customer","part"],[1000000,1000000]]) // tablename -> selected table rows
    this.$store.commit("SET_WORKLOAD",["customer.csv","part.csv"])
    this.$store.commit("SET_BENCHMARK",["TPC-H"])
    this.$store.commit("SET_OBJECTIVES",["Query Latency","Scanning Cost","Model Time Overhead"])
    // this.$store.commit("SET_WORKLOAD",["customer1621435607414.csv","lineitem1621958603752.csv","supplier1621958597615.csv","nation1621958273183.csv"])
    // this.$store.commit('SET_METHODS',[{"key":0,"label":"AUTOPART"},{"key":3,"label":"NAVATHE"},{"key":4,"label":"O2P"},{"key":6,"label":"ROW"},{"key":9,"label":"HYF"},{"key":10,"label":"SCVP"},{"key":11,"label":"AVP-RL"},{"key":12,"label":"SCVP-RV"}])
    this.$store.commit('SET_METHODS',[{"key":6,"label":"ROW"},{"key":9,"label":"HYF"},{"key":10,"label":"SCVP"},{"key":11,"label":"AVP-RL"},{"key":12,"label":"SCVP-RV"}])
    this.$store.commit('SET_COSTMODELS',["HDD"])
  },
  computed: {},
  methods: {
    handleSelect(key, keyPath) {
      this.activeIndex = key;
      this.percentage = (100 / this.activePage.length) * (Number(key) + 1);
      console.log(key, keyPath);
      this.$router.push({
        path: this.basePath + this.activePage[Number(key)]["path"],
      });
    },
  },
};
</script>

<style lang="scss" scoped>
#static-page {
  background-color: #f3f5f2;
  height: 100%;
  //min-height: 1000px;
  padding: 30px 60px;
  #main-panel{
    //padding-top: 20px;
    background-color: white;
  }
}

.inner-header {
  padding-top: 50px;
  padding-bottom: 40px;
  //margin-bottom: 40px;
  //background-color: #e9ecec;
  background-image: url("/public/back_bar_3.svg");
  opacity: 0.95;
}

.ivu-progress {
  margin-bottom: 20px;
  font-size: 20px;
}

.inner-header .el-menu-demo {
  // padding-top: 150px;
  position: absolute;
  bottom: 0px;
  right: 0px;
}

.el-divider {
  margin: -3px 0 0 0;
  margin-bottom: 50px;
  height: 2px;
  width: 100%;
}

.el-menu {
  font-weight: bolder;

  .el-menu-item {
    font-size: 20px;
    color: white;
    i{
      color: white;
    }
  }
  //el-menu-item.is-active {
  //  color: #409eff;
  //}
}
//text-color="#ffffff"
.el-menu--horizontal > .el-menu-item.is-active {
  border-bottom: 3px solid #409eff;
  //color:red
}
#main{
  background-color: #f3f5f2;
}
</style>
