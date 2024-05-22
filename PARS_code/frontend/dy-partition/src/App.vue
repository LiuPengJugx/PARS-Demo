<template>
  <el-container>
      <el-header >
        <el-row justify="center" type="flex">
          <el-col :span="5">
            <router-link
                to="/vertical"
                style="
              height: 100%;
              width: 100%;
              display: flex;
              align-items: center;
              justify-content: center;
            "
            >
              <img src="/public/favicon.svg" width="50px" style="margin-right: 3px" />
              <!--            <span>ADPS</span>-->
              <h2 style="
                font-family: Gilroy-ExtraBold, sans-serif;
                font-size: 28px;
                margin-bottom: 0;">
                PARS
              </h2>
            </router-link>
          </el-col>

          <el-col :span="8">
            <a-menu
                mode="horizontal"
                theme="light"
                @click="openItem"
                v-model="cur_page_key"
            >
              <a-menu-item key="vp" style="font-family: Gilroy-ExtraBold, sans-serif;color: black;">Static Partitioning<i class="el-icon-arrow-down"></i></a-menu-item>
              <a-menu-item key="dp" style="font-family: Gilroy-ExtraBold, sans-serif;color: black;">Dynamic Partitioning<i class="el-icon-arrow-right"></i></a-menu-item>
            </a-menu>
          </el-col>
        </el-row>
        <!--        <el-row id="outer-header">-->
        <!--        </el-row>-->
      </el-header>


    <el-main style="padding: 0;"><router-view /></el-main>
    <!--    <el-footer></el-footer>-->
<!--    <el-footer></el-footer>-->
  </el-container>
</template>

<script>
export default {
  data() {
    return {
      activePage: {
        // intro: { name: "主页", path: "/home" },
        dp: { name: "动态分区", path: "/dynamic" },
        vp: { name: "垂直分区", path: "/vertical" },
      },
      cur_page_key:['vp']
    };
  },
  created() {
    this.$router.push({path:"/vertical"})
    this.cur_page_key=['vp']
  },
  methods: {
    openItem(e) {
      this.$router.push({ path: this.activePage[e.key]["path"] });
    },
  },
};
</script>

<style lang="scss" scoped>
.el-container {
  background-color: #f3f5f2;
  height: 100%;
}
#outer-header{
  height: 160px;
  width: 100%;

}

.el-header {
  background-color: white;
  height: auto;
  border-bottom: 2.5px solid #f3f5f2;
  //background-image: url("/public/back_bar_5.jpg");
  //background-repeat:no-repeat;
  //background-size:cover;
  //background-position:center center;
}
.ant-menu {
  line-height: 60px;
  background-color:rgba(0,0,0,0);
  //color:black;
  font-size: 20px;
  /*font-weight: lighter;*/
  .ant-menu-item {
    //font-family: "Roboto", sans-serif !important;
    font-family: "Arial", sans-serif;
    font-weight: bolder;
  }
}
</style>