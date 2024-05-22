// main.js
import Vue from "vue";
import App from "./App.vue";
import store from "./store";
import router from "./router";

import ElementUI from "element-ui";
import "element-ui/lib/theme-chalk/index.css";

Vue.use(ElementUI);

import ViewUI from 'view-design';
import "view-design/dist/styles/iview.css";
//全局引入
Vue.use(ViewUI);
//按需引入
// import { Progress,Collapse } from "view-design";
// Vue.component("Progress", Progress);
// Vue.component("Collapse", Collapse);
// Vue.component("CollapsePanel", CollapsePanel);

import "ant-design-vue/dist/antd.css";
import { Button,Menu, Card, Tabs, Icon, Tag, InputNumber, Collapse, Divider,  Table, Popconfirm,  Tree, Descriptions, Select, Steps, Progress,Spin} from "ant-design-vue";

Vue.use(Menu).use(Card).use(Tabs).use(Icon).use(Tag).use(Steps).use(InputNumber).use(Collapse).use(Divider).use(Table).use(Popconfirm).use(Button).use(Tree).use(Descriptions).use(Select).use(Progress).use(Spin);

import * as echarts from "echarts/core";
import {
  DatasetComponent,
  TitleComponent,
  TooltipComponent,
  TransformComponent,
  GraphicComponent,
  LegendComponent,
  GridComponent,
  VisualMapComponent,
  ToolboxComponent,
} from "echarts/components";
import {LineChart, HeatmapChart, TreemapChart, GaugeChart, GraphChart, PieChart} from "echarts/charts";
import { LabelLayout, UniversalTransition } from "echarts/features";
import { CanvasRenderer } from "echarts/renderers";
import { BarChart } from "echarts/charts";

echarts.use([
  DatasetComponent,
  TitleComponent,
  TooltipComponent,
  GridComponent,
  TransformComponent,
  LineChart,
  CanvasRenderer,
  LabelLayout,
  UniversalTransition,
  GraphicComponent,
  BarChart,
  LegendComponent,
  VisualMapComponent,
  HeatmapChart,
  ToolboxComponent,
  TreemapChart,
  GraphChart,
  PieChart
]);
Vue.prototype.$echarts = echarts;

new Vue({
  el: "#app",
  router,
  store,
  render: (h) => h(App),
}).$mount();
