import Vue from "vue";
import Vuex from "vuex";
import getters from "./getters";
import vertical from "./modules/vertical";

Vue.use(Vuex);

const store = new Vuex.Store({
  modules: {
    vertical,
  },
  getters,
});

export default store;
