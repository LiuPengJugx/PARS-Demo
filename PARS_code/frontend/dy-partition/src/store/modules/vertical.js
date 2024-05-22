const state = {
  tabledata: "",
  tableScale:[],
  workloads: [],
  methods: [],
  costModels: [],
  optObjectives:[],
  benchmarks:[]
};

const mutations = {
  SET_TABLE: (state, [tabledata,tableScale]) => {
    state.tabledata = tabledata;
    state.tableScale = tableScale;
  },
  SET_WORKLOAD: (state, workloads) => {
    state.workloads = workloads;
  },
  SET_METHODS: (state, methods) => {
    state.methods = methods;
  },
  SET_COSTMODELS: (state, costModels) => {
    state.costModels = costModels;
  },
  SET_OBJECTIVES: (state, optObjectives) => {
    state.optObjectives = optObjectives;
  },
  SET_BENCHMARK: (state, benchmarks) => {
    state.benchmarks = benchmarks;
  },
};

const actions = {};

export default {
  state,
  mutations,
  actions,
};
