const getters = {
  tabledata: (state) => state.vertical.tabledata,
  tableScale: (state) => state.vertical.tableScale,
  workloads: (state) => state.vertical.workloads,
  methods: (state) => state.vertical.methods,
  costModels: (state) => state.vertical.costModels,
  optObjectives: (state) => state.vertical.optObjectives,
  benchmarks: (state) => state.vertical.benchmarks,
};
export default getters;
