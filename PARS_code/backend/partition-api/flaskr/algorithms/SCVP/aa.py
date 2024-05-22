res={'methods':['NAVATHE', 'AUTOPART', 'HILLCLIMB', 'O2P', 'Row', 'Column', 'Rodriguez', 'HYF', 'SCVP', 'AVP-RL'],
                        'latency':[40.36006958007599, 43.66323553085156, 39.66316987990936, 50.828470115665816, 46.16818531035552, 48.26210138320709, 40.862587165832004, 48.26210138320709, 18.248083324431875, 36.5458409309343]}
new_res={}
for id,k in enumerate(res['methods']):
    new_res[k]=res['latency'][id]
print(new_res)