col_inf={
        'customer':{
            'name':['c_custkey','c_name','c_address','c_nationkey','c_phone','c_acctbal','c_mktsegment','c_comment'],
            'length':[4,25,40,4,15,4,10,117],
            'vary':[0,1,1,0,0,0,0,1],
             'num_col':[0,3,4]
            },
        'lineitem':{
            'name':['l_orderkey','l_partkey','l_suppkey','l_linenumber','l_quantity','l_extendedprice','l_discount','l_tax','l_returnflag','l_linestatus','l_shipdate','l_commitdate','l_receiptdate','l_shipinstruct','l_shipmode','l_comment'],
            'length':[4,8,8,4,15,15,15,15,1,1,10,10,10,25,10,44],
            'vary':[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1],
            'num_col':[0,1,2,34,5,6,7]
            },
        'nation':{
            'name':['n_nationkey','n_name','n_regionkey','n_comment'],
            'length':[4,25,4,152],
            'vary':[0,0,0,1],
'num_col':[0,2]
            },
        'orders':{
            'name':['o_orderkey','o_custkey','o_orderstatus','o_totalprice','o_orderdate','o_orderpriority','o_clerk','o_shippriority','o_comment'],
            'length':[4,4,1,4,10,15,15,4,79],
            'vary':[0,0,0,0,0,0,0,0,1],
'num_col':[0,1,3,7]
            },
        'part':{
            'name':['p_partkey','p_name','p_mfgr','p_brand','p_type','p_size','p_container','p_retailprice','p_comment'],
            'length':[4,55,25,10,25,4,10,4,23],
            'vary':[0,1,0,0,1,0,0,0,1],
            'num_col':[0,5,7]
            },
        'partsupp':{
            'name':['ps_partkey','ps_suppkey','ps_availqty','ps_supplycost','ps_comment'],
            'length':[4,4,4,4,199],
            'vary':[0,0,0,0,1],
            'num_col':[0,1,2,3]
            },
        'region':{
            'name':['r_regionkey','r_name','r_comment'],
            'length':[4,25,152],
            'vary':[0,0,1],
            'num_col':[0]
            },
        'supplier':{
            'name':['s_suppkey','s_name','s_address','s_nationkey','s_phone','s_acctbal','s_comment'],
            'length':[4,25,40,4,15,4,101],
            'vary':[0,0,1,0,0,0,1],
            'num_col':[0,3,5]
        },
        'widetable30':{
            'name':['a_'+str(i) for i in range(30)],
            'length':[4 for _ in range(30)],
            'vary':[0 for _ in range(30)],
            'num_col':[_ for _ in range(15)]
        },
        'widetable50':{
            'name':['b_'+str(i) for i in range(50)],
            'length':[4 for _ in range(50)],
            'vary':[0 for _ in range(50)],
'num_col':[_ for _ in range(25)]
        },
        'widetable100':{
            'name':['c_'+str(i) for i in range(100)],
            'length':[4 for _ in range(100)],
            'vary':[0 for _ in range(100)],
'num_col':[_ for _ in range(50)]
        },
        # 'aka_title':{
        #     'name':['at_id','at_movie_id','at_title','at_imdb_index','at_kind_id','at_production_year','at_phonetic_code',"at_episode_of_id","at_season_nr","at_episode_nr","at_note","at_md5sum"],
        #     'length':[32,32,15,12,32,32,5,32,32,32,20,32],
        #     'vary':[0,0,1,1,0,0,1,0,0,0,1,1]
        # },
        # 'person_info':{
        #     'name':['pi_id','pi_person_id','pi_info_type_id','pi_info','pi_note'],
        #     'length':[32,32,32,40,20],
        #     'vary':[0,0,0,1,1]
        # },
        'movie_info':{
            'name':['mi_id','mi_movie_id','mi_info_type_id','mi_info','mi_note'],
            'length':[32,32,32,40,20],
            'vary':[0,0,0,1,1],
            'num_col':[0,1,2]
        },
        'title':{
            'name':['t_id','t_title','t_imdb_index','t_kind_id','t_production_year','t_imdb_id','t_phonetic_code','t_episode_of_id','t_season_nr','t_episode_nr','t_series_years','t_md5sum'],
            'length':[32,40,12,32,32,32,5,32,32,32,49,32],
            'vary':[0,1,1,0,0,0,1,0,0,0,1,1],
            'num_col':[0,3,4,5,7,8,9]
        },
        'cast_info':{
            'name':['ci_id','ci_person_id','ci_movie_id','ci_person_role_id','ci_note','ci_nr_order','ci_role_id'],
            'length':[32,32,32,32,20,32,32],
            'vary':[0,0,0,0,1,0,0],
            'num_col':[0,1,2,3,5,6]
        },
        'movie_companies':{
            'name':['mc_id','mc_movie_id','mc_company_id','mc_company_type_id','mc_note'],
            'length':[32,32,32,32,20],
            'vary':[0,0,0,0,1],
            'num_col':[0,1,2,3]
        },
        'movie_info_idx':{
            'name':['mi_idx_id','mi_idx_movie_id','mi_idx_info_type_id','mi_idx_info','mi_idx_note'],
            'length':[32,32,32,20,20],
            'vary':[0,0,0,1,1],
            'num_col':[0,1,2]
        },
        'movie_keyword':{
            'name':['mk_id','mk_movie_id','mk_keyword_id'],
            'length':[32,32,32],
            'vary':[0,0,0],
            'num_col':[0,1,2]
        },

    }
schema_inf={
    'tpch':["customer","lineitem","nation","orders","part","partsupp","region","supplier"],
    'synthetic':["widetable30","widetable50","widetable100"],
    'job':["title","cast_info","movie_info_idx","movie_keyword","movie_companies","movie_info"]
}