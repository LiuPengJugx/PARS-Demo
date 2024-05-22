from flask import Flask
from flask import request
from flask_cors import *
import json
app = Flask(__name__)
# 模板更改后立即生效
app.jinja_env.auto_reload = True
# app.run('0.0.0.0', debug=True, port=5000, ssl_context='adhoc')
CORS(app, supports_credentials=True)
@app.route('/')
def index():
    return 'index page'

@app.route('/aidb/user/logout',methods=['POST'])
def logout():
    return {
        "code": 20000,
        "data": 'success'
    }

@app.route('/aidb/user/login',methods=['POST'])
def login():
    # login_info=request.get_json()
    # username=request.form.get('username')
    # password=request.form.get('password')

    # print(login_info)
    
    return json.dumps({"code":20000,"data":{"token":"admin-token"}})

@app.route('/aidb/user/info',methods=['GET'])
def getUserInfo():
    token=request.args.get('token')
    print("token=",token)
    return json.dumps({"code":20000,
            "data":{"roles":["admin"],
            "introduction":"I am a super administrator",
            "avatar":"https://wpimg.wallstcn.com/f778738c-e4f8-4870-b634-56703b4acafe.gif",
            "name":"Super Admin"}})

@app.route('/aidb/storage/student',methods=['GET'])
def getStudentInfo():
    tab_name=request.args.get('tabName')
    page=request.args.get('page',type=int)
    limit=request.args.get('limit',type=int)
    row_num=request.args.get('rowNum',type=int)
    

    # print("%s %d %d"%(tab_name,page,limit))
    columns=["date","name","address"]
    student_info=[['2016-05-02','王小虎','上海市普陀区金沙江路 1518 弄'],
        ['2016-05-02','王小虎','上海市普陀区金沙江路 1518 弄'],
        ['2016-05-02','王小虎','上海市普陀区金沙江路 1518 弄']
    ]
    return json.dumps(
        {
        "code":20000,
        "data":{
            'columns':columns,
            'student_info':student_info
        }
        } 
    )
    