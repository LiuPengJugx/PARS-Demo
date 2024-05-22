from flask import(Blueprint, flash, g, redirect, render_template, request, url_for)
from werkzeug.exceptions import abort
from flaskr.db import Postgres
import json
bp=Blueprint('user',__name__,url_prefix='/aidb/user')
#用户登陆模块控制器 

@bp.route('/login',methods=['POST'])
def login():
    # login_info=request.get_json()
    # username=request.form.get('username')
    # password=request.form.get('password')

    # print(login_info)
    
    return json.dumps({"code":20000,"data":{"token":"admin-token"}})

@bp.route('/logout',methods=['POST'])
def logout():
    return json.dumps({"code":20000,"data":{"token":"admin-token"}})

@bp.route('/info',methods=['GET'])
def getUserInfo():
    token=request.args.get('token')
    print("token=",token)
    return json.dumps({"code":20000,
            "data":{"roles":["admin"],
            "introduction":"I am a super administrator",
            "avatar":"https://wpimg.wallstcn.com/f778738c-e4f8-4870-b634-56703b4acafe.gif",
            "name":"Super Admin"}})