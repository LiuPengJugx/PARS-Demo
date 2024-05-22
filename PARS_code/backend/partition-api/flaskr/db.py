import psycopg2
# PG数据库连接对象
class Postgres:
    def __init__(self):
        pass
    def getPgConnCur(self):
        #创建连接对象
        self.conn=psycopg2.connect(database="postgres",user="postgres",password="postgres",host="127.0.0.1",port="5432")
        # self.conn=psycopg2.connect(database="pars",user="liupengju",password="",host="10.77.110.133",port="5435")
        self.cur=self.conn.cursor()
        return self.cur
    # 执行查询
    def executeQuery(self,sql):
        self.cur.execute(sql)
        
        return self.cur.fetchall(),self.cur.rowcount


    # 关闭数据库连接
    def close(self):
        self.conn.close()
        self.cur.close()
