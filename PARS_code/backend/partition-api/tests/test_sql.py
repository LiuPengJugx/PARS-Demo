from flask.db import Postgres
pg=Postgres()
cur=pg.getPgConnCur()
res,row_count=pg.executeQuery("select * from \"TPCD\".customer")
print(res)
print(row_count)