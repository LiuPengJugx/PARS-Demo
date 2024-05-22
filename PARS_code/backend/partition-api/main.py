from flaskr.__init__ import create_app
import sys
from gevent import pywsgi
sys.path.append('./flaskr/algorithms/SCVP')
sys.path.append('./flaskr/algorithms')
app=create_app()
app.debug=True

app.run(host ='0.0.0.0',port = 5000)
