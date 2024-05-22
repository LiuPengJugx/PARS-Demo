import os
from flask import Flask,g
from flask_cors import *

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    CORS(app,resources={r'/*': {'origins': '*'}})
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    )
    app.config["timeout"]=5000000

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # a simple page that says hello
    @app.route('/hello')
    def hello():
        return 'Hello, World!'
    # from . import user
    # import storage
    # import index
    # import queryl
    # import dy_partition
    from . import user
    from . import storage
    from . import index
    from . import dy_partition
    app.config['WOODS_DICT']=None
    app.register_blueprint(user.bp)
    app.register_blueprint(storage.bp)
    app.register_blueprint(index.bp)
    app.register_blueprint(dy_partition.bp)
    # app.add_url_rule('/', endpoint='index')

    import logging

    app.logger.setLevel(logging.WARNING)
    file_handler = logging.FileHandler('app.log')
    file_handler.setLevel(logging.WARNING)
    app.logger.addHandler(file_handler)
    return app