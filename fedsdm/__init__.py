import os
from flask import Flask, url_for, send_from_directory, redirect


class PrefixMiddleware(object):

    def __init__(self, app, prefix=''):
        self.app = app
        self.prefix = prefix

    def __call__(self, environ, start_response):
        if environ['PATH_INFO'].startswith(self.prefix):
            environ['PATH_INFO'] = environ['PATH_INFO'][len(self.prefix):]
            environ['SCRIPT_NAME'] = self.prefix
            return self.app(environ, start_response)
        else:
            start_response('404', [('Content-Type', 'text/plain')])
            return [str(self.prefix + ". This url does not belong to the app.").encode()]


def create_app(test_config=None):
    prefix = "/"
    if "APP_PREFIX" in os.environ:
        prefix = os.environ['APP_PREFIX']
    # create and configure the app
    app = Flask(__name__, instance_relative_config=False)
    app.config["APPLICATION_ROOT"] = prefix
    app.config["SESSION_COOKIE_SAMESITE"] = "Strict"
    app.debug = True

    # app.wsgi_app = PrefixMiddleware(app.wsgi_app, prefix=prefix)

    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'fedsdm.sqlite')
    )

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

    from . import db
    db.init_app(app)

    # Register the authentication blueprint
    from . import auth
    app.register_blueprint(auth.bp)

    # Register the federations manager blueprint
    from . import dashboard
    app.register_blueprint(dashboard.bp)
    app.add_url_rule('/', endpoint='index')

    # Register the federations manager blueprint
    from . import federation
    app.register_blueprint(federation.bp)

    from . import rdfmtmgt
    app.register_blueprint(rdfmtmgt.bp)

    from . import query
    app.register_blueprint(query.bp)

    from . import mapping
    app.register_blueprint(mapping.bp)

    from . import feedback
    app.register_blueprint(feedback.bp)
    # from . import lcexplorer
    # app.register_blueprint(lcexplorer.bp)

    @app.route('/')
    def index():
        return redirect('dashboard/')

    @app.route('/js/<path:path>')
    def send_js(path):
        return send_from_directory('static/js', path)

    @app.route('/css/<path:path>')
    def send_css(path):
        return send_from_directory('static/css', path)

    @app.route('/scss/<path:path>')
    def send_scss(path):
        return send_from_directory('static/scss', path)

    @app.route('/fonts/<path:path>')
    def send_fonts(path):
        return send_from_directory('static/css/fonts', path)

    @app.route('/webfonts/<path:path>')
    def send_webfonts(path):
        return send_from_directory('static/webfonts', path)

    @app.route('/images/<path:path>')
    def send_images(path):
        return send_from_directory('static/css/images', path)


    return app
