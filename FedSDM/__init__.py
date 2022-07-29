import logging
import os

from flask import Flask, redirect


def get_logger(name: str, file: str = None, file_and_console: bool = False):
    log_formatter = logging.Formatter('%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s')
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if file is not None:
        file_handler = logging.FileHandler(file)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(log_formatter)
        logger.addHandler(file_handler)
    if file is None or file_and_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(log_formatter)
        logger.addHandler(console_handler)
    return logger


def create_app():
    app = Flask(__name__)
    app.debug = True

    app.config.from_mapping(
        APPLICATION_ROOT=os.environ['APP_PREFIX'] if 'APP_PREFIX' in os.environ else '/',
        DATABASE=os.path.join(app.instance_path, 'fedsdm.sqlite'),
        SECRET_KEY='dev',
        SESSION_COOKIE_SAMESITE='Strict'
    )

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass
    os.chmod(app.instance_path, 0o755)

    from . import db
    with app.app_context():
        db.init_app(app)

    # Register the authentication blueprint
    from . import auth
    app.register_blueprint(auth.bp)

    # Register the dashboard blueprint
    from . import dashboard
    app.register_blueprint(dashboard.bp)
    app.add_url_rule('/', endpoint='index')

    # Register the federation manager blueprint
    from . import federation
    app.register_blueprint(federation.bp)

    # Register the RDF Molecule Template blueprint
    from . import rdfmtmgt
    app.register_blueprint(rdfmtmgt.bp)

    # Register the query blueprint
    from . import query
    app.register_blueprint(query.bp)

    # Register the mappings blueprint
    # from . import mapping
    # app.register_blueprint(mapping.bp)

    # Register the feedback blueprint
    from . import feedback
    app.register_blueprint(feedback.bp)

    @app.route('/')
    def index():
        return redirect('dashboard/')

    return app
