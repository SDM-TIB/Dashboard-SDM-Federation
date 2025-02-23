import logging
import os
from urllib.parse import urlparse

from flask import Flask, redirect, render_template, send_from_directory, request, session, jsonify


def get_logger(name: str, file: str = None, file_and_console: bool = False) -> logging.Logger:
    """Gets a logger defined based on the provided parameters.

    This method is a convenience method to easily set up the loggers of
    different modules without the need of duplicating the setup code.
    By default, output will be logged to stdout. But also file loggers
    or stdout and file can be configured.

    Parameters
    ----------
    name : str
        The name to assign to the logger.
    file : str, optional
        The name (or path) of the log file assigned to the logger.
        None by default which will result in logging to stdout only.
    file_and_console : bool, optional
        Indicates whether the logger should log to file and stdout.
        The logger will only log to stdout if no file is set.

    Returns
    -------
    logging.Logger
        The logger created based on the provided parameters.

    """
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


def create_app() -> Flask:
    """Sets up the Flask application for FedSDM.

    This method creates a Flask application and configures it to serve FedSDM.
    Configuring includes:
        - Setting application configuration values like the path to the database file
        - Registering the routes in order to serve the Web content
        - Creating an instance directory for working files
        - Adding a redirect from '/' to '/dashboard'

    Returns
    -------
    flask.Flask
        A Flask application configured to serve FedSDM.

    """
    app = Flask(__name__, instance_path='/FedSDM/instance')
    app.debug = True

    app.config.from_mapping(
        APPLICATION_ROOT=os.environ['APP_PREFIX'] if 'APP_PREFIX' in os.environ else '/',
        DATABASE=os.path.join(app.instance_path, 'fedsdm.sqlite'),
        SECRET_KEY='dev',
        SESSION_COOKIE_SAMESITE='Strict'
    )

    # Ensure the instance directory exists
    os.makedirs(app.instance_path, exist_ok=True)
    os.chmod(app.instance_path, 0o755)

    from FedSDM import db
    with app.app_context():
        db.init_app(app)

    # Register the authentication blueprint
    from FedSDM import auth
    app.register_blueprint(auth.bp)

    # Register the dashboard blueprint
    from FedSDM import dashboard
    app.register_blueprint(dashboard.bp)

    # Register the federation manager blueprint
    from FedSDM import federation
    app.register_blueprint(federation.bp)

    # Register the RDF Molecule Template blueprint
    from FedSDM import rdfmtmgt
    app.register_blueprint(rdfmtmgt.bp)

    # Register the query blueprint
    from FedSDM import query
    app.register_blueprint(query.bp)

    # Register the feedback blueprint
    from FedSDM import feedback
    app.register_blueprint(feedback.bp)

    @app.route('/')
    def index():
        return redirect('dashboard')

    @app.route('/info')
    def info():
        from DeTrusty import __version__ as version_detrusty
        return render_template('info.jinja2', version_detrusty=version_detrusty)

    @app.route('/favicon.ico')
    def favicon():
        return send_from_directory(os.path.join(app.root_path, 'static', 'images'), 'Lynx_Icon.ico', mimetype='image/vnd.microsoft.icon')

    @app.errorhandler(422)
    @app.errorhandler(400)
    def handle_error(error):
        headers = error.data.get("headers", None)
        messages = error.data.get("messages", ["Invalid request."])
        if headers:
            return jsonify({"errors": messages}), error.code, headers
        else:
            return jsonify({"errors": messages}), error.code

    @app.before_request
    def before_request():
        if not (request.path.startswith('/auth/') or request.path.startswith('/static/')) and request.referrer is not None and not ('/auth/' in request.referrer or '/static/' in request.referrer):
            url = request.referrer
            if url and urlparse(url).netloc == request.host:
                session['url'] = urlparse(url).path

    return app
