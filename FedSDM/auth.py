import functools
from types import FunctionType

from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for, Response
)
from werkzeug.security import check_password_hash, generate_password_hash

from FedSDM.db import get_db

bp = Blueprint('auth', __name__, url_prefix='/auth')


@bp.route('/register', methods=('GET', 'POST'))
def register() -> Response | str:
    """Serves requests to '/auth/register'.

    If the request method is 'GET', an HTML page with the registration form will be shown.
    In the case of a POST request, the submitted form data will be validated. The validation
    includes the following checks:

        - Username is not empty
        - Password is not empty
        - Username is not taken (checks if the username is already in the database)

    If the validation succeeds, the new user is inserted into the database. For security
    reasons, no plain text passwords are stored in the database. After adding the new
    user, the user is redirected to the login page.

    If the validation fails, the initial registration form will be rendered including an
    error message helping the user to update the application.

    Returns
    -------
    flask.Response | str
        Returns the HTML page with the registration form on GET requests or after validation
        of the form data fails. A successful registration leads to a redirect to the login page.

    """
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        db = get_db()
        error = None

        if not username:
            error = 'Username is required.'
        elif not password:
            error = 'Password is required.'
        elif db.execute('SELECT id FROM user WHERE username = ?', (username, )).fetchone() is not None:
            error = 'User {} is already registered.'.format(username)

        if error is None:
            db.execute(
                'INSERT INTO user (username, password) VALUES (?, ?) ',
                (username, generate_password_hash(password))
            )
            db.commit()
            return redirect(url_for('auth.login'))

        flash(error)
    return render_template('auth.html', title='Register', operation='Register', other='Login')


@bp.route('/login', methods=('GET', "POST"))
def login() -> Response | str:
    """Serves requests to '/auth/login'.

    If the request method is 'GET', an HTML page with the login form will be shown.
    In case of a POST request, the submitted form data will be validated, i.e., it
    will be checked against the database.
    If the credentials can be verified, the user's ID and name are stored in the
    session cookie, i.e., the data persists across requests. The data is securely
    signed by Flask so that it cannot be tampered with. The user is then redirected
    to the landing page of FedSDM.
    If the credentials cannot be verified, the initial login form will be rendered
    including an error message.

    Returns
    -------
    flask.Response | str
        Returns the HTML page with the login form on GET requests or after validation of
        the credentials fails. A successful login leads to a redirect to the landing page.

    """
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        db = get_db()
        error = None
        user = db.execute('SELECT * FROM user WHERE username = ?', (username, )).fetchone()

        if user is None:
            error = 'Incorrect username. '
        elif not check_password_hash(user['password'], password):
            error = 'Incorrect password. '

        if error is None:
            session.clear()
            session['user_id'] = user['id']
            session['user_name'] = user['username']
            return redirect(url_for('index'))

        flash(error)
    return render_template('auth.html', title='Login', operation='Login', other='Register')


@bp.before_app_request
def load_logged_in_user() -> None:
    """Loads the information about the logged-in user before the request is actually handled.

    This method is executed before the actual request handlers are triggered. If a user ID is stored
    in the session cookie, the application queries the database for the data of the user which is
    stored in Flask's global variables, i.e., it is accessible as `g.user`. This variable lasts for the
    length of the request. `g.user` is None if the user does not exist or no user has logged in.

    """
    user_id = session.get('user_id')

    if user_id is None:
        g.user = None
    else:
        g.user = get_db().execute('SELECT * FROM user WHERE id = ?', (user_id, )).fetchone()


@bp.route('/logout')
def logout() -> Response:
    """Serves requests to '/auth/logout'.

    To log out, the user ID is removed from the session, i.e., it will not be loaded
    by :func:`load_logged_in_user` on subsequent requests. After clearing the session
    cookie, the user is redirected to the landing page of FedSDM.

    Returns
    -------
    flask.Response
        A redirect to the landing page of FedSDM.

    """
    session.clear()
    return redirect(url_for('index'))


def login_required(view: FunctionType) -> any:
    """Provides a decorator that requires authentication in order to access a view.

    This decorator wraps the view it is applied to and returns a new view function that
    checks if a user is logged in. If no user is logged in, it redirects to the login page.
    However, if a user is logged in, the original view is called regularly.

    Parameters
    ----------
    view
        The function that would handle the original request.

    Returns
    -------
    any
        For logged-in users, the originally accessed page will be rendered.
        Otherwise, the user will be redirected to the login page.

    """
    @functools.wraps(view)
    def wrapped_view(**kwargs):
        if g.user is None:
            return redirect(url_for('auth.login'))

        return view(**kwargs)

    return wrapped_view
