import json

from flask import (
    Blueprint, g, render_template, session, Response
)
from webargs import fields
from webargs.flaskparser import use_kwargs

from FedSDM.auth import login_required
from FedSDM.db import get_db
from FedSDM.utils import get_federations

bp = Blueprint('feedback', __name__, url_prefix='/feedback')


@bp.route('/')
@login_required
def feedback_overview() -> str:
    """Serves requests to '/feedback'.

    Serves the feedback page with all available federations loaded.

    Note
    ----
    The request is only served for logged-in users.

    Returns
    -------
    str
        Rendered template of the feedback page with all available federations.

    """
    federations = get_federations()
    g.federations = federations
    if 'fed' in session:
        if session['fed'] not in [f['uri'] for f in federations]:
            del session['fed']
    return render_template('feedback/index.jinja2', federations=g.federations)


@bp.route('/issues')
@use_kwargs({'fed': fields.Str(required=True)}, location='query')
@login_required
def feedback_list(fed) -> Response:
    """Serves requests to '/feedback/issues'.

    This route provides all the issues that have been raised for a certain federation.
    The federation needs to be included in the parameter 'fed' of the request.

    Note
    ----
    The request is only served for logged-in users.

    Returns
    -------
    flask.Response
        A JSON response including all raised issues for the given federation.
        The following data is provided for each issue:
            - id -- the ID of the issue
            - fed -- the federation for which the issue was raised
            - user -- the user who raised the issue
            - query -- the query for which the issue was raised
            - desc -- the description provided for the issue
            - status -- the current state of the issue
            - created -- the date the issue was raised

    """
    res = []
    db = get_db()
    if fed is None or fed.lower() == 'all':
        feedbacks = db.execute(
            'SELECT distinct f.id as id, f.federationID as fed, f.issueDesc as desc, f.created as created, '
            '     f.issueQuery as query , u.username as user, f.issueStatus as status'
            ' FROM feedbackreport f JOIN user u ON f.userID = u.id '
            ' ORDER BY created DESC'
        ).fetchall()
    else:
        feedbacks = db.execute(
            'SELECT distinct f.id as id, f.federationID as fed, f.issueDesc as desc, f.created as created, '
            '     f.issueQuery as query , u.username as user, f.issueStatus as status'
            ' FROM feedbackreport f JOIN user u ON f.userID = u.id WHERE f.federationID="' + fed + '" '
            ' ORDER BY created DESC'
        ).fetchall()
    for r in feedbacks:
        dd = {
            'id': r['id'],
            'fed': r['fed'],
            'user': r['user'],
            'query': r['query'],
            'desc':  r['desc'],
            'status': r['status'],
            'created': str(r['created'])
        }
        res.append(dd)

    return Response(json.dumps({'data': res}), mimetype='application/json')


@bp.route('/details')
@use_kwargs({'iid': fields.Int(required=True)}, location='query')
@login_required
def feedback_details(iid) -> Response:
    """Serves requests to '/feedback/details'.

    Provides more details about the issue in question. In order to do so, the
    ID of the issue must be included in the parameter 'iid' of the request.

    Note
    ----
    The request is only served for logged-in users.

    Returns
    -------
    flask.Response
        A JSON response including detailed information about the issue.
        The following information is provided for the issue:
            - var -- the variable in the query to which the problematic value is mapped
            - pred -- the predicate from the query which is involved in the issue
            - row -- the problematic query result (row)

        The response will include an empty JSON object if no ID was given or there
        are no details to be returned for the issue with the provided ID.

    """
    db = get_db()
    if iid is None:
        return Response(json.dumps({}), mimetype='application/json')
    else:
        feedback_data = db.execute(
            'SELECT distinct projVar, projPred, rowData '
            ' FROM feedbackdata WHERE reportID="' + str(iid) + '" '
        ).fetchone()

        if feedback_data is None:
            return Response(json.dumps({}), mimetype='application/json')

        data = {
            'var': feedback_data['projVar'],
            'pred': feedback_data['projPred'],
            'row': json.loads(feedback_data['rowData'])
        }
        return Response(json.dumps(data), mimetype='application/json')
