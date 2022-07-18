from flask import (
    Blueprint, g, render_template, session, Response, request
)

import json

from fedsdm.auth import login_required
from fedsdm.db import get_db
from fedsdm.ui.utils import get_federations

bp = Blueprint('feedback', __name__, url_prefix='/feedback')


@bp.route('/')
@login_required
def feedback_overview():
    federations = get_federations()
    g.federations = federations
    if 'fed' in session:
        if session['fed'] not in [f['uri'] for f in federations]:
            del session['fed']
    return render_template('feedback/index.html', federations=g.federations)


@bp.route('/issues')
@login_required
def feedback_list():
    try:
        fed = request.args['fed']
    except KeyError:
        return Response(json.dumps({'data': []}), mimetype='application/json')
    res = []
    db = get_db()
    if fed is None:
        feedbacks = db.execute(
            'SELECT distinct f.id as id, f.federationID as fed, f.issueDesc as desc, f.created as created, '
            '     f.issueQuery as query , u.username as user, f.issueStatus as status'
            ' FROM feedbackreport f JOIN user u On f.userID = u.id '
            ' ORDER BY created DESC'
        ).fetchall()
    else:
        feedbacks = db.execute(
            'SELECT distinct f.id as id, f.federationID as fed, f.created as created, '
            '     f.issueDesc as desc, f.issueQuery as query , u.username as user, f.issueStatus as status'
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
@login_required
def feedback_details():
    try:
        iid = request.args['id']
    except KeyError:
        return Response(json.dumps({}), mimetype='application/json')
    db = get_db()
    print(iid)
    if iid is None:
        return Response(json.dumps({}), mimetype='application/json')
    else:
        feedback_data = db.execute(
            'SELECT distinct projVar, projPred, rowData '
            ' FROM feedbackdata WHERE reportID="' + iid + '" '
        ).fetchone()

        if feedback_data is None:
            return Response(json.dumps({}), mimetype='application/json')

        data = {
            'var': feedback_data['projVar'],
            'pred': feedback_data['projPred'],
            'row': json.loads(feedback_data['rowData'])}
        return Response(json.dumps(data), mimetype='application/json')
