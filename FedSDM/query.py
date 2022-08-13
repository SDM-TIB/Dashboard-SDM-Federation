import hashlib
import json
import os
import traceback
from multiprocessing import Queue
from time import time

from flask import (
    Blueprint, g, render_template, session, Response, request
)
from flask.json import jsonify

from DeTrusty import Decomposer, Planner
from DeTrusty.Wrapper.RDFWrapper import contact_source
from FedSDM import get_logger
from FedSDM.auth import login_required
from FedSDM.config import ConfigSimpleStore
from FedSDM.db import get_db, get_mdb
from FedSDM.ui.utils import get_federations

bp = Blueprint('query', __name__, url_prefix='/query')

logger = get_logger('query')


@bp.route('/query')
@login_required
def query():
    if 'federations' not in g:
        federations = get_federations()
        g.federations = federations
    if 'fed' in session:
        if session['fed'] not in [f['uri'] for f in g.federations]:
            del session['fed']
    # if 'fed' in session:
    #     print(session['fed'])
    return render_template('query/index.html', federations=g.federations)


result_queues = {}


@bp.route('/feedback', methods=['POST'])
@login_required
def feedback():
    fed = request.args.get('fed', -1)
    e = request.form
    print(e)
    pred = e['pred']
    row = e.getlist('row[]')
    columns = e.getlist('columns[]')
    query = e['query']
    desc = e['desc']
    selected_row = {}
    for c, v in zip(columns, row):
        selected_row[c] = v
    print(fed, pred, query, selected_row, desc)

    user_id = session.get('user_id')

    db = get_db()
    db.execute(
        'INSERT INTO feedbackreport (userID, federationID, issueDesc, issueQuery)'
        ' VALUES (?, ?, ?, ?)',
        (user_id, fed, desc, query)
    )
    db.commit()
    fdb = db.execute(
        'SELECT id'
        ' FROM feedbackreport '
        ' where userID=' + str(user_id) + ' AND issueDesc = "' + desc + '" AND issueQuery="' + query + '"'
    ).fetchone()
    ds_id = fdb['id']
    print('Last inserted row selected: ', ds_id)
    db.execute(
        'INSERT INTO feedbackdata (reportID, projVar, projPred, rowData)'
        ' VALUES (?, ?, ?, ?)',
        (ds_id, ','.join(list(selected_row.keys())), pred, str(json.dumps(selected_row)))
    )
    db.commit()

    return Response(json.dumps({}), mimetype='application/json')


def finalize(process_queue: Queue):
    p = process_queue.get()
    while p != 'EOF':
        try:
            os.kill(p, 9)
        except OSError as ex:
            print(ex)
            pass
        p = process_queue.get()


@bp.route('/nextresult', methods=['POST', 'GET'])
def get_next_result():
    vars = session['vars']
    start = session['start']
    first = session['first']
    if 'hashquery' in session and session['hashquery'] in result_queues:
        output = result_queues[session['hashquery']]['output']
        process = result_queues[session['hashquery']]['process']
    else:
        total = time() - start
        return jsonify(execTime=total, firstResult=first, totalRows=1, result='EOF', error='Already finished')
    try:
        r = output.get()
        total = time() - start
        if r == 'EOF':
            finalize(process)
            del result_queues[session['hashquery']]
            del session['hashquery']

        return jsonify(vars=vars, result=r, execTime=total, firstResult=first, totalRows=1)
    except Exception as e:
        import sys
        exc_type, exc_value, exc_traceback = sys.exc_info()
        emsg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
        logger.error('Exception while returning incremental results .. ' + emsg)
        print('Exception: ')
        import pprint
        pprint.pprint(emsg)
        total = time() - start
        return jsonify(execTime=total, firstResult=first, totalRows=1, result= [], error= str(emsg))


@bp.route('/sparql', methods=['POST', 'GET'])
def sparql():
    if request.method == 'GET' or request.method == 'POST':
        try:
            query = request.args.get('query', '')
            federation = request.args.get('federation', None)
            session['fed'] = federation

            query = query.replace('\n', ' ').replace('\r', ' ')
            print('federation:', federation)
            print('query:', query)
            logger.info(query)
            session['hashquery'] = str(hashlib.md5(query.encode()).hexdigest())
            if federation is None or len(federation) < 6:
                return jsonify({'result': [], 'error': 'Please select the federation you want to query'})
            if query is None or len(query) == 0:
                return jsonify({'result': [], 'error': 'cannot read query'})

            output = Queue()
            variables, res, start, total, first, i, process_queue, all_triple_patterns = execute_query(federation, query, output)
            result_queues[session['hashquery']] = {'output': output, 'process': process_queue}
            if res is None or len(res) == 0:
                del result_queues[session['hashquery']]
                del session['hashquery']
                return jsonify(vars=variables, result=[], execTime=total, firstResult=first, totalRows=1)

            if variables is None:
                print('no results during decomposition', query)
                del result_queues[session['hashquery']]
                return jsonify({'result': [],
                                'error': 'cannot execute query on this federation. No matching molecules found'})

            session['start'] = start
            session['vars'] = variables
            session['first'] = first
            session['fed'] = federation
            process_queue.put('EOF')
            triple_patterns = [
                {'s': t.subject.name, 'p': t.predicate.name, 'o': t.theobject.name} for t in all_triple_patterns
            ]
            return jsonify(vars=variables, querytriples=triple_patterns, result=res, execTime=total, firstResult=first, totalRows=i)
        except Exception as e:
            import sys
            exc_type, exc_value, exc_traceback = sys.exc_info()
            emsg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
            logger.error('Exception while semantifying: ' + emsg)
            print('Exception: ', e)
            import pprint
            pprint.pprint(emsg)
            return jsonify({'result': [], 'error': str(emsg)})
    else:
        return jsonify({'result': [], 'error': 'Invalid HTTP method used. Use GET '})


def execute_query(graph: str, query: str, output: Queue = Queue()):
    mdb = get_mdb()
    config = ConfigSimpleStore(graph, mdb.query_endpoint, mdb.update_endpoint, 'dba', 'dba123')
    start = time()
    decomposer = Decomposer(query, config)
    decomposed_query = decomposer.decompose()
    logger.info(decomposed_query)
    if decomposed_query is None:
        logger.warning('Decomposer returned None. It might be that the query cannot be answered by the endpoints in the federation.')
        return None, None, 1, 1, 1, 0, None, []

    planner = Planner(decomposed_query, True, contact_source, 'RDF', config)
    plan = planner.createPlan()

    res = []
    logger.info(plan)
    processqueue = Queue()
    plan.execute(output, processqueue)

    i = 0
    r = output.get()
    variables = [p.name[1:] for p in decomposer.query.args]
    first = time() - start

    if r == 'EOF':
        print('END of results...')
        first = 0
    else:
        if len(variables) == 0 or (len(variables) == 1 and variables[0] == '*'):
            variables = [k for k in r.keys()]
        print(r)
        res.append(r)
        i += 1
    total = time() - start
    return variables, res, start, total, first, i, processqueue, decomposer.alltriplepatterns
