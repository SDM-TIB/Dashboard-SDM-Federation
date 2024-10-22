import hashlib
import json
import os
import traceback
from multiprocessing import Queue
from time import time

from DeTrusty import Decomposer, Planner
from DeTrusty.Wrapper.RDFWrapper import contact_source
from flask import (
    Blueprint, g, render_template, session, Response, request
)
from flask.json import jsonify
from webargs import fields
from webargs.flaskparser import use_kwargs

from FedSDM import get_logger
from FedSDM.auth import login_required
from FedSDM.config import ConfigSimpleStore
from FedSDM.db import get_db, get_mdb
from FedSDM.utils import get_federations

bp = Blueprint('query', __name__, url_prefix='/query')

logger = get_logger('query')
result_queues = {}


@bp.route('/query')
@login_required
def query() -> str:
    """Serves requests to '/query/query'.

    This route serves the main page of the querying functionality of FedSDM.
    The query page includes a drop-down menu with all available federations
    as well as a query editor and space for the query result. Additionally,
    a list of example queries is provided on the right-hand side.

    Note
    ----
    The request is only served for logged-in users.

    Returns
    -------
    str
        Rendered template of the query page with all available federations.

    """
    if 'federations' not in g:
        federations = get_federations()
        g.federations = federations
    if 'fed' in session:
        if session['fed'] not in [f['uri'] for f in g.federations]:
            del session['fed']
    return render_template('query/index.jinja2', federations=g.federations)


@bp.route('/feedback', methods=['POST'])
#@use_kwargs({'fed': fields.Str(required=True)}, location='query')
@use_kwargs({
    'fed': fields.Str(required=True),
    'pred': fields.Str(required=True),
    'row': fields.List(fields.Str(), required=True, data_key='row[]'),
    'columns': fields.List(fields.Str(), required=True, data_key='columns[]'),
    'query_': fields.Str(required=True, data_key='query'),
    'desc': fields.Str(required=True)
}, location='form')
@login_required
def feedback(fed, pred, row, columns, query_, desc) -> Response:
    """Serves requests to '/query/feedback'.

    This route receives the content from the feedback form and stores
    it in a database to be checked, confirmed, and solved later.
    The request needs to have the argument 'fed' identifying the federation
    the query in question was executed against. The payload of the request
    should include the following:
        - pred -- the predicate for which the object seems to be wrong
        - row[] -- the query result (row) which seems to be wrong
        - columns[] -- a list with the columns from the query result
        - query -- the query in question
        - desc -- a short text describing what the problem is

    Note
    ----
    This route only accepts POST requests.

    The request is only served for logged-in users.

    Returns
    -------
    flask.Response
        A JSON response with an empty JSON object.

    """
    selected_row = {}
    for c, v in zip(columns, row):
        selected_row[c] = v
    logger.debug('extracted information in feedback(): ' + fed + ', ' + pred + ', ' + query_ +
                 ', ' + str(selected_row) + ', ' + desc)

    user_id = session.get('user_id')

    db = get_db()
    db.execute(
        'INSERT INTO feedbackreport (userID, federationID, issueDesc, issueQuery)'
        ' VALUES (?, ?, ?, ?)',
        (user_id, fed, desc, query_)
    )
    db.commit()
    fdb = db.execute(
        'SELECT id '
        ' FROM feedbackreport '
        ' WHERE userID=' + str(user_id) + ' AND issueDesc = "' + desc + '" AND issueQuery="' + query_ + '"'
    ).fetchone()
    ds_id = fdb['id']
    logger.debug('last inserted row selected in feedback(): ' + str(ds_id))
    db.execute(
        'INSERT INTO feedbackdata (reportID, projVar, projPred, rowData)'
        ' VALUES (?, ?, ?, ?)',
        (ds_id, ','.join(list(selected_row.keys())), pred, str(json.dumps(selected_row)))
    )
    db.commit()

    return Response(json.dumps({}), mimetype='application/json')


def finalize(process_queue: Queue):
    """Stops all processes in a queue.

    Empties a queue holding processes and stops all processes in that queue.

    Parameters
    ----------
    process_queue : multiprocessing.Queue
        The queue holding the processes to stop.

    """
    p = process_queue.get()
    while p != 'EOF':
        try:
            os.kill(p, 9)
        except OSError as ex:
            logger.exception(ex)
            pass
        p = process_queue.get()


@bp.route('/nextresult', methods=['POST', 'GET'])
def get_next_result() -> Response:
    """Serves requests to '/query/nextresult'.

    This method retrieves the next result from a previously started query.
    Session cookies are used to figure out which of the running queries
    need to be served for the request.
    If there is another result available, the response will include the
    following information:
        - vars -- a list with the variables occurring in the query
        - result -- a dictionary with the next result for the query
        - time_total -- time elapsed until now since the query was started
        - time_first -- time elapsed between starting the query and retrieving the first result
        - total_rows -- set to 1 since only a single result will be returned

    Returns
    -------
    flask.Response
        A JSON response including the next query result. If an error
        occurred, then the key 'error' will be added with the appropriate
        value to tell the user what happened.

    """
    vars_ = session['vars']
    start = session['start']
    first = session['first']
    if 'hashquery' in session and session['hashquery'] in result_queues:
        output = result_queues[session['hashquery']]['output']
        process = result_queues[session['hashquery']]['process']
    else:
        total = time() - start
        return jsonify(time_total=total, time_first=first, total_rows=1, result='EOF', error='Already finished')
    try:
        r = output.get()
        total = time() - start
        if r == 'EOF':
            finalize(process)
            del result_queues[session['hashquery']]
            del session['hashquery']

        return jsonify(vars=vars_, result=r, time_total=total, time_first=first, total_rows=1)
    except Exception:
        import sys
        exc_type, exc_value, exc_traceback = sys.exc_info()
        emsg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
        logger.error('Exception while returning incremental results... ' + emsg)
        total = time() - start
        return jsonify(time_total=total, time_first=first, total_rows=1, result=[], error=str(emsg))


@bp.route('/sparql', methods=['POST', 'GET'])
def sparql() -> Response:
    """Serves requests to '/query/sparql'.

    Starts the execution of a SPARQL query using `DeTrusty`_.
    The request needs to include the following parameters:
        - query -- the SPARQL query to be executed
        - federation -- the federation to execute the query against

    Returns
    -------
    flask.Response
        A JSON response including the variables of the query, the triple
        patterns of the query, the first result(s), current time since
        the start of the execution time, time to the first result, and
        the total number of results. The response might not include all
        the previously mentioned fields if an error occurred. In that case,
        the response will include an error message instead.

    .. _DeTrusty:
        https://github.com/SDM-TIB/DeTrusty

    """
    try:
        query_ = request.args.get('query', '')
        federation = request.args.get('federation', None)
        session['fed'] = federation

        query_ = query_.replace('\n', ' ').replace('\r', ' ')
        logger.info('federation: ' + federation + '\nquery: ' + query_)
        session['hashquery'] = str(hashlib.md5(query_.encode()).hexdigest())
        if federation is None or len(federation) < 6:
            return jsonify({'result': [], 'error': 'Please select the federation you want to query'})
        if query_ is None or len(query_) == 0:
            return jsonify({'result': [], 'error': 'cannot read query'})

        output = Queue()
        vars_, res, start, total, first, i, process_queue, triple_patterns = execute_query(federation, query_, output)
        result_queues[session['hashquery']] = {'output': output, 'process': process_queue}
        if res is None or len(res) == 0:
            del result_queues[session['hashquery']]
            del session['hashquery']
            return jsonify(vars=vars_, result=[], time_total=total, time_first=first, total_rows=1)

        if vars_ is None:
            logger.warning('no results during decomposition: ' + query_)
            del result_queues[session['hashquery']]
            return jsonify({'result': [],
                            'error': 'Cannot execute query on this federation. No matching molecules found'})

        session['start'] = start
        session['vars'] = vars_
        session['first'] = first
        session['fed'] = federation
        process_queue.put('EOF')
        triple_patterns = [
            {'s': t.subject.name, 'p': t.predicate.name, 'o': t.theobject.name} for t in triple_patterns
        ]
        return jsonify(vars=vars_,
                       query_triples=triple_patterns,
                       result=res,
                       time_total=total,
                       time_first=first,
                       total_rows=i)
    except Exception as e:
        import sys
        exc_type, exc_value, exc_traceback = sys.exc_info()
        emsg = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
        logger.exception(e)
        logger.exception('Exception while semantifying: ' + emsg)
        return jsonify({'result': [], 'error': str(emsg)})


def execute_query(graph: str, query_: str, output: Queue = Queue()):
    """Executes a SPARQL query using DeTrusty.

    Executes a SPARQL query using the federated query engine `DeTrusty`_.
    The configuration of DeTrusty is retrieved from the :class:`FedSDM.db.MetadataDB`.

    Parameters
    ----------
    graph : str
        Identifier of the federation to be queried.
    query_ : str
        The SPARQL query to be executed.
    output : multiprocessing.Queue, optional
        If an output queue is given, the results of the query can be
        retrieved incrementally from the queue. Otherwise, the result
        can also be taken from the return value but in a blocking fashion.

    Returns
    -------
    (list | None, list | None, float, float, float, int, multiprocessing.Queue | None, list)
        A tuple with different information about the query execution:
            1. A list of variables occurring in the query; None when an error occurred
            2. A list of all the results to the query, each result is a dictionary
               with the variables being the key and t; None instead of a list when an error occurred
            3. The time when the query execution was started; 1 if an error occurred
            4. The time elapsed between starting the execution and retrieving the first result; 1 if an error occurred
            5. The time elapsed between starting the execution and retrieving the last result; 1 if an error occurred
            6. The number of results; if an error occurs, the value is 0
            7. A queue holding all the processes started during the query execution; None if an error occurred
            8. A list of all triple patterns in the query; if an error occurs, the list will be empty

    .. _DeTrusty:
        https://github.com/SDM-TIB/DeTrusty

    """
    mdb = get_mdb()
    config = ConfigSimpleStore(graph, mdb.query_endpoint, mdb.update_endpoint, 'dba', 'dba123')
    start = time()
    decomposer = Decomposer(query_, config)
    decomposed_query = decomposer.decompose()
    logger.info(decomposed_query)
    if decomposed_query is None:
        logger.warning('Decomposer returned None. '
                       'It might be that the query cannot be answered by the endpoints in the federation.')
        return None, None, 1, 1, 1, 0, None, []

    planner = Planner(decomposed_query, True, contact_source, 'RDF', config)
    plan = planner.createPlan()

    res = []
    logger.info(plan)
    process_queue = Queue()
    plan.execute(output, process_queue)

    i = 0
    r = output.get()
    variables = [p.name[1:] for p in decomposer.query.args]
    first = time() - start

    if r == 'EOF':
        logger.debug('END of results...')
        first = 0
    else:
        if len(variables) == 0 or (len(variables) == 1 and variables[0] == '*'):
            variables = [k for k in r.keys()]
        logger.debug(r)
        res.append(r)
        i += 1
    total = time() - start
    return variables, res, start, total, first, i, process_queue, decomposer.alltriplepatterns
