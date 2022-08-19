import datetime as dtime
import json
from multiprocessing import Process, Queue

from flask import (
    Blueprint, flash, g, redirect, render_template, session, Response, request, url_for, abort
)

from FedSDM import get_logger
from FedSDM.auth import login_required
from FedSDM.db import get_db, get_mdb
from FedSDM.rdfmt import RDFMTMgr
from FedSDM.rdfmt.model import *
from FedSDM.util import (
    get_num_mt_links, get_num_properties, get_num_rdfmts, get_datasources, get_federations, get_federation_stats
)

bp = Blueprint('federation', __name__, url_prefix='/federation')

logger = get_logger('federation')


@bp.route('/')
def index():
    """Serves the page '/federation'.

    This route serves the federation page of the dashboard. It contains statistics about the
    available federations as well as the datasources. The page also provides the management
    of federations and datasources. Note that the individual statistics shown and actions
    performed are actually executed via other routes called via AJAX.

    Returns
    -------
    str
        Rendered template of the federation page with all available federations.

    """
    feds = get_federations()
    g.federations = feds
    if 'fed' in session:
        if session['fed'] not in [f['uri'] for f in feds]:
            del session['fed']

    return render_template('federation/index.html', fedStats=get_federation_stats(), federations=g.federations)


@bp.route('/stats')
@login_required
def stats():
    """Serves requests send to '/federation/stats'.

    This route provides statistics about the federation(s). Those statistics include the number of
    triples, RDF Molecule Templates, properties, and links between RDF Molecule Templates per
    datasource in the federation.

    Note
    ----
    The request has to include the parameter 'graph' identifying a federation. If the specified
    federation exists, the statistics for it are returned. However, if the specified value is 'All',
    then the statistics for **all** available federations are included in the answer to the request.

    The request is only served for logged-in users.

    Returns
    -------
    flask.Response
        A JSON response including the statistics about the federation(s).

    """
    try:
        graph = request.args['graph']
    except KeyError:
        print('KeyError:', request.args)
        return Response(json.dumps({}), mimetype='application/json')

    stats = {}
    if graph is not None:
        session['fed'] = graph
        if graph == 'All':
            federations = get_federations()
            for fed in federations:
                stats.update(get_stats(fed['uri']))
        else:
            stats.update(get_stats(graph))

    return Response(json.dumps({'data': stats}), mimetype='application/json')


def get_stats(graph: str):
    """Gets the statistics for all datasources in the specified federation.

    This method provides the following statistics for each datasource in the federation:
        - number of RDF Molecule Templates
        - number of links between the RDF Molecule Templates
        - number of triples
        - number of properties (predicates)
        - URI of the datasource

    Parameters
    ----------
    graph : str
        The identifier of the federation for which to return the statistics about the datasource.

    Returns
    -------
    dict
        A dictionary with the above-mentioned statistics about the datasources in the
        federation. If there are no datasource in the federation or if the federation
        does not exist, the dictionary will be empty.

    """
    stats = {}
    datasources = get_datasources(graph)
    for datasource in list(datasources.keys()):
        num_mts = get_num_rdfmts(graph, datasource)
        props = get_num_properties(graph, datasource)
        num_links = get_num_mt_links(graph, datasource)
        stat = {
            'rdfmts': num_mts,
            'links': num_links,
            'triples': datasources[datasource]['triples'] if 'triples' in datasources[datasource] else -1,
            'properties': props,
            'ds': datasources[datasource]['source']
        }
        stats[datasources[datasource]['source']] = stat
    return stats


@bp.route('/create', methods=['POST'])
def create():
    """Serves requests to '/federation/create'.

    This method creates a new federation based on the provided data.
    The following parameters are valid for this request:
        - name -- the name of the federation
        - description -- a short description what the federation is about
        - public -- indicating whether the federation should be publicly accessible (optional)

    Note
    ----
    This route only accepts POST requests.

    Returns
    -------
    flask.Response
        A plain text response with the identifier of the federation if it was
        created successfully, None otherwise.

    """
    name = request.form['name']
    description = request.form['description']
    is_public = 'public' in request.form
    error = None

    if not name:
        error = 'Name is required. '
    federation = ''
    if error is not None:
        flash(error)
    else:
        inserted = create_federation(name, description, is_public)

        if inserted is None:
            error = 'Cannot insert new federation to endpoint.'
            flash(error)
        else:
            federation = inserted
            session['fed'] = federation
    return Response(federation, mimetype='text/plain')


@bp.route('/datasources', methods=['GET'])
@login_required
def datasources():
    """Serves requests send to '/federation/datasources'.

    This method provides the following metadata for all datasources in the specified federation:
        - identifier -- the internal identifier
        - name -- the human-readable name
        - URL -- address of the datasource
        - datasource type -- type, e.g., SPARQL endpoint
        - homepage -- homepage of the dataset if any
        - version -- version of the dataset if any
        - keywords -- optional keywords
        - parameters -- parameters used to connect to the source
        - description -- a short description about the dataset
        - organization -- organization publishing the dataset if any

    Note
    ----
    The request has to include the parameter 'graph' identifying a federation. If the specified
    federation exists, the metadata for all its datasources are returned. However, if the
    specified value is 'All', then the metadata for **all** datasources are retrieved.

    When setting the parameter 'ds_type', it is also possible to limit the output to datasources
    of a specific type. However, at this point only SPARQL endpoints are supported.

    This route only accepts GET requests.

    The request is only served for logged-in users.

    Returns
    -------
    flask.Response
        A JSON response with the above-mentioned metadata about the datasources.

    """
    try:
        graph = request.args['graph']
    except KeyError:
        logger.error("Key 'graph' not found in the parameters of the request. List of parameters: " + str(request.args))
        return Response(json.dumps({}), mimetype='application/json')

    if graph == 'All':
        graph = None

    if graph is not None:
        session['fed'] = graph

    ds_type = DataSourceType.from_str(request.args['dstype'] if 'dstype' in request.args else None)
    res = get_datasources(graph, ds_type)

    return Response(json.dumps({'data': res}), mimetype='application/json')


@bp.route('/addsource', methods=['POST'])
def api_add_source():
    """Serves requests to '/federation/addsource'.

    This method creates new datasource based on the provided data.
    If possible, the metadata for the datasource is collected.
    The following parameters are valid for this request:
        - fed -- specifying to which federation the datasource will be added
        - url -- the URL of the datasource
        - dstype -- specifying the type of the datasource
        - name -- human-readable name for the datasource
        - desc -- short description of the data (optional)
        - params -- parameters used for connecting to the source (optional)
        - keywords -- keywords categorizing the dataset (optional)
        - version -- version of the dataset (optional)
        - homepage -- homepage of the dataset providing additional information (optional)
        - organization -- organization publishing the dataset (optional)
        - ontology_graph -- URL of the endpoint providing the ontology for the datasource (optional, unused)

    Note
    ----
    This route only accepts POST requests.

    Returns
    -------
    flask.Response
        A JSON response indicating the status of adding the new datasource. Negative values indicate an error.
        0 if the source is accessible but no RDF Molecule Templates can be extracted for it. 1 for success.

    """
    try:
        e = request.form
        fed = request.args['fed']
        if fed is None or len(fed) == 0:
            return Response(json.dumps({}), mimetype='application/json')
        session['fed'] = fed

        prefix = 'http://ontario.tib.eu/'
        rid = prefix + fed[fed.rfind('/')+1:] + '/datasource/' + urlparse.quote(e['name'].replace(' ', '-'), safe=':')
        ds = DataSource(
            rid,
            e['url'],
            e['dstype'],
            name=e['name'],
            desc=e['desc'] if 'desc' in e else '',
            params=e['params'] if 'params' in e else {},
            keywords=e['keywords'] if 'keywords' in e else '',
            version=e['version'] if 'version' in e else '',
            homepage=e['homepage'] if 'homepage' in e else '',
            organization=e['organization'] if 'organization' in e else '',
            ontology_graph=e['ontology_graph'] if 'ontology_graph' in e else None
        )
    except KeyError:
        print('KeyError: ', request.args)
        return Response(json.dumps({}),  mimetype='application/json')

    res, queue = add_data_source(fed, ds)
    if res['status'] == 1:
        if 'mtcreation' not in session:
            session['mtcreation'] = [ds.name]

    return Response(json.dumps(res), mimetype='application/json')


def add_data_source(federation: str, datasource: DataSource):
    """Adds a new datasource to the metadata knowledge graph.

    The given DataSource object is transformed to RDF triples and added to the
    federation in the metadata knowledge graph using :class:`FedSDM.db.MetadataDB`.

    Parameters
    ----------
    federation : str
        The identifier of the federation the datasource should be added to.
    datasource : FedSDM.rdfmt.model.DataSource
        The DataSource object representing the datasource to be added.

    Returns
    -------
    (dict, Queue | None)
        Returns a tuple with a dictionary indicating the status of adding the
        datasource and an optional queue holding the process collecting the
        metadata from the datasource.

        Negative status values represent an error while adding the source.
        A value of 0 (zero) means that the source is accessible but no RDF
        Molecule Templates can be collected for it. A status value of 1 (one)
        represents a success in adding the source; in this case also the queue is returned.

    """
    mdb = get_mdb()
    # username and password are optional
    mgr = RDFMTMgr(mdb.query_endpoint, mdb.update_endpoint, 'dba', 'dba', federation)
    out_queue = Queue()
    logger.info(datasource.ds_type)
    if datasource.ds_type == DataSourceType.SPARQL_ENDPOINT:
        if not datasource.is_accessible():
            data = datasource.to_rdf()
            insert_query = 'INSERT DATA { GRAPH <' + federation + '> { ' + ' . \n'.join(data) + '} }'
            rr = mdb.update(insert_query)
            logger.info(datasource.url, 'endpoints cannot be accessed. Please check if you write URLs properly!')
            if rr:
                return {'status': -1}, None
            else:
                return {'status': -2}, None
        data = datasource.to_rdf()
        insert_query = 'INSERT DATA { GRAPH <' + federation + '> { ' + ' . \n'.join(data) + '} }'
        mdb.update(insert_query)
        p = Process(target=mgr.create, args=(datasource, out_queue, [], ))
        p.start()
        logger.info('Collecting RDF-MTs started')
        return {'status': 1}, out_queue
    else:
        data = datasource.to_rdf()
        insert_query = 'INSERT DATA { GRAPH <' + federation + '> { ' + ' . \n'.join(data) + '} }'
        rr = mdb.update(insert_query)
        if rr:
            logger.info('non triple store source added')
            return {'status': 0}, None
        else:
            return {'status': -2}, None


@bp.route('/editsource', methods=['POST'])
def api_edit_source():
    """Serves requests to '/federation/editsource'.

    This method edits an existing datasource based on the provided data.
    If possible, the metadata of the datasource will be updated.
    The following parameters are valid for this request:
        - fed -- specifying to which federation the datasource belongs
        - url -- the URL of the datasource
        - dstype -- specifying the type of the datasource
        - name -- human-readable name for the datasource
        - desc -- short description of the data (optional)
        - params -- parameters used for connecting to the source (optional)
        - keywords -- keywords categorizing the dataset (optional)
        - version -- version of the dataset (optional)
        - homepage -- homepage of the dataset providing additional information (optional)
        - organization -- organization publishing the dataset (optional)
        - ontology_graph -- URL of the endpoint providing the ontology for the datasource (optional, unused)

    Note
    ----
    This route only accepts POST requests.

    Returns
    -------
    flask.Response | (dict, Queue | None)
        Returns a tuple with a dictionary indicating the status of adding the
        datasource and an optional queue holding the process collecting the
        metadata from the datasource.
        Negative status values represent an error while editing the source.
        A status value of 1 (one) represents a success in editing the source;
        in this case also the queue is returned.

        If one of the required parameters is not present in the request,
        a JSON response indicating the KeyError will be returned.

    """
    try:
        fed = request.args['fed']
        if fed is None or len(fed) == 0:
            return Response(json.dumps({}), mimetype='application/json')
        session['fed'] = fed

        e = request.form
        ds = DataSource(
            e['id'],
            e['url'],
            e['dstype'],
            name=e['name'],
            desc=e['desc'] if 'desc' in e else '',
            params=e['params'] if 'params' in e else {},
            keywords=e['keywords'] if 'keywords' in e else '',
            version=e['version'] if 'version' in e else '',
            homepage=e['homepage'] if 'homepage' in e else '',
            organization=e['organization'] if 'organization' in e else '',
            ontology_graph=e['ontology_graph'] if 'ontology_graph' in e else None
        )
        data = ds.to_rdf(update=True)
        mdb = get_mdb()
        insert_query = 'INSERT { ' + ' . \n'.join(data) + ' }'
        delete_query = 'DELETE { <' + e['id'] + '> ?p ?o . }'
        where_query = 'WHERE { <' + e['id'] + '> ?p ?o .\n' \
                      '  FILTER( ?p != <http://purl.org/dc/terms/created> && ' \
                      '    ?p != mt:triples )\n}'
        rr = mdb.update('WITH GRAPH <' + fed + '>\n' + delete_query + '\n' + insert_query + '\n' + where_query)

        if not ds.is_accessible():
            if rr:
                return {'status': -1}, None
            else:
                return {'status': -2}, None
        else:
            # TODO: Is it a good idea to re-create the MTs here?
            mgr = RDFMTMgr(mdb.query_endpoint, mdb.update_endpoint, 'dba', 'dba', fed)
            out_queue = Queue()
            p = Process(target=mgr.create, args=(ds, out_queue, [],))
            p.start()
            logger.info('Collecting RDF-MTs started')
            return {'status': 1}, out_queue
    except KeyError:
        logger.error('KeyError: ' + str(request.form.keys()))
        return Response(json.dumps({}), mimetype='application/json')


@bp.route('/api/findlinks', methods=['GET', 'POST'])
def api_find_links():
    """Serves requests to '/federation/api/findlinks'.

    This method finds links between the RDF Molecule Templates of a datasource.
    The parameter 'fed' has to be present in order to identify the federation
    in which the links should be searched. If the parameter 'datasource' is
    present, all links for that particular source are searched, otherwise
    the links between all datasource of the federation will be checked.

    Returns
    -------
    flask.Response
        A JSON response indicating that the process has been started.
        The response will be empty if the parameter 'fed' was not present.

    """
    try:
        fed = request.args.get('fed')
        ds = request.args.get('datasource', None)
    except KeyError:
        return Response(json.dumps({}),  mimetype='application/json')

    res, _ = find_links(fed, ds)
    return Response(json.dumps(res), mimetype='application/json')


def find_links(federation: str, datasource: str):
    """Starts the process of finding the links between datasources.

    Starts the process to search for links between the RDF Molecule Templates
    of different sources within the same federation. Depending on the input,
    the process will look for all links for one particular source or between
    all the sources in the specified federation.

    Parameters
    ----------
    federation : str
        The identifier of the federation in which the links should be search for.
    datasource : str
        The identifier of the datasource for which to search for links to other sources.
        If it is None, then all source in the federation will be considered.

    Returns
    -------
    (dict, Queue)
        The dictionary indicates that the process has been started.
        The queue will receive results from the process.

    """
    mdb = get_mdb()
    mgr = RDFMTMgr(mdb.query_endpoint, mdb.update_endpoint, 'dba', 'dba', federation)
    out_queue = Queue()
    p = Process(target=mgr.create_inter_ds_links, args=(datasource, out_queue,))
    p.start()
    return {'status': 1}, out_queue


@bp.route('/api/recreatemts')
def api_recreate_mts():
    """Serves requests to '/federation/api/recreatemts'.

    This method recreates the RDF Molecule Templates of a datasource.
    The parameters 'fed' and 'datasource' need to be present to correctly
    identify the federation and datasource for which the RDF Molecule
    Templates should be recomputed.

    Returns
    -------
    flask.Response
        A JSON response indicating whether the process has been started.
        The response will be empty if one of the parameters is not present.

    """
    try:
        fed = request.args.get('fed')
        ds = request.args.get('datasource')
    except KeyError:
        return Response(json.dumps({}), mimetype='application/json')

    res, _ = recreate_mts(fed, ds)
    return Response(json.dumps(res), mimetype='application/json')


def recreate_mts(federation: str, ds: str):
    """Starts the process of recreating the RDF Molecule Templates for a datasource.

    Starts the process to recreate the RDF Molecule Templates for the datasource
    identified by the parameters passed to the method.

    Parameters
    ----------
    federation : str
        The identifier of the federation to which the datasource belongs.
    ds : str
        The identifier of the datasource for which to recreate the metadata.

    Returns
    -------
    (dict, Queue | None)
        The status value in the dictionary indicates whether the process of
        recreating the RDF Molecule Templates has been started.
        In the case the process was started, the queue which will be used
        by the process to communicate, is returned as well.

    """
    mdb = get_mdb()
    mgr = RDFMTMgr(mdb.query_endpoint, mdb.update_endpoint, 'dba', 'dba', federation)
    out_queue = Queue()
    datasource = mgr.get_source(ds)
    if datasource is None:
        return {'status': -1}, None
    p = Process(target=mgr.create, args=(datasource, out_queue, [], True,))
    p.start()
    return {'status': 1}, out_queue


def get_federation(id_: str, check_owner: bool = True):
    """This method is currently unused and needs checking."""
    federation = get_db().execute(
        'SELECT f.id, name, description, created, username, owner_id'
        ' FROM federation f JOIN user u ON f.owner_id = u.id'
        ' WHERE f.id = ?', (id_,)
    ).fetchone()

    if federation is None:
        abort(404, 'Federation id {0} does not exist.'.format(id_))

    if check_owner and federation['owner_id'] != g.user['id']:
        abort(403)

    return federation


def create_federation(name: str, desc: str, is_public: bool):
    """Creates a new federation based on the provided data.

    Uses :class:`FedSDM.db.MetadataDB` to register a new federation with the data provided to the method.

    Parameters
    ----------
    name : str
        Human-readable name for the new federation.
    desc : str
        Short description what the federation is about.
    is_public : bool
        Indicating whether the federation will be publicly accessible.

    Returns
    -------
    str | None
        Returns a `str` with the identifier of the created federation.
        `None` if something went wrong during the process.

    """
    mdb = get_mdb()
    prefix = 'http://ontario.tib.eu/federation/g/'
    uri = prefix + urlparse.quote(name.replace(' ', '-'), safe='/:')
    today = str(dtime.datetime.now())

    data = [
        '<' + uri + '>  a  mt:Federation ',
        '<' + uri + '>  mt:name "' + name + '"',
        '<' + uri + '>  mt:desc "' + desc + '"',
        '<' + uri + '>  mt:ispublic ' + str(is_public),
        '<' + uri + '>  <http://purl.org/dc/terms/created> "' + today + '"',
        '<' + uri + '>  <http://purl.org/dc/terms/modified> "' + today + '"'
    ]

    insert_query = 'INSERT DATA { GRAPH <' + g.default_graph + '> {\n' + ' . \n'.join(data) + '}}'
    mdb.update(insert_query)
    res = mdb.update('CREATE GRAPH <' + uri + '>')
    if res:
        return uri
    else:
        return None


def get_datasources(graph: str = None, ds_type=None):
    """Gets all datasources of a specified federation and datasource type with their metadata.

    This method provides the following metadata for all datasources in the specified federation:
        - identifier -- the internal identifier
        - name -- the human-readable name
        - URL -- address of the datasource
        - datasource type -- type, e.g., SPARQL endpoint
        - homepage -- homepage of the dataset if any
        - version -- version of the dataset if any
        - keywords -- optional keywords
        - parameters -- parameters used to connect to the source
        - description -- a short description about the dataset
        - organization -- organization publishing the dataset if any

    Note
    ----
    If no federation identifier (parameter 'graph') is given, all federations are considered.

    If no datasource type is specified, then all datasource types are considered.

    Parameters
    ----------
    graph : str, optional
        The identifier of the federation for which the datasources should
        be retrieved. If none is given, all federations will be considered.
    ds_type : Any, op
        The datasource types to be considered, i.e., a list of string values
        representing the datasource types of interest.

    Returns
    -------
    list
        A list of all datasources in the federation matching the given datasource
        type. Each entry in the list includes the above-mentioned metadata about
        the datasources.

    """
    mdb = get_mdb()
    if graph is not None:
        query = 'SELECT DISTINCT * WHERE { GRAPH <' + graph + '> {\n'
        if ds_type is None:
            query += '  OPTIONAL { ?id mt:dataSourceType ?dstype . }\n'
        elif isinstance(ds_type, list) and len(ds_type) > 0:
            query += '  ?id mt:dataSourceType ?dstype .\n'
            filters = []
            for dt in ds_type:
                filters.append('?dstype=<' + MT_RESOURCE + 'DatasourceType/' + str(dt.value) + '>')
            query += '  FILTER (' + ' || '.join(filters) + ')\n'
        else:
            query += '  OPTIONAL { ?id mt:dataSourceType ?dstype . }\n'
        query += '  ?id a mt:DataSource .\n' \
                 '  ?id mt:name ?name .\n' \
                 '  ?id mt:url ?endpoint .\n' \
                 '  OPTIONAL { ?id mt:homepage ?homepage . }\n' \
                 '  OPTIONAL { ?id mt:version ?version . }\n' \
                 '  OPTIONAL { ?id mt:keywords ?keywords . }\n' \
                 '  OPTIONAL { ?id mt:params ?params . }\n' \
                 '  OPTIONAL { ?id mt:desc ?desc . }\n' \
                 '  OPTIONAL { ?id mt:organization ?organization . }\n' \
                 '}}'
    else:
        query = 'SELECT DISTINCT * WHERE {\n' \
                '  ?id a mt:DataSource .\n' \
                '  ?id mt:name ?name .\n' \
                '  ?id mt:url ?endpoint .\n' \
                '  OPTIONAL { ?id mt:dataSourceType ?dstype . }\n' \
                '  OPTIONAL { ?id mt:homepage ?homepage . }\n' \
                '  OPTIONAL { ?id mt:version ?version . }\n' \
                '  OPTIONAL { ?id mt:keywords ?keywords . }\n' \
                '  OPTIONAL { ?id mt:params ?params . }\n' \
                '  OPTIONAL { ?id mt:desc ?desc . }\n' \
                '  OPTIONAL { ?id mt:organization ?organization . }\n' \
                '}'
    res, card = mdb.query(query)
    if card > 0:
        data = []
        for r in res:
            dd = [
                r['id'],
                r['name'],
                r['endpoint'],
                r['dstype'][r['dstype'].rfind('/') + 1:] if 'dstype' in r else '',
                r['keywords'] if 'keywords' in r else '', r['homepage'] if 'homepage' in r else '',
                r['organization'] if 'organization' in r else '', r['desc'] if 'desc' in r else '',
                r['version'] if 'version' in r else '', r['params'] if 'params' in r else ''
            ]
            data.append(dd)
        return data
    else:
        return []


@bp.route('/<int:id>/update', methods=('GET', 'POST'))
@login_required
def update(id_):
    """This method is currently unused and needs checking."""
    federation = get_federation(id_)
    if request.method == 'POST':
        name = request.form['name']
        description = request.form['description']
        is_public = 'public' in request.form
        error = None

        if not name:
            error = 'Name is required.'

        if error is not None:
            flash(error)
        else:
            db = get_db()
            db.execute(
                'UPDATE federation SET name = ?, description = ?, is_public = ? '
                ' WHERE id = ?',
                (name, description, is_public, g.user['id'])
            )
            db.commit()
            return redirect(url_for('federation.index'))

    return render_template('federation/update.html', federation=federation)


@bp.route('/<int:id>/delete', methods=['POST'])
@login_required
def delete(id_):
    """This method is currently unused and needs checking."""
    federation = get_federation(id_)
    db = get_db()
    db.execute('DELETE FROM federation WHERE id = ?', (id_,))
    db.commit()
    return redirect(url_for('federation.index'))
