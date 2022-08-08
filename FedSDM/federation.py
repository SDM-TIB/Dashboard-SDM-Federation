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
from FedSDM.ui.utils import (
    get_mtconns, get_num_properties, get_num_rdfmts, get_datasources, get_federations, get_federation_stats
)

bp = Blueprint('federation', __name__, url_prefix='/federation')

logger = get_logger('federation')


@bp.route('/')
def index():
    feds = get_federations()
    g.federations = feds
    if 'fed' in session:
        if session['fed'] not in [f['uri'] for f in feds]:
            del session['fed']

    return render_template('federation/index.html', fedStats=get_federation_stats(), federations=g.federations)


@bp.route('/stats')
@login_required
def stats():
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
    stats = {}
    datasources = get_datasources(graph)
    for datasource in list(datasources.keys()):
        nummts = get_num_rdfmts(graph, datasource)
        props = get_num_properties(graph, datasource)
        linkss = get_mtconns(graph, datasource)
        stat = {
            'rdfmts': nummts,
            'links': linkss,
            'triples': datasources[datasource]['triples'] if 'triples' in datasources[datasource] else -1,
            'properties': props,
            'ds': datasources[datasource]['source']
        }
        stats[datasources[datasource]['source']] = stat
    return stats


@bp.route('/create', methods=['POST'])
def create():
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
    try:
        graph = request.args['graph']
        if 'dstype' in request.args:
            ds_type = request.args['dstype']
        else:
            ds_type = None

    except KeyError:
        print('KeyError:', request.args)
        return Response(json.dumps({}), mimetype='application/json')

    if graph == 'All':
        graph = None

    ds_type = DataSourceType.from_str(ds_type)

    if graph is not None:
        session['fed'] = graph

    res = get_datasource(graph, ds_type)

    # print(json.dumps({'data': res}, indent=True))
    return Response(json.dumps({'data': res}), mimetype='application/json')


@bp.route('/addsource', methods=['POST'])
def api_add_source():
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
        # statusqueues[ds.name] = queue

    return Response(json.dumps(res), mimetype='application/json')


def add_data_source(federation: str, datasource: DataSource):
    """
     0 - data source added but not accessible to create MTS
     1 - data source added and MTs are being created

    :param federation:
    :param datasource:
    :return:
    """
    mdb = get_mdb()
    # username and password are optional
    mgr = RDFMTMgr(mdb.query_endpoint, mdb.update_endpoint, 'dba', 'dba', federation)
    outqueue = Queue()
    logger.info(datasource.dstype)
    if datasource.dstype == DataSourceType.SPARQL_ENDPOINT:
        if not datasource.isAccessible():
            data = datasource.to_rdf()
            insertquery = 'INSERT DATA { GRAPH <' + federation + '> { ' + ' . \n'.join(data) + '} }'
            rr = mdb.update(insertquery)
            logger.info(datasource.url, 'endpoints cannot be accessed. Please check if you write URLs properly!')
            if rr:
                return {'status': -1}, None
            else:
                return {'status': -2}, None
        data = datasource.to_rdf()
        insertquery = 'INSERT DATA { GRAPH <' + federation + '> { ' + ' . \n'.join(data) + '} }'
        rr = mdb.update(insertquery)
        p = Process(target=mgr.create, args=(datasource, outqueue, [], ))
        p.start()
        logger.info('Collecting RDF-MTs started')
        return {'status': 1}, outqueue
    else:
        data = datasource.to_rdf()
        insertquery = 'INSERT DATA { GRAPH <' + federation + '> { ' + ' . \n'.join(data) + '} }'
        rr = mdb.update(insertquery)
        if rr:
            logger.info('non triple store source added')
            return {'status': 0}, None
        else:
            return {'status': -2}, None


@bp.route('/editsource', methods=['POST'])
def api_edit_source():
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

        if not ds.isAccessible():
            if rr:
                return {'status': -1}, None
            else:
                return {'status': -2}, None
        else:
            # TODO: Is it a good idea to re-create the MTs here?
            mgr = RDFMTMgr(mdb.query_endpoint, mdb.update_endpoint, 'dba', 'dba', fed)
            outqueue = Queue()
            p = Process(target=mgr.create, args=(ds, outqueue, [],))
            p.start()
            logger.info('Collecting RDF-MTs started')
            return {'status': 1}, outqueue
    except KeyError:
        logger.error('KeyError: ' + str(request.form.keys()))
        return Response(json.dumps({}), mimetype='application/json')


@bp.route('/api/findlinks', methods=['GET', 'POST'])
def api_findlinks():
    try:
        fed = request.args.get('fed', None)
        ds = request.args.get('datasource', None)
    except KeyError:
        return Response(json.dumps({}),  mimetype='application/json')
    if fed is None:
        return Response(json.dumps({}), mimetype='application/json')
    res, queue = findlinks(fed, ds)

    return Response(json.dumps(res), mimetype='application/json')


def findlinks(federation: str, datasource: str):
    mdb = get_mdb()
    mgr = RDFMTMgr(mdb.query_endpoint, mdb.update_endpoint, 'dba', 'dba', federation)
    outqueue = Queue()
    p = Process(target=mgr.create_inter_ds_links, args=(datasource, outqueue,))
    p.start()
    return {'status': 1}, outqueue


@bp.route('/api/recreatemts')
def api_recreatemts():
    try:
        fed = request.args.get('fed', None)
        ds = request.args.get('datasource', None)
    except KeyError:
        return Response(json.dumps({}), mimetype='application/json')
    if fed is None or ds is None:
        return Response(json.dumps({}), mimetype='application/json')
    res, queue = recreatemts(fed, ds)

    return Response(json.dumps(res), mimetype='application/json')


def recreatemts(federation: str, ds: str):
    mdb = get_mdb()
    mgr = RDFMTMgr(mdb.query_endpoint, mdb.update_endpoint, 'dba', 'dba', federation)
    outqueue = Queue()
    datasource = mgr.get_source(ds)
    if len(datasource) > 0:
        datasource = datasource[0]
        datasource = DataSource(
            ds,
            datasource['url'],
            datasource['dstype'],
            name=datasource['name'],
            desc=datasource['desc'] if 'desc' in datasource else '',
            params=datasource['params'] if 'params' in datasource else {},
            keywords=datasource['keywords'] if 'keywords' in datasource else '',
            version=datasource['version'] if 'version' in datasource else '',
            homepage=datasource['homepage'] if 'homepage' in datasource else '',
            organization=datasource['organization'] if 'organization' in datasource else '',
            ontology_graph=datasource['ontology_graph'] if 'ontology_graph' in datasource else None
        )
        p = Process(target=mgr.create, args=(datasource, outqueue, [], True,))
        p.start()
        return {'status': 1}, outqueue
    return {'status': -1}, None


def get_federation(id: str, check_owner: bool = True):
    federation = get_db().execute(
        'SELECT f.id, name, description, created, username, owner_id'
        ' FROM federation f JOIN user u ON f.owner_id = u.id'
        ' WHERE f.id = ?', (id,)
    ).fetchone()

    if federation is None:
        abort(404, 'Federation id {0} does not exist.'.format(id))

    if check_owner and federation['owner_id'] != g.user['id']:
        abort(403)

    return federation


def create_federation(name: str, desc: str, is_public: bool):
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
    res = mdb.update(insert_query)
    res = mdb.update('CREATE GRAPH <' + uri + '>')
    if res:
        return uri
    else:
        return None


def get_datasource(graph: str = None, dstype=None):
    mdb = get_mdb()
    if graph is not None:
        query = 'SELECT DISTINCT * WHERE { GRAPH <' + graph + '> {\n'
        if dstype is None:
            query += '  OPTIONAL { ?id mt:dataSourceType ?dstype . }\n'
        elif isinstance(dstype, list) and len(dstype) > 0:
            query += '  ?id mt:dataSourceType ?dstype .\n'
            filters = []
            for dt in dstype:
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
def update(id):
    federation = get_federation(id)

    if request.method == 'POST':
        name = request.form['name']
        description = request.form['description']
        ispublic = 'public' in request.form
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
                (name, description, ispublic, g.user['id'])
            )
            db.commit()
            return redirect(url_for('federation.index'))

    return render_template('federation/update.html', federation=federation)


@bp.route('/<int:id>/delete', methods=['POST'])
@login_required
def delete(id):
    federation = get_federation(id)
    db = get_db()
    db.execute('DELETE FROM federation WHERE id = ?', (id,))
    db.commit()
    return redirect(url_for('federation.index'))
