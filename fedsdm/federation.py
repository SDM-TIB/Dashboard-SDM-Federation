from flask import (
    Blueprint, flash, g, redirect, render_template, session, Response, send_from_directory, request, url_for
)
from werkzeug.exceptions import abort

import datetime as dtime
import json
import logging

from fedsdm.rdfmt import RDFMTMgr
from multiprocessing import Process, Queue, active_children

from fedsdm.auth import login_required
from fedsdm.db import get_db, get_mdb
from fedsdm.ui.utils import get_mtconns, get_num_properties, get_num_rdfmts, get_datasources, get_federations

from fedsdm.rdfmt.model import *

bp = Blueprint('federation', __name__, url_prefix='/federation')


logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger()
if not logger.handlers:
    logger.setLevel(logging.INFO)
    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.INFO)
    consoleHandler.setFormatter(logFormatter)
    logger.addHandler(consoleHandler)


@bp.route('/')
def index():
    db = get_db()
    # federations = db.execute(
    #     'SELECT f.id, name, description, is_public, created, username, owner_id'
    #     ' FROM federation f JOIN user u ON f.owner_id = u.id'
    #     ' ORDER BY created DESC'
    # ).fetchall()

    federations = get_federations(g.default_graph)
    g.federations = federations
    if 'fed' in session:
        if session['fed'] not in [f['uri'] for f in federations]:
            del session['fed']

    return render_template('federation/index.html', federations=g.federations)


@bp.route('/stats')
@login_required
def stats():
    try:
        graph = request.args["graph"]
    except KeyError:
        print("KeyError: ", request.args)
        return Response(json.dumps({}),
                    mimetype="application/json")

    stats = {}
    if graph is not None:
        session['fed'] = graph
        if graph == "All":
            federations = get_federations(g.default_graph)
            for fed in federations:
                stats.update(get_stats(fed['uri']))
        else:
            stats.update(get_stats(graph))

    return Response(json.dumps({"data": stats}),
                    mimetype="application/json")


def get_stats(graph):
    stats = {}
    datasources = get_datasources(graph)
    for datasource in list(datasources.keys()):
        nummts = get_num_rdfmts(graph, datasource)
        props = get_num_properties(graph, datasource)
        linkss = get_mtconns(graph, datasource)
        stat = {"rdfmts": nummts,
            "links": linkss,
            "triples": datasources[datasource]['triples'] if 'triples' in datasources[datasource] else -1,
            "properties": props,
            "ds": datasources[datasource]['source']}
        stats[datasources[datasource]['source']] = stat
    return stats

@bp.route('/create', methods=("GET", "POST"))
@login_required
def create():
    if request.method == "POST":
        name = request.form['name']
        description = request.form['description']
        ispublic = 'public' in request.form
        error = None

        if not name:
            error = "Name is rquired. "
        federation = ""
        if error is not None:
            flash(error)
        else:
            inserted = create_federation(name, description, ispublic)

            if inserted is None:
                error = "Cannot insert new federation to endpoint."
                flash(error)
            else:
                federation = inserted
                session['fed'] = federation
                logger.info(federation)
                return render_template('federation/index.html')
        return Response(federation, mimetype='text/plain')

    return render_template('federation/create.html')


@bp.route('/datasources', methods=["GET"])
@login_required
def datasources():
    try:
        graph = request.args["graph"]
        if 'dstype' in request.args:
            dstype = request.args['dstype']
        else:
            dstype = None

    except KeyError:
        print("KeyError: ", request.args)
        return Response(json.dumps({}),
                    mimetype="application/json")

    if graph == "All":
        graph = None
    if dstype is not None:
        if 'SPARQL_Endpoint' in dstype:
            dstype = DataSourceType.SPARQL_ENDPOINT
        elif 'MongoDB' in dstype:
            dstype = DataSourceType.MONGODB
        elif "Neo4j" in dstype:
            dstype = DataSourceType.NEO4J
        elif "SPARK_CSV" in dstype:
            dstype = DataSourceType.SPARK_CSV
        elif "SPARK_XML" in dstype:
            dstype = DataSourceType.SPARK_XML
        elif "SPARK_JSON" in dstype:
            dstype = DataSourceType.SPARK_JSON
        elif "SPARK_TSV" in dstype:
            dstype = DataSourceType.SPARK_TSV
        elif "REST" in dstype:
            dstype = DataSourceType.REST_SERVICE
        elif "LOCAL_CSV" in dstype:
            dstype = DataSourceType.LOCAL_CSV
        elif "LOCAL_TSV" in dstype:
            dstype = DataSourceType.LOCAL_TSV
        elif "LOCAL_JSON" in dstype:
            dstype = DataSourceType.LOCAL_JSON
        elif "LOCAL_XML" in dstype:
            dstype = DataSourceType.LOCAL_XML
        elif "MySQL" in dstype:
            dstype = DataSourceType.MYSQL
        else:
            dstype = DataSourceType.SPARQL_ENDPOINT

    if graph is not None:
        session['fed'] = graph

    res = get_datasource(graph, dstype)

    # print(json.dumps({"data": res}, indent=True))
    return Response(json.dumps({"data": res}),
                    mimetype="application/json")


@bp.route('/addsource', methods=['GET', 'POST'])
def api_add_source():
    try:
        e = request.form
        fed = request.args['fed']
        if fed is None or len(fed) == 0:
            return Response(json.dumps({}), mimetype="application/json")
        session['fed'] = fed

        prefix = 'http://ontario.tib.eu/'
        rid = prefix + fed[fed.rfind('/')+1:] + '/datasource/' + urlparse.quote(e['name'].replace(' ', "-"), safe=":")
        ds = DataSource(rid,
                        e['url'],
                        e['dstype'],
                        name=e['name'],
                        desc=e['desc'] if "desc" in e else "",
                        params=e['params'] if "params" in e else {},
                        keywords=e['keywords'] if 'keywords' in e else "",
                        version=e['version'] if 'version' in e else "",
                        homepage=e['homepage'] if 'homepage' in e else "",
                        organization=e['organization'] if 'organization' in e else "",
                        ontology_graph=e['ontology_graph'] if 'ontology_graph' in e else None
                        )
    except KeyError:
        print("KeyError: ", request.args)
        return Response(json.dumps({}),  mimetype="application/json")

    res, queue = add_data_source(fed, ds)
    if res['status'] == 1:
        if 'mtcreation' not in session:
            session['mtcreation'] = [ds.name]
        # statusqueues[ds.name] = queue

    return Response(json.dumps(res), mimetype="application/json")


def add_data_source(federation, datasource):
    """
     0 - data source added but not accessible to create MTS
     1 - data source added and MTs are being created

    :param federation:
    :param datasource:
    :return:
    """
    mdb = get_mdb()
    # username and password are optional
    mgr = RDFMTMgr(mdb.query_endpoint, mdb.update_endpoint, "dba", "dba", federation)
    outqueue = Queue()
    logger.info(datasource.dstype)
    if datasource.dstype == DataSourceType.SPARQL_ENDPOINT:
        if not datasource.isAccessible():
            data = datasource.to_rdf()
            insertquery = "INSERT DATA { GRAPH <" + federation + "> { " + " . \n".join(data) + "} }"
            rr = mdb.update(insertquery)
            logger.info(datasource.url, "endpoints cannot be accessed. Please check if you write URLs properly!")
            if rr:
                return {"status": -1}, None
            else:
                return {"status": -2}, None
        data = datasource.to_rdf()
        insertquery = "INSERT DATA { GRAPH <" + federation + "> { " + " . \n".join(data) + "} }"
        rr = mdb.update(insertquery)
        p = Process(target=mgr.create, args=(datasource, outqueue, [], ))
        p.start()
        logger.info("Collecting RDF-MTs started")
        return {"status": 1}, outqueue
    else:
        data = datasource.to_rdf()
        insertquery = "INSERT DATA { GRAPH <" + federation + "> { " + " . \n".join(data) + "} }"
        rr = mdb.update(insertquery)
        if rr:
            logger.info("non triple store source added")
            return {"status": 0}, None
        else:
            return {"status": -2}, None


@bp.route('/editsource', methods=['POST'])
def api_edit_source():
    try:
        fed = request.args['fed']
        if fed is None or len(fed) == 0:
            return Response(json.dumps({}), mimetype="application/json")
        session['fed'] = fed

        e = request.form
        ds = DataSource(e['id'],
                        e['url'],
                        e['dstype'],
                        name=e['name'],
                        desc=e['desc'] if "desc" in e else "",
                        params=e['params'] if "params" in e else {},
                        keywords=e['keywords'] if 'keywords' in e else "",
                        version=e['version'] if 'version' in e else "",
                        homepage=e['homepage'] if 'homepage' in e else "",
                        organization=e['organization'] if 'organization' in e else "",
                        ontology_graph=e['ontology_graph'] if 'ontology_graph' in e else None)
        data = ds.to_rdf(update=True)
        mdb = get_mdb()
        insertquery = "INSERT { " + " . \n".join(data) + " }"
        deletequery = "DELETE { <" + e['id'] + "> ?p ?o . }"
        wherequery = "WHERE { <" + e['id'] + "> ?p ?o .\nFILTER( ?p != <http://purl.org/dc/terms/created> && ?p != <http://tib.eu/dsdl/ontario/ontology/triples> ) }"
        rr = mdb.update("WITH GRAPH <" + fed + ">\n" + deletequery + "\n" + insertquery + "\n" + wherequery)

        if not ds.isAccessible():
            if rr:
                return {"status": -1}, None
            else:
                return {"status": -2}, None
        else:
            # TODO: Is it a good idea to re-create the MTs here?
            mgr = RDFMTMgr(mdb.query_endpoint, mdb.update_endpoint, "dba", "dba", fed)
            outqueue = Queue()
            p = Process(target=mgr.create, args=(ds, outqueue, [],))
            p.start()
            logger.info("Collecting RDF-MTs started")
            return {"status": 1}, outqueue
    except KeyError:
        logger.error("KeyError: " + str(request.form.keys()))
        return Response(json.dumps({}), mimetype="application/json")


@bp.route('/api/findlinks', methods=['GET', 'POST'])
def api_findlinks():
    try:
        fed = request.args.get("fed", None)
        ds = request.args.get('datasource', None)
    except KeyError:
        return Response(json.dumps({}),  mimetype="application/json")
    if fed is None:
        return Response(json.dumps({}), mimetype="application/json")
    res, queue = findlinks(fed, ds)

    return Response(json.dumps(res), mimetype="application/json")


def findlinks(federation, datasource):
    mdb = get_mdb()
    mgr = RDFMTMgr(mdb.query_endpoint, mdb.update_endpoint, "dba", "dba", federation)
    outqueue = Queue()
    p = Process(target=mgr.create_inter_ds_links, args=(datasource, outqueue,))
    p.start()
    return {"status": 1}, outqueue


@bp.route('/api/recreatemts')
def api_recreatemts():
    try:
        fed = request.args.get("fed", None)
        ds = request.args.get('datasource', None)
    except KeyError:
        return Response(json.dumps({}),  mimetype="application/json")
    if fed is None or ds is None:
        return Response(json.dumps({}), mimetype="application/json")
    res, queue = recreatemts(fed, ds)

    return Response(json.dumps(res), mimetype="application/json")


def recreatemts(federation, ds):
    mdb = get_mdb()
    mgr = RDFMTMgr(mdb.query_endpoint, mdb.update_endpoint, "dba", "dba", federation)
    outqueue = Queue()
    datasource = mgr.get_source(ds)
    if len(datasource) > 0:
        datasource = datasource[0]
        datasource = DataSource(ds,
                                datasource['url'],
                                datasource['dstype'],
                                name=datasource['name'],
                                desc=datasource['desc'] if "desc" in datasource else "",
                                params=datasource['params'] if "params" in datasource else {},
                                keywords=datasource['keywords'] if 'keywords' in datasource else "",
                                version=datasource['version'] if 'version' in datasource else "",
                                homepage=datasource['homepage'] if 'homepage' in datasource else "",
                                organization=datasource['organization'] if 'organization' in datasource else "",
                                ontology_graph=datasource['ontology_graph'] if 'ontology_graph' in datasource else None
                                )
        p = Process(target=mgr.create, args=(datasource, outqueue, [], True,))
        p.start()
        return {"status": 1}, outqueue
    return {"staus": -1}, None


def get_federation(id, check_owner=True):
    federation = get_db().execute(
        'SELECT f.id, name, description, created, username, owner_id'
        ' FROM federation f JOIN user u ON f.owner_id = u.id'
        ' WHERE f.id = ?', (id,)
    ).fetchone()

    if federation is None:
        abort(404, "Federation id {0} doesn't exist.".format(id))

    if check_owner and federation['owner_id'] != g.user['id']:
        abort(403)

    return federation


def create_federation(name, desc,is_public):
    mdb = get_mdb()
    prefix = 'http://ontario.tib.eu/federation/g/'
    uri = prefix + urlparse.quote(name.replace(' ', "-"), safe="/:")
    today = str(dtime.datetime.now())

    data = [
        "<" + uri + ">  a  <http://tib.eu/dsdl/ontario/ontology/Federation> ",
        "<" + uri + '>  <http://tib.eu/dsdl/ontario/ontology/name> "' + name + '"',
        "<" + uri + '>  <http://tib.eu/dsdl/ontario/ontology/desc> "' + desc + '"',
        "<" + uri + '>  <http://tib.eu/dsdl/ontario/ontology/ispublic> ' + str(is_public),
        "<" + uri + '>  <http://purl.org/dc/terms/created> "' + today + '"',
        "<" + uri + '>  <http://purl.org/dc/terms/modified> "' + today + '"'
        ]

    insertquery = "INSERT DATA { GRAPH <" + g.default_graph + ">{ " + " . \n".join(data) + "} }"
    res = mdb.update(insertquery)
    creategraph = "CREATE GRAPH <" + uri + ">"
    res = mdb.update(creategraph)
    if res:
        return uri
    else:
        return None


def get_datasource(graph=None, dstype=None):
    mdb = get_mdb()
    if graph is not None:
        query = "SELECT distinct * " \
                "WHERE { GRAPH <" + graph + "> {  "
        if dstype is None:
            query += "optional {?id <http://tib.eu/dsdl/ontario/ontology/dataSourceType> ?dstype .}"
        elif isinstance(dstype, list) and len(dstype) > 0:
            query += "?id <http://tib.eu/dsdl/ontario/ontology/dataSourceType> ?dstype ."
            filters = []
            for dt in dstype:
                filters.append(" ?dstype=<http://tib.eu/dsdl/ontario/resource/DatasourceType/" + str(dt.value) + "> ")
            query += " FILTER (" + " || ".join(filters) + ")"
        else:
            query += "optional {?id <http://tib.eu/dsdl/ontario/ontology/dataSourceType> ?dstype .}"
        query += '''                  
                ?id a <http://tib.eu/dsdl/ontario/ontology/DataSource> .
                ?id <http://tib.eu/dsdl/ontario/ontology/name> ?name .
                ?id <http://tib.eu/dsdl/ontario/ontology/url> ?endpoint .                 			                               
                optional {?id <http://tib.eu/dsdl/ontario/ontology/homepage> ?homepage .}
                optional {?id <http://tib.eu/dsdl/ontario/ontology/version> ?version .}
                optional {?id <http://tib.eu/dsdl/ontario/ontology/keywords> ?keywords .}
                optional {?id <http://tib.eu/dsdl/ontario/ontology/params> ?params .}
                optional {?id <http://tib.eu/dsdl/ontario/ontology/desc> ?desc .}
                optional {?id <http://tib.eu/dsdl/ontario/ontology/organization> ?organization .}
            }
        } 
        '''
    else:
        query = '''
                SELECT distinct *
                WHERE {                            
                        ?id a <http://tib.eu/dsdl/ontario/ontology/DataSource> .
                        ?id <http://tib.eu/dsdl/ontario/ontology/name> ?name .
                        ?id <http://tib.eu/dsdl/ontario/ontology/url> ?endpoint . 
                        optional {?id <http://tib.eu/dsdl/ontario/ontology/dataSourceType> ?dstype .}			                               
                        optional {?id <http://tib.eu/dsdl/ontario/ontology/homepage> ?homepage .}
                        optional {?id <http://tib.eu/dsdl/ontario/ontology/version> ?version .}
                        optional {?id <http://tib.eu/dsdl/ontario/ontology/keywords> ?keywords .}
                        optional {?id <http://tib.eu/dsdl/ontario/ontology/params> ?params .}
                        optional {?id <http://tib.eu/dsdl/ontario/ontology/desc> ?desc .}
                        optional {?id <http://tib.eu/dsdl/ontario/ontology/organization> ?organization .}
                } 
                '''
    res, card = mdb.query(query)
    if card > 0:
        data = []
        for r in res:
            dd = [r['id'], r['name'], r['endpoint']]
            if 'dstype' in r:
                dst = r['dstype']
                dd.append(dst[dst.rfind('/') + 1:])
            else:
                dd.append(' ')
            if 'keywords' in r:
                dd.append(r['keywords'])
            else:
                dd.append(' ')

            if 'homepage' in r:
                dd.append(r['homepage'])
            else:
                dd.append(' ')

            if 'organization' in r:
                dd.append(r['organization'])
            else:
                dd.append(' ')

            if 'desc' in r:
                dd.append(r['desc'])
            else:
                dd.append(' ')

            if 'version' in r :
                dd.append(r['version'])
            else:
                dd.append(' ')

            if 'params' in r:
                dd.append(r['params'])
            else:
                dd.append(' ')
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


@bp.route('/<int:id>/delete', methods=('POST',))
@login_required
def delete(id):
    federation = get_federation(id)
    db = get_db()
    db.execute('DELETE FROM federation WHERE id = ?', (id,))
    db.commit()
    return redirect(url_for('federation.index'))
