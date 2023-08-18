import json
from typing import Tuple

import networkx as nx
from flask import (
    Blueprint, g, render_template, session, Response, request
)

from FedSDM.auth import login_required
from FedSDM.db import get_mdb, MetadataDB
from FedSDM.utils import get_federations

bp = Blueprint('rdfmts', __name__, url_prefix='/rdfmts')


@bp.route('/rdfmt')
@login_required
def rdfmt() -> str:
    """Serves requests to '/rdfmts/rdfmt'.

    This route serves the RDF Molecule Template page of FedSDM. The page provides
    the means to display statistics about the RDF Molecule Templates of the different
    federations available to FedSDM including statistics from network analysis.
    Additionally, the page allows to visualize the Molecule Templates and their
    connections to each other.

    Note
    ----
    The request is only served for logged-in users.

    Returns
    -------
    str
        Rendered template of the RDF Molecule Template page with all available federations.

    """
    federations = get_federations()
    g.federations = federations
    if 'fed' in session:
        if session['fed'] not in [f['uri'] for f in federations]:
            del session['fed']
    return render_template('rdfmt/index.html', federations=g.federations)


@bp.route('/api/rdfmtstats')
def rdfmt_stats() -> Response:
    """Serves requests to '/rdfmts/api/rdfmtstats'.

    This route provides statistics about the RDF Molecule Templates of the federation
    provided with the request, i.e., the request has to include the parameter 'graph'
    which is the identifier of the federation of interest. If the parameter value is
    set to 'All', then the statistics are retrieved for all available federations.
    The statistics include the following data:
        - An ID for sorting
        - The name of the RDF Molecule Template
        - The URI of the class associated with the Molecule Template
        - The number of instances belonging to the Molecule Template
        - The number of properties in the Molecule Template

    Returns
    -------
    flask.Response
        A JSON response with the statistics about the RDF Molecule
        Templates of the specified federation. If no federation was
        given, the response contains an empty JSON object.

    """
    graph = request.args.get('graph', None)
    if graph is None:  # required parameter missing, returning empty response
        return Response(json.dumps({}), mimetype='application/json')
    elif graph == 'All':  # all federations are to be considered, so no graph is passed on
        graph = None
    else:
        session['fed'] = graph
    res = get_rdfmt_stats(graph)
    return Response(json.dumps(res), mimetype='application/json')


def get_rdfmt_stats(graph: str = None) -> dict:
    """Retrieves statistics about the RDF Molecule Templates of a federation.

    This method collects statistics about the RDF Molecule Templates of a federation.
    The :class:`FedSDM.db.MetadataDB` is utilized in order to retrieve those statistics.
    The statistics include the following data:
        - An ID for sorting
        - The name of the RDF Molecule Template
        - The URI of the class associated with the Molecule Template
        - The number of instances belonging to the Molecule Template
        - The number of properties in the Molecule Template

    Parameters
    ----------
    graph : str, optional
        The identifier of the federation for which to return the statistics about the RDF Molecule Templates.
        If no identifier is given, all federations available to FedSDM are considered.

    Returns
    -------
    dict
        A dictionary with the statistics about the RDF Molecule Templates for the provided federation.
        The dictionary is empty if no data could be retrieved from the :class:`FedSDM.db.MetadataDB`.

    """
    mdb = get_mdb()
    if graph is not None:
        session['fed'] = graph
        query = 'SELECT DISTINCT ?subject ?name (sum(?scard) AS ?subjectcard) (count(?pred) AS ?preds) ' \
                'WHERE { GRAPH <' + graph + '> {\n' \
                '  ?subject a mt:RDFMT .\n' \
                '  ?subject mt:source ?source .\n' \
                '  OPTIONAL { ?source mt:cardinality ?scard . }\n' \
                '  OPTIONAL { ?subject mt:name ?name . }\n' \
                '  OPTIONAL {\n' \
                '    ?subject mt:hasProperty ?mtp .\n' \
                '    ?mtp mt:predicate ?pred .\n' \
                '  }\n' \
                '}} GROUP BY ?subject ?name'
    else:
        query = 'SELECT DISTINCT ?subject ?name (sum(?scard) AS ?subjectcard) (count(?pred) AS ?preds) WHERE {\n' \
                '  ?subject a mt:RDFMT .\n' \
                '  ?subject mt:source ?source.\n' \
                '  OPTIONAL { ?source mt:cardinality ?scard . }\n' \
                '  OPTIONAL { ?subject mt:name ?name . }\n' \
                '  OPTIONAL {\n' \
                '    ?subject mt:hasProperty ?mtp .\n' \
                '    ?mtp mt:predicate ?pred .\n' \
                '  }\n' \
                '} GROUP BY ?subject ?name'

    res, card = mdb.query(query)
    if card > 0:
        if card == 1 and 'subject' not in res[0]:
            return {}
        else:
            data = []
            i = 0
            for r in res:
                dd = [i + 1]
                nid = r['subject']
                val = [True for m in meta if m in nid]
                if True in val:
                    continue

                if 'name' in r:
                    dd.append(r['name'])
                else:
                    subj = r['subject']
                    name = subj[subj.rfind('/') + 1:]
                    dd.append(name)

                dd.append(r['subject'])
                if 'subjectcard' in r:
                    card = r['subjectcard']
                    if '^^' in card:
                        card = card[:card.find('^^')]
                    dd.append(card)
                else:
                    dd.append(0)
                if 'preds' in r:
                    card = r['preds']
                    if '^^' in card:
                        card = card[:card.find('^^')]
                    dd.append(card)
                else:
                    dd.append(0)
                if 'links' in r:
                    card = r['links']
                    if '^^' in card:
                        card = card[:card.find('^^')]
                    dd.append(card)
                else:
                    dd.append(0)
                i += 1
                data.append(dd)
            return {'data': data, 'draw': 1, 'recordsTotal': len(res), 'recordsFiltered': 100}
    else:
        return {}


@bp.route('/api/mtdetails')
def api_rdfmt_details() -> Response:
    """Serves requests to '/rdfmts/api/mtdetails'.

    This route is used to retrieve details about a single RDF Molecule
    Template. The request requires the parameters:
        - 'fed' -- the identifier of the federation to which the RDF Molecule Template belongs
        - 'mt' -- the identifier of the Molecule Template of interest

    The details about the RDF Molecule Template are used to visualize it and its connectivity.

    Returns
    -------
    flask.Response
        A JSON response including the details of the RDF Molecule Template.
        The response contains an empty JSON object if one of the parameters
        was not present in the request.

    """
    fed = request.args.get('fed', None)
    mt = request.args.get('mt', None)
    if fed is None or mt is None:
        return Response(json.dumps({}), mimetype='application/json')

    res = get_rdfmt_details(fed, mt)
    return Response(json.dumps(res), mimetype='application/json')


def _iterative_query(query: str, mdb: MetadataDB, limit: int = 10000, offset: int = 0) -> list:
    """Executes a SPARQL query iteratively.

    This method utilizes :class:`FedSDM.db.MetadataDB` to execute SPARQL queries
    over the metadata knowledge graph. Answers are retrieved iteratively, i.e.,
    in blocks of the size `limit`.

    Parameters
    ----------
    query : str
        The SPARQL query to be executed.
    mdb : FedSDM.db.MetadataDB
        The :class:`FedSDM.db.MetadataDB` instance used to retrieve metadata.
    limit : int, optional
        The maximum amount of answers to be retrieved in one block.
        Default value is 10,000.
    offset : int, optional
        Offset defines the offset with which to retrieve results. A value greater
        than 0 means that the first *n* answers are omitted. The default is 0.

    Returns
    -------
    list
        A list with the query result.

    """
    res_list = []
    while True:
        query_copy = query + ' LIMIT ' + str(limit) + ' OFFSET ' + str(offset)
        res, card = mdb.query(query_copy)
        if card == -2:
            limit = limit // 2
            limit = int(limit)
            if limit < 1:
                break
            continue
        if card > 0:
            res_list.extend(res)
        if card < limit:
            break
        offset += limit
    return res_list


def get_rdfmt_details(fed: str, mt: str) -> dict:
    """Gets the details about a specific RDF Molecule Template.

    The details are used to visualize the RDF Molecule Template and its connections.

    Parameters
    ----------
    fed : str
        The identifier of the federation to which the RDF Molecule Template belongs.
    mt : str
        The identifier of the Molecule Template of interest.

    Returns
    -------
    dict
        A dictionary with nodes, edges, and sources to visualize the RDF
        Molecule Template and its connections within the federation.

    """
    mdb = get_mdb()
    query = 'SELECT DISTINCT ?datasource ?endpoint ?mtp ?preddatasource ?mtrdatasource ?card ?pred ?mtr ' \
            'WHERE { GRAPH <' + fed + '> {\n' \
            '  <' + mt + '> a mt:RDFMT .\n' \
            '  <' + mt + '> mt:source ?source .\n' \
            '  OPTIONAL { ?source mt:cardinality ?card. }\n' \
            '  ?source mt:datasource ?datasource .\n' \
            '  ?datasource mt:url ?endpoint .\n' \
            '  <' + mt + '> mt:hasProperty ?mtp .\n' \
            '  ?mtp mt:predicate ?pred .\n' \
            '  ?mtp mt:propSource ?mtpsource .\n' \
            '  ?mtpsource mt:datasource ?preddatasource .\n' \
            '  OPTIONAL {\n' \
            '    ?mtp mt:linkedTo ?mtrange .\n' \
            '    ?mtrange mt:name ?mtr .\n'\
            '    ?mtr mt:source ?mtrsource .\n'\
            '    ?mtrsource mt:datasource ?mtrdatasource .\n' \
            '  }\n' \
            '}}'

    res = _iterative_query(query, mdb)
    if len(res) > 0:
        nodes = {}
        edges = []
        nodeids = {}
        nodecards = {}
        nodeswithnocard = []
        i = 0
        sources = {}
        j = 0

        for r in res:
            nid = r['pred']
            nlabel = r['pred']
            dssource = r['preddatasource']
            mdssource = r['datasource']
            if dssource not in sources:
                sources[dssource] = j
                j += 1

            if mdssource not in sources:
                sources[mdssource] = j
                j += 1

            if mt+mdssource not in nodes:
                nodes[mt+mdssource] = {
                    'id': mt,
                    'label': mt,
                    'datasource': sources[mdssource],
                    'weight': r['card'][:r['card'].find('^^')] if 'card' in r and '^' in r['card'] else 10,
                    'type': 'root'
                }
                nodeids[mt + mdssource] = i
                i += 1

            if nid+dssource not in nodes:
                if 'predcard' in r:
                    weight = r['predcard']
                    if '^' in weight:
                        weight = weight[:weight.find('^^')]
                    if nid + dssource in nodeswithnocard:
                        nodes[nid + dssource]['weight'] = weight
                        nodeswithnocard.remove(nid + dssource)
                else:
                    weight = -1

                nodecards[nid + dssource] = weight
                nodes[nid+dssource] = {
                    'id': nid+dssource,
                    'label': nlabel,
                    'datasource': sources[dssource],
                    'weight': weight,
                    'type': 'square'
                }
                nodeids[nid+dssource] = i
                i += 1
                edges.append({
                    'source': mt+mdssource,
                    'target': nid + dssource,
                    'weight': weight,
                    'pred': 'hasPredicate',
                    'ltype': 'predicate'
                })

            if 'mtr' in r:
                lnid = r['mtr']
                ldssource = r['mtrdatasource']
                lnlabel = r['mtr']
                if ldssource not in sources:
                    sources[ldssource] = j
                    j += 1
                if lnid+ldssource in nodecards:
                    lweight = nodecards[lnid+ldssource]
                else:
                    nodeswithnocard.append(lnid+ldssource)
                    lweight = -1

                if lnid + ldssource not in nodes:
                    nodes[lnid + ldssource] = {
                        'id': lnid + ldssource,
                        'label': lnlabel,
                        'datasource': sources[ldssource],
                        'weight': lweight,
                        'type': 'circle',
                        'predicateid': nid + dssource
                    }
                    nodeids[lnid + ldssource] = i
                    i += 1
                if 'predcard' in r:
                    lcard = r['predcard']
                    if '^^' in lcard:
                        lcard = lcard[:lcard.find('^^')]
                else:
                    lcard = -1

                edges.append({
                    'source': nid + dssource,
                    'target': lnid + ldssource,
                    'weight': lcard,
                    'ltype': 'link',
                    'type': 'link',
                    'pred': r['pred']
                })

        sources = [{'id': v, 'name': k} for k, v in sources.items()]
        return {
            'nodes': nodes,
            'links': edges,
            'sources': sources
        }
    else:
        return {'nodes': {}, 'links': {}, 'sources': {}}


@bp.route('/api/rdfmts')
def api_rdfmts() -> Response:
    """Serves requests to '/rdfmts/api/rdfmts'.

    This method is used to retrieve information about the RDF Molecule
    Templates of a federation in order to visualize them and their
    connectivity. The request requires the parameter 'graph' which
    is the identifier of the federation of interest. If the value
    is 'all', then all available federations are considered.

    Returns
    -------
    flask.Response
        A JSON response with the requested data.

    """
    graph = request.args.get('graph', None)
    if graph is None:  # required parameter missing, returning empty response
        return Response(json.dumps({}), mimetype='application/json')
    elif graph == 'All':  # all federations are to be considered, so no graph is passed on
        graph = None
    else:
        session['fed'] = graph
    res, sources = get_rdfmt_nodes(graph)
    res.update(get_rdfmt_edges(sources, graph))
    return Response(json.dumps(res), mimetype='application/json')


def get_rdfmt_edges(rdfmt_sources: dict, graph: str = None) -> dict:
    """Gets the connections between RDF Molecule Templates of a federation.

    Makes use of :class:`FedSDM.db.MetadataDB` to retrieve the links between
    RDF Molecule Templates in the federation `graph`. The connections are then
    filtered by the RDF Molecule Templates in `rdfmt_sources`, i.e., if the range
    of an RDF Molecule Template is not in `rdfmt_sources`, then it will not be
    added to the result.

    Parameters
    ----------
    rdfmt_sources : dict
        Information about the sources of interest, i.e., connections to
        which source should be considered for the result.
    graph : str, optional
        The identifier of the federation for which the links between
        RDF Molecule Templates should be searched. If no value is
        passed, all available federations are considered.

    Returns
    -------
    dict
        A dictionary with the requested data in the key 'links'.

    """
    mdb = get_mdb()
    if graph is not None:
        session['fed'] = graph
        query = 'SELECT DISTINCT ?subject ?mt WHERE { GRAPH <' + graph + '> {\n' \
                '  ?subject mt:hasProperty ?mtp .\n' \
                '  ?mtp mt:linkedTo ?mtrange .\n' \
                '  ?mtrange mt:rdfmt ?mt .\n' \
                '}}'
    else:
        query = 'SELECT COUNT DISTINCT ?subject ?mt WHERE {\n' \
                '  ?subject mt:hasProperty ?mtp .\n' \
                '  ?mtp mt:linkedTo ?mtrange .\n' \
                '  ?mtrange mt:rdfmt ?mt .\n' \
                '}'

    res = _iterative_query(query, mdb, 5000)
    card = len(res)
    if card > 0:
        if card == 1 and 'subject' not in res[0]:
            return {'links': []}
        else:
            edges = []
            edges_key = []
            for r in res:
                nid = r['subject']
                if 'mt' in r:
                    lnid = r['mt']
                    if lnid not in rdfmt_sources or lnid == nid:
                        continue
                    if lnid + nid not in edges_key:
                        edges_key.append(nid + lnid)
                        edges_key.append(lnid + nid)

                        lds_source = rdfmt_sources[lnid]['source']

                        lcard = -1
                        edges.append({
                            'source': nid + rdfmt_sources[nid]['source'],
                            'target': lnid + lds_source,
                            'weight': lcard,
                            'pred': 'linkedto'
                        })
            return {'links': list(edges)}
    else:
        return {'links': []}


def get_rdfmt_nodes(graph: str = None) -> Tuple[dict, dict]:
    """Gets all RDF Molecule Templates of a federation and to which source they belong.

    Makes use of :class:`FedSDM.db.MetadataDB` to retrieve the RDF Molecule Templates
    of a federation and the information to which source they belong.
    This method is one out of two that need to be called in order to retrieve the
    necessary data to visualize the RDF Molecule Templates of a federation.

    Parameters
    ----------
    graph : str
        The identifier of the federation of interest. If no value is
        passed, all available federations are considered.

    Returns
    -------
    (dict, dict)
        The first dictionary includes information about the nodes and sources in
        order to visualize the RDF Molecule Templates. The second dictionary keeps
        information about the RDF Molecule Templates and to which source they belong.

    """
    mdb = get_mdb()
    if graph is not None:
        session['fed'] = graph
        query = 'SELECT DISTINCT ?subject ?source ?name ?datasource WHERE { GRAPH <' + graph + '> {\n' \
                '  ?subject a mt:RDFMT .\n' \
                '  ?subject mt:source ?ds .\n' \
                '  ?ds mt:datasource ?source .\n' \
                '  ?source mt:name ?datasource .\n' \
                '  OPTIONAL { ?subject mt:name ?name . }\n' \
                '}}'
    else:
        query = 'SELECT DISTINCT ?subject ?name ?source ?datasource WHERE {\n' \
                '  ?subject a mt:RDFMT .\n' \
                '  ?subject mt:source ?ds .\n' \
                '  ?ds mt:datasource ?source .\n' \
                '  ?source mt:name ?datasource .\n' \
                '  OPTIONAL { ?subject mt:name ?name . }\n' \
                '}'

    res = _iterative_query(query, mdb, 9000)
    if len(res) > 0:
        card = len(res)
        if card == 1 and 'subject' not in res[0]:
            return {'nodes': [], 'sources': []}, {}
        else:
            nodes = {}
            i = 0
            sources = {}
            rdfmt_sources = {}
            j = 0

            for r in res:
                nid = r['subject']
                if 'name' in r:
                    nlabel = r['name']
                else:
                    nlabel = r['subject']

                source_id = r['source']

                if 'datasource' in r:
                    dssource = r['datasource']
                else:
                    print('unknown source for MT: ', r['subject'])
                    dssource = 'Unknown'

                rdfmt_sources[nid] = {'source': source_id}
                rdfmt_sources[nid]['name'] = nlabel

                if dssource not in sources:
                    sources[dssource] = j
                    j += 1

                weight = -1

                if nid + source_id not in nodes:
                    nodes[nid + source_id] = {
                        'id': nid + source_id,
                        'label': dssource + '-' + nlabel,
                        'datasource': sources[dssource],
                        'node_type': sources[dssource],
                        'cluster': sources[dssource],
                        'weight': weight
                    }
                    i += 1

            print('total nodes', len(nodes))
            sourcenamess = [{'id': v, 'name': k} for k, v in sources.items()]
            return {'nodes': nodes, 'sources': sourcenamess}, rdfmt_sources
    else:
        return {'nodes': [], 'sources': []}, {}


def get_rdfmt_links(graph: str = None):
    """This method is currently unused and needs checking."""
    mdb = get_mdb()
    if graph is not None:
        session['fed'] = graph
        query = 'SELECT DISTINCT ?subject ?datasource ?pred ?mt ?mtrangesource WHERE { GRAPH <' + graph + '> {\n' \
                '  ?subject a mt:RDFMT .\n' \
                '  ?subject mt:source ?source .\n' \
                '  ?source mt:datasource ?datasource .\n' \
                '  OPTIONAL {\n' \
                '    ?subject mt:hasProperty ?mtp .\n' \
                '    ?mtp mt:predicate ?pred .\n' \
                '    OPTIONAL {\n' \
                '      ?mtp mt:linkedTo ?mtrange .\n' \
                '      ?mtrange mt:rdfmt ?mt .\n' \
                '      ?mtrange mt:datasource ?mtrangesource .\n' \
                '    }\n' \
                '  }\n' \
                '}}'
    else:
        query = 'SELECT DISTINCT ?subject ?datasource ?pred ?mt ?mtrangesource WHERE {\n' \
                '  ?subject a mt:RDFMT .\n' \
                '  ?subject mt:source ?source .\n' \
                '  ?source mt:datasource ?datasource .\n' \
                '  OPTIONAL {\n' \
                '    ?subject mt:hasProperty ?mtp .\n' \
                '    ?mtp mt:predicate ?pred .\n' \
                '    OPTIONAL {\n' \
                '      ?mtp mt:linkedTo ?mtrange .\n' \
                '      ?mtrange mt:rdfmt ?mt .\n' \
                '      ?mtrange mt:datasource ?mtrangesource .\n' \
                '    }' \
                '  }' \
                '}'

    res = _iterative_query(query, mdb, 100)
    print(len(res))
    if len(res) > 0:
        card = len(res)
        if card == 1 and 'subject' not in res[0]:
            return {'nodes': [], 'links': [], 'sources': []}
        else:
            nodes = {}
            edges = []
            nodeids = {}
            nodecards = {}
            nodeswithnocard = []
            i = 0
            sources = {}
            j = 0

            for r in res:
                nid = r['subject']
                val = [True for m in meta if m in nid]
                if True in val:
                    continue

                nlabel = r['subject']

                if 'datasource' in r:
                    dssource = r['datasource']
                else:
                    print('unknown source for MT:', r['subject'])
                    dssource = 'Unknown'
                if dssource not in sources:
                    sources[dssource] = j
                    j += 1

                if '/' in nlabel:
                    nlabel = nlabel[nlabel.rfind('/') + 1:]  # dssource[dssource.rfind('/')+1:] + ':' +
                else:
                    nlabel = nlabel  # dssource[dssource.rfind('/')+1:] + ':' +

                weight = -1

                nodecards[nid + dssource] = weight

                if nid + dssource not in nodes:
                    nodes[nid + dssource] = {
                        'id': nid + dssource,
                        'label': nlabel,
                        'datasource': sources[dssource],
                        'weight': weight
                    }
                    nodeids[nid + dssource] = i
                    i += 1

                if 'mt' in r:
                    lnid = r['mt']
                    ldssource = r['mtrangesource']
                    lnlabel = r['mt']
                    if '/' in lnlabel:
                        lnlabel = lnlabel[lnlabel.rfind('/') + 1:]  # ldssource[ldssource.rfind('/') + 1:] + ':' +
                    else:
                        lnlabel = lnlabel  # ldssource[ldssource.rfind('/') + 1:] + ':' +

                    if ldssource not in sources:
                        sources[ldssource] = j
                        j += 1
                    if lnid + ldssource in nodecards:
                        lweight = nodecards[lnid + ldssource]
                    else:
                        nodeswithnocard.append(lnid + ldssource)
                        lweight = -1

                    if lnid + ldssource not in nodes:
                        nodes[lnid + ldssource] = {
                            'id': lnid + ldssource,
                            'label': lnlabel,
                            'datasource': sources[ldssource],
                            'weight': lweight
                        }
                        nodeids[lnid + ldssource] = i
                        i += 1
                    lcard = -1

                    edges.append({
                        'source': nid + dssource,
                        'target': lnid + ldssource,
                        'weight': lcard,
                        'pred': r['pred']
                    })

            print('total nodes:', len(nodes))
            print('total edges:', len(edges))

            sources = [{'id': v, 'name': k} for k, v in sources.items()]
            print(sources)
            return {'nodes': nodes, 'links': list(edges), 'sources': sources}
    else:
        return {'nodes': [], 'links': [], 'sources': []}


@bp.route('/api/rdfmtanalysis')
def api_rdfmtanalysis() -> Response:
    """Serves requests to '/rdfmts/api/rdfmtanalysis'.

    This route performs a network analysis of the RDF Molecule Templates
    of a data source. The request requires the following parameters:
        - 'graph' -- the identifier of the federation to which the source belongs
        - 'source' -- the data source of interest

    If the value for a parameter is 'All', all available federations and/or
    sources are considered.

    Returns
    -------
    flask.Response
        A JSON response with the results from the network analysis.

    """
    try:
        graph = request.args['graph']
        source = request.args['source']
    except KeyError:
        return Response(json.dumps({}), mimetype='application/json')
    if graph == 'All' or graph is None:
        graph = None
    else:
        session['fed'] = graph
    if source == 'All' or source is None:
        source = None

    res = get_graph_stat(graph, source)
    return Response(json.dumps({'data': res}), mimetype='application/json')


def get_graph_stat(graph: str = None, source: str = None) -> list:
    """Gets graph statistics about the RDF Molecule Templates in a specific source.

    This method performs a network analysis on the RDF Molecule Templates of the
    specified federation and data source. The reported metrics are:
        - Density
        - Number of nodes
        - Number of edges
        - Number of connected components
        - Average clustering
        - Transitivity
        - Average number of neighbors

    An instance of :class:`FedSDM.db.MetadataDB` is used to retrieve the necessary
    metadata from the metadata knowledge graph. The retrieved data is then prepared
    in a way such that the method :func:`~FedSDM.rdfmtmgt.compute_graph_properties`
    can perform the actual network analysis.

    Parameters
    ----------
    graph : str, optional
        The identifier of the federation of interest. If None, then all federations are considered.
    source : str, optional
        The identifier of the data source of interest. If None, then all source are considered.

    Returns
    -------
    list
        A list with the above-mentioned metrics from network analysis.

    """
    mdb = get_mdb()
    if source is None:
        source = '?name'
    else:
        source = '"' + source + '"'
    if graph is not None:
        query = 'SELECT DISTINCT ?subject ?target WHERE { GRAPH <' + graph + '> {\n' \
                '  ?subject a mt:RDFMT .\n' \
                '  ?subject mt:source ?source .\n' \
                '  ?source mt:datasource ?datasource .\n' \
                '  ?datasource mt:name ' + source + ' .\n' \
                '  OPTIONAL {\n' \
                '    ?subject mt:hasProperty ?mtp .\n' \
                '    OPTIONAL {\n' \
                '      ?mtp mt:linkedTo ?mtrange .\n' \
                '      ?mtrange mt:rdfmt ?target .\n' \
                '    }\n' \
                '  }\n' \
                '}}'
    else:
        query = 'SELECT DISTINCT ?subject ?target WHERE {\n' \
                '  ?subject a mt:RDFMT .\n' \
                '  ?subject mt:source ?source .\n' \
                '  ?source mt:datasource ?datasource .\n' \
                '  ?datasource mt:name ' + source + ' .\n' \
                '  OPTIONAL {\n' \
                '    ?subject mt:hasProperty ?mtp .\n' \
                '    OPTIONAL {\n' \
                '      ?mtp mt:linkedTo ?mtrange .\n' \
                '      ?mtrange mt:rdfmt ?target .\n' \
                '    }\n' \
                '  }\n' \
                '}'

    res = _iterative_query(query, mdb, 5000)
    if len(res) > 0:
        card = len(res)
        if card == 1 and 'subject' not in res[0]:
            return []
        else:
            nodes = []
            edges = []
            for r in res:
                nodes.append(r['subject'])
                if 'target' in r:
                    if r['target'] not in nodes:
                        nodes.append(r['target'])

                    if (r['subject'], r['target']) not in edges:
                        edges.append((r['subject'], r['target']))
            result = compute_graph_properties(list(set(nodes)), edges)
            return result
    else:
        return []


def compute_graph_properties(nodes: list, edges: list) -> list:
    """Performs a network analysis based on the given nodes and edges.

    This method performs a network analysis over the given nodes and edges.
    The reported metrics are:
        - Density
        - Number of nodes
        - Number of edges
        - Number of connected components
        - Average clustering
        - Transitivity
        - Average number of neighbors

    Parameters
    ----------
    nodes : list
        The nodes in the network to be analyzed.
    edges : list
        The edges in the network to be analyzed.

    Returns
    -------
    list
        A list with the above-mentioned metrics from network analysis.

    """
    graph = nx.Graph()
    graph.add_nodes_from(nodes)
    graph.add_edges_from(edges)
    deg = dict(graph.degree())
    sum_of_edges = sum(deg.values())
    avg_neighbors = sum_of_edges / nx.number_of_nodes(graph)
    density = nx.density(graph)
    n = nx.number_of_nodes(graph)
    e = nx.number_of_edges(graph)
    c = nx.average_clustering(graph)
    cc = nx.number_connected_components(graph)
    t = nx.transitivity(graph)

    print('calculating...', density, n, e, c, cc)
    res = [
        ['Density', density],
        ['Nodes', n],
        ['Edges', e],
        ['Connected Components', cc],
        ['Avg. Clustering', c],
        ['Transitivity', t],
        ['Avg. Neighbors', avg_neighbors]
    ]
    print(res)
    return res


meta = [
    'http://purl.org/goodrelations/',
    'http://rdfs.org/ns/void#',
    'http://www.w3.org/ns/dcat',
    'http://www.w3.org/2001/vcard-rdf/',
    'http://www.ebusiness-unibw.org/ontologies/eclass',
    'http://bio2rdf.org/bio2rdf.dataset_vocabulary:Dataset'
]
"""list: Commonly used prefixes of classes that should not be considered in RDF Molecule Templates."""
