import json
from typing import Tuple

import networkx as nx
from flask import (
    Blueprint, g, render_template, session, Response
)
from webargs import fields
from webargs.flaskparser import use_kwargs

from FedSDM import get_logger
from FedSDM.auth import login_required
from FedSDM.db import get_mdb, MetadataDB
from FedSDM.utils import get_federations

bp = Blueprint('rdfmts', __name__, url_prefix='/rdfmts')

logger = get_logger('rdfmtmgt')
"""Logger for this module. It logs to stdout only."""


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
    return render_template('rdfmt/index.jinja2', federations=g.federations)


@bp.route('/api/rdfmtstats')
@use_kwargs({'graph': fields.Str(required=True)}, location='query')
def rdfmt_stats(graph) -> Response:
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
    if graph == 'All':  # all federations are to be considered, so no graph is passed on
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
        query = 'SELECT DISTINCT ?subject ?name (SUM(?scard) AS ?subjectcard) (COUNT(?pred) AS ?preds) ' \
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
        query = 'SELECT DISTINCT ?subject ?name (SUM(?scard) AS ?subjectcard) (COUNT(?pred) AS ?preds) WHERE {\n' \
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
@use_kwargs({'fed': fields.Str(required=True), 'mt': fields.Str(required=True)}, location='query')
def api_rdfmt_details(fed, mt) -> Response:
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
        node_ids = {}
        node_cards = {}
        nodes_with_no_card = []
        i = 0
        sources = {}
        j = 0

        for r in res:
            nid = r['pred']
            node_label = r['pred']
            pred_source = r['preddatasource']
            source = r['datasource']
            if pred_source not in sources:
                sources[pred_source] = j
                j += 1

            if source not in sources:
                sources[source] = j
                j += 1

            if mt+source not in nodes:
                nodes[mt+source] = {
                    'id': mt,
                    'label': mt,
                    'datasource': sources[source],
                    'weight': r['card'][:r['card'].find('^^')] if 'card' in r and '^^' in r['card'] else 10,
                    'type': 'root'
                }
                node_ids[mt + source] = i
                i += 1

            if nid+pred_source not in nodes:
                if 'predcard' in r:
                    weight = r['predcard']
                    if '^^' in weight:
                        weight = weight[:weight.find('^^')]
                    if nid + pred_source in nodes_with_no_card:
                        nodes[nid + pred_source]['weight'] = weight
                        nodes_with_no_card.remove(nid + pred_source)
                else:
                    weight = -1

                node_cards[nid + pred_source] = weight
                nodes[nid + pred_source] = {
                    'id': nid + pred_source,
                    'label': node_label,
                    'datasource': sources[pred_source],
                    'weight': weight,
                    'type': 'square'
                }
                node_ids[nid + pred_source] = i
                i += 1
                edges.append({
                    'source': mt + source,
                    'target': nid + pred_source,
                    'weight': weight,
                    'pred': 'hasPredicate',
                    'ltype': 'predicate'
                })

            if 'mtr' in r:
                lid = r['mtr']
                link_source = r['mtrdatasource']
                link_label = r['mtr']
                if link_source not in sources:
                    sources[link_source] = j
                    j += 1
                if lid + link_source in node_cards:
                    link_weight = node_cards[lid + link_source]
                else:
                    nodes_with_no_card.append(lid + link_source)
                    link_weight = -1

                if lid + link_source not in nodes:
                    nodes[lid + link_source] = {
                        'id': lid + link_source,
                        'label': link_label,
                        'datasource': sources[link_source],
                        'weight': link_weight,
                        'type': 'circle',
                        'predicateid': nid + pred_source
                    }
                    node_ids[lid + link_source] = i
                    i += 1
                if 'predcard' in r:
                    link_card = r['predcard']
                    if '^^' in link_card:
                        link_card = link_card[:link_card.find('^^')]
                else:
                    link_card = -1

                edges.append({
                    'source': nid + pred_source,
                    'target': lid + link_source,
                    'weight': link_card,
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
@use_kwargs({'graph': fields.Str(required=True)}, location='query')
def api_rdfmts(graph) -> Response:
    """Serves requests to '/rdfmts/api/rdfmts'.

    This method is used to retrieve information about the RDF Molecule
    Templates of a federation in order to visualize them and their
    connectivity. The request requires the parameter 'graph' which
    is the identifier of the federation of interest. If the value
    is 'All', then all available federations are considered.

    Returns
    -------
    flask.Response
        A JSON response with the requested data.

    """
    if graph == 'All':  # all federations are to be considered, so no graph is passed on
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
        query = 'SELECT DISTINCT ?subject ?mt WHERE {\n' \
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
                    lid = r['mt']
                    if lid not in rdfmt_sources or lid == nid:
                        continue
                    if lid + nid not in edges_key:
                        edges_key.append(nid + lid)
                        edges_key.append(lid + nid)

                        lds_source = rdfmt_sources[lid]['source']

                        link_card = -1
                        edges.append({
                            'source': nid + rdfmt_sources[nid]['source'],
                            'target': lid + lds_source,
                            'weight': link_card,
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
    card = len(res)
    if card > 0:
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
                    node_label = r['name']
                else:
                    node_label = r['subject']

                source_id = r['source']

                if 'datasource' in r:
                    source = r['datasource']
                else:
                    logger.warning('unknown source for MT: ' + r['subject'])
                    source = 'Unknown'

                rdfmt_sources[nid] = {'source': source_id}
                rdfmt_sources[nid]['name'] = node_label

                if source not in sources:
                    sources[source] = j
                    j += 1

                weight = -1

                if nid + source_id not in nodes:
                    nodes[nid + source_id] = {
                        'id': nid + source_id,
                        'label': source + '-' + node_label,
                        'datasource': sources[source],
                        'node_type': sources[source],
                        'cluster': sources[source],
                        'weight': weight
                    }
                    i += 1

            source_names = [{'id': v, 'name': k} for k, v in sources.items()]
            return {'nodes': nodes, 'sources': source_names}, rdfmt_sources
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
    card = len(res)
    if card > 0:
        if card == 1 and 'subject' not in res[0]:
            return {'nodes': [], 'links': [], 'sources': []}
        else:
            nodes = {}
            edges = []
            node_ids = {}
            node_cards = {}
            nodes_with_no_card = []
            i = 0
            sources = {}
            j = 0

            for r in res:
                nid = r['subject']
                val = [True for m in meta if m in nid]
                if True in val:
                    continue

                node_label = r['subject']

                if 'datasource' in r:
                    source = r['datasource']
                else:
                    logger.warning('unknown source for MT: ' + r['subject'])
                    source = 'Unknown'
                if source not in sources:
                    sources[source] = j
                    j += 1

                if '/' in node_label:
                    node_label = node_label[node_label.rfind('/') + 1:]

                weight = -1

                node_cards[nid + source] = weight

                if nid + source not in nodes:
                    nodes[nid + source] = {
                        'id': nid + source,
                        'label': node_label,
                        'datasource': sources[source],
                        'weight': weight
                    }
                    node_ids[nid + source] = i
                    i += 1

                if 'mt' in r:
                    lid = r['mt']
                    link_source = r['mtrangesource']
                    link_label = r['mt']
                    if '/' in link_label:
                        link_label = link_label[link_label.rfind('/') + 1:]

                    if link_source not in sources:
                        sources[link_source] = j
                        j += 1
                    if lid + link_source in node_cards:
                        link_weight = node_cards[lid + link_source]
                    else:
                        nodes_with_no_card.append(lid + link_source)
                        link_weight = -1

                    if lid + link_source not in nodes:
                        nodes[lid + link_source] = {
                            'id': lid + link_source,
                            'label': link_label,
                            'datasource': sources[link_source],
                            'weight': link_weight
                        }
                        node_ids[lid + link_source] = i
                        i += 1
                    link_card = -1

                    edges.append({
                        'source': nid + source,
                        'target': lid + link_source,
                        'weight': link_card,
                        'pred': r['pred']
                    })

            sources = [{'id': v, 'name': k} for k, v in sources.items()]
            return {'nodes': nodes, 'links': list(edges), 'sources': sources}
    else:
        return {'nodes': [], 'links': [], 'sources': []}


@bp.route('/api/rdfmtanalysis')
@use_kwargs({'graph': fields.Str(required=True), 'source': fields.Str(required=True)}, location='query')
def api_rdfmtanalysis(graph, source) -> Response:
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
    if graph == 'All':  # all federations are to be considered, so no graph is passed on
        graph = None
    else:
        session['fed'] = graph
    if source == 'All':  # all sources are to be considered, so none is passed on
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
    source = '"' + source + '"' if source is not None else '?name'
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
    card = len(res)
    if card > 0:
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
    density = nx.density(graph)
    n = nx.number_of_nodes(graph)
    e = nx.number_of_edges(graph)
    c = nx.average_clustering(graph)
    cc = nx.number_connected_components(graph)
    t = nx.transitivity(graph)
    avg_neighbors = 2*e / n  # count edges as undirected, hence, duplicate them

    res = [
        ['Density', density],
        ['Nodes', n],
        ['Edges', e],
        ['Connected Components', cc],
        ['Avg. Clustering', c],
        ['Transitivity', t],
        ['Avg. Neighbors', avg_neighbors]
    ]
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
