import json
import time

import networkx as nx
from flask import (
    Blueprint, g, render_template, session, Response, request
)

from FedSDM.auth import login_required
from FedSDM.db import get_mdb, MetadataDB
from FedSDM.ui.utils import get_federations

bp = Blueprint('rdfmts', __name__, url_prefix='/rdfmts')


@bp.route('/rdfmt')
@login_required
def rdfmt():
    federations = get_federations()
    g.federations = federations
    if 'fed' in session:
        if session['fed'] not in [f['uri'] for f in federations]:
            del session['fed']
    return render_template('rdfmt/index.html', federations=g.federations)


@bp.route('/api/rdfmtstats')
def rdfmtstats():
    try:
        graph = request.args['graph']
    except KeyError:
        return Response(json.dumps({}), mimetype='application/json')
    if graph == 'All' or graph is None:
        graph = None
    else:
        session['fed'] = graph
    res = get_rdfmt_stats(graph)
    return Response(json.dumps(res), mimetype='application/json')


def get_rdfmt_stats(graph: str = None):
    mdb = get_mdb()
    if graph is not None:
        session['fed'] = graph
        query = 'SELECT DISTINCT ?subject ?name (sum(?scard) as ?subjectcard) (count(?pred) as ?preds) ' \
                'WHERE {  GRAPH <' + graph + '> {\n' \
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
        query = 'SELECT DISTINCT ?subject ?name (sum(?scard) as ?subjectcard) (count(?pred) as ?preds) WHERE {\n' \
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
            return []
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
        return []


@bp.route('/api/mtdetails')
def api_rdfmtdetails():
    try:
        fed = request.args['fed']
        mt = request.args['mt']
        print('MT:', mt, fed)
    except KeyError:
        return Response(json.dumps({}), mimetype='application/json')
    if mt is None:
        return Response(json.dumps({}), mimetype='application/json')

    res = get_rdfmt_details(fed, mt)
    return Response(json.dumps(res), mimetype='application/json')


def _iterative_query(query: str, mdb: MetadataDB, limit: int = 10000, offset: int = 0):
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


def get_rdfmt_details(fed: str, mt: str):
    mdb = get_mdb()
    print(fed, mt, 'get_rdfmt_details')
    query = 'SELECT DISTINCT ?datasource ?endpoint ?mtp ?preddatasource ?mtrdatasource ?card ?pred ?mtr ' \
            'WHERE { graph <' + fed + '> {\n' \
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
            '    ?mtr  mt:source ?mtrsource .\n'\
            '    ?mtrsource mt:datasource ?mtrdatasource .\n' \
            '  }\n' \
            '}}'

    res = _iterative_query(query, mdb)
    print(len(res), 'results found')
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

        print('total nodes:', len(nodes), nodes)
        print('total edges:', len(edges), edges)

        sources = [{'id': v, 'name': k} for k, v in sources.items()]
        print(edges)
        return {
            'nodes': nodes,
            'links': edges,
            'sources': sources
        }
    else:
        return {'nodes': [], 'links': [], 'sources': []}


@bp.route('/api/rdfmts')
def api_rdfmts():
    try:
        graph = request.args['graph']
    except KeyError:
        return Response(json.dumps({}), mimetype='application/json')
    if graph == 'All' or graph is None:
        graph = None
    else:
        session['fed'] = graph
    res, sources = get_rdfmt_nodes(graph)
    res.update(get_rdfmt_edges(sources, graph))
    return Response(json.dumps(res), mimetype='application/json')


def get_rdfmt_edges(rdfmtsources, graph: str = None):
    mdb = get_mdb()
    if graph is not None:
        session['fed'] = graph
        query = 'SELECT DISTINCT ?subject ?mt WHERE {  graph <' + graph + '> {\n' \
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

    start = time.time()
    res = _iterative_query(query, mdb, 5000)
    processtime = time.time()
    print('Query edges time:', (processtime - start))
    if len(res) > 0:
        card = len(res)
        if card == 1 and 'subject' not in res[0]:
            return {'links': []}
        else:
            edges = []
            edgeskey = []
            for r in res:
                nid = r['subject']
                if 'mt' in r:
                    lnid = r['mt']
                    if lnid not in rdfmtsources or lnid == nid:
                        print('Skipped range: ', lnid)
                        continue
                    if lnid + nid in edgeskey:
                        continue
                    edgeskey.append(nid + lnid)
                    edgeskey.append(lnid+nid)

                    ldssource = rdfmtsources[lnid]['source']

                    lcard = -1
                    edges.append({
                        'source': nid + rdfmtsources[nid]['source'],
                        'target': lnid + ldssource,
                        'weight': lcard,
                        'pred': 'linkedto'
                    })

            print('total edges:', len(edges))
            print('Process time:', (time.time()-processtime))
            return {'links': list(edges)}
    else:
        return {'links': []}


def get_rdfmt_nodes(graph: str = None):
    mdb = get_mdb()
    if graph is not None:
        session['fed'] = graph
        query = 'SELECT DISTINCT ?subject ?source ?name ?datasource WHERE {  graph <' + graph + '> {\n' \
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

    start = time.time()
    res = _iterative_query(query, mdb, 9000)
    processtime = time.time()
    print('query time:', (processtime - start))
    if len(res) > 0:
        card = len(res)
        if card == 1 and 'subject' not in res[0]:
            return {'nodes': [], 'sources': []}, {}
        else:
            nodes = {}
            i = 0
            sources = {}
            rdfmtsources = {}
            j = 0

            for r in res:
                nid = r['subject']
                if 'name' in r:
                    nlabel = r['name']
                else:
                    nlabel = r['subject']

                sourceid = r['source']

                if 'datasource' in r:
                    dssource = r['datasource']
                else:
                    print('unknown source for MT: ', r['subject'])
                    dssource = 'Unknown'

                rdfmtsources[nid] = {'source': sourceid}
                rdfmtsources[nid]['name'] = nlabel

                if dssource not in sources:
                    sources[dssource] = j
                    j += 1

                weight = -1

                if nid + sourceid not in nodes:
                    nodes[nid + sourceid] = {
                        'id': nid + sourceid,
                        'label': dssource + '-' + nlabel,
                        'datasource': sources[dssource],
                        'node_type': sources[dssource],
                        'cluster': sources[dssource],
                        'weight': weight
                    }
                    i += 1

            print('total nodes', len(nodes))
            sourcenamess = [{'id': v, 'name': k} for k, v in sources.items()]
            print('Process time', (time.time()-processtime))
            return {'nodes': nodes, 'sources': sourcenamess}, rdfmtsources
    else:
        return {'nodes': [], 'sources': []}, {}


def get_rdfmt_links(graph: str = None):
    mdb = get_mdb()
    if graph is not None:
        session['fed'] = graph
        query = 'SELECT DISTINCT ?subject  ?datasource ?pred ?mt  ?mtrangesource WHERE {  graph <' + graph + '> {\n' \
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
                        # 'left': False,
                        # 'right': True,
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
def api_rdfmtanalysis():
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


def get_graph_stat(graph: str = None, source: str = None):
    mdb = get_mdb()
    if source is None:
        source = ' ?name '
    else:
        source = '"' + source + '" '
    if graph is not None:
        query = 'SELECT distinct ?subject  ?target WHERE { graph <' + graph + '> {\n' \
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
        query = 'SELECT distinct ?subject  ?target WHERE {\n' \
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

    start = time.time()
    res = _iterative_query(query, mdb, 5000)
    processtime = time.time()
    print('Graph analysis query time:', (processtime - start))
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
                        edges.append((r['subject'], r['target']))  # , {'relation': r['mtp'] if 'mtp' in r else ' '}
            result = compute_graph_properties(list(set(nodes)), edges)
            print('Graph analysis time: ', (time.time() - processtime))
            return result
    else:
        return []


def compute_graph_properties(nodes: list, edges: list):
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
    #
    # x = nx.average_node_connectivity(G)
    #
    # print('t,x,cc', t, x, cc)
    res = [
        ['Density', density],
        ['Nodes', n],
        ['Edges', e],
        ['Connected Components', cc],
        ['Avg. Clustering', c],
        ['Transitivity', t],
        # [6, 'Avg. Node Connectivity', x],
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
