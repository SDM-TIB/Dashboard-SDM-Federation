from flask import g

from FedSDM.db import get_mdb, MetadataDB


def _process_numeric_result(mdb: MetadataDB, query: str):
    res, card = mdb.query(query)
    if card > 0:
        card = res[0]['count']
        if '^^' in card:
            card = int(card[:card.find('^^')])
        return int(card)
    else:
        return 0


def get_federations():
    mdb = get_mdb()

    query = 'SELECT DISTINCT ?uri ?name WHERE { GRAPH <' + g.default_graph + '> {\n' \
            '  ?uri a mt:Federation .\n' \
            '  ?uri mt:name ?name .\n' \
            '}}'
    res, card = mdb.query(query)
    if card > 0:
        return res
    else:
        print('no federations available...')
        return []


def get_datasources(graph: str = None):
    mdb = get_mdb()
    graph_clause = '' if graph is None else ' GRAPH <' + graph + '> {'
    closing_brackets = '}' if graph is None else '}}'
    query = 'SELECT DISTINCT ?uri ?source ?triples WHERE {' + graph_clause + '\n' \
            '  ?uri a mt:DataSource .\n' \
            '  ?uri  mt:name ?source .\n' \
            '  OPTIONAL { ?uri mt:triples ?triples . }\n' + closing_brackets
    res, card = mdb.query(query)
    if card > 0:
        return {d['uri']: d for d in res}
    else:
        return {}


def get_num_rdfmts(graph: str, datasource: str = None):
    mdb = get_mdb()
    if datasource is not None:
        query = 'SELECT (COUNT (DISTINCT ?mt) AS ?count) WHERE { GRAPH <' + graph + '> {\n' \
                '  ?mt a mt:RDFMT .\n' \
                '  ?mt  mt:source  ?mtsource .\n'\
                '  ?mtsource mt:datasource <' + datasource + '> .\n' \
                '}}'

    else:
        query = 'SELECT (COUNT (DISTINCT ?mt) AS ?count) WHERE { GRAPH <' + graph + '> {\n' \
                '  ?mt a mt:RDFMT .\n' \
                '  ?mt  mt:source  ?mtsource .\n' \
                '  ?mtsource mt:datasource ?ds .\n' \
                '}}'

    return _process_numeric_result(mdb, query)


def get_mtconns(graph: str, datasource: str = None):
    mdb = get_mdb()
    if datasource is not None:
        query = 'SELECT (COUNT(DISTINCT ?d) as ?count) WHERE { GRAPH <' + graph + '> {\n' \
                '  ?d a mt:PropRange .\n' \
                '  ?d mt:name ?mt .\n' \
                '  ?d mt:datasource <' + datasource + '> .\n' \
                '}}'
    else:
        query = 'SELECT (COUNT(DISTINCT ?d) as ?count) WHERE { GRAPH <' + graph + '> {\n' \
                '  ?d a mt:PropRange .\n' \
                '  ?d mt:name ?mt .\n' \
                '  ?d mt:datasource ?ds .\n' \
                '}}'

    return _process_numeric_result(mdb, query)


def get_num_properties(graph: str, datasource: str = None):
    mdb = get_mdb()
    if datasource is not None:
        query = 'SELECT (COUNT (DISTINCT ?mtp) AS ?count) WHERE { GRAPH <' + graph + '> {\n' \
                '  ?mt a mt:RDFMT .\n' \
                '  ?mt mt:source  ?mtsource .\n' \
                '  ?mt mt:hasProperty ?mtp .\n' \
                '  ?mtsource mt:datasource <' + datasource + '> .\n' \
                '}}'

    else:
        query = 'SELECT (COUNT (DISTINCT ?mtp) AS ?count) WHERE { GRAPH <' + graph + '> {\n' \
                '  ?mt a mt:RDFMT .\n' \
                '  ?mt mt:source  ?mtsource .\n' \
                '  ?mt mt:hasProperty ?mtp .\n' \
                '  ?mtsource mt:datasource ?ds .\n' \
                '}}'

    return _process_numeric_result(mdb, query)


def get_federation_stats():
    mdb = get_mdb()
    query = 'SELECT DISTINCT ?fed ?name (COUNT(DISTINCT ?ds) AS ?sources) (SUM(COALESCE(?count_mts, 0)) AS ?rdfmts) ' \
            '(SUM(COALESCE(?count_links, 0)) AS ?links) (SUM(COALESCE(?count_prop, 0)) AS ?properties) ' \
            '(SUM(COALESCE(?ds_triples, 0)) AS ?triples) WHERE {\n' \
            '  ?fed a mt:Federation .\n' \
            '  ?fed mt:name ?name .\n' \
            '  OPTIONAL { SELECT DISTINCT ?fed ?ds (COUNT (DISTINCT ?mt) AS ?count_mts) ' \
            '  (COUNT(DISTINCT ?d) as ?count_links) (COUNT (DISTINCT ?mtp) AS ?count_prop) WHERE { GRAPH ?fed {\n' \
            '    OPTIONAL {\n' \
            '      ?ds a mt:DataSource .\n' \
            '      OPTIONAL {\n' \
            '        ?d a mt:PropRange .\n' \
            '        ?d mt:datasource ?ds .\n' \
            '      }\n' \
            '    }\n' \
            '    OPTIONAL {\n' \
            '      ?mt a mt:RDFMT .\n' \
            '      ?mt mt:source ?mtsource .\n' \
            '      ?mt mt:hasProperty ?mtp .\n' \
            '      ?mtsource mt:datasource ?ds .\n' \
            '    }\n' \
            '  }} GROUP BY ?fed ?ds }\n' \
            '  OPTIONAL { SELECT DISTINCT ?fed ?ds ?ds_triples WHERE { GRAPH ?fed {\n' \
            '    ?ds mt:triples ?ds_triples .\n' \
            '  }}}\n' \
            '} GROUP BY ?fed ?name'

    res, _ = mdb.query(query)
    return res
