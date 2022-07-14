from flask import g
from fedsdm.db import get_mdb


def _process_numeric_result(mdb, query):
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
            '  ?uri a <' + mdb.mtonto + 'Federation> .\n' \
            '  ?uri <' + mdb.mtonto + 'name> ?name .\n' \
            '}}'
    res, card = mdb.query(query)
    if card > 0:
        return res
    else:
        print('no federations available...')
        return []


def get_datasources(graph=None):
    mdb = get_mdb()
    if graph is not None:
        query = 'SELECT DISTINCT ?uri ?source ?triples WHERE { GRAPH <' + graph + '> {\n' \
                '  ?uri a <' + mdb.mtonto + 'DataSource> .\n' \
                '  ?uri  <' + mdb.mtonto + 'name> ?source .\n' \
                '  OPTIONAL { ?uri <' + mdb.mtonto + 'triples> ?triples . }\n' \
                '}}'
    else:
        query = 'SELECT DISTINCT ?uri ?source ?triples WHERE {\n' \
                '  ?uri a <' + mdb.mtonto + 'DataSource> .\n' \
                '  ?uri  <' + mdb.mtonto + 'name> ?source .\n' \
                '  ?uri <' + mdb.mtonto + 'triples> ?triples .\n' \
                '}'
    res, card = mdb.query(query)
    if card > 0:
        return {d['uri']: d for d in res}
    else:
        return {}


def get_num_rdfmts(graph, datasource=None):
    mdb = get_mdb()
    if datasource is not None:
        query = 'SELECT (COUNT (DISTINCT ?mt) AS ?count) WHERE { GRAPH <' + graph + '> {\n' \
                '  ?mt a <' + mdb.mtonto + 'RDFMT> .\n' \
                '  ?mt  <' + mdb.mtonto + 'source>  ?mtsource .\n'\
                '  ?mtsource <' + mdb.mtonto + 'datasource> <' + datasource + '> .\n' \
                '}}'

    else:
        query = 'SELECT (COUNT (DISTINCT ?mt) AS ?count) WHERE { GRAPH <' + graph + '> {\n' \
                '  ?mt a <' + mdb.mtonto + 'RDFMT> .\n' \
                '  ?mt  <' + mdb.mtonto + 'source>  ?mtsource .\n' \
                '  ?mtsource <' + mdb.mtonto + 'datasource> ?ds .\n' \
                '}}'

    return _process_numeric_result(mdb, query)


def get_mtconns(graph, datasource=None):
    mdb = get_mdb()
    if datasource is not None:
        query = 'SELECT (COUNT(DISTINCT ?d) as ?count) WHERE { GRAPH <' + graph + '> {\n' \
                '  ?d a <' + mdb.mtonto + 'PropRange> .\n' \
                '  ?d <' + mdb.mtonto + 'name> ?mt .\n' \
                '  ?d <' + mdb.mtonto + 'datasource> <' + datasource + '> .\n' \
                '}}'
    else:
        query = 'SELECT (COUNT(DISTINCT ?d) as ?count) WHERE { GRAPH <' + graph + '> {\n' \
                '  ?d a <' + mdb.mtonto + 'PropRange> .\n' \
                '  ?d <' + mdb.mtonto + 'name> ?mt .\n' \
                '  ?d <' + mdb.mtonto + 'datasource> ?ds .\n' \
                '}}'

    return _process_numeric_result(mdb, query)


def get_num_properties(graph, datasource=None):
    mdb = get_mdb()
    if datasource is not None:
        query = 'SELECT (COUNT (DISTINCT ?mtp) AS ?count) WHERE { GRAPH <' + graph + '> {\n' \
                '  ?mt a <' + mdb.mtonto + 'RDFMT> .\n' \
                '  ?mt  <' + mdb.mtonto + 'source>  ?mtsource .\n' \
                '  ?mt <' + mdb.mtonto + 'hasProperty> ?mtp .\n' \
                '  ?mtsource <' + mdb.mtonto + 'datasource> <' + datasource + '> .\n' \
                '}}'

    else:
        query = 'SELECT (COUNT (DISTINCT ?mtp) AS ?count) WHERE { GRAPH <' + graph + '> {\n' \
                '  ?mt a <' + mdb.mtonto + 'RDFMT> .\n' \
                '  ?mt  <' + mdb.mtonto + 'source>  ?mtsource .\n' \
                '  ?mt <' + mdb.mtonto + 'hasProperty> ?mtp .\n' \
                '  ?mtsource <' + mdb.mtonto + 'datasource> ?ds .\n' \
                '}}'

    return _process_numeric_result(mdb, query)


def get_federation_stats():
    mdb = get_mdb()
    query = 'SELECT DISTINCT ?fed ?name (COUNT(DISTINCT ?ds) AS ?sources) (SUM(COALESCE(?count_mts, 0)) AS ?rdfmts) ' \
            '(SUM(COALESCE(?count_links, 0)) AS ?links) (SUM(COALESCE(?count_prop, 0)) AS ?properties) ' \
            '(SUM(COALESCE(?ds_triples, 0)) AS ?triples) WHERE {\n' \
            '  ?fed a <' + mdb.mtonto + 'Federation> .\n' \
            '  ?fed <' + mdb.mtonto + 'name> ?name .\n' \
            '  OPTIONAL { SELECT DISTINCT ?fed ?ds (COUNT (DISTINCT ?mt) AS ?count_mts) ' \
            '  (COUNT(DISTINCT ?d) as ?count_links) (COUNT (DISTINCT ?mtp) AS ?count_prop) WHERE { GRAPH ?fed {\n' \
            '    OPTIONAL {\n' \
            '      ?ds a <' + mdb.mtonto + 'DataSource> .\n' \
            '      OPTIONAL {\n' \
            '        ?d a <' + mdb.mtonto + 'PropRange> .\n' \
            '        ?d <' + mdb.mtonto + 'datasource> ?ds .\n' \
            '      }\n' \
            '    }\n' \
            '    OPTIONAL {\n' \
            '      ?mt a <' + mdb.mtonto + 'RDFMT> .\n' \
            '      ?mt <' + mdb.mtonto + 'source> ?mtsource .\n' \
            '      ?mt <' + mdb.mtonto + 'hasProperty> ?mtp .\n' \
            '      ?mtsource <' + mdb.mtonto + 'datasource> ?ds .\n' \
            '    }\n' \
            '  }} GROUP BY ?fed ?ds }\n' \
            '  OPTIONAL { SELECT DISTINCT ?fed ?ds ?ds_triples WHERE { GRAPH ?fed {\n' \
            '    ?ds <' + mdb.mtonto + 'triples> ?ds_triples .\n' \
            '  }}}\n' \
            '} GROUP BY ?fed ?name'

    from fedsdm import get_logger
    logger = get_logger(__name__)
    logger.info(query)
    res, _ = mdb.query(query)
    return res
