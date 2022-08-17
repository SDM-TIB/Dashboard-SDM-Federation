from flask import g

from FedSDM.db import get_mdb, MetadataDB


def _process_numeric_result(mdb: MetadataDB, query: str) -> int:
    """
    Executes a SPARQL query and returns the result as an integer.

    The method assumes that the SPARQL query will return at most one result with a variable called *count*.

    :param mdb: An instance of *MetadataDB* used for executing the SPARQL query against a SPARQL endpoint
    :type mdb: MetadataDB
    :param query: The SPARQL query as string; see above for the assumptions made
    :type query: str
    :return: Integer value of the query result
    :rtype: int
    """
    res, card = mdb.query(query)
    if card > 0:
        card = res[0]['count']
        if '^^' in card:
            card = int(card[:card.find('^^')])
        return int(card)
    else:
        return 0


def get_federations() -> list:
    """
    Gets the identifier and human-readable name for all available federations.

    This method uses the *MetadataDB* to retrieve information about all federations available.
    Each query result will include the identifier of the federation in the variable *uri* and
    the human-readable name in the variable *name*.

    :return: A list containing information about all federations available.
    :rtype: list
    """
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


def get_datasources(graph: str = None) -> dict:
    """
    Gets all datasources belonging to a particular federation; or all of them.

    Makes use of *MetadataDB* to retrieve the metadata about the datasource of a
    particular federation. The federation is specified as its identifier (URI).
    If no federation is given to the method, all datasources are retrieved.

    :param graph: The URI of the federation all datasources should be returned for.
    :type graph: str
    :return: A dictionary with the metadata about the datasources, their identifiers are the keys.
    :rtype: dict
    """
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


def get_num_rdfmts(graph: str, datasource: str = None) -> int:
    """
    Gets the number of RDF Molecule Templates of a federation; or a datasource in that federation.

    Uses *MetadataDB* to retrieve the number of RDF Molecule Templates of the specified federation.
    If a datasource was passed as well, the number of RDF Molecule Templates of that datasource in
    the mentioned federation is retrieved.

    :param graph: The URI of the federation the number of RDF Molecule Templates should be returned for.
    :type graph: str
    :param datasource: The URI of the datasource of interest.
    :type datasource: str
    :return: The number of RDF Molecule Templates in the federation (or the datasource).
    :rtype: int
    """
    mdb = get_mdb()
    source = '?ds' if datasource is None else '<' + datasource + '>'
    query = 'SELECT (COUNT (DISTINCT ?mt) AS ?count) WHERE { GRAPH <' + graph + '> {\n' \
            '  ?mt a mt:RDFMT .\n' \
            '  ?mt  mt:source  ?mtsource .\n'\
            '  ?mtsource mt:datasource ' + source + ' .\n' \
            '}}'
    return _process_numeric_result(mdb, query)


def get_num_mt_links(graph: str, datasource: str = None) -> int:
    """
    Gets the number of links between RDF Molecule Templates in a federation; or a datasource in that federation.

    Uses the *MetadataDB* to retrieve th number of links between RDF Molecule Templates in the specified federation.
    If a datasource is passed as well, the number of links within that datasource of the specified federation
    is returned instead.

    :param graph: The URI of the federation the number of links between RDF Molecule Templates should be returned for.
    :type graph: str
    :param datasource: The URI of the datasource of interest.
    :type datasource: str
    :return: The number of links between RDF Molecule Templates in the federation (or the datasource).
    :rtype: int
    """
    mdb = get_mdb()
    source = '?ds' if datasource is None else '<' + datasource + '>'
    query = 'SELECT (COUNT(DISTINCT ?d) as ?count) WHERE { GRAPH <' + graph + '> {\n' \
            '  ?d a mt:PropRange .\n' \
            '  ?d mt:name ?mt .\n' \
            '  ?d mt:datasource ' + source + ' .\n' \
            '}}'
    return _process_numeric_result(mdb, query)


def get_num_properties(graph: str, datasource: str = None) -> int:
    """
    Gets the number of predicates (properties) within a particular federation; or datasource in that federation.

    Makes use of *MetadataDB* to count the number of distinct predicates (properties) within the
    specified federation. If a datasource is passed, the number of distinct predicates (properties)
    of that datasource in the specified federation is returned instead.

    :param graph: The URI of the federation the number of properties should be returned for.
    :type graph: str
    :param datasource: The URI of the datasource of interest.
    :type datasource: str
    :return: The number of properties in the federation (or the datasource).
    :rtype: int
    """
    mdb = get_mdb()
    source = '?ds' if datasource is None else '<' + datasource + '>'
    query = 'SELECT (COUNT (DISTINCT ?mtp) AS ?count) WHERE { GRAPH <' + graph + '> {\n' \
            '  ?mt a mt:RDFMT .\n' \
            '  ?mt mt:source  ?mtsource .\n' \
            '  ?mt mt:hasProperty ?mtp .\n' \
            '  ?mtsource mt:datasource ' + source + ' .\n' \
            '}}'
    return _process_numeric_result(mdb, query)


def get_federation_stats() -> list:
    """
    Gets detailed statistics about the available federations.

    Uses *MetadataDB* to retrieve detailed statistics about the available federations.
    These statistics include the number of datasources (*sources*), number of RDF
    Molecule Templates (*rdfmts*), number of links between RDF Molecule Templates (*links*),
    number of predicates (*properties*), and the number of triples (*triples*).

    :return: A list including the above-mentioned statistics for all available federations.
    :rtype: list
    """
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
