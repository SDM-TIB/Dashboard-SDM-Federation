from flask import g
from fedsdm.db import get_mdb


def get_federations():
    mdb = get_mdb()

    query = "SELECT DISTINCT ?uri ?name WHERE {" \
            " GRAPH <" + g.default_graph + "> {?uri a <http://tib.eu/dsdl/ontario/ontology/Federation>. "\
                                           " ?uri <http://tib.eu/dsdl/ontario/ontology/name> ?name } }"
    res, card = mdb.query(query)
    if card > 0:
        return res
    else:
        print("no federations available ...")
        return []


def get_datasources(graph=None):
    mdb = get_mdb()
    if graph is not None:
        query = "SELECT DISTINCT ?uri ?source ?triples WHERE { GRAPH <" + graph + "> { " \
                    "?uri a <" + mdb.mtonto + "DataSource> . " \
                    "?uri  <" + mdb.mtonto + "name> ?source . " \
                    "OPTIONAL {?uri <" + mdb.mtonto + "triples> ?triples .} " \
                    "}}"
    else:
        query = "SELECT DISTINCT ?uri ?source ?triples WHERE {" \
                "    ?uri a <" + mdb.mtonto + "DataSource>." \
                "    ?uri  <" + mdb.mtonto + "name> ?source . " \
                "    ?uri <" + mdb.mtonto + "triples> ?triples ." \
                "   }"
    res, card = mdb.query(query)
    if card > 0:
        return {d['uri']: d for d in res}
    else:
        return {}


def get_num_rdfmts(graph, datasource=None):
    mdb = get_mdb()
    if datasource is not None:
        query = " SELECT (COUNT (DISTINCT ?mt) AS ?count) WHERE { GRAPH <" + graph + "> { ?mt a <" + mdb.mtonto + "RDFMT> ." \
                "           ?mt  <" + mdb.mtonto + "source>  ?mtsource. "\
                "           ?mtsource <" + mdb.mtonto + "datasource> <" + datasource + "> . } }"

    else:
        query = " SELECT (COUNT (DISTINCT ?mt) AS ?count) WHERE { GRAPH <" + graph + "> { ?mt a <" + mdb.mtonto + "RDFMT> ." \
               "           ?mt  <" + mdb.mtonto + "source>  ?mtsource. " \
               "           ?mtsource <" + mdb.mtonto + "datasource> ?ds . } }"

    res, card = mdb.query(query)

    if card > 0:
        card = res[0]['count']

        if '^^' in card:
            card = int(card[:card.find("^^")])

        return int(card)
    else:

        return 0


def get_mtconns(graph, datasource=None):
    mdb = get_mdb()
    if datasource is not None:
        query = "SELECT  (COUNT(DISTINCT ?d) as ?count) WHERE { GRAPH <" + graph + "> {" \
            " ?d a <" + mdb.mtonto + "PropRange> . " \
            " ?d <" + mdb.mtonto + "name> ?mt ." \
            " ?d <" + mdb.mtonto + "datasource> <" + datasource +"> ." \
            " }}"
    else:
        query = "SELECT  (COUNT(DISTINCT ?d) as ?count) WHERE { GRAPH <" + graph + "> {" \
                  "?d a <" + mdb.mtonto + "PropRange> . " \
                  "?d <" + mdb.mtonto + "name> ?mt ." \
                  "?d <" + mdb.mtonto + "datasource> ?ds ." \
                "}}"
    res, card = mdb.query(query)
    if card > 0:
        card = res[0]['count']
        if '^^' in card:
            card = int(card[:card.find("^^")])
        return int(card)
    else:
        return 0


def get_num_properties(graph, datasource=None):
    mdb = get_mdb()
    if datasource is not None:
        query = " SELECT (COUNT (DISTINCT ?mtp) AS ?count) WHERE { GRAPH <" + graph + "> { " \
                "           ?mt a <" + mdb.mtonto + "RDFMT> ." \
                "           ?mt  <" + mdb.mtonto + "source>  ?mtsource. " \
                "           ?mt <" + mdb.mtonto + "hasProperty> ?mtp . " \
                "           ?mtsource <" + mdb.mtonto + "datasource> <" + datasource + "> . } }"

    else:
        query = " SELECT (COUNT (DISTINCT ?mtp) AS ?count) WHERE { GRAPH <" + graph + "> { " \
                  "           ?mt a <" + mdb.mtonto + "RDFMT> ." \
                  "           ?mt  <" + mdb.mtonto + "source>  ?mtsource. " \
                  "           ?mt <" + mdb.mtonto + "hasProperty> ?mtp . " \
                  "           ?mtsource <" + mdb.mtonto + "datasource> ?ds . } }"

    res, card = mdb.query(query)
    if card > 0:
        card = res[0]['count']
        if '^^' in card:
            card = int(card[:card.find("^^")])
        return int(card)
    else:
        return 0