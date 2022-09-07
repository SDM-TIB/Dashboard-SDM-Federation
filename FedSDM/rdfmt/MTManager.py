from FedSDM import get_logger
from FedSDM.rdfmt.model import *
from FedSDM.rdfmt.prefixes import *
from FedSDM.rdfmt.utils import contact_rdf_source, iterative_query

logger = get_logger('rdfmts', './rdfmts.log', True)
"""Logger for this module. It logs to the file 'rdfmts.log' as well as to stdout."""


class MTManager(object):
    """Provides an abstract way to access the RDF Molecule Templates in :class:`FedSDM.config.ConfigSimpleStore`.

    This class is used in the configuration of FedSDM in order to access the RDF Molecule Templates.

    """

    def __init__(self, query_url: str, user: str, passwd: str, graph: str):
        """Creates a new *MTManager* instance.

        The *MTManager* object can be used to access the RDF Molecule Templates
        of the federation the instance is initialized for.

        Parameters
        ----------
        query_url : str
            The URL of the SPARQL endpoint used for querying the metadata.
        user : str
            The username required in order to get access.
        passwd : str
            The user's password required for authentication.
        graph : str
            The graph URI used in the SPARQL endpoint for storing the metadata of the federation.

        """
        self.graph = graph
        self.query_endpoint = query_url
        self.user = user
        self.passwd = passwd

    def get_data_sources(self) -> list:
        """Gets all sources of the federation the :class:`MTManager` instance was initialized for.

        Returns
        -------
        list
            A list containing the IDs and URLs of all datasources associated with the federation.

        """
        query = 'SELECT DISTINCT ?rid ?endpoint WHERE { GRAPH <' + self.graph + '> {\n' \
                '  ?rid a <' + MT_ONTO + 'DataSource> .\n' \
                '  ?rid <' + MT_ONTO + 'url> ?endpoint .\n' \
                '}}'
        res_list, _ = iterative_query(query, self.query_endpoint, limit=1000)
        return res_list

    def get_rdfmts(self) -> dict:
        """Gets the RDF Molecule Templates of the federation the :class:`MTManager` instance was initialized for.

        Extracts all RDF Molecule Templates of the federation from the metadata knowledge graph.

        Returns
        -------
        dict
            A dictionary with the RDF Molecule Templates of the federation.

        """
        query = 'SELECT DISTINCT ?rid ?datasource ?pred ?mtr ?mtrange WHERE { GRAPH <' + self.graph + '> {\n' \
                '  ?rid a <' + MT_ONTO + 'RDFMT> .\n' \
                '  ?rid <' + MT_ONTO + 'source> ?source .\n' \
                '  ?rid <' + MT_ONTO + 'hasProperty> ?mtp .\n' \
                '  ?mtp <' + MT_ONTO + 'predicate> ?pred .\n' \
                '  ?source <' + MT_ONTO + 'datasource> ?datasource .\n' \
                '  OPTIONAL {\n' \
                '    ?mtp <' + MT_ONTO + 'linkedTo> ?mtrange .\n' \
                '    ?mtrange <' + MT_ONTO + 'rdfmt> ?mtr .\n' \
                '  }\n' \
                '}}'
        return self.prepare_rdfmts_from_query(query)

    def get_rdfmt(self, rdf_class: str) -> dict:
        """Gets the RDF Molecule Template of a specific RDF class.

        Extracts the RDF Molecule Template of the specified RDF class from the metadata knowledge graph.

        Parameters
        ----------
        rdf_class : str
            The RDF class for which the RDF Molecule Template should be extracted.

        Returns
        -------
        dict
            A dictionary representing the RDF Molecule Template of the RDF class *rdf_class*.

        """
        query = 'SELECT DISTINCT ?datasource ?pred WHERE { GRAPH <' + self.graph + '> {\n' \
                '  <' + rdf_class + '> <' + MT_ONTO + 'source> ?source .\n' \
                '  <' + rdf_class + '> <' + MT_ONTO + 'hasProperty> ?mtp .\n' \
                '  ?mtp <' + MT_ONTO + 'predicate> ?pred .\n' \
                '  ?source <' + MT_ONTO + 'datasource> ?datasource.\n' \
                '}}'
        return self.prepare_rdfmts_from_query(query, rdf_class)

    def prepare_rdfmts_from_query(self, query: str, rdf_class: str = None) -> dict:
        """Prepares the RDF Molecule Template dictionary from a SPARQL query.

        This method executed the given SPARQL query and transforms the returned
        result into the known RDF Molecule Template dictionary structure.

        Parameters
        ----------
        query : str
            The SPARQL query to be executed in order to retrieve the metadata.
        rdf_class : str, optional
            The RDF class for which the RDF Molecule Template should be
            returned. If no value is passed, all RDF Molecule Templates
            will be included in the result.

        Returns
        -------
        dict
            A dictionary with the known RDF Molecule Template structure for
            the RDF Molecule Templates that got extracted by executing the
            SPARQL query against the metadata knowledge graph.

        """
        res_list, _ = iterative_query(query, self.query_endpoint, limit=1000)
        results = {}
        for r in res_list:
            if rdf_class is not None:
                r['rid'] = rdf_class
            if r['rid'] not in results:
                results[r['rid']] = {
                    'rootType': r['rid'],
                    'linkedTo': [r['mtr']] if 'mtr' in r else [],
                    'wrappers': [{
                        'url': self.get_data_source(r['datasource']).url,
                        'predicates': [r['pred']],
                        'urlparam': '',
                        'wrapperType': 'SPARQLEndpoint'
                    }],
                    'predicates': [{
                        'predicate': r['pred'],
                        'range': [r['mtr']] if 'mtr' in r else []
                    }],
                    'subclass': []
                }
            else:
                if 'mtr' in r:
                    results[r['rid']]['linkedTo'].append(r['mtr'])
                    results[r['rid']]['linkedTo'] = list(set(results[r['rid']]['linkedTo']))
                pred_found = False
                for p in results[r['rid']]['predicates']:
                    if p['predicate'] == r['pred']:
                        if 'mtr' in r:
                            p['range'].append(r['mtr'])
                        pred_found = True

                if not pred_found:
                    results[r['rid']]['predicates'].append({
                        'predicate': r['pred'],
                        'range': [r['mtr']] if 'mtr' in r else []
                    })
                wrapper_found = False
                for w in results[r['rid']]['wrappers']:
                    if w['url'] == r['datasource']:  # TODO: is this correct due to the changes?
                        wrapper_found = True
                        w['predicates'].append(r['pred'])
                        w['predicates'] = list(set(w['predicates']))
                if not wrapper_found:
                    results[r['rid']]['wrappers'].append({
                        'url': self.get_data_source(r['datasource']).url,
                        'predicates': [
                            r['pred']
                        ],
                        'urlparam': '',
                        'wrapperType': 'SPARQLEndpoint'
                    })
        if rdf_class is not None:
            return results[rdf_class] if rdf_class in results else {}
        else:
            return results

    def get_data_source(self, ds_id: str) -> Optional[DataSource]:
        """Gets a single source of the federation as :class:`DataSource` instance.

        This method extracts all information available about the specified datasource
        for the federation the :class:`MTManger` instance was initialized for.
        The extracted data is then transformed into a :class:`DataSource` object.

        Parameters
        ----------
        ds_id : str
            The identifier of the datasource of interest.

        Returns
        -------
        DataSource | None
            A :class:`DataSource` instance representing the datasource with the ID *ds_id*.
            If no such datasource was found, None is returned.

        """
        query = 'SELECT DISTINCT *  WHERE { GRAPH <' + self.graph + '> {\n' \
                '  <' + ds_id + '> <' + MT_ONTO + 'url> ?url .\n' \
                '  <' + ds_id + '> <' + MT_ONTO + 'dataSourceType> ?dstype .\n' \
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'name> ?name }\n' \
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'version> ?version }\n' \
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'keywords> ?keywords }\n' \
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'organization> ?organization }\n' \
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'homepage> ?homepage }\n' \
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'params> ?params }\n' \
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'desc> ?desc }\n' \
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'triples> ?triples }\n' \
                '}}'
        res_list, _ = iterative_query(query, self.query_endpoint, limit=1000)
        if len(res_list) > 0:
            e = res_list[0]
            return DataSource(
                ds_id,
                e['url'],
                e['dstype'],
                name=e['name'],
                desc=e['desc'] if 'desc' in e else '',
                params=e['params'] if 'params' in e else {},
                keywords=e['keywords'] if 'keywords' in e else '',
                version=e['version'] if 'version' in e else '',
                homepage=e['homepage'] if 'homepage' in e else '',
                organization=e['organization'] if 'organization' in e else '',
                triples=e['triples'] if 'triples' in e else -1,
                ontology_graph=e['ontology_graph'] if 'ontology_graph' in e else None
            )
        else:
            return None

    def get_mappings(self, ds_id: str) -> list:
        """Gets the mappings of a datasource from the metadata knowledge graph.

        This method uses a SPARQL query to retrieve the RDF Mapping Language (RML)
        mappings associated with the specified datasource.

        Parameters
        ----------
        ds_id : str
            The identifier of the datasource of interest.

        Returns
        -------
        list
            A list with the mappings of the datasource in form a SPARQL query result.

        """
        mt_query = 'PREFIX rr: <http://www.w3.org/ns/r2rml#> ' \
                   'PREFIX rml: <http://semweb.mmlab.be/ns/rml#>' \
                   'SELECT DISTINCT ?t ?p ?r ?rds WHERE { GRAPH <' + self.graph + '> {\n' \
                   '  ?tm rml:logicalSource ?ls .\n' \
                   '  ?ls rml:source <' + ds_id + '> .\n' \
                   '  ?tm rr:subjectMap ?sm .\n' \
                   '  ?sm rr:class ?t .\n' \
                   '  ?tm rr:predicateObjectMap ?pom .\n' \
                   '  ?pom rr:predicate ?p .\n' \
                   '  ?pom rr:objectMap ?om .\n' \
                   '  OPTIONAL {\n' \
                   '    ?om rr:parentTriplesMap ?pt .\n' \
                   '    ?pt rr:subjectMap ?ptsm .\n' \
                   '    ?ptsm rr:class ?r .\n' \
                   '    ?pt rml:logicalSource ?ptls .\n' \
                   '    ?ptls rml:source ?rds .\n' \
                   '  }\n' \
                   '}}'
        print(mt_query)
        res, card = contact_rdf_source(mt_query, self.query_endpoint)
        return res

    def get_rdfmts_by_preds(self, predicates: list) -> dict:
        """Gets all RDF Molecule Templates that cover a set of predicates.

        This method returns all RDF Molecule Templates that cover all predicates specified.
        The metadata knowledge graph is queried in order to get the IDs of all RDF Molecule
        Templates that contain all the predicates specified by the user. Then, for each
        result, the actual RDF Molecule Template is extracted and added to the result.

        Parameters
        ----------
        predicates : list
            A list of predicates that need to be covered by the RDF Molecule Template.

        Returns
        -------
        dict
            A dictionary with all RDF Molecule Templates that cover the specified predicates.

        """
        query = 'SELECT DISTINCT ?rid WHERE { GRAPH <' + self.graph + '> {\n' \
                '  ?rid a <' + MT_ONTO + 'RDFMT> .\n'
        i = 0
        for p in predicates:
            query += '  ?rid <' + MT_ONTO + 'hasProperty> ?mtp' + str(i) + '.\n' \
                     '  ?mtp' + str(i) + ' <' + MT_ONTO + 'predicate> <' + p + '> .\n'
            i += 1

        query += '}}'
        res_list, _ = iterative_query(self.query_endpoint, query, limit=1000)

        results = {}
        for r in res_list:
            res = self.get_rdfmt(r['rid'])
            if len(res) > 0:
                results[r['rid']] = res
        return results

    def get_preds_mt(self, predicates: list = None) -> dict:
        """Gets a mapping from predicates to RDF Molecule Templates limited to a certain set of predicates.

        This method returns a dictionary mapping predicates to RDF Molecule Templates that cover
        this particular predicate. The dictionary is limited to the predicates specified by the
        user. If no value is passed, the dictionary includes all predicates available.

        Parameters
        ----------
        predicates : list, optional
            A list of predicates to which the predicate to RDF Molecule Template map should be limited to.

        Returns
        -------
        dict
            A dictionary mapping predicates to the RDF Molecule Templates that cover them.

        """
        filters = ' || '.join(['?pred=<' + p + '> ' for p in predicates]) if predicates is not None else ''
        query = 'SELECT DISTINCT ?rid ?pred WHERE { GRAPH <' + self.graph + '> {\n' \
                '  ?rid a <' + MT_ONTO + 'RDFMT> .\n' \
                '  ?rid <' + MT_ONTO + 'hasProperty> ?mtp .\n' \
                '  ?mtp <' + MT_ONTO + 'predicate> ?pred .\n'
        if len(filters) > 0:
            query += '  FILTER (' + filters + ')\n'
        query += '}}'
        res_list, _ = iterative_query(query, self.query_endpoint, limit=1000)

        results = {}
        for r in res_list:
            results.setdefault(r['pred'], []).append(r['rid'])
        results = {r: list(set(results[r])) for r in results}
        return results
