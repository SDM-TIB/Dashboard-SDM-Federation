from __future__ import annotations  # Python 3.12 still has issues with if TYPE_CHECKING during runtime

import datetime
import hashlib
from multiprocessing import Queue, Process
from queue import Empty
from typing import List, Optional, TYPE_CHECKING

from FedSDM import get_logger
from FedSDM.rdfmt.model import RDFMT, MTProperty, PropRange, DataSource, DataSourceType, Source
from FedSDM.rdfmt.prefixes import RDFS, XSD, metas, MT_RESOURCE, MT_ONTO
from FedSDM.rdfmt.utils import contact_rdf_source, iterative_query

if TYPE_CHECKING:
    from FedSDM.db import MetadataDB

logger = get_logger('rdfmts', './rdfmts.log', True)
"""Logger for this module. It logs to the file 'rdfmts.log' as well as to stdout."""


class RDFMTMgr(object):
    """Provides an abstract way to manage the RDF Molecule Templates of a federation.

    The *RDFMTMgr* allows to easily create and modify the RDF Molecule Templates of a single federation.
    The class contains several utility functions necessary to collect the metadata or update it.
    The managed metadata is stored in an RDF knowledge graph.

    """

    def __init__(self, mdb: MetadataDB, graph: str):
        """Creates a new *RDFMTMgr* instance.

        The *RDFMTMgr* object can be used to create and/or update the RDF Molecule Templates
        of the federation the instance is initialized for.

        Parameters
        ----------
        mdb : MetadataDB
            Instance of the metadata endpoint holding source descriptions.
        graph : str
            The graph URI used in the SPARQL endpoint for storing the metadata of the federation.

        """
        self.graph = graph
        self.mdb = mdb

    def create(self, ds: DataSource, out_queue: Queue = Queue(), is_update: bool = False) -> dict:
        """(Re-)creates the RDF Molecule Templates of a datasource within the federation.

        This method (re-)creates the RDF Molecule Templates of a single datasource within the
        federation the :class:`RDFMTMgr` instance was initialized for.

        Parameters
        ----------
        ds : DataSource
            The datasource the RDF Molecule Templates should be collected for.
        out_queue : multiprocessing.Queue, optional
            A queue object used to transmit the results. If none is given, a new one will be created.
            However, in that case it is not possible to retrieve the results from the queue.
        is_update : bool, optional
            A Boolean indicating whether this is an update of the RDF Molecule Templates for this datasource.
            By default, it is set to false which indicates the first creation of the RDF Molecule Templates
            for the datasource. The number of triples in the dataset are recorded during the initial collection
            of the metadata. In all subsequent updates, only the RDF Molecule Templates and the modification
            date are changed.

        Returns
        -------
        dict
            A dictionary with the RDF Molecule Templates for the datasource *ds*.

        """
        if not is_update:
            # Get #triples of a dataset
            triples = self.get_cardinality(ds)
            ds.triples = triples
            data = '<' + ds.rid + '> <' + MT_ONTO + 'triples> ' + str(triples)
            self.update_graph([data])
        else:
            today = str(datetime.datetime.now())
            data = ['<' + ds.rid + '> <http://purl.org/dc/terms/modified> "' + today + '"']
            delete = ['<' + ds.rid + '> <http://purl.org/dc/terms/modified> ?modified ']
            self.delete_insert_data(delete, data, delete)

        results = self.get_rdfmts(ds)
        # self.create_inter_ds_links(datasource=ds)
        out_queue.put('EOF')
        return results

    def get_rdfmts(self, datasource: DataSource) -> dict:
        """Extracts the RDF Molecule Templates from a datasource.

        Extracts the RDF Molecule Templates from a datasource.
        This method is the entrypoint for the metadata extraction.
        The RDF Molecule Templates are either collected from the source
        via SPARQL queries or from the ontology graph if one is available.

        Parameters
        ---------
        datasource : DataSource
            The datasource from which the RDF Molecule Templates should be extracted.

        """
        rdf_molecules = {}
        endpoint = datasource.url

        if datasource.ontology_graph is None:
            results = self.get_typed_concepts(datasource)
        else:
            results = self.get_mts_from_owl(datasource, datasource.ontology_graph)

        rdf_molecules[endpoint] = results

        return rdf_molecules

    def get_typed_concepts(self, endpoint: DataSource) -> List[dict]:
        """Entrypoint for extracting RDF Molecule Templates from a datasource.

        The RDF Molecule Templates present in a datasource are extracted using this method.
        Basically, it is a list of RDF class concepts and their predicates.

        Parameters
        ----------
        endpoint : DataSource
            The datasource from which the RDF Molecule Templates are to be extracted.

        Returns
        -------
        List[dict]
            A list of dictionaries representing the RDF class concepts and their metadata
            such as predicates and cardinality.

        """
        types = endpoint.types_to_list()
        if types is None or len(types) == 0:
            query = 'SELECT DISTINCT ?t ?label WHERE {\n' \
                    '  ?s a ?t .\n' \
                    '  OPTIONAL {\n' \
                    '    ?t  <' + RDFS + 'label> ?label .\n' \
                    '    FILTER langMatches( lang(?label), "EN" ) . \n' \
                    '  }\n}'
            res_list, _ = iterative_query(query, endpoint, limit=100)
            to_remove = [r for m in metas for r in res_list if m in str(r['t'])]
            for r in to_remove:
                res_list.remove(r)
        else:
            res_list = [{'t': t} for t in types]

        results = []
        already_processed = []
        for r in res_list:
            t = r['t']
            if '^^' in t:
                continue
            if t in already_processed:
                continue
            already_processed.append(t)
            card = self.get_cardinality(endpoint, t)

            source_uri = MT_RESOURCE + str(hashlib.md5(str(endpoint.url + t).encode()).hexdigest())
            source = Source(source_uri, endpoint, card)
            # Get subclasses
            subc = self.get_subclasses(endpoint, t)
            subclasses = [r['subc'] for r in subc] if subc is not None else []

            rdf_properties = []
            # Get predicates of the molecule t
            predicates = self.get_predicates(endpoint, t)
            properties_processed = []
            for p in predicates:
                rn = {'t': t, 'cardinality': str(card), 'subclasses': subclasses}
                pred = p['p']
                if pred in properties_processed:
                    continue
                properties_processed.append(pred)

                mt_predicate_uri = MT_RESOURCE + str(hashlib.md5(str(t + pred).encode()).hexdigest())
                property_source_uri = MT_RESOURCE + str(hashlib.md5(str(endpoint.url + t + pred).encode()).hexdigest())
                # Get cardinality of this predicate from this RDF-MT
                pred_card = self.get_cardinality(endpoint, t, prop=pred)
                rn['p'] = pred
                rn['predcard'] = pred_card

                # Get range of this predicate from this RDF-MT t
                rn['range'] = self.get_rdfs_ranges(endpoint, pred)
                if len(rn['range']) == 0:
                    rn['r'] = self.find_instance_range(endpoint, t, pred)
                    mt_ranges = list(set(rn['range'] + rn['r']))
                else:
                    mt_ranges = rn['range']
                ranges = []

                for mr in mt_ranges:
                    if '^^' in mr:
                        continue
                    mr_pid = MT_RESOURCE + str(hashlib.md5(str(endpoint.url + t + pred + mr).encode()).hexdigest())
                    if XSD not in mr:
                        range_card = self.get_cardinality(endpoint, t, prop=pred, mr=mr)
                        rtype = 0
                    else:
                        range_card = self.get_cardinality(endpoint, t, prop=pred, mr=mr, mr_datatype=True)
                        rtype = 1

                    ran = PropRange(mr_pid, mr, endpoint, range_type=rtype, cardinality=range_card)
                    ranges.append(ran)
                if 'label' in p:
                    property_label = p['label']
                else:
                    property_label = ''

                pred_source = Source(property_source_uri, endpoint, pred_card)
                mt_property = MTProperty(mt_predicate_uri, pred, [pred_source], ranges=ranges, label=property_label)
                rdf_properties.append(mt_property)

                results.append(rn)

            name = r['label'] if 'label' in r else t
            desc = r['desc'] if 'desc' in r else None

            mt = RDFMT(t, name, properties=rdf_properties, desc=desc, sources=[source], subclass_of=subclasses)
            data = mt.to_rdf()
            self.update_graph(data)

        return results

    @staticmethod
    def get_rdfs_ranges(endpoint: str | DataSource, predicate: str) -> list:
        """Extracts the range of a predicate defined using `rdfs:range`.

        Extracts the range of a predicate using the predicate `range` of the RDF Schema (RDFS).

        Parameters
        ----------
        endpoint : str | DataSource
            The URL of the endpoint in which to check the range of the predicate or, alternatively,
            the :class:`DataSource` instance representing the endpoint.
        predicate : str
            The predicate of interest.

        Returns
        -------
        list
            A list containing all classes and types that are defined as the range of the
            predicate of interest via `rdfs:range`.

        """
        query = 'SELECT DISTINCT ?range WHERE { <' + predicate + '> <' + RDFS + 'range> ?range . }'
        res_list, _ = iterative_query(query, endpoint, limit=100)
        return [r['range'] for r in res_list if True not in [m in str(r['range']) for m in metas]]

    @staticmethod
    def find_instance_range(endpoint: str | DataSource, type_: str, predicate: str) -> list:
        """Extracts the range of a predicate by checking the RDF class of the objects.

        Extracts the range of a predicate using a SPARQL query to check the RDF class of
        the objects occurring in RDF triples with this predicate. The RDF triples are
        limited by the association of the subject to the class *type_*.

        Parameters
        ----------
        endpoint : str | DataSource
            The URL of the endpoint in which to check the range of the predicate or, alternatively,
            the :class:`DataSource` instance representing the endpoint.
        type_ : str
            The RDF class the subject of the triples to consider belongs to.
        predicate : str
            The predicate of interest.

        Returns
        -------
        list
            A list containing all RDF classes that occur as the range of the predicate
            in triples where the subject belongs to the class *type_*.

        """
        query = 'SELECT DISTINCT ?range WHERE {\n' \
                '  ?s a <' + type_ + '> .\n' \
                '  ?s <' + predicate + '> ?pt .\n' \
                '  ?pt a ?range .\n}'
        res_list, _ = iterative_query(query, endpoint, limit=50)
        return [r['range'] for r in res_list if True not in [m in str(r['range']) for m in metas]]

    def get_predicates(self, endpoint: str | DataSource, type_: str) -> list:
        """Gets a list of predicates associated with the specified RDF class.

        Extracts all predicates that are associated with the RDF class *type_*.
        If the initial SPARQL query fails to retrieve the data, the predicates
        are extracted from some randomly selected instances of the class.

        Parameters
        ----------
        endpoint : str | DataSource
            The URL of the endpoint from which to extract the predicates or, alternatively,
            the :class:`DataSource` instance representing the endpoint.
        type_ : str
            The RDF class for which all predicates should be extracted.

        Returns
        -------
        list
            A list containing all predicates that are associated with the RDF class *type_*.

        """
        query = 'SELECT DISTINCT ?p ?label WHERE {\n' \
                '  ?s a <' + type_ + '> .\n' \
                '  ?s ?p ?pt .\n' \
                '  OPTIONAL { ?p  <' + RDFS + 'label> ?label }\n}'
        res_list, status = iterative_query(query, endpoint, limit=50)
        existing_predicates = [r['p'] for r in res_list]

        if status == -1:  # fallback - get predicates from randomly selected instances of the type
            logger.warning('giving up on ' + query)
            logger.warning('trying instances ...')
            rand_inst_res = self.get_preds_of_random_instances(endpoint, type_)
            for r in rand_inst_res:
                if r not in existing_predicates:
                    res_list.append({'p': r})
        return res_list

    def get_preds_of_random_instances(self, endpoint: str | DataSource, type_: str) -> list:
        """Gets the predicates associated with randomly selected instances of a specified RDF class.

        This method is used when extracting the predicates of a class failed. In order to reduce
        the load on the endpoint, the predicates of randomly selected instances of the RDF class
        are extracted to approximate the predicates associated with that class.

        Parameters
        ----------
        endpoint : str | DataSource
            The URL of the endpoint from which to extract the predicates or, alternatively,
            the :class:`DataSource` instance representing the endpoint.
        type_ : str
            The RDF class for which the predicates should be extracted.

        Returns
        -------
        list
            A list containing all predicates that are associated with the randomly selected
            instances of the RDF class *type_*. Note that this might only be a subset of
            all the predicates that are associated to the RDF class.

        """
        query = 'SELECT DISTINCT ?s WHERE { ?s a <' + type_ + '> . }'
        res_instances, _ = iterative_query(query, endpoint, limit=50, max_tries=100)
        res_list = []
        card = len(res_instances)
        if card > 0:
            # TODO: actually retrieve the result from more than one instance
            import random
            rand = random.randint(0, card - 1)
            inst = res_instances[rand]
            inst_res = self.get_preds_of_instance(endpoint, inst['s'])
            inst_res = [r['p'] for r in inst_res]
            res_list.extend(inst_res)
            res_list = list(set(res_list))
        return res_list

    @staticmethod
    def get_preds_of_instance(endpoint: str | DataSource, instance: str) -> list:
        """Gets all predicates that are associated with a specific instance in the data.

        Extracts the predicates that occur in RDF triples where the subject is *instance*.

        Parameters
        ----------
        endpoint : str | DataSource
            The URL of the endpoint from which to extract the predicates.
        instance : str
            The instance in the data for which to extract the predicates.

        Returns
        -------
        list
            A list containing all the predicates that are associated with the instance *instance*.

        """
        query = 'SELECT DISTINCT ?p ?label WHERE {\n' \
                '  <' + instance + '> ?p ?pt .\n' \
                '  OPTIONAL { ?p  <' + RDFS + 'label> ?label }\n}'
        res_list, _ = iterative_query(query, endpoint, limit=1000)
        return res_list

    def get_mts_from_owl(self, endpoint: DataSource, graph: str) -> List[dict]:
        """Extracts the RDF Molecule Templates of a datasource from the associated ontology.

        The RDF Molecule Templates of the datasource are extracted from the ontology.

        endpoint : DataSource
            The datasource from which the RDF Molecule Templates are to be extracted.
        graph : str
            The graph within the SPARQL endpoint that stores the ontology.

        Returns
        -------
        List[dict]
            A list of dictionaries representing the RDF class concepts and their metadata
            such as predicates and cardinality.

        """
        types = endpoint.types_to_list()
        if types is None or len(types) == 0:
            query = 'SELECT DISTINCT ?t ?p ?range ?plabel ?tlabel WHERE { GRAPH <' + graph + '> {\n' \
                    '  ?p <' + RDFS + 'domain> ?t .\n' \
                    '  OPTIONAL { ?p <' + RDFS + 'range> ?range }\n' \
                    '  OPTIONAL { ?p <' + RDFS + "label> ?plabel . FILTER langMatches(?plabel, 'EN') }\n" \
                    '  OPTIONAL { ?t <' + RDFS + "label> ?tlabel . FILTER langMatches(?tlabel, 'EN') }\n" \
                    '}}'
            res_list, _ = iterative_query(query, endpoint, limit=50)

            to_remove = [r for m in metas for r in res_list if m in str(r['t'])]
            for r in to_remove:
                res_list.remove(r)
        else:
            res_list = [{'t': t} for t in types]

        results = []
        already_processed = {}
        mts = {}
        for r in res_list:
            t = r['t']
            if '^^' in t:
                continue

            subclasses = []
            if t not in already_processed:
                mt_card = -1
                source_uri = MT_RESOURCE + str(hashlib.md5(str(endpoint.url + t).encode()).hexdigest())
                source = Source(source_uri, endpoint, mt_card)
                already_processed[t] = mt_card
                subc = self.get_subclasses(endpoint, t)
                subclasses = [r['subc'] for r in subc]
                name = r['tlabel'] if 'tlabel' in r else t
                desc = r['tdesc'] if 'tdesc' in r else None
                mts[t] = {'name': name, 'properties': [], 'desc': desc, 'sources': [source], 'subClassOf': subclasses}
            else:
                mt_card = already_processed[t]

            pred = r['p']
            rn = {'t': t, 'cardinality': mt_card, 'subclasses': subclasses}
            mt_predicate_uri = MT_RESOURCE + str(hashlib.md5(str(t + pred).encode()).hexdigest())
            property_source_uri = MT_RESOURCE + str(hashlib.md5(str(endpoint.url + t + pred).encode()).hexdigest())
            # Get cardinality of this predicate from this RDF-MT
            pred_card = -1

            rn['p'] = pred
            rn['predcard'] = pred_card

            # Get range of this predicate from this RDF-MT t
            rn['range'] = []

            ranges = []
            if 'range' in r and XSD not in r['range']:
                rn['range'].append(r['range'])
                mr = r['range']
                mr_pid = MT_RESOURCE + str(hashlib.md5(str(endpoint.url + t + pred + mr).encode()).hexdigest())

                if XSD not in mr:
                    range_card = -1
                    rtype = 0
                else:
                    range_card = -1
                    rtype = 1

                ran = PropRange(mr_pid, mr, endpoint, range_type=rtype, cardinality=range_card)
                ranges.append(ran)
            if 'plabel' in r:
                property_label = r['plabel']
            else:
                property_label = ''

            pred_source = Source(property_source_uri, endpoint, pred_card)
            mt_prop = MTProperty(mt_predicate_uri, pred, [pred_source], ranges=ranges, label=property_label)
            mts[t]['properties'].append(mt_prop)

            results.append(rn)

        for t in mts:
            self.update_graph(RDFMT(
                rid=t,
                name=mts[t]['name'],
                sources=mts[t]['sources'],
                subclass_of=mts[t]['subClassOf'],
                properties=mts[t]['properties'],
                desc=mts[t]['desc']
            ).to_rdf())
        return results

    def update_graph(self, data: list) -> None:
        """Adds new data of the RDF Molecule Templates into the RDF knowledge graph.

        This method uses INSERT queries to add new data to the RDF knowledge graph
        containing the RDF Molecule Templates. Based on the length of the data,
        several requests might be sent since Virtuoso only supports 49 triples at a time.

        Parameters
        ----------
        data : list
            A list of RDF triples to insert into the knowledge graph.

        """
        i = 0
        # Virtuoso supports only 49 triples at a time.
        for i in range(0, len(data), 49):
            if i + 49 > len(data):
                update_query = 'INSERT DATA { GRAPH <' + self.graph + '> { ' + ' . \n'.join(data[i:]) + '} }'
            else:
                update_query = 'INSERT DATA { GRAPH <' + self.graph + '> { ' + ' . \n'.join(data[i:i + 49]) + '} }'
            self.mdb.update(update_query)
        if i < len(data) + 49:
            update_query = 'INSERT DATA { GRAPH <' + self.graph + '> { ' + ' . \n'.join(data[i:]) + '} }'
            self.mdb.update(update_query)

    def delete_insert_data(self, delete: list, insert: list, where: list = None) -> None:
        """Updates the RDF Molecule Templates in the RDF knowledge graph.

        This method uses DELETE and INSERT statements to modify the RDF Molecule
        Templates that are already stored in the RDF knowledge graph. Based on
        the length of the data, several requests might be sent since Virtuoso
        only supports 49 triples at a time.

        Parameters
        ----------
        delete : list
            A list of RDF triples that should be deleted from the knowledge graph.
        insert : list
            A list of RDF triples that should be inserted into the knowledge graph.
        where : list
            A list of SPARQL triple patterns stating the condition on when to delete
            and insert the triples mentioned in the other parameters.

        """
        if where is None:
            where = []
        i = 0
        update_query = 'WITH <' + self.graph + '> DELETE {'
        # Virtuoso supports only 49 triples at a time.
        for i in range(0, len(delete), 49):
            if i + 49 > len(delete):
                update_query += ' . \n'.join(delete[i:]) + '} ' \
                                'INSERT {' + ' . \n'.join(insert[i:]) + '} ' \
                                'WHERE {' + ' . \n'.join(where[i:]) + '}'
            else:
                update_query += ' . \n'.join(delete[i:i + 49]) + '} ' \
                                'INSERT {' + ' . \n'.join(insert[i:i + 49]) + '} ' \
                                'WHERE {' + ' . \n'.join(where[i:i + 49]) + '}'
            self.mdb.update(update_query)
        update_query = 'WITH <' + self.graph + '> DELETE {'
        if i < len(delete) + 49:
            update_query += ' . \n'.join(delete[i:]) + '} ' \
                           'INSERT {' + ' . \n'.join(insert[i:]) + '} ' \
                           'WHERE {' + ' . \n'.join(where[i:]) + '}'
            self.mdb.update(update_query)

    @staticmethod
    def get_cardinality(endpoint: str | DataSource,
                        mt: str = None,
                        prop: str = None,
                        mr: str = None,
                        mr_datatype: bool = False) -> int:
        """Gets the number of triples in a datasource.

        Based on the passed arguments, it is possible to retrieve the cardinality of
        different elements, e.g., the datasource itself, the RDF Molecule Template,
        or even a predicate.

        Parameters
        ----------
        endpoint : str | DataSource
            The URL of the endpoint from which the cardinality is to be extracted or, alternatively,
            the :class:`DataSource` instance representing the endpoint.
        mt : str, optional
            The RDF class of interest for the cardinality calculation.
        prop : str, optional
            The predicate for which the cardinality should be returned.
        mr : str, optional
            The class or datatype of the object that appears together with the predicate.
        mr_datatype : bool, optional
            A Boolean indicating whether *mr* is a datatype. False by default, i.e., *mr* is an RDF class.

        Returns
        -------
        int
            The requested cardinality. If only the endpoint is specified, the cardinality
            is the number of triples in the datasource. If *endpoint* and *mt* are passed,
            the number of instances of RDF class *mt* is returned. If additionally a predicate
            is present, the number of RDF triples in which the predicate appears in conjunction
            with a subject of class *mt* is returned. In case *mr* is also passed in, the
            cardinality represents the number of triple in *endpoint* for which the predicate
            is *prop*, the subject belongs to the RDF class *mt*, and the object belongs to the
            RDF class *mr*. *mr* can also be a datatype is *mr_datatype* is set to true.
            If no cardinality with the specified parameters could be calculated, -1 is returned.
        """
        if mt is None:
            query = 'SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }'
        elif prop is None:
            query = 'SELECT (COUNT(?s) AS ?count) WHERE { ?s a <' + mt.replace(' ', '_') + '> }'
        else:
            mt = mt.replace(' ', '_')
            if mr is None:
                query = 'SELECT (COUNT(?s) AS ?count) WHERE {\n  ?s a <' + mt + '> .\n  ?s <' + prop + '> ?o\n}'
            else:
                mr = mr.replace(' ', '_')
                if not mr_datatype:
                    query = 'SELECT (COUNT(?s) AS ?count) WHERE {\n' \
                            '  ?s a <' + mt + '> .\n' \
                            '  ?s <' + prop + '> ?o .\n' \
                            '  ?o a <' + mr + '>\n' \
                            '}'
                else:
                    query = 'SELECT (COUNT(?s) AS ?count) WHERE {\n' \
                            '  ?s a <' + mt + '> .\n' \
                            '  ?s <' + prop + '> ?o .\n' \
                            '  FILTER( datatype(?o) = <' + mr + '> )\n' \
                            '}'

        res, _ = contact_rdf_source(query, endpoint)
        if res is not None and len(res) > 0 and len(res[0]['count']) > 0:
            card = res[0]['count']
            if isinstance(card, str) and '^^' in card:
                card = int(card[:card.find('^^')])
            else:
                card = int(card)
            return card
        else:
            return -1

    @staticmethod
    def get_subclasses(endpoint: str | DataSource, root: str) -> list:
        """Gets the subclasses of an RDF class.

        Extracts a list of all subclasses of the specified RDF class.

        Parameters
        ----------
        endpoint : str | DataSource
            The URL of the endpoint from which the subclasses should be extracted or, alternatively,
            the :class:`DataSource` instance representing the endpoint.
        root : str
            The RDF class for which the subclasses should be extracted.

        Returns
        -------
        list
            A list with all subclasses extracted for the RDF class *root*.

        """
        query = 'SELECT DISTINCT ?subc WHERE { <' + root.replace(' ', '_') + '> <' + RDFS + 'subClassOf> ?subc }'
        res, _ = contact_rdf_source(query, endpoint)
        return res

    def get_sources(self) -> list:
        """Gets all sources of the federation the :class:`RDFMTMgr` instance was initialized for.

        Returns
        -------
        list
            A list containing the IDs and URLs of all datasources associated with the federation.

        """
        query = 'SELECT DISTINCT ?subject ?url WHERE { GRAPH <' + self.graph + '> {\n' \
                '  ?subject a <' + MT_ONTO + 'DataSource> .\n' \
                '  ?subject <' + MT_ONTO + 'url> ?url .\n' \
                '}}'
        res_list, _ = self.mdb.iterative_query(query, limit=1000)
        return res_list

    def get_source(self, ds_id: str) -> Optional[DataSource]:
        """Gets a single source of the federation as :class:`DataSource` instance.

        This method extracts all information available about the specified datasource
        for the federation the :class:`RDFMTMgr` instance was initialized for.
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
        query = 'SELECT DISTINCT * WHERE { GRAPH <' + self.graph + '> {\n' \
                '  <' + ds_id + '> <' + MT_ONTO + 'url> ?url .\n' \
                '  <' + ds_id + '> <' + MT_ONTO + 'dataSourceType> ?dstype .\n' \
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'name> ?name }\n' \
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'version> ?version }\n' \
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'keywords> ?keywords }\n' \
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'organization> ?organization }\n' \
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'homepage> ?homepage }\n' \
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'params> ?params }\n' \
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'types> ?types }\n' \
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'desc> ?desc }\n' \
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'triples> ?triples }\n' \
                '}}'
        res_list, _ = self.mdb.iterative_query(query, limit=1, max_answers=1)
        if len(res_list) > 0:
            res = res_list[0]
            return DataSource(
                ds_id,
                res['url'],
                res['dstype'],
                name=res['name'] if 'name' in res else '',
                desc=res['desc'] if 'desc' in res else '',
                params=res['params'] if 'params' in res else '',
                keywords=res['keywords'] if 'keywords' in res else '',
                version=res['version'] if 'version' in res else '',
                homepage=res['homepage'] if 'homepage' in res else '',
                organization=res['organization'] if 'organization' in res else '',
                types=res['types'] if 'types' in res else ''
            )
        return None

    def get_ds_rdfmts(self, datasource: str) -> list:
        """Get the RDF Molecule Templates for a datasource.

        Extracts the RDF Molecule Templates for the specified datasource
        from the metadata knowledge graph.

        Parameters
        ----------
        datasource : str
            The identifier of the datasource of interest.

        Returns
        -------
        list
            A list containing the RDF Molecule Templates for the specified
            datasource as returned from the metadata knowledge graph, i.e.,
            in the form of a SPARQL query result.

        """
        query = 'SELECT DISTINCT ?subject ?card WHERE { GRAPH <' + self.graph + '> {\n' \
                '  ?subject a <' + MT_ONTO + 'RDFMT> .\n' \
                '  ?subject <' + MT_ONTO + 'source> ?source .\n' \
                '  OPTIONAL { ?source <' + MT_ONTO + 'cardinality> ?card }\n' \
                '  ?source <' + MT_ONTO + 'datasource> <' + datasource + '> .\n' \
                '}}'
        res_list, _ = self.mdb.iterative_query(query, limit=1000)
        return res_list

    def create_inter_ds_links(self, datasource: DataSource | str = None, output_queue: Queue = Queue()) -> None:
        """Entrypoint for checking the interlinks between datasources in the federation.

        This method serves as the entrypoint to find all interlinks between datasources
        in the federation the :class:`RDFMTMgr` instance was initialized for.

        Parameters
        ----------
        datasource : DataSource | str, optional
            The datasource for which all interlinks will be searched. If none was passed,
            the interlinks between all datasources of the federation will be investigated.
        output_queue : multiprocessing.Queue, optional
            A queue object used to transmit the results. If none is given, a new one will be created.
            However, in that case it is not possible to retrieve the results from the queue.

        """
        sources = self.get_sources()
        rdfmts = {}
        if len(sources) == 0:
            output_queue.put('EOF')
            return

        sourcemaps = {s['subject']: s for s in sources}
        logger.debug(sourcemaps)
        for s in sources:
            mts = self.get_ds_rdfmts(s['subject'])
            rdfmts[s['subject']] = {m['subject']: int(m['card']) if 'card' in m else -1 for m in mts}

        logger.debug(rdfmts.keys())
        if datasource is not None:
            self.update_links(rdfmts, sourcemaps, datasource)
        else:
            self.find_all_links(rdfmts, sourcemaps)

        output_queue.put('EOF')

    def find_all_links(self, rdfmts: dict, sourcemaps: dict) -> None:
        """Searches for interlinks between all RDF Molecule Templates of a federation.

        This method searches for interlinks between all RDF Molecule Templates of a
        federation, i.e., one datasource is serving the instances belonging to the
        RDF class associated with the RDF Molecule Template while another datasource
        serves instances of another RDF Molecule Template that appears as object
        in RDF triples where the subject belongs to the first RDF Molecule Template.

        Parameters
        ----------
        rdfmts : dict
            A dictionary with the RDF Molecule Templates of the federation.
        sourcemaps : dict
            A dictionary mapping the sources to the RDF Molecule Templates.

        """
        queues = {}
        processes = {}

        for si in rdfmts:
            s = sourcemaps[si]
            logger.info('Searching inter links from ', s['subject'])
            for ti in rdfmts:
                t = sourcemaps[ti]
                if si == ti:
                    continue
                queue = Queue()
                queues[ti] = queue
                p = Process(target=self.get_inter_ds_links_bn, args=(s, rdfmts[si], t, rdfmts[ti], queue,))
                p.start()
                processes[ti] = p
                if len(queues) > 2:
                    to_remove = []
                    while len(queues) > 0:
                        for endpoint in queues:
                            try:
                                queue = queues[endpoint]
                                r = queue.get(False)
                                if r == 'EOF':
                                    to_remove.append(endpoint)
                            except Empty:
                                pass
                        for r in to_remove:
                            if r in queues:
                                del queues[r]
                            if r in processes and processes[r].is_alive():
                                logger.warning('terminating: ', r)
                                processes[r].terminate()
                                del processes[r]

    def update_links(self, rdfmts, sourcemaps, datasource: DataSource | str) -> None:
        """Updates the interlinks of a specified datasource.

        Searches for the interlinks of the datasource *datasource* within the federation
        for which the :class:`RDFMTMgr` instance was initialized for. The previously
        recorded interlinks are then updated based on the current findings.

        Parameters
        ----------
        rdfmts : dict
            A dictionary with the RDF Molecule Templates of the federation.
        sourcemaps : dict
            A dictionary mapping the sources to the RDF Molecule Templates.
        datasource : DataSource | str
            The datasource for which the interlinks should be updated.

        """
        queues = {}
        processes = {}
        if isinstance(datasource, DataSource):
            did = datasource.rid
            ds = sourcemaps[datasource.rid]
        else:
            datasource = self.get_source(datasource)
            if datasource is None:
                return
            ds = sourcemaps[datasource.rid]
            did = datasource.rid

        today = str(datetime.datetime.now())
        data = ['<' + datasource.rid + '> <http://purl.org/dc/terms/modified> "' + today + '"']
        delete = ['<' + datasource.rid + '> <http://purl.org/dc/terms/modified> ?modified ']
        self.delete_insert_data(delete, data, delete)
        for si in sourcemaps:
            s = sourcemaps[si]
            if si == did:
                continue

            def inter_ds_links(s1, rdfmts_s1, s2, rdfmts_s2, queue_):
                logger.info('Linking between ', s1['subject'], len(rdfmts_s1), ' and (to) ', s2['subject'], len(rdfmts_s2))
                p = Process(target=self.get_inter_ds_links_bn, args=(s1, rdfmts_s1, s2, rdfmts_s2, queue_,))
                p.start()
                return p

            queue1 = Queue()
            queues[did] = queue1
            processes[did] = inter_ds_links(ds, rdfmts[did], s, rdfmts[si], queue1)
            queue2 = Queue()
            queues[si] = queue2
            processes[si] = inter_ds_links(s, rdfmts[si], ds, rdfmts[did], queue2)
            if len(queues) >= 2:
                while len(queues) > 0:
                    to_remove = []
                    for endpoint in queues:
                        try:
                            queue = queues[endpoint]
                            r = queue.get(False)
                            if r == 'EOF':
                                to_remove.append(endpoint)
                        except Empty:
                            pass
                    for r in to_remove:
                        if r in queues:
                            del queues[r]
                        if r in processes and processes[r].is_alive():
                            logger.warning('terminating: ', r)
                            processes[r].terminate()
                        if r in processes:
                            del processes[r]
        logger.info('linking DONE!', did)

    def get_inter_ds_links_bn(self,
                              endpoint1: dict,
                              rdfmts_endpoint1: dict,
                              endpoint2: dict,
                              rdfmts_endpoint2: dict,
                              queue: Queue = Queue()) -> None:
        """Checks whether there is an interlink between two datasources.

        This method checks, based on the RDF Molecule Templates and SPARQL queries,
        whether there is an interlink between two datasources of the federation.

        Parameters
        ----------
        endpoint1 : dict
            A dictionary representing the first datasource.
        rdfmts_endpoint1 : dict
            A dictionary of the RDF Molecule Templates associated with the first datasource.
        endpoint2 : dict
            A dictionary representing the second datasource.
        rdfmts_endpoint2 : dict
            A dictionary of the RDF Molecule Templates associated with the second datasource.
        queue : multiprocessing.Queue, optional
            A queue object used to transmit the results. If none is given, a new one will be created.
            However, in that case it is not possible to retrieve the results from the queue.

        """
        url_endpoint1 = endpoint1['url']
        url_endpoint2 = endpoint2['url']
        for m1 in rdfmts_endpoint1:
            logger.info(m1)
            logger.info('--------------------------------------------------')
            pred_query = 'SELECT DISTINCT ?p WHERE {\n  ?s a <' + m1 + '> .\n  ?s ?p ?t .\n  FILTER (isURI(?t))\n}'
            res_pred_query, _ = iterative_query(pred_query, url_endpoint1, limit=1000)
            predicates = [r['p'] for r in res_pred_query]
            res_list = {}
            for p in predicates:
                query = 'SELECT DISTINCT ?t WHERE {\n' \
                        '  ?s a <' + m1 + '> .\n' \
                        '  ?s <' + p + '> ?t .\n' \
                        '  FILTER (isURI(?t))\n}'
                res, _ = iterative_query(query, url_endpoint1, limit=500, max_answers=500)
                res_list.setdefault(p, []).extend([r['t'] for r in res])

            types_found = self.get_links_bn_ds(res_list, rdfmts_endpoint2, url_endpoint2)
            for link in types_found:
                data = []
                if len(types_found[link]) > 0:
                    logger.info(len(types_found[link]), 'links found')
                    try:
                        for m2 in types_found[link]:
                            val = str(url_endpoint2 + m1 + link + m2).encode()
                            mr_pid = MT_RESOURCE + str(hashlib.md5(val).hexdigest())

                            card = -1
                            rs = DataSource(endpoint2['subject'], endpoint2['url'], DataSourceType.SPARQL_ENDPOINT)
                            ran = PropRange(mr_pid, m2, rs, range_type=0, cardinality=card)
                            data.extend(ran.to_rdf())
                            mt_pid = MT_RESOURCE + str(hashlib.md5(str(m1 + link).encode()).hexdigest())
                            data.append('<' + mt_pid + '> <' + MT_ONTO + 'linkedTo> <' + mr_pid + '> ')
                        if len(data) > 0:
                            self.update_graph(data)
                    except Exception as e:
                        logger.error('Exception while collecting data' + str(e))
                        logger.error(m1 + ' --- Vs --- ' + 'in [' + url_endpoint2 + ']')
                        logger.error(types_found)
                        logger.error(data)

        logger.info('get_inter_ds_links_bn Done!')
        queue.put('EOF')

    def get_links_bn_ds(self, predicate_instance_list: dict, rdfmts: dict, endpoint: str) -> dict:
        """Checks based on instances whether there is a link between two datasources.

        This method receives object instances from a datasource and checks whether
        they appear as subjects in the datasource passed to this method. If so,
        there is a link between the datasources.

        Parameters
        ----------
        predicate_instance_list : dict
            A dictionary mapping predicates from another datasource to their object instances.
        rdfmts : dict
            A dictionary representing the RDF Molecule Templates of the federation.
        endpoint : str
            The URL of the endpoint in which to check for the instances in subject position.

        Returns
        -------
        dict
            A dictionary mapping predicates of another datasource to the RDF classes in this source.

        """
        results = {}
        for pred in predicate_instance_list:
            rdfmts_found = self.get_mts_matches(predicate_instance_list[pred], endpoint)
            results[pred] = [r for r in rdfmts_found if r in rdfmts]
        return results

    @staticmethod
    def get_mts_matches(instances: list, endpoint: str) -> list:
        """Checks whether there are RDF classes to which the instances match.

        Given a list of instances, this method checks whether they appear in
        the specified endpoint and if so to which RDF classes they belong.

        Parameters
        ----------
        instances : list
            A list of instances to check for their existence.
        endpoint : str
            The URL of the endpoint in which to check for the existence of the instances.

        Returns
        -------
        list
            A list of RDF classes to which the instances belong.

        """
        # Checks if there are subjects with prefixes matching
        batches = [instances[i:i+50] for i in range(0, len(instances), 50)]
        for batch in batches:
            subjects = ['?s=<' + r + '>' for r in batch]
            tquery = 'SELECT DISTINCT ?t WHERE {\n  ?s a ?t .\n  FILTER (' + ' || '.join(subjects) + ')\n}'
            res_list, _ = iterative_query(tquery, endpoint, limit=1000)
            res = [r['t'] for r in res_list]
            if len(res) > 0:
                return res
        return []

    def get_rdfmts_from_mapping(self, datasource: DataSource) -> List[dict]:
        """Extracts the RDF Molecule Templates of a datasource from its mappings.

        If the RDF Mapping Language (RML) mappings used to create the datasource
        are known, they can be used to extract the RDF Molecule Templates.

        Parameters
        ----------
        datasource : DataSource
            The datasource for which the RDF Molecule Templates should be extracted from the mappings.

        Returns
        -------
        List[dict]
            A list of dictionaries representing the RDF class concepts and their metadata
            such as predicates and cardinality.

        """
        types = datasource.types_to_list()
        if types is None:  # TODO: shortcut if types is set
            types = []
        mt_query = 'PREFIX rr: <http://www.w3.org/ns/r2rml#> ' \
                   'PREFIX rml: <http://semweb.mmlab.be/ns/rml#>' \
                   'SELECT DISTINCT ?t ?p ?r ?rds WHERE { GRAPH <' + self.graph + '> {\n' \
                   '  ?tm rml:logicalSource ?ls .\n' \
                   '  ?ls rml:source <' + datasource.rid + '> .\n' \
                   '  ?tm rr:subjectMap  ?sm. ?sm rr:class ?t .\n' \
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
        res, card = self.mdb.query(mt_query)
        results = []
        data = []
        if card > 0:
            res_list = {}
            for r in res:
                t = r['t']
                p = r['p']
                if t in res_list:
                    if p in res_list[t]:
                        if 'r' in r:
                            if r['r'] in res_list[t][p]:
                                if r['rds'] in res_list[t][p][r['r']]:
                                    continue
                                else:
                                    res_list[t][p][r['r']].append(r['rds'])
                            else:
                                res_list[t][p][r['r']] = [r['rds']]
                        else:
                            continue
                    else:
                        res_list[t][p] = {}
                        if 'r' in r:
                            res_list[t][p][r['r']] = [r['rds']]
                        else:
                            continue
                else:
                    res_list[t] = {}
                    res_list[t][p] = {}
                    if 'r' in r:
                        res_list[t][p][r['r']] = [r['rds']]
                    else:
                        continue

            for t in res_list:
                source_uri = MT_RESOURCE + str(hashlib.md5(str(datasource.url + t).encode()).hexdigest())
                source = Source(source_uri, datasource)

                rdf_properties = []
                predicates = res_list[t]

                for p in predicates:
                    rn = {
                        't': t,
                        'cardinality': -1,
                        'subclasses': [],
                        'p': p,
                        'predcard': -1,
                        'range': predicates[p].keys()
                    }
                    mt_predicate_uri = MT_RESOURCE + str(hashlib.md5(str(t + p).encode()).hexdigest())
                    property_source_uri = MT_RESOURCE + str(hashlib.md5(str(datasource.url + t + p).encode()).hexdigest())
                    ranges = []
                    if len(predicates[p]) > 0:
                        for mr in predicates[p]:
                            mr_pid = MT_RESOURCE + str(hashlib.md5(str(datasource.url + t + p + mr).encode()).hexdigest())
                            rtype = 0
                            for mrr in predicates[p][mr]:
                                rds = DataSource(mrr, datasource.url, DataSourceType.MONGODB)  # Only mrr is important here for the range
                                ran = PropRange(mr_pid, mr, rds, range_type=rtype, cardinality=-1)
                                ranges.append(ran)

                    pred_source = Source(property_source_uri, datasource, -1)
                    mt_property = MTProperty(mt_predicate_uri, p, [pred_source], ranges=ranges, label=p)
                    rdf_properties.append(mt_property)

                    results.append(rn)
                # add rdf:type property
                p = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
                mt_predicate_uri = MT_RESOURCE + str(hashlib.md5(str(t + p).encode()).hexdigest())
                property_source_uri = MT_RESOURCE + str(hashlib.md5(str(datasource.url + t + p).encode()).hexdigest())

                pred_source = Source(property_source_uri, datasource, -1)
                mt_prop = MTProperty(mt_predicate_uri, p, [pred_source], ranges=[], label='RDF type')
                rdf_properties.append(mt_prop)

                name = t
                desc = None
                mt = RDFMT(t, name, properties=rdf_properties, desc=desc, sources=[source], subclass_of=[])
                mtd = mt.to_rdf()
                data.extend(mtd)
                data = list(set(data))

        if len(data) > 0:
            self.update_graph(data)
            # self.create_inter_ds_links(datasource=ds)
        return results
