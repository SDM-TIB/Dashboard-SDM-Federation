import datetime
import hashlib
from multiprocessing import Queue, Process
from pprint import pprint
from queue import Empty
from typing import List, Optional

from FedSDM import get_logger
from FedSDM.rdfmt.model import RDFMT, MTProperty, PropRange, DataSource, DataSourceType, Source
from FedSDM.rdfmt.prefixes import RDFS, XSD, metas, MT_RESOURCE, MT_ONTO
from FedSDM.rdfmt.utils import contact_rdf_source, update_rdf_source, iterative_query

logger = get_logger('rdfmts', './rdfmts.log', True)
"""Logger for this module. It logs to the file 'rdfmts.log' as well as to stdout."""


class RDFMTMgr(object):
    """Provides an abstract way to manage the RDF Molecule Templates of a federation.

    The *RDFMTMgr* allows to easily create and modify the RDF Molecule Templates of a single federation.
    The class contains several utility functions necessary to collect the metadata or update it.
    The managed metadata is stored in an RDF knowledge graph.

    """

    def __init__(self, query_url: str, update_url: str, user: str, passwd: str, graph: str):
        """Creates a new *RDFMTMgr* instance.

        The *RDFMTMgr* object can be used to create and/or update the RDF Molecule Templates
        of the federation the instance is initialized for.

        Parameters
        ----------
        query_url : str
            The URL of the SPARQL endpoint to be used when sending queries.
        update_url : str
            The URL of the SPARQL endpoint to be used when updating the metadata.
        user : str
            The username required in order to get the update permissions.
        passwd : str
            The user's password required to authenticate for update permissions.
        graph : str
            The graph URI used in the SPARQL endpoint for storing the metadata of the federation.

        """
        self.graph = graph
        self.query_endpoint = query_url
        self.update_endpoint = update_url
        self.user = user
        self.passwd = passwd

    def create(self, ds: DataSource, out_queue: Queue = Queue(), types: list = None, is_update: bool = False) -> dict:
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
        types : list, optional
            A list containing the types that are present in the datasource. By default, this is set to None
            which leads to the actual extraction of the metadata from the datasource.
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
        if types is None:
            types = []

        endpoint = ds.url
        logger.info('----------------------' + endpoint + '-------------------------------------')

        if not is_update:
            # Get #triples of a dataset
            triples = self.get_cardinality(endpoint)
            ds.triples = triples
            data = '<' + ds.rid + '> <' + MT_ONTO + 'triples> ' + triples
            self.update_graph([data])
        else:
            today = str(datetime.datetime.now())
            data = ['<' + ds.rid + '> <http://purl.org/dc/terms/modified> "' + today + '"']
            delete = ['<' + ds.rid + '> <http://purl.org/dc/terms/modified> ?modified ']
            self.delete_insert_data(delete, data, delete)

        results = self.get_rdfmts(ds, types)
        # self.create_inter_ds_links(datasource=ds)
        out_queue.put('EOF')
        return results

    def get_rdfmts(self, datasource: DataSource, types: list = None) -> dict:
        """Extracts the RDF Molecule Templates from a datasource.

        Extracts the RDF Molecule Templates from a datasource.
        This method is the entrypoint for the metadata extraction.
        The RDF Molecule Templates are either collected from the source
        via SPARQL queries or from the ontology graph if one is available.

        Parameters
        ---------
        datasource : DataSource
            The datasource from which the RDF Molecule Templates should be extracted.
        types : list, optional
            A list containing the types that are present in the datasource. By default,
            this is set to None which leads to the actual extraction of the metadata
            from the datasource.

        """
        rdf_molecules = {}
        endpoint = datasource.url

        if datasource.ontology_graph is None:
            results = self.get_typed_concepts(datasource, types)
        else:
            results = self.get_mts_from_owl(datasource, datasource.ontology_graph, types)

        rdf_molecules[endpoint] = results

        pprint(results)
        logger.info('*********** ' + endpoint + ' ***********')
        logger.info('*********** finished ***********')
        return rdf_molecules

    def get_typed_concepts(self, endpoint: DataSource, types: list = None) -> List[dict]:
        """Entrypoint for extracting RDF Molecule Templates from a datasource.

        The RDF Molecule Templates present in a datasource are extracted using this method.
        Basically, it is a list of RDF class concepts and their predicates.

        Parameters
        ----------
        endpoint : DataSource
            The datasource from which the RDF Molecule Templates are to be extracted.
        types : list, optional
            A list containing the types that are present in the datasource. By default, this is
            set to None which leads to the actual extraction of the metadata from the datasource.

        Returns
        -------
        List[dict]
            A list of dictionaries representing the RDF class concepts and their metadata
            such as predicates and cardinality.

        """
        endpoint_url = endpoint.url
        if types is None or len(types) == 0:
            query = 'SELECT DISTINCT ?t ?label WHERE {\n' \
                    '  ?s a ?t .\n' \
                    '  OPTIONAL { ?t  <' + RDFS + 'label> ?label }\n}'
            res_list, _ = iterative_query(query, endpoint_url, limit=100)
            to_remove = [r for m in metas for r in res_list if m in str(r['t'])]
            for r in to_remove:
                res_list.remove(r)
        else:
            res_list = [{'t': t} for t in types]

        logger.info(endpoint_url)
        logger.info(res_list)
        pprint(res_list)

        results = []
        already_processed = []
        for r in res_list:
            t = r['t']
            if '^^' in t:
                continue
            if t in already_processed:
                continue
            print(t)
            print('---------------------------------------')
            already_processed.append(t)
            card = self.get_cardinality(endpoint_url, t)

            source_uri = MT_RESOURCE + str(hashlib.md5(str(endpoint_url + t).encode()).hexdigest())
            source = Source(source_uri, endpoint, card)
            # Get subclasses
            subc = self.get_subclasses(endpoint_url, t)
            subclasses = [r['subc'] for r in subc] if subc is not None else []

            rdf_properties = []
            # Get predicates of the molecule t
            predicates = self.get_predicates(endpoint_url, t)
            properties_processed = []
            for p in predicates:
                rn = {'t': t, 'cardinality': str(card), 'subclasses': subclasses}
                pred = p['p']
                if pred in properties_processed:
                    continue
                properties_processed.append(pred)

                mtpredicateURI = MT_RESOURCE + str(hashlib.md5(str(t + pred).encode()).hexdigest())
                propsourceURI = MT_RESOURCE + str(hashlib.md5(str(endpoint_url + t + pred).encode()).hexdigest())
                # Get cardinality of this predicate from this RDF-MT
                pred_card = str(self.get_cardinality(endpoint_url, t, prop=pred))
                print(pred, pred_card)
                rn['p'] = pred
                rn['predcard'] = pred_card

                # Get range of this predicate from this RDF-MT t
                rn['range'] = self.get_rdfs_ranges(endpoint_url, pred)
                if len(rn['range']) == 0:
                    rn['r'] = self.find_instance_range(endpoint_url, t, pred)
                    mt_ranges = list(set(rn['range'] + rn['r']))
                else:
                    mt_ranges = rn['range']
                ranges = []

                for mr in mt_ranges:
                    if '^^' in mr:
                        continue
                    mrpid = MT_RESOURCE + str(hashlib.md5(str(endpoint_url + t + pred + mr).encode()).hexdigest())
                    if XSD not in mr:
                        rcard = self.get_cardinality(endpoint_url, t, prop=pred, mr=mr)
                        rtype = 0
                    else:
                        rcard = self.get_cardinality(endpoint_url, t, prop=pred, mr=mr, mr_datatype=True)
                        rtype = 1

                    ran = PropRange(mrpid, mr, endpoint, range_type=rtype, cardinality=rcard)
                    ranges.append(ran)
                if 'label' in p:
                    plab = p['label']
                else:
                    plab = ''

                pred_source = Source(propsourceURI, endpoint, pred_card)
                mtprop = MTProperty(mtpredicateURI, pred, [pred_source], ranges=ranges, label=plab)
                rdf_properties.append(mtprop)

                results.append(rn)

            name = r['label'] if 'label' in r else t
            desc = r['desc'] if 'desc' in r else None

            mt = RDFMT(t, name, properties=rdf_properties, desc=desc, sources=[source], subclass_of=subclasses)
            data = mt.to_rdf()
            self.update_graph(data)

        return results

    @staticmethod
    def get_rdfs_ranges(endpoint_url: str, predicate: str) -> list:
        """Extracts the range of a predicate defined using `rdfs:range`.

        Extracts the range of a predicate using the predicate `range` of the RDF Schema (RDFS).

        Parameters
        ----------
        endpoint_url : str
            The URL of the endpoint in which to check the range of the predicate.
        predicate : str
            The predicate of interest.

        Returns
        -------
        list
            A list containing all classes and types that are defined as the range of the
            predicate of interest via `rdfs:range`.

        """
        query = 'SELECT DISTINCT ?range WHERE { <' + predicate + '> <' + RDFS + 'range> ?range . }'
        res_list, _ = iterative_query(query, endpoint_url, limit=100)
        return [r['range'] for r in res_list if True not in [m in str(r['range']) for m in metas]]

    @staticmethod
    def find_instance_range(endpoint_url: str, type_: str, predicate: str) -> list:
        """Extracts the range of a predicate by checking the RDF class of the objects.

        Extracts the range of a predicate using a SPARQL query to check the RDF class of
        the objects occurring in RDF triples with this predicate. The RDF triples are
        limited by the association of the subject to the class *type_*.

        Parameters
        ----------
        endpoint_url : str
            The URL of the endpoint in which to check the range of the predicate.
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
        res_list, _ = iterative_query(query, endpoint_url, limit=50)
        return [r['range'] for r in res_list if True not in [m in str(r['range']) for m in metas]]

    def get_predicates(self, endpoint_url: str, type_: str) -> list:
        """Gets a list of predicates associated with the specified RDF class.

        Extracts all predicates that are associated with the RDF class *type_*.
        If the initial SPARQL query fails to retrieve the data, the predicates
        are extracted from some randomly selected instances of the class.

        Parameters
        ----------
        endpoint_url : str
            The URL of the endpoint from which to extract the predicates.
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
        res_list, status = iterative_query(query, endpoint_url, limit=50)
        existing_predicates = [r['p'] for r in res_list]

        if status == -1:  # fallback - get predicates from randomly selected instances of the type
            print('giving up on ' + query)
            print('trying instances .....')
            rand_inst_res = self.get_preds_of_random_instances(endpoint_url, type_)
            for r in rand_inst_res:
                if r not in existing_predicates:
                    res_list.append({'p': r})
        return res_list

    def get_preds_of_random_instances(self, endpoint_url: str, type_: str) -> list:
        """Gets the predicates associated with randomly selected instances of a specified RDF class.

        This method is used when extracting the predicates of a class failed. In order to reduce
        the load on the endpoint, the predicates of randomly selected instances of the RDF class
        are extracted to approximate the predicates associated with that class.

        Parameters
        ----------
        endpoint_url : str
            The URL of the endpoint from which to extract the predicates.
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
        res_instances, _ = iterative_query(query, endpoint_url, limit=50, max_tries=100)
        res_list = []
        card = len(res_instances)
        if card > 0:
            # TODO: actually retrieve the result from more than one instance
            import random
            rand = random.randint(0, card - 1)
            inst = res_instances[rand]
            inst_res = self.get_preds_of_instance(endpoint_url, inst['s'])
            inst_res = [r['p'] for r in inst_res]
            res_list.extend(inst_res)
            res_list = list(set(res_list))
        return res_list

    @staticmethod
    def get_preds_of_instance(endpoint_url: str, instance: str) -> list:
        """Gets all predicates that are associated with a specific instance in the data.

        Extracts the predicates that occur in RDF triples where the subject is *instance*.

        Parameters
        ----------
        endpoint_url : str
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
        res_list, _ = iterative_query(query, endpoint_url, limit=1000)
        return res_list

    def get_mts_from_owl(self, endpoint: DataSource, graph: str, types: list = None) -> List[dict]:
        """Extracts the RDF Molecule Templates of a datasource from the associated ontology.

        The RDF Molecule Templates of the datasource are extracted from the ontology.

        endpoint : DataSource
            The datasource from which the RDF Molecule Templates are to be extracted.
        graph : str
            The graph within the SPARQL endpoint that stores the ontology.
        types : list, optional
            A list containing the types that are present in the datasource. By default, this is
            set to None which leads to the actual extraction of the metadata from the datasource.

        Returns
        -------
        List[dict]
            A list of dictionaries representing the RDF class concepts and their metadata
            such as predicates and cardinality.

        """
        endpoint_url = endpoint.url
        if types is None or len(types) == 0:
            query = 'SELECT DISTINCT ?t ?p ?range ?plabel ?tlabel WHERE { GRAPH <' + graph + '> {\n' \
                    '  ?p <' + RDFS + 'domain> ?t .\n' \
                    '  OPTIONAL { ?p <' + RDFS + 'range> ?range }\n' \
                    '  OPTIONAL { ?p <' + RDFS + "label> ?plabel . FILTER langMatches(?plabel, 'EN') }\n" \
                    '  OPTIONAL { ?t <' + RDFS + "label> ?tlabel . FILTER langMatches(?tlabel, 'EN') }\n" \
                    '}}'
            res_list, _ = iterative_query(query, endpoint_url, limit=50)

            to_remove = [r for m in metas for r in res_list if m in str(r['t'])]
            for r in to_remove:
                res_list.remove(r)
        else:
            res_list = [{'t': t} for t in types]

        logger.info(endpoint_url)
        logger.info(res_list)
        results = []
        already_processed = {}
        mts = {}
        for r in res_list:
            t = r['t']
            if '^^' in t:
                continue

            subclasses = []
            if t not in already_processed:
                # card = self.get_cardinality(endpoint_url, t)
                # if isinstance(card, str) and '^' in card:
                #     card = int(card[:card.find('^^')])
                #
                # # molecules[m]['wrappers'][0]['cardinality'] = card
                # if isinstance(card, str) and '^^' in card:
                #     mcard = card[:card.find('^^')]
                # else:
                #     mcard = str(card)
                mcard = -1
                print(t)
                source_uri = MT_RESOURCE + str(hashlib.md5(str(endpoint_url + t).encode()).hexdigest())
                source = Source(source_uri, endpoint, mcard)
                already_processed[t] = mcard
                subc = self.get_subclasses(endpoint_url, t)
                subclasses = [r['subc'] for r in subc]
                name = r['tlabel'] if 'tlabel' in r else t
                desc = r['tdesc'] if 'tdesc' in r else None
                mts[t] = {'name': name, 'properties': [], 'desc': desc, 'sources': [source], 'subClassOf': subclasses}
            else:
                mcard = already_processed[t]

            pred = r['p']
            print(pred)
            rn = {'t': t, 'cardinality': mcard, 'subclasses': subclasses}
            mtpredicateURI = MT_RESOURCE + str(hashlib.md5(str(t + pred).encode()).hexdigest())
            propsourceURI = MT_RESOURCE + str(hashlib.md5(str(endpoint_url + t + pred).encode()).hexdigest())
            # Get cardinality of this predicate from this RDF-MT
            predcard = -1  # self.get_cardinality(endpoint_url, t, prop=pred)
            if isinstance(predcard, str) and '^^' in predcard:
                predcard = predcard[:predcard.find('^^')]
            else:
                predcard = str(predcard)

            rn['p'] = pred
            rn['predcard'] = predcard

            # Get range of this predicate from this RDF-MT t
            rn['range'] = []  # self.get_rdfs_ranges(referer, pred)

            ranges = []
            if 'range' in r and XSD not in r['range']:
                print('range', r['range'])
                rn['range'].append(r['range'])
                mr = r['range']
                mrpid = MT_RESOURCE + str(hashlib.md5(str(endpoint_url + t + pred + mr).encode()).hexdigest())

                if XSD not in mr:
                    rcard = -1  # self.get_cardinality(endpoint_url, t, prop=pred, mr=mr)
                    rtype = 0
                else:
                    rcard = -1  # self.get_cardinality(endpoint_url, t, prop=pred, mr=mr, mrtype=mr)
                    rtype = 1

                ran = PropRange(mrpid, mr, endpoint, range_type=rtype, cardinality=rcard)
                ranges.append(ran)
            if 'plabel' in r:
                plab = r['plabel']
            else:
                plab = ''

            predsouce = Source(propsourceURI, endpoint, predcard)
            mtprop = MTProperty(mtpredicateURI, pred, [predsouce], ranges=ranges, label=plab)
            mts[t]['properties'].append(mtprop)
            # rdfpropteries.append(mtprop)

            results.append(rn)

        for t in mts:
            mt = RDFMT(t, mts[t]['name'], mts[t]['properties'], mts[t]['desc'], mts[t]['sources'], mts[t]['subClassOf'])
            data = mt.to_rdf()
            self.update_graph(data)
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
            logger.info(update_query)
            update_rdf_source(update_query, self.update_endpoint)
        if i < len(data) + 49:
            update_query = 'INSERT DATA { GRAPH <' + self.graph + '> { ' + ' . \n'.join(data[i:]) + '} }'
            logger.info(update_query)
            update_rdf_source(update_query, self.update_endpoint)

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
            logger.info(update_query)
            update_rdf_source(update_query, self.update_endpoint)
        update_query = 'WITH <' + self.graph + '> DELETE {'
        if i < len(delete) + 49:
            update_query += ' . \n'.join(delete[i:]) + '} ' \
                           'INSERT {' + ' . \n'.join(insert[i:]) + '} ' \
                           'WHERE {' + ' . \n'.join(where[i:]) + '}'
            logger.info(update_query)
            update_rdf_source(update_query, self.update_endpoint)

    @staticmethod
    def get_cardinality(endpoint: str,
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
        endpoint : str
            The URL of the endpoint from which the cardinality is to be extracted.
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
    def get_subclasses(endpoint_url: str, root: str) -> list:
        """Gets the subclasses of an RDF class.

        Extracts a list of all subclasses of the specified RDF class.

        Parameters
        ----------
        endpoint_url : str
            The URL of the endpoint from which the subclasses should be extracted.
        root : str
            The RDF class for which the subclasses should be extracted.

        Returns
        -------
        list
            A list with all subclasses extracted for the RDF class *root*.

        """
        query = 'SELECT DISTINCT ?subc WHERE { <' + root.replace(' ', '_') + '> <' + RDFS + 'subClassOf> ?subc }'
        res, _ = contact_rdf_source(query, endpoint_url)
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
        res_list, _ = iterative_query(query, self.query_endpoint, limit=1000)
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
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'desc> ?desc }\n' \
                '  OPTIONAL { <' + ds_id + '> <' + MT_ONTO + 'triples> ?triples }\n' \
                '}}'
        res_list, _ = iterative_query(query, self.query_endpoint, limit=1, max_answers=1)
        if len(res_list) > 0:
            res = res_list[0]
            return DataSource(
                ds_id,
                res['url'],
                res['dstype'],
                name=res['name'] if 'name' in res else '',
                desc=res['desc'] if 'desc' in res else '',
                params=res['params'] if 'params' in res else {},
                keywords=res['keywords'] if 'keywords' in res else '',
                version=res['version'] if 'version' in res else '',
                homepage=res['homepage'] if 'homepage' in res else '',
                organization=res['organization'] if 'organization' in res else ''
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
        res_list, _ = iterative_query(query, self.query_endpoint, limit=1000)
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
        print(sourcemaps)
        for s in sources:
            mts = self.get_ds_rdfmts(s['subject'])
            rdfmts[s['subject']] = {m['subject']: int(m['card']) if 'card' in m else -1 for m in mts}

        print(rdfmts.keys())
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
            print('Searching inter links from ', s['subject'])
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
                                print('terminating: ', r)
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
        print(did)
        for si in sourcemaps:
            s = sourcemaps[si]
            if si == did:
                continue
            print(si)
            queue1 = Queue()
            queues[did] = queue1
            print('Linking between ', ds['subject'], len(rdfmts[did]), ' and (to) ', s['subject'], len(rdfmts[si]))
            p = Process(target=self.get_inter_ds_links_bn, args=(ds, rdfmts[did], s, rdfmts[si], queue1,))
            p.start()
            processes[did] = p
            queue2 = Queue()
            queues[si] = queue2
            print('Linking between ', s['subject'], len(rdfmts[si]), ' and (to) ', ds['subject'], len(rdfmts[did]))
            p2 = Process(target=self.get_inter_ds_links_bn, args=(s, rdfmts[si], ds, rdfmts[did], queue2,))
            p2.start()
            processes[si] = p2
            if len(queues) >= 2:
                while len(queues) > 0:
                    to_remove = []
                    for endpoint in queues:
                        try:
                            queue = queues[endpoint]
                            r = queue.get(False)
                            print(r)
                            if r == 'EOF':
                                to_remove.append(endpoint)
                        except Empty:
                            pass
                    for r in to_remove:
                        if r in queues:
                            del queues[r]
                        print(r, queues.keys())
                        if r in processes and processes[r].is_alive():
                            print('terminating: ', r)
                            processes[r].terminate()
                        if r in processes:
                            del processes[r]
        print('linking DONE!', did)

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
            print(m1)
            print('--------------------------------------------------')
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
                if 'Movie' in m1:
                    print(m1, ' ====== ', link)
                if len(types_found[link]) > 0:
                    print(len(types_found[link]), 'links found')
                    try:
                        for m2 in types_found[link]:
                            val = str(url_endpoint2 + m1 + link + m2).encode()
                            mrpid = MT_RESOURCE + str(hashlib.md5(val).hexdigest())

                            card = -1
                            rs = DataSource(endpoint2['subject'], endpoint2['url'], DataSourceType.SPARQL_ENDPOINT)
                            ran = PropRange(mrpid, m2, rs, range_type=0, cardinality=card)
                            data.extend(ran.to_rdf())
                            mtpid = MT_RESOURCE + str(hashlib.md5(str(m1 + link).encode()).hexdigest())
                            data.append('<' + mtpid + '> <' + MT_ONTO + 'linkedTo> <' + mrpid + '> ')
                        if len(data) > 0:
                            self.update_graph(data)
                    except Exception as e:
                        print('Exception : ', e)
                        logger.error('Exception while collecting data' + str(e))
                        logger.error(m1 + ' --- Vs --- ' + 'in [' + url_endpoint2 + ']')
                        logger.error(types_found)
                        logger.error(data)

        print('get_inter_ds_links_bn Done!')
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
            print(pred)
            rdfmts_found = self.get_mts_matches(predicate_instance_list[pred], endpoint)
            results[pred] = [r for r in rdfmts_found if r in rdfmts]
            print(results[pred])
            print('=-=-=-=-=-=-')
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

    def get_rdfmts_from_mapping(self, datasource: DataSource, types: list = None) -> List[dict]:
        """Extracts the RDF Molecule Templates of a datasource from its mappings.

        If the RDF Mapping Language (RML) mappings used to create the datasource
        are known, they can be used to extract the RDF Molecule Templates.

        Parameters
        ----------
        datasource : DataSource
            The datasource for which the RDF Molecule Templates should be extracted from the mappings.
        types : list, optional
            A list containing the types that are present in the datasource. By default, this is
            set to None which leads to the actual extraction of the metadata from the mappings.

        Returns
        -------
        List[dict]
            A list of dictionaries representing the RDF class concepts and their metadata
            such as predicates and cardinality.

        """
        logger.info('----------------------' + datasource.url + '-------------------------------------')
        if types is None:
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
        print(mt_query)
        res, card = contact_rdf_source(mt_query, self.query_endpoint)
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
                    mtpredicateURI = MT_RESOURCE + str(hashlib.md5(str(t + p).encode()).hexdigest())
                    propsourceURI = MT_RESOURCE + str(hashlib.md5(str(datasource.url + t + p).encode()).hexdigest())
                    ranges = []
                    if len(predicates[p]) > 0:
                        for mr in predicates[p]:
                            mrpid = MT_RESOURCE + str(hashlib.md5(str(datasource.url + t + p + mr).encode()).hexdigest())
                            rtype = 0
                            for mrr in predicates[p][mr]:
                                print(p, mr, mrr)
                                rds = DataSource(mrr, datasource.url, DataSourceType.MONGODB)  # Only mrr is important here for the range
                                ran = PropRange(mrpid, mr, rds, range_type=rtype, cardinality=-1)
                                ranges.append(ran)

                    pred_source = Source(propsourceURI, datasource, -1)
                    mtprop = MTProperty(mtpredicateURI, p, [pred_source], ranges=ranges, label=p)
                    rdf_properties.append(mtprop)

                    results.append(rn)
                # add rdf:type property
                p = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
                mtpredicateURI = MT_RESOURCE + str(hashlib.md5(str(t + p).encode()).hexdigest())
                propsourceURI = MT_RESOURCE + str(hashlib.md5(str(datasource.url + t + p).encode()).hexdigest())

                pred_source = Source(propsourceURI, datasource, -1)
                mtprop = MTProperty(mtpredicateURI, p, [pred_source], ranges=[], label='RDF type')
                rdf_properties.append(mtprop)

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
