import hashlib
import time
from multiprocessing import Queue, Process
from pprint import pprint
from queue import Empty
from typing import Tuple

from FedSDM import get_logger
from FedSDM.rdfmt.model import *
from FedSDM.rdfmt.prefixes import *
from FedSDM.rdfmt.utils import contact_rdf_source, update_rdf_source

logger = get_logger('rdfmts', './rdfmts.log', True)
"""Logger for this module. It logs to the file 'rdfmts.log' as well as to stdout."""

metas = [
    'http://www.w3.org/ns/sparql-service-description',
    'http://www.openlinksw.com/schemas/virtrdf#',
    'http://www.w3.org/2000/01/rdf-schema#',
    'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
    'http://purl.org/dc/terms/Dataset',
    'http://bio2rdf.org/dataset_vocabulary:Endpoint',
    'http://www.w3.org/2002/07/owl#',
    'http://purl.org/goodrelations/',
    'http://www.ontologydesignpatterns.org/ont/d0.owl#',
    'http://www.wikidata.org/',
    'http://dbpedia.org/ontology/Wikidata:',
    'http://dbpedia.org/class/yago/',
    'http://rdfs.org/ns/void#',
    'http://www.w3.org/ns/dcat',
    'http://www.w3.org/2001/vcard-rdf/',
    'http://www.ebusiness-unibw.org/ontologies/eclass',
    'http://bio2rdf.org/bio2rdf.dataset_vocabulary:Dataset',
    'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/',
    'nodeID://'
]
"""Common prefixes of RDF classes and predicates that are not to be included in the metadata of the federation."""


def _iterative_query(query: str,
                     server: str,
                     limit: int = 10000,
                     max_tries: int = -1,
                     max_answers: int = -1) -> Tuple[list, int]:
    """Executes a query iteratively.

    The given SPARQL query is executed iteratively, i.e., the results are retrieved in blocks of size *limit*.
    It is also possible to specify the maximum number of results or requests made.

    Parameters
    ----------
    query : str
        The SPARQL query to be executed.
    server : str
        The URL of the SPARQL endpoint against which the query should be executed.
    limit : int, optional
        The number of results to be retrieved in one request.
        If no limit is given, it will be set to 10,000 by default.
    max_tries : int, optional
        The maximum number of requests allowed to be sent to the server.
        By default, it is set to -1 to disable this behavior.
    max_answers : int, optional
        The maximum number of answers to be retrieved. Note that all answers are returned that were
        retrieved from the server in the block of answers that exceeds the limit.
        By default, it is set to -1 to disable limiting the number of answers returned.

    Returns
    -------
    (list, int)
        The list returned as the first element of the tuple contains the query result.
        The second element is an integer indicating the status of the query execution.
        The status is 0 if the query was executed successfully, -1 otherwise.

    """
    offset = 0
    res_list = []
    status = 0
    num_requests = 0

    while True:
        query_copy = query + ' LIMIT ' + str(limit) + ' OFFSET ' + str(offset)
        num_requests += 1
        res, card = contact_rdf_source(query_copy, server)

        # if receiving the answer fails, try with a decreasing limit
        if card == -2:
            limit = limit // 2
            if limit < 1:
                status = -1
                break
            continue
        # results returned from the endpoint are appended to the result list
        if card > 0:
            res_list.extend(res)
        # stop if all results are retrieved or the maximum number of tries is reached
        if card < limit or (0 < max_answers <= len(res_list)) or num_requests >= max_tries:
            break

        offset += limit
        time.sleep(.5)
    return res_list, status


class RDFMTMgr(object):

    def __init__(self, query_url: str, update_url: str, user: str, passwd: str, graph: str):
        self.graph = graph
        self.query_endpoint = query_url
        self.update_endpoint = update_url
        self.user = user
        self.passwd = passwd

    def create(self, ds: DataSource, out_queue: Queue = Queue(), types: list = None, is_update: bool = False) -> dict:
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
        return self.extractMTLs(datasource, types)

    def extractMTLs(self, datasource: DataSource, types: list = None) -> dict:
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
        """
        Entry point for extracting RDF-MTs of an endpoint.
        Extracts list of rdf:Class concepts and predicates of an endpoint
        """
        endpoint_url = endpoint.url
        if types is None or len(types) == 0:
            query = 'SELECT DISTINCT ?t ?label WHERE {\n' \
                    '  ?s a ?t .\n' \
                    '  OPTIONAL { ?t  <' + RDFS + 'label> ?label }\n}'
            res_list, _ = _iterative_query(query, endpoint_url, limit=100)
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
                    mtranges = list(set(rn['range'] + rn['r']))
                else:
                    mtranges = rn['range']
                ranges = []

                for mr in mtranges:
                    if '^^' in mr:
                        continue
                    mrpid = MT_RESOURCE + str(hashlib.md5(str(endpoint_url + t + pred + mr).encode()).hexdigest())
                    if XSD not in mr:
                        rcard = self.get_cardinality(endpoint_url, t, prop=pred, mr=mr)
                        rtype = 0
                    else:
                        rcard = self.get_cardinality(endpoint_url, t, prop=pred, mr=mr, mr_datatype=True)
                        rtype = 1

                    ran = PropRange(mrpid, mr, endpoint, range_type=rtype, cardinality=str(rcard))
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
        query = 'SELECT DISTINCT ?range WHERE { <' + predicate + '> <' + RDFS + 'range> ?range . }'
        res_list, _ = _iterative_query(query, endpoint_url, limit=100)
        return [r['range'] for r in res_list if True not in [m in str(r['range']) for m in metas]]

    @staticmethod
    def find_instance_range(endpoint_url: str, type_: str, predicate: str) -> list:
        query = 'SELECT DISTINCT ?range WHERE {\n' \
                '  ?s a <' + type_ + '> .\n' \
                '  ?s <' + predicate + '> ?pt .\n' \
                '  ?pt a ?range .\n}'
        res_list, _ = _iterative_query(query, endpoint_url, limit=50)
        return [r['range'] for r in res_list if True not in [m in str(r['range']) for m in metas]]

    def get_predicates(self, endpoint_url: str, type_: str) -> list:
        """
        Get list of predicates of a class t
        """
        query = 'SELECT DISTINCT ?p ?label WHERE {\n' \
                '  ?s a <' + type_ + '> .\n' \
                '  ?s ?p ?pt .\n' \
                '  OPTIONAL { ?p  <' + RDFS + 'label> ?label }\n}'
        res_list, status = _iterative_query(query, endpoint_url, limit=50)
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
        """
        get a union of predicated from 'randomly' selected 10 entities from the first 100 subjects returned
        """
        query = 'SELECT DISTINCT ?s WHERE{ ?s a <' + type_ + '> . }'
        res_instances, _ = _iterative_query(query, endpoint_url, limit=50, max_tries=100)
        res_list = []
        card = len(res_instances)
        if card > 0:
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
        query = 'SELECT DISTINCT ?p ?label WHERE {\n' \
                '  <' + instance + '> ?p ?pt .\n' \
                '  OPTIONAL {?p  <' + RDFS + 'label> ?label}\n}'
        res_list, _ = _iterative_query(query, endpoint_url, limit=1000)
        return res_list

    def get_mts_from_owl(self, endpoint: DataSource, graph: str, types: list = None) -> List[dict]:
        endpoint_url = endpoint.url
        if types is None or len(types) == 0:
            query = 'SELECT DISTINCT ?t ?p ?range ?plabel ?tlabel WHERE { GRAPH <' + graph + '> {\n' \
                    '  ?p <' + RDFS + 'domain> ?t .\n' \
                    '  OPTIONAL { ?p <' + RDFS + 'range> ?range }\n' \
                    '  OPTIONAL { ?p <' + RDFS + "label> ?plabel . FILTER langMatches(?plabel, 'EN') }\n" \
                    '  OPTIONAL { ?t <' + RDFS + "label> ?tlabel . FILTER langMatches(?tlabel, 'EN') }\n" \
                    '}}'
            res_list, _ = _iterative_query(query, endpoint_url, limit=50)

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

                if isinstance(rcard, str) and '^^' in rcard:
                    rcard = rcard[:rcard.find('^^')]
                else:
                    rcard = str(rcard)
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
        i = 0
        # Virtuoso supports only 49 triples at a time.
        for i in range(0, len(data), 49):
            if i + 49 > len(data):
                update_query = 'INSERT DATA { GRAPH <' + self.graph + '>{ ' + ' . \n'.join(data[i:]) + '} }'
            else:
                update_query = 'INSERT DATA { GRAPH <' + self.graph + '>{ ' + ' . \n'.join(data[i:i + 49]) + '} }'
            logger.info(update_query)
            update_rdf_source(update_query, self.update_endpoint)
        if i < len(data) + 49:
            update_query = 'INSERT DATA { GRAPH <' + self.graph + '>{ ' + ' . \n'.join(data[i:]) + '} }'
            logger.info(update_query)
            update_rdf_source(update_query, self.update_endpoint)

    def delete_insert_data(self, delete: list, insert: list, where: list = None) -> None:
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
        if mt is None:
            query = 'SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }'
        elif prop is None:
            query = 'SELECT (COUNT(?s) as ?count) WHERE { ?s a <' + mt.replace(' ', '_') + '> }'
        else:
            mt = mt.replace(' ', '_')
            if mr is None:
                query = 'SELECT (COUNT(?s) as ?count) WHERE {\n  ?s a <' + mt + '> .\n  ?s <' + prop + '> ?o\n}'
            else:
                mr = mr.replace(' ', '_')
                if not mr_datatype:
                    query = 'SELECT (COUNT(?s) as ?count) WHERE {\n' \
                            '  ?s a <' + mt + '> .\n' \
                            '  ?s <' + prop + '> ?o .\n' \
                            '  ?o a <' + mr + '>\n' \
                            '}'
                else:
                    query = 'SELECT (COUNT(?s) as ?count) WHERE {\n' \
                            '  ?s a <' + mt + '> .\n' \
                            '  ?s <' + prop + '> ?o .\n' \
                            '  FILTER((datatype(?o))=<' + mr + '>)\n' \
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
        query = 'SELECT DISTINCT ?subc WHERE { <' + root.replace(' ', '_') + '> <' + RDFS + 'subClassOf> ?subc }'
        res, _ = contact_rdf_source(query, endpoint_url)
        return res

    def get_sources(self) -> list:
        query = 'SELECT DISTINCT ?subject ?url WHERE { GRAPH <' + self.graph + '> {\n' \
                '  ?subject a <' + MT_ONTO + 'DataSource> .\n' \
                '  ?subject <' + MT_ONTO + 'url> ?url .\n' \
                '}}'
        res_list, _ = _iterative_query(query, self.query_endpoint, limit=1000)
        return res_list

    def get_source(self, ds_id: str) -> Optional[DataSource]:
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
        res_list, _ = _iterative_query(query, self.query_endpoint, limit=1, max_answers=1)
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
        query = 'SELECT DISTINCT ?subject ?card WHERE { GRAPH <' + self.graph + '> {\n' \
                '  ?subject a <' + MT_ONTO + 'RDFMT> .\n' \
                '  ?subject <' + MT_ONTO + 'source> ?source .\n' \
                '  OPTIONAL { ?source <' + MT_ONTO + 'cardinality> ?card }\n' \
                '  ?source <' + MT_ONTO + 'datasource> <' + datasource + '> .\n' \
                '}}'
        res_list, _ = _iterative_query(query, self.query_endpoint, limit=1000)
        return res_list

    def create_inter_ds_links(self, datasource: DataSource | str = None, output_queue: Queue = Queue()) -> None:
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
        url_endpoint1 = endpoint1['url']
        url_endpoint2 = endpoint2['url']
        for m1 in rdfmts_endpoint1:
            print(m1)
            print('--------------------------------------------------')
            pred_query = 'SELECT DISTINCT ?p WHERE {\n  ?s a <' + m1 + '> .\n  ?s ?p ?t .\n  FILTER (isURI(?t))\n}'
            res_pred_query, _ = _iterative_query(pred_query, url_endpoint1, limit=1000)
            predicates = [r['p'] for r in res_pred_query]
            res_list = {}
            for p in predicates:
                query = 'SELECT DISTINCT ?t WHERE {\n' \
                        '  ?s a <' + m1 + '> .\n' \
                        '  ?s <' + p + '> ?t .\n' \
                        '  FILTER (isURI(?t))\n}'
                res, _ = _iterative_query(query, url_endpoint1, limit=500, max_answers=500)
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
        # Checks if there are subjects with prefixes matching
        batches = [instances[i:i+50] for i in range(0, len(instances), 50)]
        for batch in batches:
            subjects = ['?s=<' + r + '>' for r in batch]
            tquery = 'SELECT DISTINCT ?t WHERE {\n  ?s a ?t .\n  FILTER (' + ' || '.join(subjects) + ')\n}'
            res_list, _ = _iterative_query(tquery, endpoint, limit=1000)
            res = [r['t'] for r in res_list]
            if len(res) > 0:
                return res
        return []

    def create_from_mapping(self, datasource: DataSource, out_queue: Queue = Queue(), types: list = None) -> list:
        logger.info('----------------------' + datasource.url + '-------------------------------------')
        results = self.get_rdfmts_from_mapping(datasource, types)
        # self.create_inter_ds_links(datasource=ds)
        out_queue.put('EOF')
        return results

    def get_rdfmts_from_mapping(self, datasource: DataSource, types: list = None) -> list:
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
        return results


class MTManager(object):
    """
    Used in Config to access RDF-MTs in the data lake
    """
    def __init__(self, query_url: str, user: str, passwd: str, graph: str):
        self.graph = graph
        self.query_endpoint = query_url
        self.user = user
        self.passwd = passwd

    def get_data_sources(self) -> list:
        query = 'SELECT DISTINCT ?rid ?endpoint WHERE { GRAPH <' + self.graph + '> {\n' \
                '  ?rid a <' + MT_ONTO + 'DataSource> .\n' \
                '  ?rid <' + MT_ONTO + 'url> ?endpoint .\n' \
                '}}'
        res_list, _ = _iterative_query(query, self.query_endpoint, limit=1000)
        return res_list

    def get_rdfmts(self) -> dict:
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
        query = 'SELECT DISTINCT ?datasource ?pred WHERE { GRAPH <' + self.graph + '> {\n' \
                '  <' + rdf_class + '> <' + MT_ONTO + 'source> ?source .\n' \
                '  <' + rdf_class + '> <' + MT_ONTO + 'hasProperty> ?mtp .\n' \
                '  ?mtp <' + MT_ONTO + 'predicate> ?pred .\n' \
                '  ?source <' + MT_ONTO + 'datasource> ?datasource.\n' \
                '}}'
        return self.prepare_rdfmts_from_query(query, rdf_class)

    def prepare_rdfmts_from_query(self, query: str, rdf_class: str = None) -> dict:
        res_list, _ = _iterative_query(query, self.query_endpoint, limit=1000)
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
        res_list, _ = _iterative_query(query, self.query_endpoint, limit=1000)
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
        query = 'SELECT DISTINCT ?rid WHERE { GRAPH <' + self.graph + '> {\n' \
                '  ?rid a <' + MT_ONTO + 'RDFMT> .\n'
        i = 0
        for p in predicates:
            query += '  ?rid <' + MT_ONTO + 'hasProperty> ?mtp' + str(i) + '.\n' \
                     '  ?mtp' + str(i) + ' <' + MT_ONTO + 'predicate> <' + p + '> .\n'
            i += 1

        query += '}}'
        res_list, _ = _iterative_query(self.query_endpoint, query, limit=1000)

        results = {}
        for r in res_list:
            res = self.get_rdfmt(r['rid'])
            if len(res) > 0:
                results[r['rid']] = res
        return results

    def get_preds_mt(self, predicates: list = None) -> dict:
        filters = ' || '.join(['?pred=<' + p + '> ' for p in predicates]) if predicates is not None else ''
        query = 'SELECT DISTINCT ?rid ?pred WHERE { GRAPH <' + self.graph + '> {\n' \
                '  ?rid a <' + MT_ONTO + 'RDFMT> .\n' \
                '  ?rid <' + MT_ONTO + 'hasProperty> ?mtp .\n' \
                '  ?mtp <' + MT_ONTO + 'predicate> ?pred .\n'
        if len(filters) > 0:
            query += '  FILTER (' + filters + ')\n'
        query += '}}'
        res_list, _ = _iterative_query(query, self.query_endpoint, limit=1000)

        results = {}
        for r in res_list:
            results.setdefault(r['pred'], []).append(r['rid'])
        results = {r: list(set(results[r])) for r in results}
        return results
