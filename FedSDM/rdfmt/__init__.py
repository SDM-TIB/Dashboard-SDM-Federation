import hashlib
import time
from multiprocessing import Queue, Process
from pprint import pprint
from queue import Empty

from FedSDM import get_logger
from FedSDM.rdfmt.model import *
from FedSDM.rdfmt.prefixes import *
from FedSDM.rdfmt.utils import contactRDFSource, updateRDFSource

logger = get_logger('rdfmts', './rdfmts.log', True)

metas = ['http://www.w3.org/ns/sparql-service-description',
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
         'nodeID://']


def _iterative_query(query: str, server: str, limit: int = 10000, max_tries: int = -1, max_answers: int = -1):
    offset = 0
    res_list = []
    status = 0
    num_requests = 0

    while True:
        query_copy = query + ' LIMIT ' + str(limit) + ' OFFSET ' + str(offset)
        num_requests += 1
        res, card = contactRDFSource(query_copy, server)

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

    def __init__(self, query_url, update_url, user, passwd, graph):
        self.graph = graph
        self.query_endpoint = query_url
        self.update_endpoint = update_url
        self.user = user
        self.passwd = passwd

    def create(self, ds, out_queue=Queue(), types=None, is_update=False):
        if types is None:
            types = []

        endpoint = ds.url
        logger.info('----------------------' + endpoint + '-------------------------------------')

        if not is_update:
            # Get #triples of a dataset
            triples = self.get_cardinality(endpoint)
            if isinstance(triples, str) and '^' in triples:
                triples = int(triples[:triples.find('^^')])
            ds.triples = triples

            data = '<' + ds.rid + '> <' + MT_ONTO + 'triples> ' + triples
            self.updateGraph([data])
        else:
            today = str(datetime.datetime.now())
            data = ['<' + ds.rid + '> <http://purl.org/dc/terms/modified> "' + today + '"']
            delete = ['<' + ds.rid + '> <http://purl.org/dc/terms/modified> ?modified ']
            self.delete_insert_data(delete, data, delete)

        results = self.get_rdfmts(ds, types)
        # self.create_inter_ds_links(datasource=ds)
        out_queue.put('EOF')

        return results

    def get_rdfmts(self, ds, types=None):
        return self.extractMTLs(ds, types)

    def extractMTLs(self, datasource, types=None):
        rdf_molecules = {}
        endpoint = datasource.url

        if datasource.ontology_graph is None:
            results = self.get_typed_concepts(datasource, types)
        else:
            results = self.get_mts_from_owl(datasource, datasource.ontology_graph, types)

        rdf_molecules[endpoint] = results

        pprint(results)
        logger.info('*****' + endpoint + ' ***********')
        logger.info('*************finished *********************' )

        return rdf_molecules

    def get_typed_concepts(self, e, types=None):
        """
        Entry point for extracting RDF-MTs of an endpoint.
        Extracts list of rdf:Class concepts and predicates of an endpoint
        :param endpoint:
        :param types:
        :return:
        """
        endpoint = e.url
        referer = endpoint
        if types is None or len(types) == 0:
            query = 'SELECT DISTINCT ?t ?label WHERE {\n' \
                    '  ?s a ?t .\n' \
                    '  OPTIONAL { ?t  <' + RDFS + 'label> ?label }\n}'
            res_list, _ = _iterative_query(query, endpoint, limit=100)
            to_remove = [r for m in metas for r in res_list if m in str(r['t'])]
            for r in to_remove:
                res_list.remove(r)
        else:
            res_list = [{'t': t} for t in types]

        logger.info(endpoint)
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
            card = self.get_cardinality(endpoint, t)
            if isinstance(card, str) and '^' in card:
                card = int(card[:card.find('^^')])

            if isinstance(card, str) and '^^' in card:
                mcard = card[:card.find('^^')]
            else:
                mcard = str(card)

            source_uri = MT_RESOURCE + str(hashlib.md5(str(endpoint + t).encode()).hexdigest())
            source = Source(source_uri, e, mcard)
            # Get subclasses
            subc = self.get_subclasses(endpoint, t)
            subclasses = []
            if subc is not None:
                subclasses = [r['subc'] for r in subc]

            rdf_properties = []
            # Get predicates of the molecule t
            predicates = self.get_predicates(referer, t)
            properties_processed = []
            for p in predicates:
                rn = {'t': t, 'cardinality': mcard, 'subclasses': subclasses}
                pred = p['p']
                if pred in properties_processed:
                    continue
                properties_processed.append(pred)

                mtpredicateURI = MT_RESOURCE + str(hashlib.md5(str(t + pred).encode()).hexdigest())
                propsourceURI = MT_RESOURCE + str(hashlib.md5(str(endpoint + t + pred).encode()).hexdigest())
                # Get cardinality of this predicate from this RDF-MT
                pred_card = self.get_cardinality(endpoint, t, prop=pred)
                if isinstance(pred_card, str) and '^^' in pred_card:
                    pred_card = pred_card[:pred_card.find('^^')]
                else:
                    pred_card = str(pred_card)

                print(pred, pred_card)
                rn['p'] = pred
                rn['predcard'] = pred_card

                # Get range of this predicate from this RDF-MT t
                rn['range'] = self.get_rdfs_ranges(referer, pred)
                if len(rn['range']) == 0:
                    rn['r'] = self.find_instance_range(referer, t, pred)
                    mtranges = list(set(rn['range'] + rn['r']))
                else:
                    mtranges = rn['range']
                ranges = []

                for mr in mtranges:
                    if '^^' in mr:
                        continue
                    mrpid = MT_RESOURCE + str(hashlib.md5(str(endpoint + t + pred + mr).encode()).hexdigest())
                    rcard = -1
                    if XSD not in mr:
                        rcard = self.get_cardinality(endpoint, t, prop=pred, mr=mr)
                        rtype = 0
                    else:
                        rcard = self.get_cardinality(endpoint, t, prop=pred, mr=mr, mrtype=mr)
                        rtype = 1

                    if isinstance(rcard, str) and '^^' in rcard:
                        rcard = rcard[:rcard.find('^^')]
                    else:
                        rcard = str(rcard)
                    ran = PropRange(mrpid, mr, e, range_type=rtype, cardinality=rcard)
                    ranges.append(ran)
                if 'label' in p:
                    plab = p['label']
                else:
                    plab = ''

                pred_source = Source(propsourceURI, e, pred_card)
                mtprop = MTProperty(mtpredicateURI, pred, [pred_source], ranges=ranges, label=plab)
                rdf_properties.append(mtprop)

                results.append(rn)

            name = r['label'] if 'label' in r else t
            desc = r['desc'] if 'desc' in r else None

            mt = RDFMT(t, name, properties=rdf_properties, desc=desc, sources=[source], subClassOf=subclasses)
            data = mt.to_rdf()
            self.updateGraph(data)

        return results

    @staticmethod
    def get_rdfs_ranges(referer, p):
        query = 'SELECT DISTINCT ?range WHERE { <' + p + '> <' + RDFS + 'range> ?range . }'
        res_list, _ = _iterative_query(query, referer, limit=100)
        return [r['range'] for r in res_list if True not in [m in str(r['range']) for m in metas]]

    @staticmethod
    def find_instance_range(referer, t, p):
        query = 'SELECT DISTINCT ?range WHERE {\n' \
                '  ?s a <' + t + '> .\n' \
                '  ?s <' + p + '> ?pt .\n' \
                '  ?pt a ?range .\n}'
        res_list, _ = _iterative_query(query, referer, limit=50)
        return [r['range'] for r in res_list if True not in [m in str(r['range']) for m in metas]]

    def get_predicates(self, referer, t):
        """
        Get list of predicates of a class t

        :param referer: endpoint
        :param server: server address of an endpoint
        :param path:  path in an endpoint (after server url)
        :param t: RDF class Concept extracted from an endpoint
        :return:
        """
        query = 'SELECT DISTINCT ?p ?label WHERE {\n' \
                '  ?s a <' + t + '> .\n' \
                '  ?s ?p ?pt .\n' \
                '  OPTIONAL { ?p  <' + RDFS + 'label> ?label }\n}'
        res_list, status = _iterative_query(query, referer, limit=50)
        existing_predicates = [r['p'] for r in res_list]

        if status == -1:  # fallback - get predicates from randomly selected instances of the type
            print('giving up on ' + query)
            print('trying instances .....')
            rand_inst_res = self.get_preds_of_random_instances(referer, t)
            for r in rand_inst_res:
                if r not in existing_predicates:
                    res_list.append({'p': r})

        return res_list

    def get_preds_of_random_instances(self, referer, t):
        """
        get a union of predicated from 'randomly' selected 10 entities from the first 100 subjects returned

        :param referer: endpoint
        :param server:  server name
        :param path: path
        :param t: rdf class concept of and endpoint
        :param limit:
        :return:
        """
        query = 'SELECT DISTINCT ?s WHERE{ ?s a <' + t + '> . }'
        res_instances, _ = _iterative_query(query, referer, limit=50, max_tries=100)
        res_list = []
        card = len(res_instances)
        if card > 0:
            import random
            rand = random.randint(0, card - 1)
            inst = res_instances[rand]
            inst_res = self.get_preds_of_instance(referer, inst['s'])
            inst_res = [r['p'] for r in inst_res]
            res_list.extend(inst_res)
            res_list = list(set(res_list))

        return res_list

    def get_preds_of_instance(self, referer, inst):
        query = 'SELECT DISTINCT ?p ?label WHERE {\n' \
                '  <' + inst + '> ?p ?pt .\n' \
                '  OPTIONAL {?p  <' + RDFS + 'label> ?label}\n}'
        res_list, _ = _iterative_query(query, referer, limit=1000)
        return res_list

    def get_mts_from_owl(self, e, graph, types=None):
        endpoint = e.url
        if types is None or len(types) == 0:
            query = 'SELECT DISTINCT ?t ?p ?range ?plabel ?tlabel WHERE { GRAPH <' + graph + '> {\n' \
                    '  ?p <' + RDFS + 'domain> ?t .\n' \
                    '  OPTIONAL { ?p <' + RDFS + 'range> ?range }\n' \
                    '  OPTIONAL { ?p <' + RDFS + "label> ?plabel . FILTER langMatches(?plabel, 'EN') }\n" \
                    '  OPTIONAL { ?t <' + RDFS + "label> ?tlabel . FILTER langMatches(?tlabel, 'EN') }\n" \
                    '}}'
            res_list, _ = _iterative_query(query, endpoint, limit=50)

            to_remove = [r for m in metas for r in res_list if m in str(r['t'])]
            for r in to_remove:
                res_list.remove(r)
        else:
            res_list = [{'t': t} for t in types]

        logger.info(endpoint)
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
                # card = self.get_cardinality(endpoint, t)
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
                source_uri = MT_RESOURCE + str(hashlib.md5(str(endpoint + t).encode()).hexdigest())
                source = Source(source_uri, e, mcard)
                already_processed[t] = mcard
                subc = self.get_subclasses(endpoint, t)
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
            propsourceURI = MT_RESOURCE + str(hashlib.md5(str(endpoint + t + pred).encode()).hexdigest())
            # Get cardinality of this predicate from this RDF-MT
            predcard = -1# self.get_cardinality(endpoint, t, prop=pred)
            if isinstance(predcard, str) and '^^' in predcard:
                predcard = predcard[:predcard.find('^^')]
            else:
                predcard = str(predcard)

            rn['p'] = pred
            rn['predcard'] = predcard

            # Get range of this predicate from this RDF-MT t
            rn['range'] = [] # self.get_rdfs_ranges(referer, pred)

            ranges = []
            if 'range' in r and XSD not in r['range']:
                print('range', r['range'])
                rn['range'].append(r['range'])
                mr = r['range']
                mrpid = MT_RESOURCE + str(hashlib.md5(str(endpoint + t + pred + mr).encode()).hexdigest())

                if XSD not in mr:
                    rcard = -1#self.get_cardinality(endpoint, t, prop=pred, mr=mr)
                    rtype = 0
                else:
                    rcard = -1#self.get_cardinality(endpoint, t, prop=pred, mr=mr, mrtype=mr)
                    rtype = 1

                if isinstance(rcard, str) and '^^' in rcard:
                    rcard = rcard[:rcard.find('^^')]
                else:
                    rcard = str(rcard)
                ran = PropRange(mrpid, mr, e, range_type=rtype, cardinality=rcard)
                ranges.append(ran)
            if 'plabel' in r:
                plab = r['plabel']
            else:
                plab = ''

            predsouce = Source(propsourceURI, e, predcard)
            mtprop = MTProperty(mtpredicateURI, pred, [predsouce], ranges=ranges, label=plab)
            mts[t]['properties'].append(mtprop)
            # rdfpropteries.append(mtprop)

            results.append(rn)

        for t in mts:
            mt = RDFMT(t, mts[t]['name'], mts[t]['properties'], mts[t]['desc'], mts[t]['sources'], mts[t]['subClassOf'])
            data = mt.to_rdf()
            self.updateGraph(data)

        return results

    def updateGraph(self, data):
        i = 0
        # Virtuoso supports only 49 triples at a time.
        for i in range(0, len(data), 49):
            if i + 49 > len(data):
                update_query = 'INSERT DATA { GRAPH <' + self.graph + '>{ ' + ' . \n'.join(data[i:]) + '} }'
            else:
                update_query = 'INSERT DATA { GRAPH <' + self.graph + '>{ ' + ' . \n'.join(data[i:i + 49]) + '} }'
            logger.info(update_query)
            updateRDFSource(update_query, self.update_endpoint)
        if i < len(data) + 49:
            update_query = 'INSERT DATA { GRAPH <' + self.graph + '>{ ' + ' . \n'.join(data[i:]) + '} }'
            logger.info(update_query)
            updateRDFSource(update_query, self.update_endpoint)

    def delete_insert_data(self, delete, insert, where=None):
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
            updateRDFSource(update_query, self.update_endpoint)
        update_query = 'WITH <' + self.graph + '> DELETE {'
        if i < len(delete) + 49:
            update_query += ' . \n'.join(delete[i:]) + '} ' \
                           'INSERT {' + ' . \n'.join(insert[i:]) + '} ' \
                           'WHERE {' + ' . \n'.join(where[i:]) + '}'
            logger.info(update_query)
            updateRDFSource(update_query, self.update_endpoint)

    def get_cardinality(self, endpoint, mt=None, prop=None, mr=None, mrtype=None):
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
                if mrtype is None:
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

        res, card = contactRDFSource(query, endpoint)
        if res is not None and len(res) > 0 and len(res[0]['count']) > 0:
            return res[0]['count']

        return -1

    @staticmethod
    def get_subclasses(endpoint, root):
        referer = endpoint
        query = 'SELECT DISTINCT ?subc WHERE { <' + root.replace(' ', '_') + '> <' + RDFS + 'subClassOf> ?subc }'
        res, card = contactRDFSource(query, referer)
        return res

    def get_sources(self):
        query = 'SELECT DISTINCT ?subject ?url WHERE { GRAPH <' + self.graph + '> {\n' \
                '  ?subject a <' + MT_ONTO + 'DataSource> .\n' \
                '  ?subject <' + MT_ONTO + 'url> ?url .\n' \
                '}}'
        res_list, _ = _iterative_query(query, self.query_endpoint, limit=1000)
        return res_list

    def get_source(self, ds_id: str) -> DataSource | None:
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

    def get_ds_rdfmts(self, datasource):
        query = 'SELECT DISTINCT ?subject ?card WHERE { GRAPH <' + self.graph + '> {\n' \
                '  ?subject a <' + MT_ONTO + 'RDFMT> .\n' \
                '  ?subject <' + MT_ONTO + 'source> ?source .\n' \
                '  OPTIONAL { ?source <' + MT_ONTO + 'cardinality> ?card }\n' \
                '  ?source <' + MT_ONTO + 'datasource> <' + datasource + '> .\n' \
                '}}'
        res_list, _ = _iterative_query(query, self.query_endpoint, limit=1000)
        return res_list

    def create_inter_ds_links(self, datasource=None, output_queue=Queue()):
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

    def find_all_links(self, rdfmts, sourcemaps):
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

    def update_links(self, rdfmts, sourcemaps, datasource):
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

    def get_inter_ds_links_bn(self, s, srdfmts, t, trdfmts, queue=Queue()):
        endpoint1 = s['url']
        endpoint2 = t['url']
        for m1 in srdfmts:
            print(m1)
            print('--------------------------------------------------')
            pred_query = 'SELECT DISTINCT ?p WHERE {\n  ?s a <' + m1 + '> .\n  ?s ?p ?t .\n  FILTER (isURI(?t))\n}'
            res_pred_query, _ = _iterative_query(pred_query, endpoint1, limit=1000)
            predicates = [r['p'] for r in res_pred_query]
            res_list = {}
            for p in predicates:
                query = 'SELECT DISTINCT ?t WHERE {\n' \
                        '  ?s a <' + m1 + '> .\n' \
                        '  ?s <' + p + '> ?t .\n' \
                        '  FILTER (isURI(?t))\n}'
                res, _ = _iterative_query(query, endpoint1, limit=500, max_answers=500)
                res_list.setdefault(p, []).extend([r['t'] for r in res])

            types_found = self.get_links_bn_ds(res_list, trdfmts, endpoint2)
            for link in types_found:
                data = []
                if 'Movie' in m1:
                    print(m1, ' ====== ', link)
                if len(types_found[link]) > 0:
                    print(len(types_found[link]), 'links found')
                    try:
                        for m2 in types_found[link]:
                            val = str(endpoint2 + m1 + link + m2).encode()
                            mrpid = MT_RESOURCE + str(hashlib.md5(val).hexdigest())

                            card = -1
                            rs = DataSource(t['subject'], t['url'], DataSourceType.SPARQL_ENDPOINT)
                            ran = PropRange(mrpid, m2, rs, range_type=0, cardinality=card)
                            data.extend(ran.to_rdf())
                            mtpid = MT_RESOURCE + str(hashlib.md5(str(m1 + link).encode()).hexdigest())
                            data.append('<' + mtpid + '> <' + MT_ONTO + 'linkedTo> <' + mrpid + '> ')
                        if len(data) > 0:
                            self.updateGraph(data)
                    except Exception as e:
                        print('Exception : ', e)
                        logger.error('Exception while collecting data' + str(e))
                        logger.error(m1 + ' --- Vs --- ' + 'in [' + endpoint2 + ']')
                        logger.error(types_found)
                        logger.error(data)

        print('get_inter_ds_links_bn Done!')
        queue.put('EOF')

    def get_links_bn_ds(self, res_list, trdfmts, e2):
        results = {}
        for p in res_list:
            print(p)
            rdfmts_found = self.get_mts_matches(res_list[p], e2)
            results[p] = [r for r in rdfmts_found if r in trdfmts]
            print(results[p])
            print('=-=-=-=-=-=-')
        return results

    def get_mts_matches(self, results, e):
        # Checks if there are subjects with prefixes matching
        batches = [results[i:i+50] for i in range(0, len(results), 50)]
        for batch in batches:
            subjects = ['?s=<' + r + '>' for r in batch]
            tquery = 'SELECT DISTINCT ?t WHERE {\n  ?s a ?t .\n  FILTER (' + ' || '.join(subjects) + ')\n}'
            res = self.get_results(tquery, e)
            if len(res) > 0:
                return res
        return {}

    def get_links_bn_ds_prefixed(self, res_list, m2, e2):
        res_dict = {}
        results = {}
        prefixes = {}
        for r in res_list:
            if r['p'] in res_dict:
                res_dict[r['p']].append(r['t'])
            else:
                res_dict[r['p']] = [r['t']]

            obj = r['t']
            if r['p'] in prefixes:
                prefixes[r['p']].append(obj[:obj.rfind('/')])
            else:
                prefixes[r['p']] = [obj[:obj.rfind('/')]]
        print('linking properties:', res_dict.keys())
        for p in res_dict:
            prefs = list(set(prefixes[p]))
            res_list = self.get_if_prefix_matches(m2, prefs, e2)
            e1res = res_dict[p]
            matching = list(set(res_list).intersection(set(e1res)))
            if len(matching) > 0:
                results[p] = len(matching)
                print(len(matching), ' links out of 10000 subject found for', p, m2, 'in', e2)
            else:
                print(p, 'no matching found')
        return results

    def get_if_prefix_matches(self, m2, prefixes, e):
        # Checks if there are subjects with prefixes matching
        res_list = []
        batches = [prefixes[i:i+10] for i in range(0, len(prefixes), 10)]
        for batch in batches:
            prefs = [" regex(str(?t), '" + p + "', 'i') " for p in batch]
            tquery = 'SELECT DISTINCT * WHERE {\n  ?t a <' + m2 + '> .\n  FILTER (' + ' || '.join(prefs) + ')\n}'
            print(tquery)
            res_list.extend(self.get_results(tquery, e))
        return res_list

    def get_results(self, query, endpoint):
        res, _ = _iterative_query(query, endpoint, limit=1000)
        return [r['t'] for r in res]

    def create_from_mapping(self, ds, out_queue=Queue(), types=None):
        logger.info('----------------------' + ds.url + '-------------------------------------')
        results = self.get_rdfmts(ds, types)
        # self.create_inter_ds_links(datasource=ds)
        out_queue.put('EOF')
        return results

    def get_rdfmts_from_mapping(self, ds, types=None):
        if types is None:
            types = []
        mt_query = 'PREFIX rr: <http://www.w3.org/ns/r2rml#> ' \
                   'PREFIX rml: <http://semweb.mmlab.be/ns/rml#>' \
                   'SELECT DISTINCT ?t ?p ?r ?rds WHERE { GRAPH <' + self.graph + '> {\n' \
                   '  ?tm rml:logicalSource ?ls .\n' \
                   '  ?ls rml:source <' + ds.rid + '> .\n' \
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
        res, card = contactRDFSource(mt_query, self.query_endpoint)
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
                source_uri = MT_RESOURCE + str(hashlib.md5(str(ds.url + t).encode()).hexdigest())
                source = Source(source_uri, ds)

                rdf_properties = []
                preds = res_list[t]

                for p in preds:
                    rn = {'t': t, 'cardinality': -1, 'subclasses': [], 'p': p, 'predcard': -1, 'range': preds[p].keys()}
                    mtpredicateURI = MT_RESOURCE + str(hashlib.md5(str(t + p).encode()).hexdigest())
                    propsourceURI = MT_RESOURCE + str(hashlib.md5(str(ds.url + t + p).encode()).hexdigest())
                    ranges = []
                    if len(preds[p]) > 0:
                        for mr in preds[p]:
                            mrpid = MT_RESOURCE + str(hashlib.md5(str(ds.url + t + p + mr).encode()).hexdigest())
                            rtype = 0
                            for mrr in preds[p][mr]:
                                print(p, mr, mrr)
                                rds = DataSource(mrr, ds.url, DataSourceType.MONGODB) # Only mrr is important here for the range
                                ran = PropRange(mrpid, mr, rds, range_type=rtype, cardinality=-1)
                                ranges.append(ran)

                    pred_source = Source(propsourceURI, ds, -1)
                    mtprop = MTProperty(mtpredicateURI, p, [pred_source], ranges=ranges, label=p)
                    rdf_properties.append(mtprop)

                    results.append(rn)
                # add rdf:type property
                p = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
                mtpredicateURI = MT_RESOURCE + str(hashlib.md5(str(t + p).encode()).hexdigest())
                propsourceURI = MT_RESOURCE + str(hashlib.md5(str(ds.url + t + p).encode()).hexdigest())

                pred_source = Source(propsourceURI, ds, -1)
                mtprop = MTProperty(mtpredicateURI, p, [pred_source], ranges=[], label='RDF type')
                rdf_properties.append(mtprop)

                name = t
                desc = None
                mt = RDFMT(t, name, properties=rdf_properties, desc=desc, sources=[source], subClassOf=[])
                mtd = mt.to_rdf()
                data.extend(mtd)
                data = list(set(data))
                #pprint(data)

        if len(data) > 0:
            self.updateGraph(data)

        return results


class MTManager(object):
    """
    Used in Config to access RDF-MTs in the data lake
    """
    def __init__(self, query_url, user, passwd, graph):
        self.graph = graph
        self.query_endpoint = query_url
        self.user = user
        self.passwd = passwd

    def get_data_sources(self):
        query = 'SELECT DISTINCT ?rid ?endpoint WHERE { GRAPH <' + self.graph + '> {\n' \
                '  ?rid a <' + MT_ONTO + 'DataSource> .\n' \
                '  ?rid <' + MT_ONTO + 'url> ?endpoint .\n' \
                '}}'
        res_list, _ = _iterative_query(query, self.query_endpoint, limit=1000)
        return res_list

    def get_rdfmt_links(self, rdf_class, predicates=None):
        if predicates is None:
            predicates = []
        else:
            filters = ['?pred=<' + p + '>' for p in predicates]
            predicates = 'FILTER (' + (' || '.join(filters)) + ')'

        query = 'SELECT DISTINCT ?datasource  ?pred ?mtr WHERE { GRAPH <' + self.graph + '> {\n' \
                '  <' + rdf_class + '> <' + MT_ONTO + 'source> ?source .\n' \
                '  ?source <' + MT_ONTO + 'datasource> ?datasource .\n' \
                '  <' + rdf_class + '> <' + MT_ONTO + 'hasProperty> ?mtp .\n' \
                '  ?mtp <' + MT_ONTO + 'predicate> ?pred .\n' \
                '  ?mtp <' + MT_ONTO + 'linkedTo> ?mtrange .\n' \
                '  ?mtrange <' + MT_ONTO + 'rdfmt> ?mtr .\n  ' \
                + predicates + '\n}}'
        res_list, _ = _iterative_query(query, self.query_endpoint, limit=1000)
        results = {}
        for r in res_list:
            r['rid'] = rdf_class
            if r['rid'] not in results:
                results[r['rid']] = {
                    'rootType': r['rid'],
                    'linkedTo': [r['mtr']] if 'mtr' in r else [],
                    'wrappers': [
                        {
                            'url': self.get_data_source(r['datasource']).url,
                            'predicates': [
                                r['pred']
                            ],
                            'urlparam': '',
                            'wrapperType': 'SPARQLEndpoint'
                        }
                    ],
                    'predicates': [
                        {'predicate': r['pred'],
                         'range': [r['mtr']] if 'mtr' in r else []}
                    ],
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
        res = results[rdf_class] if rdf_class in results else {}

        return res

    # def get_pred_ranges(self, mt, pred):
    def get_rdfmts(self):
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
        reslist, _ = _iterative_query(query, self.query_endpoint, limit=1000)
        results = {}
        for r in reslist:
            if r['rid'] not in results:
                results[r['rid']] = {
                            'rootType': r['rid'],
                            'linkedTo': [r['mtr']] if 'mtr' in r else [],
                            'wrappers': [
                                {
                                    'url': self.get_data_source(r['datasource']).url,
                                    'predicates': [
                                        r['pred']
                                         ],
                                    'urlparam': '',
                                    'wrapperType': 'SPARQLEndpoint'
                                }
                            ],
                            'predicates': [
                                {'predicate': r['pred'],
                                 'range':[r['mtr']] if 'mtr' in r else []}
                                ],
                            'subclass': []
                            }
            else:
                if 'mtr' in r:
                    results[r['rid']]['linkedTo'].append(r['mtr'])
                    results[r['rid']]['linkedTo'] = list(set(results[r['rid']]['linkedTo']))
                pfound = False
                for p in results[r['rid']]['predicates']:
                    if p['predicate'] == r['pred']:
                        if 'mtr' in r:
                            p['range'].append(r['mtr'])
                        pfound = True

                if not pfound:
                    results[r['rid']]['predicates'].append({
                        'predicate': r['pred'],
                        'range': [r['mtr']] if 'mtr' in r else []
                    })
                wfound = False
                for w in results[r['rid']]['wrappers']:
                    if w['url'] == r['datasource']:  # TODO: is this correct due to the changes
                        wfound = True
                        w['predicates'].append(r['pred'])
                        w['predicates'] = list(set(w['predicates']))
                if not wfound:
                    results[r['rid']]['wrappers'].append({
                        'url': self.get_data_source(r['datasource']).url,
                        'predicates': [
                            r['pred']
                            ],
                        'urlparam': '',
                        'wrapperType': 'SPARQLEndpoint'
                    })

        return results

    def get_rdfmt(self, rdfclass):
        query = 'SELECT DISTINCT ?datasource ?pred WHERE { GRAPH <' + self.graph + '> {\n' \
                '  <' + rdfclass + '> <' + MT_ONTO + 'source> ?source .\n' \
                '  <' + rdfclass + '> <' + MT_ONTO + 'hasProperty> ?mtp .\n' \
                '  ?mtp <' + MT_ONTO + 'predicate> ?pred .\n' \
                '  ?source <' + MT_ONTO + 'datasource> ?datasource.\n' \
                '}}'
        reslist, _ = _iterative_query(query, self.query_endpoint, limit=1000)
        results = {}
        for r in reslist:
            r['rid'] = rdfclass
            if r['rid'] not in results:
                results[r['rid']] = {
                            'rootType': r['rid'],
                            'linkedTo': [r['mtr']] if 'mtr' in r else [],
                            'wrappers': [
                                {
                                    'url': self.get_data_source(r['datasource']).url,
                                    'predicates': [
                                        r['pred']
                                         ],
                                    'urlparam': '',
                                    'wrapperType': 'SPARQLEndpoint'
                                }
                            ],
                            'predicates': [
                                {'predicate': r['pred'],
                                 'range':[r['mtr']] if 'mtr' in r else []}
                                ],
                            'subclass': []
                            }
            else:
                if 'mtr' in r:
                    results[r['rid']]['linkedTo'].append(r['mtr'])
                    results[r['rid']]['linkedTo'] = list(set(results[r['rid']]['linkedTo']))
                pfound = False
                for p in results[r['rid']]['predicates']:
                    if p['predicate'] == r['pred']:
                        if 'mtr' in r:
                            p['range'].append(r['mtr'])
                        pfound = True

                if not pfound:
                    results[r['rid']]['predicates'].append({
                        'predicate': r['pred'],
                        'range': [r['mtr']] if 'mtr' in r else []
                    })
                wfound = False
                for w in results[r['rid']]['wrappers']:
                    if w['url'] == r['datasource']:  # TODO: is this correct due to the changes?
                        wfound = True
                        w['predicates'].append(r['pred'])
                        w['predicates'] = list(set(w['predicates']))
                if not wfound:
                    results[r['rid']]['wrappers'].append({
                        'url': self.get_data_source(r['datasource']).url,
                        'predicates': [
                            r['pred']
                            ],
                        'urlparam': '',
                        'wrapperType': 'SPARQLEndpoint'
                    })

        return results[rdfclass] if rdfclass in results else {}

    def get_data_source(self, dsid):
        query = 'SELECT DISTINCT *  WHERE { GRAPH <' + self.graph + '> {\n' \
                '  <' + dsid + '> <' + MT_ONTO + 'url> ?url .\n' \
                '  <' + dsid + '> <' + MT_ONTO + 'dataSourceType> ?dstype .\n' \
                '  OPTIONAL { <' + dsid + '> <' + MT_ONTO + 'name> ?name }\n' \
                '  OPTIONAL { <' + dsid + '> <' + MT_ONTO + 'version> ?version }\n' \
                '  OPTIONAL { <' + dsid + '> <' + MT_ONTO + 'keywords> ?keywords }\n' \
                '  OPTIONAL { <' + dsid + '> <' + MT_ONTO + 'organization> ?organization }\n' \
                '  OPTIONAL { <' + dsid + '> <' + MT_ONTO + 'homepage> ?homepage }\n' \
                '  OPTIONAL { <' + dsid + '> <' + MT_ONTO + 'params> ?params }\n' \
                '  OPTIONAL { <' + dsid + '> <' + MT_ONTO + 'desc> ?desc }\n' \
                '  OPTIONAL { <' + dsid + '> <' + MT_ONTO + 'triples> ?triples }\n' \
                '}}'
        reslist, _ = _iterative_query(query, self.query_endpoint, limit=1000)
        if len(reslist) > 0:
            e = reslist[0]
            ds = DataSource(dsid,
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
            return ds
        else:
            return None

    def get_mappings(self, dsid):
        prefix = 'prefix rr: <http://www.w3.org/ns/r2rml#> ' \
                 'prefix rml: <http://semweb.mmlab.be/ns/rml#>'
        mtquery = prefix + \
            'SELECT DISTINCT ?t ?p ?r ?rds WHERE { GRAPH <' + self.graph + '> {\n' \
            '  ?tm rml:logicalSource ?ls .\n' \
            '  ?ls rml:source <' + dsid + '> .\n' \
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
        print(mtquery)
        res, card = contactRDFSource(mtquery, self.query_endpoint)
        return res

    def get_rdfmts_by_preds(self, preds):
        query = 'SELECT DISTINCT ?rid WHERE { GRAPH <' + self.graph + '> {\n' \
                '  ?rid a <' + MT_ONTO + 'RDFMT> .\n'
        i = 0
        for p in preds:
            query += '  ?rid <' + MT_ONTO + 'hasProperty> ?mtp' + str(i) + '.\n' \
                     '  ?mtp' + str(i) + ' <' + MT_ONTO + 'predicate> <' + p + '> .\n'
            i += 1

        query += '}}'
        reslist, _ = _iterative_query(self.query_endpoint, query, limit=1000)

        results = {}
        for r in reslist:
            res = self.get_rdfmt(r['rid'])
            if len(res) > 0:
                results[r['rid']] = res

        return results

    def get_preds_mt(self, props=None):
        filter = ''
        if props is not None:
            filter = ' || '.join(['?pred=<' + p + '> ' for p in props])

        query = 'SELECT DISTINCT ?rid ?pred WHERE { GRAPH <' + self.graph + '> {\n' \
                '  ?rid a <' + MT_ONTO + 'RDFMT> .\n' \
                '  ?rid <' + MT_ONTO + 'hasProperty> ?mtp .\n' \
                '  ?mtp <' + MT_ONTO + 'predicate> ?pred .\n'
        if len(filter) > 0:
            query += '  FILTER (' + filter + ')\n'
        query += '}}'
        reslist, _ = _iterative_query(query, self.query_endpoint, limit=1000)

        results = {}
        for r in reslist:
            results.setdefault(r['pred'], []).append(r['rid'])
        results = {r: list(set(results[r])) for r in results}
        return results
