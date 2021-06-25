
import rdflib
from fedsdm.rdfmt.utils import *
from fedsdm.rdfmt import RDFMTMgr

import networkx as nx
import datetime as dtime
from mulder.mediator.decomposition.MediatorDecomposer import MediatorDecomposer
from mulder.mediator.planner.MediatorPlanner import MediatorPlanner
from mulder.mediator.planner.MediatorPlanner import contactSource as clm
from time import time
from fedsdm.config import ConfigSimpleStore

from fedsdm.rdfmt.model import *

xsd = "http://www.w3.org/2001/XMLSchema#"
owl = ""
rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
rdfs = "http://www.w3.org/2000/01/rdf-schema#"
mtonto = "http://tib.eu/dsdl/ontario/ontology/"
mtresource = "http://tib.eu/dsdl/ontario/resource/"

sparql_endpoint = "http://localhost:1300/sparql"
update_endpoint = "http://localhost:1300/sparql"

meta = ["http://purl.org/goodrelations/",
                     "http://rdfs.org/ns/void#",
                     'http://www.w3.org/ns/dcat',
                     'http://www.w3.org/2001/vcard-rdf/',
                     'http://www.ebusiness-unibw.org/ontologies/eclass',
                     "http://bio2rdf.org/bio2rdf.dataset_vocabulary:Dataset"]
default_graph = "http://ontario.tib.eu"


def get_federations():
    query = "SELECT DISTINCT ?d WHERE {" \
            " GRAPH <" + default_graph + "> {?d a <http://tib.eu/dsdl/ontario/ontology/Federation>. } }"
    res, card = contactRDFSource(query, sparql_endpoint)
    if card > 0:
        res = [r['d'] for r in res]
        return res
    else:
        print("no federations available ...")
        return []


def create_federation(name, desc):
    prefix = 'http://ontario.tib.eu/federation/g/'
    uri = prefix + urlparse.quote(name.replace(' ', "-"), safe="/:")
    today = str(dtime.datetime.now())

    data = [
        "<" + uri + ">  a  <http://tib.eu/dsdl/ontario/ontology/Federation> ",
        "<" + uri + '>  <http://tib.eu/dsdl/ontario/ontology/name> "' + name + '"',
        "<" + uri + '>  <http://tib.eu/dsdl/ontario/ontology/desc> "' + desc + '"',
        "<" + uri + '>  <http://purl.org/dc/terms/created> "' + today + '"',
        "<" + uri + '>  <http://purl.org/dc/terms/modified> "' + today + '"'
        ]

    insertquery = "INSERT DATA { GRAPH <" + default_graph + ">{ " + " . \n".join(data) + "} }"
    res = updateRDFSource(insertquery, update_endpoint)
    creategraph = "CREATE GRAPH <" + uri + ">"
    res = updateRDFSource(creategraph, update_endpoint)
    print(res, uri)
    if res:
        return uri
    else:
        return ''


def findlinks(federation, datasource):
    mgr = RDFMTMgr(sparql_endpoint, update_endpoint, "dba", "dba", federation)
    outqueue = Queue()
    p = Process(target=mgr.create_inter_ds_links, args=(datasource, outqueue,))
    p.start()
    return {"status": 1}, outqueue


def recreatemts(federation, ds):
    mgr = RDFMTMgr(sparql_endpoint, update_endpoint, "dba", "dba", federation)
    outqueue = Queue()
    datasource = mgr.get_source(ds)
    if len(datasource) > 0:
        datasource = datasource[0]
        datasource = DataSource(ds,
                                datasource['url'],
                                datasource['dstype'],
                                name=datasource['name'],
                                desc=datasource['desc'] if "desc" in datasource else "",
                                params=datasource['params'] if "params" in datasource else {},
                                keywords=datasource['keywords'] if 'keywords' in datasource else "",
                                version=datasource['version'] if 'version' in datasource else "",
                                homepage=datasource['homepage'] if 'homepage' in datasource else "",
                                organization=datasource['organization'] if 'organization' in datasource else "",
                                ontology_graph=datasource['ontology_graph'] if 'ontology_graph' in datasource else None
                                )
        p = Process(target=mgr.create, args=(datasource, outqueue, [], True,))
        p.start()
        return {"status": 1}, outqueue
    return {"staus": -1}, None


def remove_data_source(federation, datasourceId):
    # username and password are optional
    mgr = RDFMTMgr(sparql_endpoint, update_endpoint, "dba", "dba", federation)
    delquery = "DELETE DATA { GRAPH <" + federation + "> { <" + datasourceId + "> ?p ?o} }"
    status = updateRDFSource(delquery, update_endpoint)
    return status


def add_data_source(federation, datasource):
    """
     0 - data source added but not accessible to create MTS
     1 - data source added and MTs are being created

    :param federation:
    :param datasource:
    :return:
    """

    # username and password are optional
    mgr = RDFMTMgr(sparql_endpoint, update_endpoint, "dba", "dba", federation)
    outqueue = Queue()
    print(datasource.url)
    if datasource.dstype == DataSourceType.SPARQL_ENDPOINT:
        if not datasource.isAccessible():
            data = datasource.to_rdf()
            insertquery = "INSERT DATA { GRAPH <" + federation + "> { " + " . \n".join(data) + "} }"
            updateRDFSource(insertquery, update_endpoint)
            print(datasource.url, "endpoints cannot be accessed. Please check if you write URLs properly!")
            return {"status": 0}, None
        p = Process(target=mgr.create, args=(datasource, outqueue, [], ))
        p.start()
        return {"status": 1}, outqueue
    else:
        data = datasource.to_rdf()
        insertquery = "INSERT DATA { GRAPH <" + federation + "> { " + " . \n".join(data) + "} }"
        updateRDFSource(insertquery, update_endpoint)
        return {"status": 0}, None


def get_total_datasources(graph=None):
    if graph is not None:
        query = "SELECT (COUNT(DISTINCT *) as ?count) WHERE { GRAPH <" + graph + "> { ?d a <" + mtonto + "DataSource> }}"
    else:
        query = "SELECT  (COUNT(DISTINCT *) as ?count) WHERE { ?d a <" + mtonto + "DataSource> }"
    res, card = contactRDFSource(query, sparql_endpoint)
    if card > 0:
        card = res[0]['count']
        if '^^' in card:
            card = card[:card.find("^^")]
        return card
    else:
        return 0


def get_total_rdfmts(graph=None):
    if graph is not None:
        query = "SELECT  (COUNT(DISTINCT *) as ?count) WHERE { GRAPH <" + graph + "> { ?d a <" + mtonto + "RDFMT> }}"
    else:
        query = "SELECT  (COUNT(DISTINCT *) as ?count) WHERE { ?d a <" + mtonto + "RDFMT> }"

    res, card = contactRDFSource(query, sparql_endpoint)
    if card > 0:
        card = res[0]['count']
        if '^^' in card:
            card = card[:card.find("^^")]
        return card
    else:
        return 0


def get_total_mtconns(graph=None):
    if graph is not None:
        query = "SELECT  (COUNT(DISTINCT *) as ?count) WHERE { GRAPH <" + graph + "> {" \
            "?d a <" + mtonto + "PropRange> . " \
            "?d <" + mtonto + "rdfmt> ?mt }}"
    else:
        query = "SELECT  (COUNT(DISTINCT *) as ?count) WHERE { " \
                "?d a <" + mtonto + "PropRange> . " \
                                    "?d <" + mtonto + "rdfmt> ?mt }"
    res, card = contactRDFSource(query, sparql_endpoint)
    if card > 0:
        card = res[0]['count']
        if '^^' in card:
            card = card[:card.find("^^")]
        return card
    else:
        return 0


def execute_query(graph, query, output=Queue()):
    configuration = ConfigSimpleStore(graph, sparql_endpoint, update_endpoint, "dba", 'dba123')
    #pprint.pprint(configuration.metadata)
    print("config loaded!")
    start = time()
    dc = MediatorDecomposer(query, configuration)
    quers = dc.decompose()
    print("Mediator Decomposer: \n", quers)
    logger.info(quers)
    if quers is None:
        print("Query decomposer returns None")
        return None, None, 1, 1, 1, 0

    res = []
    planner = MediatorPlanner(quers, True, clm, None, configuration)
    plan = planner.createPlan()
    print("Mediator Planner: \n", plan)
    logger.info(plan)
    plan.execute(output)

    i = 0
    r = output.get()
    variables = [p.name[1:] for p in dc.query.args]
    first = time() - start

    if r == "EOF":
        print("END of results ....")
        first = 0
    else:
        if len(variables) == 0 or (len(variables) == 1 and variables[0] == '*'):
            variables = [k for k in r.keys()]
        print(r)
        res.append(r)
        i += 1

    total = time() - start
    return variables, res, start, total, first, i


def get_area_chart_stats(graph=None):
    if graph is not None:
        query = "select distinct ?source  ?triples (count(distinct ?pred) as ?properties) (count( distinct  ?subject) as ?rdfmts) (count(distinct ?mtrange) as ?links) " \
                "      where {  graph <" + graph + ">{"
        query += """
                ?ds a <http://tib.eu/dsdl/ontario/ontology/DataSource> .
                optional {
                    ?ds <http://tib.eu/dsdl/ontario/ontology/triples> ?triples .
                }    
                ?ds <http://tib.eu/dsdl/ontario/ontology/name> ?source .   
                optional{
                   ?subject a <http://tib.eu/dsdl/ontario/ontology/RDFMT> .
                   ?subject <http://tib.eu/dsdl/ontario/ontology/source>  ?mtsource.
                   optional{
                                ?mtsource <http://tib.eu/dsdl/ontario/ontology/cardinality> ?subjectcard .
                           }
                   ?mtsource <http://tib.eu/dsdl/ontario/ontology/datasource> ?ds .
                   
                   optional{
                    ?subject <http://tib.eu/dsdl/ontario/ontology/hasProperty> ?mtp .
                    ?mtp <http://tib.eu/dsdl/ontario/ontology/predicate> ?pred .
                    optional {
                        ?mtp <http://tib.eu/dsdl/ontario/ontology/linkedTo> ?mtrange .
                        ?mtrange <http://tib.eu/dsdl/ontario/ontology/name> ?mtr .
                        ?mtr <http://tib.eu/dsdl/ontario/ontology/name> ?mt .
                        ?mtrange <http://tib.eu/dsdl/ontario/ontology/datasource> ?mtrangesource .                      
                    }
                   } 
                }
             }
             } group by ?source ?triples order by DESC(?triples) limit 15
             """
    else:
        query = """select distinct ?source  ?triples (count(distinct ?pred) as ?properties) (count( distinct  ?subject) as ?rdfmts) (count(distinct ?mtrange) as ?links) 
                      where {
                        ?ds a <http://tib.eu/dsdl/ontario/ontology/DataSource> .
                        optional {
                            ?ds <http://tib.eu/dsdl/ontario/ontology/triples> ?triples .
                        }
                        ?ds <http://tib.eu/dsdl/ontario/ontology/name>  ?source .
                        optional{
                           ?subject a <http://tib.eu/dsdl/ontario/ontology/RDFMT> .
                           ?subject <http://tib.eu/dsdl/ontario/ontology/source>  ?mtsource.                           
                           ?mtsource <http://tib.eu/dsdl/ontario/ontology/datasource> ?ds .
                            optional{
                                ?mtsource <http://tib.eu/dsdl/ontario/ontology/cardinality> ?subjectcard .
                                }
                           optional{
                                ?subject <http://tib.eu/dsdl/ontario/ontology/hasProperty> ?mtp .
                                ?mtp <http://tib.eu/dsdl/ontario/ontology/predicate> ?pred .
    
                                optional {
                                    ?mtp <http://tib.eu/dsdl/ontario/ontology/linkedTo> ?mtrange .
                                    ?mtrange <http://tib.eu/dsdl/ontario/ontology/name> ?mtr .
                                    ?mtr <http://tib.eu/dsdl/ontario/ontology/name> ?mt .
                                    ?mtrange <http://tib.eu/dsdl/ontario/ontology/datasource> ?mtrangesource .                      
                                }
                           } 
                     }
                     } group by ?source ?triples order by DESC(?triples) limit 15
                     """
    res, card = contactRDFSource(query, sparql_endpoint)
    print("card", card)
    if card > 0:
        if card == 1 and 'source' not in res[0]:
            return []
        results = []
        for r in res:
            if 'triples' in r:
                triples = r['triples']
            else:
                triples = "-1"
            triples = int(triples[:triples.find("^^")]) if "^^" in triples else int(triples)
            properties = r['properties'] if 'properties' in r else None
            properties = int(properties[:properties.find("^^")]) if properties is not None and "^^" in properties else int(properties)
            rdfmts = r['rdfmts'] if 'rdfmts' in r else None
            rdfmts = int(rdfmts[:rdfmts.find("^^")]) if rdfmts is not None and "^^" in rdfmts else int(rdfmts)
            links = r['links'] if 'links' in r else None
            links = int(links[:links.find("^^")]) if links is not None and "^^" in links else int(links)

            r = {"source": r['source'],
                 "triples": triples,
                 "properties": properties,
                 "rdfmts": rdfmts,
                 "links": links
            }
            results.append(r)

        return results
    else:
        return [{"source": None,
                 "triples": None,
                 "properties": None,
                 "rdfmts": None,
                 "links": None
            }]


def get_datasource_stats(graph=None):
    if graph is not None:
        query = "select distinct ?ds ?triples (count(?subject) as ?rdfmts) (sum(?subjectcard) as ?entities) " \
            "where {" \
             " GRAPH <" + graph + "> {" \
             "   ?source a <http://tib.eu/dsdl/ontario/ontology/DataSource> ." \
            "   ?source <http://tib.eu/dsdl/ontario/ontology/name> ?ds ." \
            "    optional{?source <http://tib.eu/dsdl/ontario/ontology/triples> ?triples . }" \
            "    optional{" \
            "      ?subject a <http://tib.eu/dsdl/ontario/ontology/RDFMT> ." \
            "      ?subject <http://tib.eu/dsdl/ontario/ontology/source>  ?mtsource." \
            "      optional {?mtsource <http://tib.eu/dsdl/ontario/ontology/cardinality> ?subjectcard .}" \
            "      ?mtsource <http://tib.eu/dsdl/ontario/ontology/datasource> ?source .       " \
            "    } " \
            "  } " \
            "   } group by ?ds ?triples "

    else:
        query = '''
                select distinct ?ds ?triples (count(?subject) as ?rdfmts) (sum(?subjectcard) as ?entities)
                where {                  
                    ?source a <http://tib.eu/dsdl/ontario/ontology/DataSource> .                        
                    ?source <http://tib.eu/dsdl/ontario/ontology/name> ?ds .  
                    optional{?source <http://tib.eu/dsdl/ontario/ontology/triples> ?triples .} 
                    optional{
                      ?subject a <http://tib.eu/dsdl/ontario/ontology/RDFMT> .
                      ?subject <http://tib.eu/dsdl/ontario/ontology/source>  ?mtsource.
                      optional{?mtsource <http://tib.eu/dsdl/ontario/ontology/cardinality> ?subjectcard .}
                      ?mtsource <http://tib.eu/dsdl/ontario/ontology/datasource> ?source .       
                    }                  
                    } group by ?ds ?triples
                '''

    print(query)
    res, card = contactRDFSource(query, sparql_endpoint)
    if card > 0:
        print(res)
        if card == 1 and 'ds' not in res[0]:
            return []
        return res
    else:
        return []


def get_rdfmt_stats(graph=None):
    if graph is not None:
        query = "SELECT distinct ?subject ?name (sum(?scard) as ?subjectcard)  (count(?pred) as ?preds)  " \
                "    WHERE {  graph <" + graph + "> {"
        query += '''          
            ?subject a <http://tib.eu/dsdl/ontario/ontology/RDFMT> .            
            ?subject <http://tib.eu/dsdl/ontario/ontology/source> ?source.                
            optional{
              ?source <http://tib.eu/dsdl/ontario/ontology/cardinality> ?scard .
            }
            optional {
              ?subject <http://tib.eu/dsdl/ontario/ontology/name> ?name .
            }
            optional{
              ?subject <http://tib.eu/dsdl/ontario/ontology/hasProperty> ?mtp .
              ?mtp <http://tib.eu/dsdl/ontario/ontology/predicate> ?pred .
            }
           
          }
        } group by ?subject ?name 

    '''
    else:
        query = '''  
                SELECT distinct ?subject ?name (sum(?scard) as ?subjectcard)  (count(?pred) as ?preds)
                WHERE {                  
                    ?subject a <http://tib.eu/dsdl/ontario/ontology/RDFMT> .                  
                    ?subject <http://tib.eu/dsdl/ontario/ontology/source> ?source.                                    
                    optional{
                      ?source <http://tib.eu/dsdl/ontario/ontology/cardinality> ?scard .
                    }
                    optional {
                      ?subject <http://tib.eu/dsdl/ontario/ontology/name> ?name .
                    }
                    optional{
                      ?subject <http://tib.eu/dsdl/ontario/ontology/hasProperty> ?mtp .
                      ?mtp <http://tib.eu/dsdl/ontario/ontology/predicate> ?pred .
                    }
                               
                } group by ?subject ?name 

            '''

        """
        optional {
                      ?mtp <http://tib.eu/dsdl/ontario/ontology/linkedTo> ?mtrange .
                      ?mtrange <http://tib.eu/dsdl/ontario/ontology/rdfmt> ?mt .
                      ?mtrange <http://tib.eu/dsdl/ontario/ontology/datasource> ?mtrangesource .  
                      optional{?mtrange <http://tib.eu/dsdl/ontario/ontology/cardinality> ?mtrangecard .}
                    }     
        
        """
    res, card = contactRDFSource(query, sparql_endpoint)
    if card > 0:
        print(res)
        if card == 1 and 'subject' not in res[0]:
            return []
        else:
            data = []
            i = 0
            # if len(res) > 1000:
            #     print("a total of ", len(res), " was found!")
            #     res = res[:1001]
            #     print("result reduced to 1000 for performance reasons")
            for r in res:
                dd = [i+1]
                nid = r['subject']
                val = [True for m in meta if m in nid]
                if True in val:
                    continue

                if 'name' in r:
                    dd.append(r['name'])
                else:
                    subj = r['subject']
                    name = subj[subj.rfind("'/") + 1:]
                    dd.append(name)

                dd.append(r['subject'])
                if 'subjectcard' in r:
                    card = r['subjectcard']
                    if '^^' in card:
                        card = card[:card.find("^^")]
                    dd.append(card)
                else:
                    dd.append(0)
                if 'preds' in r:
                    card = r['preds']
                    if '^^' in card:
                        card = card[:card.find("^^")]
                    dd.append(card)
                else:
                    dd.append(0)
                if 'links' in r:
                    card = r['links']
                    if '^^' in card:
                        card = card[:card.find("^^")]
                    dd.append(card)
                else:
                    dd.append(0)
                i += 1
                data.append(dd)
            print(data)
            return {"data": data, "draw": 1, "recordsTotal": len(res), "recordsFiltered": 100}
    else:

        return []


def get_rdfmt_details(fed, mt):
    print(fed, mt, "get_rdfmt_details")
    query = "SELECT distinct ?name ?datasource ?endpoint ?predendpoint  ?mtp ?preddatasource ?mtrdatasource ?card ?pred " \
            " ?predcard ?mtr ?linkpredpred ?mtrname ?linkpredcard  " \
            " WHERE { graph <" + fed+ ">{" \
            "  <" + mt + "> a <http://tib.eu/dsdl/ontario/ontology/RDFMT> ." \
            "  <" + mt + "> <http://tib.eu/dsdl/ontario/ontology/source> ?source. " \
            "  <" + mt + "> <http://tib.eu/dsdl/ontario/ontology/name> ?name . " \
            "  optional{" \
                         "?source <http://tib.eu/dsdl/ontario/ontology/cardinality> ?card. " \
                         "}" \
            "  ?source <http://tib.eu/dsdl/ontario/ontology/datasource> ?datasource. " \
            "  ?datasource <http://tib.eu/dsdl/ontario/ontology/url> ?endpoint ." \
            "  <"+ mt + "> <http://tib.eu/dsdl/ontario/ontology/hasProperty> ?mtp . " \
            "  ?mtp <http://tib.eu/dsdl/ontario/ontology/predicate> ?pred . " \
            "  ?mtp <http://tib.eu/dsdl/ontario/ontology/propSource> ?mtpsource." \
            "  optional{" \
                        "?mtpsource <http://tib.eu/dsdl/ontario/ontology/cardinality> ?predcard ." \
                        "} " \
            "  ?mtpsource <http://tib.eu/dsdl/ontario/ontology/datasource> ?preddatasource .  " \
            "  ?preddatasource <http://tib.eu/dsdl/ontario/ontology/url> ?predendpoint ." \
            "  optional {" \
            "        ?mtp <http://tib.eu/dsdl/ontario/ontology/linkedTo> ?mtrange ." \
            "        ?mtrange <http://tib.eu/dsdl/ontario/ontology/name> ?mtr . " \
            "        ?mtr <http://tib.eu/dsdl/ontario/ontology/name> ?mtrname . " \
            "        ?mtr <http://tib.eu/dsdl/ontario/ontology/hasProperty> ?linkmtp ." \
            "        ?linkmtp <http://tib.eu/dsdl/ontario/ontology/predicate> ?linkpredpred ." \
            "        ?linkmtp <http://tib.eu/dsdl/ontario/ontology/propSource> ?linkmtpsource. " \
            "        ?linkmtpsource <http://tib.eu/dsdl/ontario/ontology/datasource> ?mtrdatasource . " \
            "        ?mtrdatasource <http://tib.eu/dsdl/ontario/ontology/url>  ?mtrendpoint ."\
            "       optional{ " \
                        "?linkmtpsource <http://tib.eu/dsdl/ontario/ontology/cardinality> ?linkpredcard . }" \
            "    } }}  "

    print(query)
    limit = 10000
    offset = 0
    reslist = []
    while True:
        query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
        res, card = contactRDFSource(query_copy, sparql_endpoint)
        if card == -2:
            limit = limit // 2
            limit = int(limit)
            if limit < 1:
                break
            continue
        if card > 0:
            reslist.extend(res)
        if card < limit:
            break
        offset += limit
    res = reslist
    print(len(res), ' results found')
    if len(reslist) > 0:
        card = len(reslist)
        nodes = {}
        edges = []
        nodeids = {}
        nodecards = {}
        nodeswithnocard = []
        i = 0
        sources = {}
        j = 0

        for r in res:
            pprint.pprint(r)
            print("=====================================================")
            nid = r['pred']
            nlabel = r['pred']
            dssource = r['preddatasource']
            mdssource = r['datasource']
            print(nid, dssource)
            if dssource not in sources:
                sources[dssource] = j
                j += 1

            if mdssource not in sources:
                sources[mdssource] = j
                j += 1

            if mt+mdssource not in nodes:
                nodes[mt+mdssource] = {"id": mt,
                                       "label": mt,
                                       "datasource": sources[mdssource],
                                       "weight": r['card'][:r['card'].find("^^")] if 'card' in r and '^' in r['card'] else 10,
                                       "type": "root"
                                      }
                nodeids[mt + mdssource] = i
                i += 1

            if nid+dssource not in nodes:
                if 'predcard' in r:
                    weight = r['predcard']
                    if "^" in weight:
                        weight = weight[:weight.find("^^")]
                    if nid + dssource in nodeswithnocard:
                        nodes[nid + dssource]['weight'] = weight
                        nodeswithnocard.remove(nid + dssource)
                else:
                    weight = -1

                nodecards[nid + dssource] = weight
                nodes[nid+dssource] = {"id": nid + dssource,
                                       "label": nlabel,
                                       "datasource": sources[dssource],
                                       "weight": weight,
                                       "type": "square"
                                       }
                nodeids[nid+dssource] = i
                i += 1
                edges.append({"source": mt + mdssource,
                              "target": nid + dssource,
                              "weight": weight,
                              "pred": 'hasPredicate',
                              'ltype': "predicate"
                              })

            if 'mtr' in r:
                lnid = r['mtr']
                ldssource = r['mtrdatasource']
                lnlabel = r['mtr']
                print("Link: " , lnid, ldssource)
                if ldssource not in sources:
                    sources[ldssource] = j
                    j += 1
                if lnid+ldssource in nodecards:
                    lweight = nodecards[lnid+ldssource]
                else:
                    nodeswithnocard.append(lnid+ldssource)
                    lweight = -1

                if lnid + ldssource not in nodes:
                    nodes[lnid + ldssource] = {"id": lnid + ldssource,
                                               "label": lnlabel,
                                               "datasource": sources[ldssource],
                                               "weight": lweight,
                                               "type": "circle",
                                               "predicateid": nid+dssource
                                               }
                    nodeids[lnid + ldssource] = i
                    i += 1
                if "predcard" in r:
                    lcard = r['predcard']
                    if '^^' in lcard:
                        lcard = lcard[:lcard.find("^^")]
                else:
                    lcard = -1

                edges.append({"source": nid+dssource,
                              "target": lnid + ldssource,
                              "weight": lcard,
                              'ltype': "link",
                              'type': "link",
                              "pred": r['pred']
                              })

                llnid = r['linkpredpred']
                lnlabel = r['linkpredpred']
                print("linkpred: ", llnid, ldssource)
                if llnid+ldssource in nodecards:
                    lweight = nodecards[llnid+ldssource]
                else:
                    nodeswithnocard.append(llnid+ldssource)
                    lweight = -1

                if llnid + ldssource not in nodes:
                    nodes[llnid + ldssource] = {"id": llnid + ldssource,
                                               "label": lnlabel,
                                               "datasource": sources[ldssource],
                                               "weight": lweight,
                                               "type": "square"
                                               }
                    nodeids[llnid + ldssource] = i
                    i += 1
                if "linkpredcard" in r:
                    lcard = r['linkpredcard']
                    if '^^' in lcard:
                        lcard = lcard[:lcard.find("^^")]
                else:
                    lcard = -1

                edges.append({"source": lnid+ldssource,
                              "target": llnid + ldssource,
                              "weight": lcard,
                              "pred": 'hasPredicate',
                              'ltype': "predicate"
                              })

        print("total nodes", len(nodes))
        print("total edges: ", len(edges))

        sources = [{"id": v, "name": k} for k, v in sources.items()]
        print(edges)
        return {"nodes": nodes,
                "links": list(edges),
                "sources": sources
                }
    else:
        return {"nodes":[], "links":[], "sources":[]}


def get_rdfmts(graph=None):
    if graph is not None:
        query = "SELECT distinct ?subject ?name ?card  ?datasource ?pred ?mt ?mtr  ?mtrangesource ?mtrcard " \
                " WHERE {  graph <" + graph + "> {"
        query += '''                
            ?subject a <http://tib.eu/dsdl/ontario/ontology/RDFMT> .                                                 
              ?subject <http://tib.eu/dsdl/ontario/ontology/source> ?source.    
              ?source <http://tib.eu/dsdl/ontario/ontology/datasource> ?ds . 
              ?ds <http://tib.eu/dsdl/ontario/ontology/name> ?datasource.    
              optional{?source <http://tib.eu/dsdl/ontario/ontology/cardinality> ?card.}            
            optional {
              ?subject <http://tib.eu/dsdl/ontario/ontology/name> ?name .
            }
            optional{
                ?subject <http://tib.eu/dsdl/ontario/ontology/hasProperty> ?mtp .
                ?mtp <http://tib.eu/dsdl/ontario/ontology/predicate> ?pred .
           
                optional {
                    ?mtp <http://tib.eu/dsdl/ontario/ontology/linkedTo> ?mtrange .
                    ?mtrange <http://tib.eu/dsdl/ontario/ontology/rdfmt> ?mtr .
                    ?mtr <http://tib.eu/dsdl/ontario/ontology/name> ?mt .
                    ?mtrange <http://tib.eu/dsdl/ontario/ontology/datasource> ?mtds .
                    ?mtds <http://tib.eu/dsdl/ontario/ontology/name> ?mtrangesource.  
                    optional{
                            ?mtrange <http://tib.eu/dsdl/ontario/ontology/cardinality> ?mtrcard .
                    }                  
                }
            }
          }
        } 

    '''
    else:

        query = '''        
                SELECT distinct ?subject ?name ?card ?datasource ?pred ?mt ?mtr  ?mtrangesource ?mtrcard
                 WHERE {
                      ?subject a <http://tib.eu/dsdl/ontario/ontology/RDFMT> .                                                 
                      ?subject <http://tib.eu/dsdl/ontario/ontology/source> ?source.    
                      ?source <http://tib.eu/dsdl/ontario/ontology/datasource> ?ds . 
                      optional{?ds <http://tib.eu/dsdl/ontario/ontology/name> ?datasource.}    
                      optional{?source <http://tib.eu/dsdl/ontario/ontology/cardinality> ?card.}            
                      optional {
                          ?subject <http://tib.eu/dsdl/ontario/ontology/name> ?name .
                      }
                      optional{
                          ?subject <http://tib.eu/dsdl/ontario/ontology/hasProperty> ?mtp .
                          ?mtp <http://tib.eu/dsdl/ontario/ontology/predicate> ?pred .                   
                        optional {
                            ?mtp <http://tib.eu/dsdl/ontario/ontology/linkedTo> ?mtrange .
                            ?mtrange <http://tib.eu/dsdl/ontario/ontology/rdfmt> ?mtr .
                            ?mtr <http://tib.eu/dsdl/ontario/ontology/name> ?mt .
                            ?mtrange <http://tib.eu/dsdl/ontario/ontology/datasource> ?mtds .
                            ?mtds <http://tib.eu/dsdl/ontario/ontology/name> ?mtrangesource.  
                            optional{
                                    ?mtrange <http://tib.eu/dsdl/ontario/ontology/cardinality> ?mtrcard .
                            }                  
                        }
                    }
                  }

            '''
    # group by ?subject ?name  ?datasource ?pred ?mt ?mtrangesource ?mtr
    # res, card = contactSource(query, sparql_endpoint)

    limit = 100
    offset = 0
    reslist = []
    while True:
        query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
        res, card = contactRDFSource(query_copy, sparql_endpoint)
        if card == -2:
            limit = limit // 2
            limit = int(limit)
            if limit < 1:
                break
            continue
        if card > 0:
            reslist.extend(res)
        if card < limit:
            break
        offset += limit
    res = reslist
    print(len(res))
    if len(reslist) > 0:
        card = len(reslist)
        if card == 1 and 'subject' not in res[0]:
            return {"nodes": [], "links": [], "sources": []}
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
                if 'name' in r:
                    nlabel = r['name']
                else:
                    nlabel = r['subject']

                if 'datasource' in r:
                    dssource = r['datasource']
                else:
                    print("unkown source for MT: ", r['subject'])
                    dssource = "Unknown"
                if dssource not in sources:
                    sources[dssource] = j
                    j += 1

                if "/" in nlabel:
                    nlabel = nlabel[nlabel.rfind("/")+1:] # dssource[dssource.rfind("/")+1:] + ":" +
                else:
                    nlabel =  nlabel #dssource[dssource.rfind("/")+1:] + ":" +

                if 'card' in r:
                    weight = r['card']
                    if "^" in weight:
                        weight = weight[:weight.find("^^")]
                    if nid + dssource in nodeswithnocard:
                        nodes[nid+dssource]['weight'] = weight
                        nodeswithnocard.remove(nid+dssource)
                else:
                    weight = -1

                nodecards[nid+dssource] = weight

                if nid+dssource not in nodes:
                    nodes[nid+dssource] = {"id": nid + dssource,
                                           "label": nlabel,
                                           "datasource": sources[dssource],
                                           "weight": weight
                                           }
                    nodeids[nid+dssource] = i
                    i += 1

                if 'mt' in r:
                    lnid = r['mtr']
                    ldssource = r['mtrangesource']
                    lnlabel = r['mt']
                    if "/" in lnlabel:
                        lnlabel =  lnlabel[lnlabel.rfind("/") + 1:] # ldssource[ldssource.rfind("/") + 1:] + ":" +
                    else:
                        lnlabel = lnlabel # ldssource[ldssource.rfind("/") + 1:] + ":" +

                    if ldssource not in sources:
                        sources[ldssource] = j
                        j += 1
                    if lnid+ldssource in nodecards:
                        lweight = nodecards[lnid+ldssource]
                    else:
                        nodeswithnocard.append(lnid+ldssource)
                        lweight = -1

                    if lnid + ldssource not in nodes:
                        nodes[lnid + ldssource] = {"id": lnid + ldssource,
                                                   "label": lnlabel,
                                                   "datasource": sources[ldssource],
                                                   "weight": lweight
                                                   }
                        nodeids[lnid + ldssource] = i
                        i += 1
                    if "mtrcard" in r:
                        lcard = r['mtrcard']
                        if '^^' in lcard:
                            lcard = lcard[:lcard.find("^^")]
                    else:
                        lcard = -1

                    edges.append({"source": nid+dssource,
                                  "target": lnid + ldssource,
                                  "weight": lcard,
                                  # "left": False,
                                  # "right": True,
                                  "pred": r['pred']
                                  })

            print("total nodes", len(nodes))
            print("total edges: ", len(edges))

            sources = [{"id": v, "name": k} for k, v in sources.items()]
            print(sources)
            return {"nodes": nodes,
                    "links": list(edges),
                    "sources": sources
                    }
    else:
        return {"nodes":[], "links":[], "sources":[]}


def get_dataource(graph=None, dstype=None):
    print(graph)
    if graph is not None:
        query = "SELECT distinct * " \
                "WHERE { GRAPH <" + graph + "> {  "
        if dstype is None:
            query += "optional {?id <http://tib.eu/dsdl/ontario/ontology/dataSourceType> ?dstype .}"
        elif len(dstype) > 0:
            query += "?id <http://tib.eu/dsdl/ontario/ontology/dataSourceType> ?dstype ."
            filters = []
            for dt in dstype:
                filters.append(" ?dstype=<http://tib.eu/dsdl/ontario/resource/DatasourceType/" + str(dt.value) + "> ")
            query += " FILTER (" + " || ".join(filters) + ")"
            # i = 0
            # for dt in dstype:
            #     if i > 0:
            #         query += " UNION "
            #     if len(dstype) > 1:
            #         query += "{"
            #     query += " ?id <http://tib.eu/dsdl/ontario/ontology/dataSourceType> " \
            #              " <http://tib.eu/dsdl/ontario/resource/DatasourceType/" + str(dt.value) + "> . "
            #     if len(dstype) > 1:
            #         query += "}"
            #     i += 1
        else:
            query += "optional {?id <http://tib.eu/dsdl/ontario/ontology/dataSourceType> ?dstype .}"
        query += '''                  
                ?id a <http://tib.eu/dsdl/ontario/ontology/DataSource> .
                ?id <http://tib.eu/dsdl/ontario/ontology/name> ?name .
                ?id <http://tib.eu/dsdl/ontario/ontology/url> ?endpoint .                 			                               
                optional {?id <http://tib.eu/dsdl/ontario/ontology/homepage> ?homepage .}
                optional {?id <http://tib.eu/dsdl/ontario/ontology/version> ?version .}
                optional {?id <http://tib.eu/dsdl/ontario/ontology/keywords> ?keywords .}
                optional {?id <http://tib.eu/dsdl/ontario/ontology/organization> ?organization .}
            }
        } 
        '''
    else:
        query = '''
                SELECT distinct *
                WHERE {                            
                        ?id a <http://tib.eu/dsdl/ontario/ontology/DataSource> .
                        ?id <http://tib.eu/dsdl/ontario/ontology/name> ?name .
                        ?id <http://tib.eu/dsdl/ontario/ontology/url> ?endpoint . 
                        optional {?id <http://tib.eu/dsdl/ontario/ontology/dataSourceType> ?dstype .}			                               
                        optional {?id <http://tib.eu/dsdl/ontario/ontology/homepage> ?homepage .}
                        optional {?id <http://tib.eu/dsdl/ontario/ontology/version> ?version .}
                        optional {?id <http://tib.eu/dsdl/ontario/ontology/keywords> ?keywords .}
                        optional {?id <http://tib.eu/dsdl/ontario/ontology/organization> ?organization .}
                    
                } 
                '''
    res, card = contactRDFSource(query, sparql_endpoint)
    print(card)
    if card > 0:

        data = []
        for r in res:
            dd =[r['id'], r['name'], r['endpoint']]

            if 'dstype' in r:
                dst = r['dstype']
                dd.append(dst[dst.rfind('/') + 1:])
            else:
                dd.append(' ')
            if 'keywords' in r:
                dd.append(r['keywords'])
            else:
                dd.append(' ')

            if 'homepage' in r:
                dd.append(r['homepage'])
            else:
                dd.append(' ')

            if 'organization' in r :
                dd.append(r['organization'])
            else:
                dd.append(' ')

            # if 'desc' in r:
            #     dd.append(r['desc'])
            # else:
            #     dd.append(' ')
            #
            # if 'version' in r :
            #     dd.append(r['version'])
            # else:
            #     dd.append(' ')
            #
            # if 'params' in r:
            #     dd.append(r['params'])
            # else:
            #     dd.append(' ')
            data.append(dd)
        return data
    else:

        return []


def get_graph_stat(graph=None, source=None):
    if source is None:
        source = " ?name "
    else:
        source = '"' + source + '" '
    if graph is not None:
        query = "SELECT distinct ?subject  ?target " \
                "WHERE { graph <" + graph + "> {"\
                "?subject a <http://tib.eu/dsdl/ontario/ontology/RDFMT> ."\
                "  ?subject <http://tib.eu/dsdl/ontario/ontology/source> ?source. "\
                "  ?source <http://tib.eu/dsdl/ontario/ontology/datasource> ?datasource." \
                "  ?datasource <http://tib.eu/dsdl/ontario/ontology/name> "+ source +" ."\
                "optional{"\
                "    ?subject <http://tib.eu/dsdl/ontario/ontology/hasProperty> ?mtp ."\
                "    optional {"\
                "        ?mtp <http://tib.eu/dsdl/ontario/ontology/linkedTo> ?mtrange ."\
                "        ?mtrange <http://tib.eu/dsdl/ontario/ontology/rdfmt> ?target . "\
                              "?target a <http://tib.eu/dsdl/ontario/ontology/RDFMT> .  " \
                "    } }  }  } "

    else:
        query = "SELECT distinct ?subject  ?target " \
                " WHERE { " \
                    "?subject a <http://tib.eu/dsdl/ontario/ontology/RDFMT> ." \
                    "  ?subject <http://tib.eu/dsdl/ontario/ontology/source> ?source. " \
                    "  ?source <http://tib.eu/dsdl/ontario/ontology/datasource> ?datasource." \
                    "  ?datasource <http://tib.eu/dsdl/ontario/ontology/name> " + source + " ." \
                     "optional{" \
                     "    ?subject <http://tib.eu/dsdl/ontario/ontology/hasProperty> ?mtp ." \
                     "    optional {" \
                     "        ?mtp <http://tib.eu/dsdl/ontario/ontology/linkedTo> ?mtrange ." \
                     "        ?mtrange <http://tib.eu/dsdl/ontario/ontology/rdfmt> ?target . " \
                              "?target a <http://tib.eu/dsdl/ontario/ontology/RDFMT> .  " \
                     "    } }  } "
    # res, card = contactSource(query, sparql_endpoint)
    limit = 10000
    offset = 0
    reslist = []
    print(query)
    while True:
        query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
        res, card = contactRDFSource(query_copy, sparql_endpoint)
        if card == -2:
            limit = limit // 2
            limit = int(limit)
            if limit < 1:
                break
            continue
        if card > 0:
            reslist.extend(res)
        if card < limit:
            break
        offset += limit
    res = reslist
    if len(reslist) > 0:
        card = len(reslist)
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
                        edges.append((r['subject'], r['target'])) # , {'relation': r['mtp'] if 'mtp' in r else " "}
            result = compute_graph_properties(list(set(nodes)), edges)
            return result
    else:
        return []


def compute_graph_properties(nodes, edges):
    G = nx.Graph()
    print(len(nodes), len(edges))
    G.add_nodes_from(nodes)
    G.add_edges_from(edges)
    deg = dict(G.degree())
    sum_of_edges = sum(deg.values())
    avg_neig = sum_of_edges / nx.number_of_nodes(G)
    density = nx.density(G)
    n = nx.number_of_nodes(G)
    e = nx.number_of_edges(G)
    c = nx.average_clustering(G)
    cc = nx.number_connected_components(G)
    t = nx.transitivity(G)

    print('calculating ... ', density, n,e,c, cc)
    #
    # x = nx.average_node_connectivity(G)
    #
    # print("t,x,cc", t, x, cc)
    res = [
        ["Density", density],
        ["Nodes", n],
        ["Edges", e],
        ["Connected Components", cc],
        ["Avg. Clustering", c],
        ["Transitivity", t],
        # [6, "Avg. Node Connectivity", x],
        [ "Avg. Neighbours", avg_neig]
        ]
    print(res)
    return res


def save_mapping(federation,  mapping, prefix):
    insert_mapping = prefix + "INSERT DATA { GRAPH <" + federation +">{" + mapping + "}}"
    res = updateRDFSource(insert_mapping, update_endpoint)
    return res


def get_mapping(federation, ds, iter="?itera"):
    iter = "?itera"
    query = "prefix rml: <http://semweb.mmlab.be/ns/rml#> " \
            "prefix rr: <http://www.w3.org/ns/r2rml#>  " \
            "SELECT DISTINCT * " \
            " where { graph <" + federation + "> {" + \
            " ?tm rml:logicalSource ?s . " \
            "     ?s rml:source <" + ds + "> ; " \
            "        rml:referenceFormulation ?form; " \
            "        rml:iterator " + iter + " . " + \
            " ?tm rr:subjectMap ?sm . " \
            "     ?sm rr:template ?templ ; " \
            "         rr:class ?smclass . " \
            " ?tm  rr:predicateObjectMap  ?pom . " \
            "     ?pom  rr:predicate  ?pred ; " \
            "           rr:objectMap ?pomom . " \
            "        ?pomom ?pomomp ?pomomo ." \
            " }}"

    res, card = contactRDFSource(query, sparql_endpoint)
    print(query)
    results = {}
    rdftxt = {}
    subjmaps = {}
    for r in res:
        print(r)
        ls = r['s']
        form = r['form']
        ite = iter
        sm = r['sm']
        tm = r['tm']
        tmpl = r['templ']
        smcls = r['smclass']

        rdftxt.setdefault(tm, []).append(" <" + r['tm'] + "> rml:logicalSource <" + ls + "> ")
        rdftxt.setdefault(tm, []).append(" <" + ls + "> rml:referenceFormulation <" + form + "> ")
        rdftxt.setdefault(tm, []).append(" <" + ls + "> rml:iterator " + ("<" if '?' not in ite else "\"") + ite + (">" if '?' not in ite else "\""))
        rdftxt.setdefault(tm, []).append(" <" + ls + "> rml:source <" + ds + "> ")

        ls = {ls: "<" + ls + "> rml:source <" + ds + "> "
                                    "; \n\t rml:referenceFormulation <" + form + "> "
                                    "; \n\t rml:iterator " + ("<" if '?' in ite else "\"") + ite + (">" if '?' in ite else "\"")}
        results.setdefault(r['tm'], {})["rml:logicalSource"] = ls

        rdftxt.setdefault(tm, []).append(" <" + r['tm'] + "> rr:subjectMap <" + sm + "> ")
        rdftxt.setdefault(tm, []).append(" <" + sm + "> rr:template \"" + tmpl + "\" ")
        rdftxt.setdefault(tm, []).append(" <" + sm + "> rr:class <" + smcls + "> ")

        prop = tmpl[tmpl.find('{')+1:tmpl.find('}')]
        subjmaps[prop + "-" + sm + "(" + smcls + ")"] = tm
        sm = {sm: "<" + sm + "> rr:template \"" + tmpl + "\";\n\t rr:class <" + smcls + ">. "}
        results.setdefault(r['tm'], {})['rr:subjectMap'] = sm

        pom = r['pom']
        pred = r['pred']
        pomom = r['pomom']
        pomompred = r['pomomp']
        pomomobj = r['pomomo']

        rdftxt.setdefault(tm, []).append(" <" + r['tm'] + "> rr:predicateObjectMap <" + pom + "> ")
        rdftxt.setdefault(tm, []).append(" <" + pom + "> rr:predicate \"" + pred + "\" ")
        rdftxt.setdefault(tm, []).append(" <" + pom + "> rr:objectMap <" + pomom + "> ")
        rdftxt.setdefault(tm, []).append(" <" + pomom + "> " + ("<" if 'http' in pomomobj else "\"") + pomomobj + (">" if 'http' in pomomobj else "\""))

        pomom = {pomom: "<" + pomom + "> <" + pomompred + "> " + ("<" if 'http' in pomomobj else "\"") + pomomobj + (
            ">" if 'http' in pomomobj else "\"") + ".\n"}
        pom = {pom: {"rr:predicate": "\"" + pred + "\"",
                     "rr:objectMap": pomom}}

        results.setdefault(r['tm'], {}).setdefault("rr:predicateObjectMap", {}).update(pom)

    for r in rdftxt:
        rdftxt[r] = sorted(list(set(rdftxt[r])))
        rdftxt[r] = ".\n".join(rdftxt[r])

    return results, subjmaps, rdftxt