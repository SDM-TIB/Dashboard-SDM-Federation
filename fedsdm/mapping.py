# import json
# import logging
#
# from flask import (
#     Blueprint, g, render_template, Response, request
# )
#
# from fedsdm.auth import login_required
# from fedsdm.db import get_db, get_mdb
# from fedsdm.rdfmt import RDFMTMgr
# from fedsdm.rdfmt.model import *
# from fedsdm.ui.utils import get_federations
# from ontario.wrappers.flatfile import LocalFlatFileClient, CSVTSVFileClient
# from ontario.wrappers.mysql import MySQLClient
# from ontario.wrappers.neo4j.sparql2cypher import Neo4jClient
#
# bp = Blueprint('mapping', __name__, url_prefix='/mapping')
#
#
# logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
# logger = logging.getLogger()
# if not logger.handlers:
#     logger.setLevel(logging.INFO)
#     consoleHandler = logging.StreamHandler()
#     consoleHandler.setLevel(logging.INFO)
#     consoleHandler.setFormatter(logFormatter)
#     logger.addHandler(consoleHandler)
#
#
# @bp.route('/mapping')
# @login_required
# def mapping():
#     db = get_db()
#     # federations = db.execute(
#     #     'SELECT f.id, name, description, is_public, created, username, owner_id'
#     #     ' FROM federation f JOIN user u ON f.owner_id = u.id'
#     #     ' ORDER BY created DESC'
#     # ).fetchall()
#
#     if 'federations' not in g:
#         federations = get_federations(g.default_graph)
#         g.federations = federations
#
#     return render_template('mapping/index.html', federations=g.federations)
#
# #################################
# ######### Mappings ##############
# #################################
#
# @bp.route('/api/get_ds_collections')
# def api_get_ds_collections():
#     try:
#         fed = request.args["fed"]
#         ds = request.args["ds"]
#     except KeyError:
#         return Response(json.dumps({}), mimetype="application/json")
#     res = get_ds_collections(fed, ds)
#
#     return Response(json.dumps({"data": res}), mimetype="application/json")
#
#
# def get_ds_collections(fed, ds):
#     mdb = get_mdb()
#     mtmgr = RDFMTMgr(mdb.query_endpoint, mdb.update_endpoint, "dba", "dba", fed)
#     datasource = mtmgr.get_source(ds)
#
#     if len(datasource) > 0:
#         datasource = datasource[0]
#         datasource = DataSource(ds,
#                                 datasource['url'],
#                                 datasource['dstype'],
#                                 name=datasource['name'],
#                                 desc=datasource['desc'] if "desc" in datasource else "",
#                                 params=datasource['params'] if "params" in datasource else {},
#                                 keywords=datasource['keywords'] if 'keywords' in datasource else "",
#                                 version=datasource['version'] if 'version' in datasource else "",
#                                 homepage=datasource['homepage'] if 'homepage' in datasource else "",
#                                 organization=datasource['organization'] if 'organization' in datasource else "",
#                                 ontology_graph=datasource['ontology_graph'] if 'ontology_graph' in datasource else None
#                                 )
#         print(datasource.dstype)
#         if datasource.dstype == DataSourceType.NEO4J:
#             neo = init_neo4j_client(datasource)
#             return neo.list_labels()
#         elif datasource.dstype == DataSourceType.MYSQL:
#             mysql = init_mysql_client(datasource)
#             return mysql.list_tables()
#         elif datasource.dstype == DataSourceType.LOCAL_CSV or \
#                 datasource.dstype == DataSourceType.LOCAL_TSV or \
#                 datasource.dstype == DataSourceType.LOCAL_JSON or \
#                 datasource.dstype == DataSourceType.LOCAL_XML:
#             mcl = LocalFlatFileClient(datasource)
#             return mcl.list_collections()
#
#     return []
#
#
# @bp.route('/api/get_columns_names')
# def api_get_columns_names():
#     try:
#         fed = request.args["fed"]
#         ds = request.args["ds"]
#         dbname = request.args['dbname']
#         colls = request.args['collname']
#         dstype = request.args['dstype']
#     except KeyError:
#         print('request args exception ... ', request.args)
#         return Response(json.dumps({}), mimetype="application/json")
#
#     res, count = get_document(fed, ds, dbname, colls, dstype)
#     columns = []
#     if count > 0:
#         cols = res[0].keys()
#         for c in cols:
#             columns.append({'title': c, 'data': c})
#
#     return Response(json.dumps({"columns": columns, 'data': res}, indent=True), mimetype="application/json")
#
#
# @bp.route('/api/show_sample_rows',methods=['POST'])
# def api_show_sample_rows():
#     try:
#         fed = request.args["fed"]
#         ds = request.args["ds"]
#         dbname = request.args['dbname']
#         colls = request.args['collname']
#         dstype = request.args['dstype']
#     except KeyError:
#         print('request args exception ... ', request.args)
#         return Response(json.dumps({}), mimetype="application/json")
#     res, count = get_document(fed, ds, dbname, colls, dstype)
#
#     return Response(json.dumps({"data": res}, indent=True), mimetype="application/json")
#
#
# @bp.route("/api/savemapping", methods=['POST'])
# def api_savemapping():
#     try:
#         e = request.form
#         fed = request.args["fed"]
#         mapping = e['mapping']
#         prefix = e['prefix']
#     except KeyError:
#         print('request args/form exception ... ', request.args, request.form)
#         return Response(json.dumps({}), mimetype="application/json")
#
#     res = save_mapping(fed, mapping, prefix)
#
#     return Response(json.dumps({"data": res}, indent=True), mimetype="application/json")
#
#
# @bp.route("/api/get_mapping", methods=['GET'])
# def api_show_mapping():
#     try:
#         fed = request.args["fed"]
#         ds = request.args["ds"]
#         if 'dbname' in request.args:
#             dbname = request.args['dbname']
#         else:
#             dbname = None
#         if 'collname' in request.args:
#             colls = request.args['collname']
#         else:
#             colls = None
#     except KeyError:
#         print('request args/form exception ... ', request.args, request.form)
#         return Response(json.dumps({}), mimetype="application/json")
#
#     if dbname is not None and len(dbname) > 0 and colls is not None and len(colls) > 0:
#         results, subjmaps, rdftxt = get_mapping(fed, ds, iter='\"' +dbname + "/" + colls+'\"')
#     elif colls is not None:
#         results, subjmaps, rdftxt = get_mapping(fed, ds, iter="\"node." + colls + '\"')
#     else:
#         results, subjmaps, rdftxt = get_mapping(fed, ds)
#
#     return Response(json.dumps({"data": rdftxt, "subjmap": subjmaps, "other": results}, indent=True),
#                     mimetype="application/json")
#
#
# @bp.route('/api/get_label_properties')
# def api_get_label_properties():
#     try:
#         fed = request.args["fed"]
#         ds = request.args["ds"]
#         label = request.args['label']
#     except KeyError:
#         print('request args exception ... ', request.args)
#         return Response(json.dumps({}), mimetype="application/json")
#
#     res = get_neo4j_lbl_properties(fed, ds, label)
#     columns = []
#
#     for c in res:
#         columns.append({'title': c, 'data': c})
#
#     data = get_document(fed, ds, None, label, "Neo4j")
#     return Response(json.dumps({"columns": columns, 'data': data}, indent=True), mimetype="application/json")
#
#
# def get_neo4j_lbl_properties(fed, ds, label):
#     mdb = get_mdb()
#     mtmgr = RDFMTMgr(mdb.query_endpoint, mdb.update_endpoint, "dba", "dba", fed)
#     datasource = mtmgr.get_source(ds)
#     if len(datasource) > 0:
#         datasource = datasource[0]
#         datasource = DataSource(ds,
#                                 datasource['url'],
#                                 datasource['dstype'],
#                                 name=datasource['name'],
#                                 desc=datasource['desc'] if "desc" in datasource else "",
#                                 params=datasource['params'] if "params" in datasource else {},
#                                 keywords=datasource['keywords'] if 'keywords' in datasource else "",
#                                 version=datasource['version'] if 'version' in datasource else "",
#                                 homepage=datasource['homepage'] if 'homepage' in datasource else "",
#                                 organization=datasource['organization'] if 'organization' in datasource else "",
#                                 ontology_graph=datasource['ontology_graph'] if 'ontology_graph' in datasource else None
#                                 )
#     neo = init_neo4j_client(datasource)
#     return neo.list_properties(label)
#
#
# def init_mysql_client(datasource):
#     username = None
#     password = None
#     if datasource.params is not None and len(datasource.params) > 0:
#         import json
#         maps = datasource.params.split(';')
#         for m in maps:
#             params = m.split(':')
#             if len(params) > 0:
#                 if 'username' == params[0]:
#                     username = params[1]
#                 if 'password' == params[0]:
#                     password = params[1]
#     mysql = MySQLClient(datasource.url, username, password)
#     return mysql
#
#
# def init_neo4j_client(datasource):
#     username = None
#     password = None
#     if datasource.params is not None and len(datasource.params) > 0:
#         import json
#         maps = datasource.params.split(';')
#         for m in maps:
#             params = m.split(':')
#             if len(params) > 0:
#                 if 'username' == params[0]:
#                     username = params[1]
#                 if 'password' == params[0]:
#                     password = params[1]
#     print(username, password)
#     neo = Neo4jClient(datasource.url, username, password)
#     return neo
#
#
# def get_document(federation, ds, dbname, colname, dstype, limit=15):
#     mdb = get_mdb()
#     mtmgr = RDFMTMgr(mdb.query_endpoint, mdb.update_endpoint, "dba", "dba", federation)
#     datasource = mtmgr.get_source(ds)
#     if len(datasource) > 0:
#         datasource = datasource[0]
#         datasource = DataSource(ds,
#                                 datasource['url'],
#                                 datasource['dstype'],
#                                 name=datasource['name'],
#                                 desc=datasource['desc'] if "desc" in datasource else "",
#                                 params=datasource['params'] if "params" in datasource else {},
#                                 keywords=datasource['keywords'] if 'keywords' in datasource else "",
#                                 version=datasource['version'] if 'version' in datasource else "",
#                                 homepage=datasource['homepage'] if 'homepage' in datasource else "",
#                                 organization=datasource['organization'] if 'organization' in datasource else "",
#                                 ontology_graph=datasource['ontology_graph'] if 'ontology_graph' in datasource else None
#                                 )
#         if datasource.dstype == DataSourceType.LOCAL_CSV:
#             return CSVTSVFileClient(dbname).get_documents()
#         elif datasource.dstype == DataSourceType.LOCAL_TSV:
#             return CSVTSVFileClient(dbname, '\t').get_documents()
#         elif datasource.dstype == DataSourceType.NEO4J:
#             neo = init_neo4j_client(datasource)
#             return neo.get_sample(colname, limit)
#         elif datasource.dstype == DataSourceType.MYSQL:
#             mysql = init_mysql_client(datasource)
#             return mysql.get_samples(dbname, colname, limit)
#     return [], -1
#
#
# def save_mapping(federation,  mapping, prefix):
#     mdb = get_mdb()
#     insert_mapping = prefix + "INSERT DATA { GRAPH <" + federation +">{" + mapping + "}}"
#     res = mdb.update(insert_mapping)
#     return res
#
#
# def get_mapping(federation, ds, iter="?itera"):
#     mdb = get_mdb()
#     iter = "?itera"
#     query = "prefix rml: <http://semweb.mmlab.be/ns/rml#> " \
#             "prefix rr: <http://www.w3.org/ns/r2rml#>  " \
#             "SELECT DISTINCT * " \
#             " where { graph <" + federation + "> {" + \
#             " ?tm rml:logicalSource ?s . " \
#             "     ?s rml:source <" + ds + "> ; " \
#             "        rml:referenceFormulation ?form; " \
#             "        rml:iterator " + iter + " . " + \
#             " ?tm rr:subjectMap ?sm . " \
#             "     ?sm rr:template ?templ ; " \
#             "         rr:class ?smclass . " \
#             " ?tm  rr:predicateObjectMap  ?pom . " \
#             "     ?pom  rr:predicate  ?pred ; " \
#             "           rr:objectMap ?pomom . " \
#             "        ?pomom ?pomomp ?pomomo ." \
#             " }}"
#
#     res, card = mdb.query(query)
#     print(query)
#     results = {}
#     rdftxt = {}
#     subjmaps = {}
#     for r in res:
#         print(r)
#         ls = r['s']
#         form = r['form']
#         ite = iter
#         sm = r['sm']
#         tm = r['tm']
#         tmpl = r['templ']
#         smcls = r['smclass']
#
#         rdftxt.setdefault(tm, []).append(" <" + r['tm'] + "> rml:logicalSource <" + ls + "> ")
#         rdftxt.setdefault(tm, []).append(" <" + ls + "> rml:referenceFormulation <" + form + "> ")
#         rdftxt.setdefault(tm, []).append(" <" + ls + "> rml:iterator " + ("<" if '?' not in ite else "\"") + ite + (">" if '?' not in ite else "\""))
#         rdftxt.setdefault(tm, []).append(" <" + ls + "> rml:source <" + ds + "> ")
#
#         ls = {ls: "<" + ls + "> rml:source <" + ds + "> "
#                                     "; \n\t rml:referenceFormulation <" + form + "> "
#                                     "; \n\t rml:iterator " + ("<" if '?' in ite else "\"") + ite + (">" if '?' in ite else "\"")}
#         results.setdefault(r['tm'], {})["rml:logicalSource"] = ls
#
#         rdftxt.setdefault(tm, []).append(" <" + r['tm'] + "> rr:subjectMap <" + sm + "> ")
#         rdftxt.setdefault(tm, []).append(" <" + sm + "> rr:template \"" + tmpl + "\" ")
#         rdftxt.setdefault(tm, []).append(" <" + sm + "> rr:class <" + smcls + "> ")
#
#         prop = tmpl[tmpl.find('{')+1:tmpl.find('}')]
#         subjmaps[prop + "-" + sm + "(" + smcls + ")"] = tm
#         sm = {sm: "<" + sm + "> rr:template \"" + tmpl + "\";\n\t rr:class <" + smcls + ">. "}
#         results.setdefault(r['tm'], {})['rr:subjectMap'] = sm
#
#         pom = r['pom']
#         pred = r['pred']
#         pomom = r['pomom']
#         pomompred = r['pomomp']
#         pomomobj = r['pomomo']
#
#         rdftxt.setdefault(tm, []).append(" <" + r['tm'] + "> rr:predicateObjectMap <" + pom + "> ")
#         rdftxt.setdefault(tm, []).append(" <" + pom + "> rr:predicate \"" + pred + "\" ")
#         rdftxt.setdefault(tm, []).append(" <" + pom + "> rr:objectMap <" + pomom + "> ")
#         rdftxt.setdefault(tm, []).append(" <" + pomom + "> " + ("<" if 'http' in pomomobj else "\"") + pomomobj + (">" if 'http' in pomomobj else "\""))
#
#         pomom = {pomom: "<" + pomom + "> <" + pomompred + "> " + ("<" if 'http' in pomomobj else "\"") + pomomobj + (
#             ">" if 'http' in pomomobj else "\"") + ".\n"}
#         pom = {pom: {"rr:predicate": "\"" + pred + "\"",
#                      "rr:objectMap": pomom}}
#
#         results.setdefault(r['tm'], {}).setdefault("rr:predicateObjectMap", {}).update(pom)
#
#     for r in rdftxt:
#         rdftxt[r] = sorted(list(set(rdftxt[r])))
#         rdftxt[r] = ".\n".join(rdftxt[r])
#
#     return results, subjmaps, rdftxt
