import logging
import os
import sqlite3
import urllib.parse as urlparse
from http import HTTPStatus
from multiprocessing import Queue

import requests
from flask import current_app, g

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger('mtupdate')
logger.setLevel(logging.INFO)
fileHandler = logging.FileHandler("{0}/{1}.log".format('.', 'ontario-update-log'))
fileHandler.setLevel(logging.INFO)
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)


class MetadataDB:

    def __init__(self, query_endpoint, update_endpoint=None, username='', password=''):
        self.query_endpoint = query_endpoint
        if update_endpoint is not None:
            self.update_endpoint = update_endpoint
        else:
            self.update_endpoint = query_endpoint

        if 'https' in query_endpoint:
            server = query_endpoint.split("https://")[1]
        else:
            server = query_endpoint.split("http://")[1]

        (server, path) = server.split("/", 1)
        self.query_server = server
        self.query_path = path

        if 'https' in self.update_endpoint:
            server = self.update_endpoint.split("https://")[1]
        else:
            server = self.update_endpoint.split("http://")[1]

        (server, path) = server.split("/", 1)
        self.update_server = server
        self.update_path = path
        self.xsd = "http://www.w3.org/2001/XMLSchema#"
        self.owl = ""
        self.rdf = ""
        self.rdfs = "http://www.w3.org/2000/01/rdf-schema#"
        self.mtonto = "http://tib.eu/dsdl/ontario/ontology/"
        self.mtresource = "http://tib.eu/dsdl/ontario/resource/"

    def query(self, query, outputqueue=Queue(), format="application/sparql-results+json"):
        # Formats of the response.
        json = format
        # Build the query and header.
        params = urlparse.urlencode({'query': query, 'format': json, 'timeout': 10000000})
        headers = {"Accept": "*/*", "Referer": self.query_endpoint, "Host": self.query_server}

        try:
            resp = requests.get(self.query_endpoint, params=params, headers=headers)
            if resp.status_code == HTTPStatus.OK:
                res = resp.text
                reslist = []
                if format != "application/sparql-results+json":
                    return res

                try:
                    res = res.replace("false", "False")
                    res = res.replace("true", "True")
                    res = eval(res)
                except Exception as ex:
                    print("EX processing res", ex)

                if type(res) is dict:
                    if "results" in res:
                        for x in res['results']['bindings']:
                            for key, props in x.items():
                                # Handle typed-literals and language tags
                                suffix = ''
                                if props['type'] == 'typed-literal':
                                    if isinstance(props['datatype'], bytes):
                                        suffix = ''  # "^^<" + props['datatype'].decode('utf-8') + ">"
                                    else:
                                        suffix = ''  # "^^<" + props['datatype'] + ">"
                                elif "xml:lang" in props:
                                    suffix = ''  # '@' + props['xml:lang']
                                try:
                                    if isinstance(props['value'], bytes):
                                        x[key] = props['value'].decode('utf-8') + suffix
                                    else:
                                        x[key] = props['value'] + suffix
                                except:
                                    x[key] = props['value'] + suffix

                                if isinstance(x[key], bytes):
                                    x[key] = x[key].decode('utf-8')
                            outputqueue.put(x)
                            reslist.append(x)
                        # reslist = res['results']['bindings']
                        return reslist, len(reslist)
                    else:
                        outputqueue.put(res['boolean'])

                        return res['boolean'], 1

            else:
                print("Endpoint->", self.query_endpoint, resp.reason, resp.status_code, query)

        except Exception as e:
            print("Exception during query execution to", self.query_endpoint, ': ', e)

        return None, -2

    def update(self, insertquery):
        # Build the header.
        headers = {"Accept": "*/*",
                   "Referer": self.update_endpoint,
                   "Host": self.update_server,
                   "Content-type": "application/sparql-update"}

        try:
            resp = requests.post(self.update_endpoint, data=insertquery, headers=headers)
            if resp.status_code == HTTPStatus.OK or resp.status_code == HTTPStatus.ACCEPTED or resp.status_code == HTTPStatus.NO_CONTENT:
                return True
            else:
                print("Update Endpoint->", self.update_endpoint, resp.reason, resp.status_code, insertquery)
                logger.error("______/_________/________/________/______________")
                logger.error(self.update_endpoint + " - " + str(resp.reason) + " - " + str(resp.status_code))
                logger.error("ERROR On: " + insertquery)
                logger.error("________________________________________________")
        except Exception as e:
            print("Exception during update query execution to", self.update_endpoint, ': ', e, insertquery)
            logger.error("______/_________/________/________/______________")
            logger.error("Exception on update: " + self.update_endpoint + " " + str(e))
            logger.error("EXCEPTION ON: " + insertquery)
            logger.error("________________________________________________")

        return False


def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(
            current_app.config["DATABASE"],
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row

    return g.db


def get_mdb():
    import os
    if 'METADATA_ENDPOINT' in os.environ:
        metaendpoint = os.environ['METADATA_ENDPOINT']
    else:
        metaendpoint = "http://localhost:1300/sparql"

    if metaendpoint is None or metaendpoint == "":
        metaendpoint = "http://localhost:1300/sparql"
    if 'mdb' not in g:
        g.mdb = MetadataDB(metaendpoint)

        if 'DEFAULT_GRAPH' in os.environ:
            g.default_graph = os.environ['DEFAULT_GRAPH']
        else:
            g.default_graph = "http://ontario.tib.eu"
    return g.mdb


def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()


def init_db():
    db_nonexistent = False
    if not os.path.isfile(current_app.config["DATABASE"]):
        db_nonexistent = True

    db = get_db()

    if db_nonexistent:
        # database did not exist; initialize database structure
        with current_app.open_resource('schema.sql') as f:
            db.executescript(f.read().decode('utf8'))


def init_app(app):
    app.teardown_appcontext(close_db)
    init_db()
