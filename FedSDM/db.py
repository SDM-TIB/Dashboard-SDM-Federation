import os
import sqlite3
import urllib.parse as urlparse
from http import HTTPStatus
from multiprocessing import Queue

import requests
from flask import Flask, current_app, g

from FedSDM import get_logger
from FedSDM.rdfmt.prefixes import MT_ONTO, MT_RESOURCE, XSD

logger = get_logger('mtupdate', './mt-update.log')


class MetadataDB:

    def __init__(self, query_endpoint: str, update_endpoint: str = None, username: str = '', password: str = ''):
        self.query_endpoint = query_endpoint
        self.update_endpoint = update_endpoint if update_endpoint is not None else query_endpoint

        server = self.query_endpoint.split('https://')[1] if 'https' in self.query_endpoint else self.query_endpoint.split('http://')[1]
        (server, path) = server.split('/', 1)
        self.query_server = server
        self.query_path = path

        server = self.update_endpoint.split('https://')[1] if 'https' in self.update_endpoint else self.update_endpoint.split('http://')[1]
        (server, path) = server.split('/', 1)
        self.update_server = server
        self.update_path = path
        self.prefixes = 'PREFIX xsd: <' + XSD + '>\n' \
                        'PREFIX mt: <' + MT_ONTO + '>\n' \
                        'PREFIX mtres: <' + MT_RESOURCE + '>\n'

    def query(self, query: str, output_queue: Queue = Queue(), format: str = 'application/sparql-results+json'):
        # Build the query and header.
        query = self.prefixes + query
        params = urlparse.urlencode({'query': query, 'format': format, 'timeout': 10000000})
        headers = {'Accept': '*/*', 'Referer': self.query_endpoint, 'Host': self.query_server}

        try:
            resp = requests.get(self.query_endpoint, params=params, headers=headers)
            if resp.status_code == HTTPStatus.OK:
                res = resp.text
                res_list = []
                if format != 'application/sparql-results+json':
                    return res

                try:
                    res = res.replace('false', 'False')
                    res = res.replace('true', 'True')
                    res = eval(res)
                except Exception as ex:
                    print('EX processing res', ex)

                if type(res) is dict:
                    if 'results' in res:
                        for x in res['results']['bindings']:
                            for key, props in x.items():
                                # Handle typed-literals and language tags
                                suffix = ''
                                if props['type'] == 'typed-literal':
                                    if isinstance(props['datatype'], bytes):
                                        suffix = ''  # '^^<' + props['datatype'].decode('utf-8') + '>'
                                    else:
                                        suffix = ''  # '^^<' + props['datatype'] + '>'
                                elif 'xml:lang' in props:
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
                            output_queue.put(x)
                            res_list.append(x)
                        # reslist = res['results']['bindings']
                        return res_list, len(res_list)
                    else:
                        output_queue.put(res['boolean'])
                        return res['boolean'], 1
            else:
                print('Endpoint->', self.query_endpoint, resp.reason, resp.status_code, query)
        except Exception as e:
            print('Exception during query execution to', self.query_endpoint, ':', e)

        return None, -2

    def update(self, insert_query: str):
        # Build the header.
        insert_query = self.prefixes + insert_query
        headers = {'Accept': '*/*',
                   'Referer': self.update_endpoint,
                   'Host': self.update_server,
                   'Content-type': 'application/sparql-update'}

        try:
            resp = requests.post(self.update_endpoint, data=insert_query, headers=headers)
            if resp.status_code == HTTPStatus.OK or resp.status_code == HTTPStatus.ACCEPTED or resp.status_code == HTTPStatus.NO_CONTENT:
                return True
            else:
                print('Update Endpoint->', self.update_endpoint, resp.reason, resp.status_code, insert_query)
                logger.error('______/_________/________/________/______________')
                logger.error(self.update_endpoint + ' - ' + str(resp.reason) + ' - ' + str(resp.status_code))
                logger.error('ERROR ON: ' + insert_query)
                logger.error('________________________________________________')
        except Exception as e:
            print('Exception during update query execution to', self.update_endpoint, ':', e, insert_query)
            logger.error('______/_________/________/________/______________')
            logger.error('Exception on update: ' + self.update_endpoint + ' ' + str(e))
            logger.error('EXCEPTION ON: ' + insert_query)
            logger.error('________________________________________________')

        return False


def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(
            current_app.config['DATABASE'],
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row

    return g.db


def get_mdb():
    import os
    if 'METADATA_ENDPOINT' in os.environ and \
            os.environ['METADATA_ENDPOINT'] is not None and \
            os.environ['METADATA_ENDPOINT'] != '':
        meta_endpoint = os.environ['METADATA_ENDPOINT']
    else:
        meta_endpoint = 'http://localhost:9000/sparql'

    if 'mdb' not in g:
        g.mdb = MetadataDB(meta_endpoint)
        g.default_graph = os.environ['DEFAULT_GRAPH'] if 'DEFAULT_GRAPH' in os.environ else 'http://ontario.tib.eu'
    return g.mdb


def close_db(exception: Exception = None):
    db = g.pop('db', None)

    if db is not None:
        db.close()

    if exception:
        raise exception


def init_db():
    db_nonexistent = False
    if not os.path.isfile(current_app.config['DATABASE']):
        db_nonexistent = True

    db = get_db()

    if db_nonexistent:  # database did not exist; initialize database structure
        with current_app.open_resource('schema.sql') as f:
            db.executescript(f.read().decode('utf8'))


def init_app(app: Flask):
    app.teardown_appcontext(close_db)
    init_db()
