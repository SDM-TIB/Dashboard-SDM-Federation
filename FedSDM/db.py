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
    """Provides an abstract way to query and update the metadata knowledge graph.

    The *MetadataDB* is a knowledge graph containing the information about the
    available federations, data sources, and their metadata. This class provides
    an abstract way to query and update said knowledge graph.

    """

    def __init__(self, query_endpoint: str, update_endpoint: str = None, username: str = '', password: str = ''):
        """Creates a new *MetadataDB* object.

        The *MetadataDB* object can be used to query and update a specific instance
        of a knowledge graph containing the metadata for FedSDM.

        Parameters
        ----------
        query_endpoint : str
            The URL used for querying the knowledge graph containing the metadata.
        update_endpoint : str, optional
            The URL used for updating the knowledge graph containing the metadata.
            If None, the query endpoint will be used for updates as well.
        username : str, optional
            The username required to authenticate for updating the endpoint.
            Due to the implementation of Virtuoso, this is currently unused.
        password : str, optional
            The password required to authenticate for updating the endpoint.
            Due to the implementation of Virtuoso, this is currently unused.

        """
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
        """Executes a SPARQL query over the query endpoint of the instance.

        Executes the given SPARQL query over the query endpoint belonging
        to the instance of *MetadataDB*.

        Parameters
        ----------
        query : str
            The SPARQL query to be executed.
        output_queue :  multiprocessing.Queue, optional
            If an output queue is given, results can be consumed from the queue
            as soon as they are retrieved. Otherwise, the result can only be
            used in a blocking fashion from the return value.
        format : str, optional
            Accepted return format to be included in the body of the request to the query endpoint.

        Returns
        -------
        (list | None, int)
            A tuple containing the query result as a list as well as the cardinality of
            the query result. The first part of the tuple is None if there was an error
            during the execution. In that case, the cardinality will be marked as -2.

        """
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

    def update(self, insert_query: str) -> bool:
        """Executes a SPARQL UPDATE query over the update endpoint of the instance.

        Executed the given SPARQL UPDATE query over the update endpoint
        belonging to the instance of *MetadataDB*.

        Parameters
        ----------
        insert_query : str
            The SPARQL UPDATE query to be executed.

        Returns
        -------
        bool
            Indicating whether executing the update was successful.

        """
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


def get_db() -> sqlite3.Connection:
    """Gets the connection to the relational database with the user information.

    If the connection has not yet been set up for the running instance
    of FedSDM, the database connection will be configured. Otherwise,
    the already existing connection is returned.

    Returns
    -------
    sqlite3.Connection
        The connection to the relational database storing the user information.

    """
    if 'db' not in g:
        g.db = sqlite3.connect(
            current_app.config['DATABASE'],
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row

    return g.db


def get_mdb() -> MetadataDB:
    """Gets the database holding all the metadata.

    The metadata DB holds information about the available federations,
    data sources, RDF Molecule Templates, etc. If the metadata DB has
    not yet been configured for the running instance of FedSDM, it
    will be configured when this method is called. Otherwise, the
    existing :class:`MetadataDB` object is returned.

    Returns
    -------
    MetadataDB
        The :class:`MetadataDB` object used to connect to the metadata DB.

    """
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
    """Closes the connection to the relational database.

    If a connection to the relational database is still open,
    it will be closed by calling this method.
    This method is called during the app teardown context.

    Parameters
    ----------
    exception : Exception
        Any exception that occurred during the teardown of the app.

    Raises
    ------
    Exception
        The exception that was passed to the method is simply raised again.

    """
    db = g.pop('db', None)

    if db is not None:
        db.close()

    if exception:
        raise exception


def init_db():
    """Initialized the user database.

    If the database file does not exist, the schema of the user database
    will be created and saved in the app's database file.

    """
    db_nonexistent = False
    if not os.path.isfile(current_app.config['DATABASE']):
        db_nonexistent = True

    db = get_db()

    if db_nonexistent:  # database did not exist; initialize database structure
        with current_app.open_resource('schema.sql') as f:
            db.executescript(f.read().decode('utf8'))


def init_app(app: Flask):
    """Initialized the Flask application of FedSDM.

    The method closing the connection to the relational database is added to
    the teardown context of the application and the database is initialized.

    Parameters
    ----------
    flask.Flask
        The Flask application of FedSDM for which the database will be configured.

    """
    app.teardown_appcontext(close_db)
    init_db()
