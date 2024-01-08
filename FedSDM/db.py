import os
import sqlite3
import urllib.parse as urlparse
from multiprocessing import Queue

from flask import Flask, current_app, g

from FedSDM import get_logger
from FedSDM.rdfmt.prefixes import MT_ONTO, MT_RESOURCE, XSD
from FedSDM.rdfmt.utils import contact_rdf_source, update_rdf_source

logger = get_logger('mt-update', './mt-update.log')


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

        server = self.query_endpoint.split('https://')[1] if 'https' in self.query_endpoint \
            else self.query_endpoint.split('http://')[1]
        (server, path) = server.split('/', 1)
        self.query_server = server
        self.query_path = path

        server = self.update_endpoint.split('https://')[1] if 'https' in self.update_endpoint \
            else self.update_endpoint.split('http://')[1]
        (server, path) = server.split('/', 1)
        self.update_server = server
        self.update_path = path
        self.prefixes = 'PREFIX xsd: <' + XSD + '>\n' \
                        'PREFIX mt: <' + MT_ONTO + '>\n' \
                        'PREFIX mtres: <' + MT_RESOURCE + '>\n'

        self.username = username
        self.password = password

    def query(self, query: str, output_queue: Queue = Queue(), format_: str = 'application/sparql-results+json'):
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
        format_ : str, optional
            Accepted return format to be included in the body of the request to the query endpoint.

        Returns
        -------
        (list | None, int)
            A tuple containing the query result as a list as well as the cardinality of
            the query result. The first part of the tuple is None if there was an error
            during the execution. In that case, the cardinality will be marked as -2.

        """
        query = self.prefixes + query
        params = urlparse.urlencode({'query': query, 'format': 'JSON', 'timeout': 10000000})
        headers = {'Accept': format_, 'Referer': self.query_endpoint, 'Host': self.query_server}
        return contact_rdf_source(query, self.query_endpoint, output_queue, params_=params, headers_=headers)

    def update(self, update_query: str) -> bool:
        """Executes a SPARQL UPDATE query over the update endpoint of the instance.

        Executed the given SPARQL UPDATE query over the update endpoint
        belonging to the instance of *MetadataDB*.

        Parameters
        ----------
        update_query : str
            The SPARQL UPDATE query to be executed.

        Returns
        -------
        bool
            Indicating whether executing the update was successful.

        """
        update_query = self.prefixes + update_query
        return update_rdf_source(update_query, self.update_endpoint, self.username, self.password)


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
    if 'METADATA_ENDPOINT_UPDATE' in os.environ:
        meta_endpoint_update = os.environ['METADATA_ENDPOINT_UPDATE']
    else:
        meta_endpoint_update = 'http://localhost:9000/sparql-auth'

    if 'mdb' not in g:
        g.mdb = MetadataDB(meta_endpoint, meta_endpoint_update, os.environ.get('METADATA_ENDPOINT_USER', None), os.environ.get('METADATA_ENDPOINT_PASSWORD', None))
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
