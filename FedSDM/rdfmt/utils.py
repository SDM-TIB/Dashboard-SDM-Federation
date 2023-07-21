from __future__ import annotations  # Python 3.10 still has issues with typing when using classes from the same module

import time
import urllib.parse as urlparse
from http import HTTPStatus
from multiprocessing import Queue
from typing import Tuple, TYPE_CHECKING

import requests

if TYPE_CHECKING:
    from FedSDM.rdfmt.model import DataSource
from FedSDM import get_logger

logger = get_logger('mtupdate', './mt-update.log', True)
"""Logger for this module. It logs to the file 'mt-update.log' as well as to stdout."""


def iterative_query(query: str,
                    server: str | 'DataSource',
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
    server : str | DataSource
        The URL of the SPARQL endpoint against which the query should be executed or, alternatively,
        the :class:`DataSource` instance representing the endpoint.
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


def contact_rdf_source(query: str,
                       endpoint: any,  # any really means str or FedSDM.rdfmt.model.DataSource but that causes issues
                       output_queue: Queue = Queue(),
                       format_: str = 'application/sparql-results+json') -> str | Tuple[list | str | None, int]:
    """Executes a SPARQL query over an RDF datasource.

    The provided SPARQL query is executed over the specified endpoint.
    The results can be fetched from a queue in an incremental manner
    or retrieved as a list once all results are retrieved, i.e., in
    a blocking fashion.

    Parameters
    ----------
    query : str
        The SPARQL query to be executed.
    endpoint : str | DataSource
        The URL of the SPARQL endpoint to which the query should be sent or, alternatively,
        the :class:`DataSource` instance representing the endpoint.
    output_queue : multiprocessing.Queue, optional
        The queue to use for fetching the result in an incremental manner.
        If no queue is passed, a new one will be created. However, it is
        then not possible to retrieve the results from the queue.
    format_ : str, optional
        The result format to be requested from the endpoint. If the
        format is different from the default SPARQL JSON result,
        the raw result will be returned.

    Returns
    -------
    str | (list | str | None, int)
        The query result retrieved from the endpoint when executing the provided
        SPARQL query. The raw answer will be returned if the requested format
        differs from SPARQL JSON result. Otherwise, the return value is a tuple
        containing the query result and the cardinality. The query result is a
        list for regular queries. In the case of an ASK query, the Boolean return
        value in form of a string will be returned. If an error occurred during
        the query execution, the query result will be None and the cardinality
        is set to -2 to signal the error.

    """
    # Build the query and header.
    params = urlparse.urlencode({'query': query, 'format': 'JSON', 'timeout': 600})
    headers = {'Accept': format_}

    if not isinstance(endpoint, str):  # actually means it is FedSDM.rdfmt.model.DataSource
        auth = endpoint.get_auth()
        if auth is not None:
            headers['Authorization'] = auth
        endpoint = endpoint.url

    try:
        resp = requests.get(endpoint, params=params, headers=headers)
        if resp.status_code == HTTPStatus.OK:
            res = resp.text
            res_list = []
            if format_ != 'application/sparql-results+json':
                return res

            try:
                res = res.replace('false', 'False')
                res = res.replace('true', 'True')
                res = eval(res)
            except Exception as ex:
                print('EX processing res', ex)

            if isinstance(res, dict):
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
            print('Endpoint->', endpoint, resp.reason, resp.status_code, query)
    except Exception as e:
        print('Exception during query execution to', endpoint, ': ', e)

    return None, -2


def update_rdf_source(update_query: str, endpoint: str) -> bool:
    """Updates a SPARQL endpoint using a SPARQL update query.

    The provided SPARQL endpoint is updated by executing the SPARQL update query.
    The server's response is analyzed in order to indicate the success of the update.

    Parameters
    ----------
    update_query : str
        The SPARQL update query that needs to be executed for the intended update.
    endpoint : str
        The URL of the SPARQL endpoint that should be updated.

    Returns
    -------
    bool
        A Boolean indicating the success of the update. Obviously, true
        means that the update was successful while false indicates otherwise.

    """
    headers = {'Accept': '*/*', 'Content-type': 'application/sparql-update'}
    try:
        resp = requests.post(endpoint, data=update_query, headers=headers)
        if resp.status_code == HTTPStatus.OK or \
                resp.status_code == HTTPStatus.ACCEPTED or \
                resp.status_code == HTTPStatus.NO_CONTENT:
            return True
        else:
            print('Update Endpoint->', endpoint, resp.reason, resp.status_code, update_query)
            logger.error(endpoint+' - ' + str(resp.reason) + ' - ' + str(resp.status_code))
            logger.error('ERROR ON: ' + update_query)
    except Exception as e:
        print('Exception during update query execution to', endpoint, ': ', e, update_query)
        logger.error('Exception on update: ' + endpoint + ' ' + str(e))
        logger.error('EXCEPTION ON: ' + update_query)

    return False
