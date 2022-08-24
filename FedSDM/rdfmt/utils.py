import urllib.parse as urlparse
from http import HTTPStatus
from multiprocessing import Queue
from typing import Tuple

import requests

from FedSDM import get_logger

logger = get_logger('mtupdate', './mt-update.log', True)


def contactRDFSource(query: str,
                     endpoint: str,
                     output_queue: Queue = Queue(),
                     format_: str = 'application/sparql-results+json') -> str | Tuple[list | str | None, int]:
    # Build the query and header.
    params = urlparse.urlencode({'query': query, 'format': format_, 'timeout': 600})
    headers = {'Accept': format_}

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
            print('Endpoint->', endpoint, resp.reason, resp.status_code, query)
    except Exception as e:
        print('Exception during query execution to', endpoint, ': ', e)

    return None, -2


def updateRDFSource(update_query: str, endpoint: str) -> bool:
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
