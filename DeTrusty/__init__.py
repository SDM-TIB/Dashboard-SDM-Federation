import logging
import re
import time
from DeTrusty.Molecule.MTManager import ConfigFile
from DeTrusty.Decomposer.Decomposer import Decomposer
from DeTrusty.Decomposer.Planner import Planner
from DeTrusty.Wrapper.RDFWrapper import contact_source
from multiprocessing import Queue


re_https = re.compile("https?://")


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


# TODO: maybe the default config file needs to be changed appropriately
def run_query(query: str, sparql_one_dot_one: bool = False, config: ConfigFile = ConfigFile('/DeTrusty/Config/rdfmts.json')):
    start_time = time.time()
    decomposer = Decomposer(query, config, sparql_one_dot_one=sparql_one_dot_one)
    decomposed_query = decomposer.decompose()

    if decomposed_query is None:
        return {"results": {}, "error": "The query cannot be answered by the endpoints in the federation."}

    planner = Planner(decomposed_query, True, contact_source, 'RDF', config)
    plan = planner.createPlan()

    output = Queue()
    plan.execute(output)

    result = []
    r = output.get()
    card = 0
    while r != 'EOF':
        card += 1
        res = {}
        for key, value in r.items():
            res[key] = {"value": value, "type": "uri" if re_https.match(value) else "literal"}
        res['__meta__'] = {"is_verified": True}

        result.append(res)
        r = output.get()
    end_time = time.time()

    return {"head": {"vars": decomposed_query.variables()},
                    "cardinality": card,
                    "results": {"bindings": result},
                    "execution_time": end_time - start_time,
                    "output_version": "2.0"}
