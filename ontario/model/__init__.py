__author__ = 'Kemele M. Endris'

from enum import Enum


class DataSourceType(Enum):
    SPARQL_ENDPOINT = "SPARQL_Endpoint"
    NEO4J = "Neo4j"
    MYSQL = "MySQL"
    REST_SERVICE = "REST_Service"
    LOCAL_CSV = "LOCAL_CSV"
    LOCAL_TSV = "LOCAL_TSV"
    LOCAL_JSON = "LOCAL_JSON"
    LOCAL_XML = "LOCAL_XML"
