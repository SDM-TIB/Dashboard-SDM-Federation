from __future__ import annotations  # Python 3.10 still has issues with typing when using classes from the same module

import datetime
import urllib.parse as urlparse
from enum import Enum
from typing import List, Optional

from FedSDM.rdfmt.prefixes import MT_ONTO, MT_RESOURCE
from FedSDM.rdfmt.utils import contact_rdf_source


class RDFMT(object):

    def __init__(self,
                 rid: str,
                 name: str = None,
                 mt_type: int = 0,
                 sources: list = None,
                 subclass_of: list = None,
                 properties: List[MTProperty] = None,
                 desc: str = None):
        self.rid = urlparse.quote(rid, safe='/:#-')
        self.sources = [] if sources is None else sources
        self.subClassOf = [] if subclass_of is None else subclass_of
        self.properties = [] if properties is None else properties
        if desc is not None and '^^' in desc:
            desc = desc[:desc.find('^^')]
        if name is not None and '^^' in name:
            name = name[:name.find('^^')]
        self.name = name if name is not None else rid
        self.desc = desc if desc is not None else ''
        self.mt_type = mt_type

    def to_rdf(self) -> List[str]:
        data = ['<' + self.rid + '> a <' + MT_ONTO + 'RDFMT> ']
        if self.mt_type == 0:
            data.append('<' + self.rid + '> a <' + MT_ONTO + 'TypedRDFMT> ')
        for s in self.sources:
            data.extend(s.to_rdf())
            data.append('<' + self.rid + '> <' + MT_ONTO + 'source> <' + s.rid + '>')
        for s in self.subClassOf:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'subClassOf> <' + s + '> ')
        if self.desc is not None and self.desc != '':
            self.desc = self.desc.replace('"', "'").replace('\n', ' ')
            data.append('<' + self.rid + '> <' + MT_ONTO + 'desc> "' + self.desc + '" ')
        if self.name is not None and self.name != '':
            self.name = self.name.replace('"', "'").replace('\n', ' ')
            data.append('<' + self.rid + '> <' + MT_ONTO + 'name> "' + self.name + '" ')
        else:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'name> "' + self.rid + '" ')

        for r in self.properties:
            data.extend(r.to_rdf())
            data.append('<' + self.rid + '> <' + MT_ONTO + 'hasProperty> <' + r.rid + '> ')

        today = str(datetime.datetime.now())
        data.append('<' + self.rid + '>  <http://purl.org/dc/terms/created> "' + today + '"')
        data.append('<' + self.rid + '>  <http://purl.org/dc/terms/modified> "' + today + '"')
        return data


class MTProperty(object):

    def __init__(self,
                 rid: str,
                 predicate: str,
                 sources: List[DataSource],
                 cardinality: int = -1,
                 ranges: List[PropRange] = None,
                 policies: list = None,
                 label: str = None):
        self.rid = urlparse.quote(rid, safe='/:#-')
        self.predicate = urlparse.quote(predicate, safe='/:#-')
        self.sources = sources
        self.cardinality = cardinality
        self.ranges = [] if ranges is None else ranges
        self.policies = [] if policies is None else policies

        if label is not None and '^^' in label:
            label = label[:label.find('^^')]
        self.label = label if label is not None else ''

    def to_rdf(self) -> List[str]:
        data = ['<' + self.rid + '> a <' + MT_ONTO + 'MTProperty> ']
        if self.cardinality != '' and int(self.cardinality) >= 0:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'cardinality> ' + str(self.cardinality))
        for s in self.sources:
            data.extend(s.to_rdf())
            data.append('<' + self.rid + '> <' + MT_ONTO + 'propSource> <' + s.rid + '> ')

        if self.label is not None and self.label != '':
            self.label = self.label.replace('"', "'").replace('\n', ' ')
            data.append('<' + self.rid + '> <' + MT_ONTO + 'label> "' + self.label + '" ')
        if self.predicate is not None:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'predicate> <' + self.predicate + '> ')
        for r in self.ranges:
            data.extend(r.to_rdf())
            data.append('<' + self.rid + '> <' + MT_ONTO + 'linkedTo> <' + r.rid + '> ')

        for p in self.policies:
            data.extend(p.to_rdf())
            data.append('<' + self.rid + '> <' + MT_ONTO + 'policies> <' + p.rid + '> ')
        return data


class PropRange(object):

    def __init__(self,
                 rid: str,
                 prange: str,
                 source: DataSource,
                 range_type: int = 0,
                 cardinality: int = -1):
        self.rid = urlparse.quote(rid, safe='/:#-')
        self.prange = prange
        self.source = source
        self.cardinality = cardinality
        self.range_type = range_type

    def to_rdf(self) -> List[str]:
        data = ['<' + self.rid + '> a <' + MT_ONTO + 'PropRange> ',
                '<' + self.rid + '> <' + MT_ONTO + 'datasource> <' + self.source.rid + '> ',
                '<' + self.rid + '> <' + MT_ONTO + 'name> <' + self.prange + '> ']
        if self.cardinality != '' and int(self.cardinality) >= 0:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'cardinality> ' + str(self.cardinality))
        if self.range_type == 0:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'rdfmt> <' + self.prange + '> ')
        else:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'xsdtype> <' + self.prange + '> ')
        return data


class Source(object):

    def __init__(self,
                 rid: str,
                 source: DataSource,
                 cardinality: int = -1):
        self.rid = urlparse.quote(rid, safe='/:#-')
        self.source = source
        self.cardinality = cardinality

    def to_rdf(self) -> List[str]:
        data = ['<' + self.rid + '> a <' + MT_ONTO + 'Source> ',
                '<' + self.rid + '> <' + MT_ONTO + 'datasource> <' + self.source.rid + '> ']
        if self.cardinality != '' and int(self.cardinality) >= 0:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'cardinality> ' + str(self.cardinality))
        return data


class DataSource(object):

    def __init__(self,
                 rid: str,
                 url: str,
                 ds_type: str | DataSourceType,
                 name: str = None,
                 desc: str = None,
                 params: dict = None,
                 keywords: str = None,
                 homepage: str = None,
                 version: str = None,
                 organization: str = None,
                 ontology_graph: str = None,
                 triples: int = -1):
        self.rid = urlparse.quote(rid, safe='/:#-')
        self.url = url
        self.ds_type = ds_type if isinstance(ds_type, DataSourceType) else DataSourceType.from_str(ds_type)
        self.name = self.url if name is None else name.replace('"', "'")
        self.desc = desc if desc is not None else ''
        self.params = {} if params is None else params
        self.keywords = keywords if keywords is not None else ''
        self.homepage = homepage if homepage is not None else ''
        self.version = version if version is not None else ''
        self.organization = organization if organization is not None else ''
        self.triples = triples
        self.ontology_graph = ontology_graph

    def is_accessible(self) -> bool:
        ask = 'ASK {?s ?p ?o}'
        e = self.url
        referer = e
        if self.ds_type == DataSourceType.SPARQL_ENDPOINT:
            print('checking endpoint accessibility', e)
            val, c = contact_rdf_source(ask, referer)
            if c == -2:
                print(e, '-> is not accessible. Hence, will not be included in the federation!')
            if val:
                return True
            else:
                print(e, '-> is returning empty results. Hence, will not be included in the federation!')
        return False

    def to_rdf(self, update: bool = False) -> List[str]:
        data = ['<' + self.rid + '> a <' + MT_ONTO + 'DataSource> ',
                '<' + self.rid + '> <' + MT_ONTO + 'dataSourceType> <' + MT_RESOURCE + 'DatasourceType/' + str(self.ds_type.value) + '> ',
                '<' + self.rid + '> <' + MT_ONTO + 'url> "' + urlparse.quote(self.url, safe='/:') + '" ']
        if self.name is not None and self.name != '':
            self.name = self.name.replace('"', "'").replace('\n', ' ')
            data.append('<' + self.rid + '> <' + MT_ONTO + 'name> "' + self.name + '" ')
        if self.version is not None and self.version != '':
            data.append('<' + self.rid + '> <' + MT_ONTO + 'version> "' + self.version + '" ')
        if self.keywords is not None and self.keywords != '':
            data.append('<' + self.rid + '> <' + MT_ONTO + 'keywords> "' + self.keywords + '" ')
        if self.organization is not None and self.organization != '':
            data.append('<' + self.rid + '> <' + MT_ONTO + 'organization> "' + self.organization + '" ')
        if self.homepage is not None and self.homepage != '':
            data.append('<' + self.rid + '> <' + MT_ONTO + 'homepage> "' + self.homepage + '" ')
        if self.params is not None and len(self.params) > 0:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'params> "' + str(self.params) + '" ')
        if self.desc is not None and self.desc != '':
            data.append('<' + self.rid + '> <' + MT_ONTO + 'desc> "' + self.desc.replace('"', "'").replace('`', "'") + '"')
        if self.triples != '' and int(self.triples) >= 0:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'triples> ' + str(self.triples))

        today = str(datetime.datetime.now())
        if not update:
            data.append('<' + self.rid + '>  <http://purl.org/dc/terms/created> "' + today + '"')
        data.append('<' + self.rid + '>  <http://purl.org/dc/terms/modified> "' + today + '"')
        return data

    def __repr__(self) -> str:
        return '{\n' + \
               '  rid: ' + self.rid + ',\n' \
               '  url: ' + self.url + ',\n' \
               '  dstype: ' + str(self.ds_type) + ',\n' \
               '  params: ' + str(self.params) + '\n' \
               '}'


class DataSourceType(Enum):
    SPARQL_ENDPOINT = 'SPARQL_Endpoint'
    MONGODB = 'MongoDB'
    NEO4J = 'Neo4j'
    MYSQL = 'MySQL'
    SPARK_CSV = 'SPARK_CSV'
    SPARK_TSV = 'SPARK_TSV'
    SPARK_JSON = 'SPARK_JSON'
    SPARK_XML = 'SPARK_XML'
    REST_SERVICE = 'REST'
    LOCAL_CSV = 'LOCAL_CSV'
    LOCAL_TSV = 'LOCAL_TSV'
    LOCAL_JSON = 'LOCAL_JSON'
    LOCAL_XML = 'LOCAL_XML'

    @staticmethod
    def from_str(value: str) -> Optional[DataSourceType]:
        if value is None:
            return None
        try:
            if MT_RESOURCE in value:
                value = value.split(MT_RESOURCE + 'DatasourceType/')[1]
            return DataSourceType(value)
        except KeyError:
            return DataSourceType.SPARQL_ENDPOINT


class ACPolicy(object):

    def __init__(self,
                 authorized_by: AppUser,
                 operations: List[ACOperation],
                 authorized_to: AppUser,
                 valid_from: datetime = None,
                 valid_until: datetime = None,
                 desc: str = ''):
        self.authorized_by = authorized_by
        self.authorized_to = authorized_to
        self.operations = operations
        self.valid_from = valid_from
        self.valid_until = valid_until
        self.desc = desc


class ACOperation(object):

    def __init__(self, name: str, desc: str = None):
        self.name = name
        self.desc = '' if desc is None else desc


class AppUser(object):

    def __init__(self, name: str, username: str, passwd: str = None, params: dict = None):
        self.name = name
        self.username = username
        self.passwd = passwd
        self.params = {} if params is None else params
