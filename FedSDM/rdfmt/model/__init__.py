import datetime
import urllib.parse as urlparse
from enum import Enum

from FedSDM.rdfmt.prefixes import MT_ONTO, MT_RESOURCE
from FedSDM.rdfmt.utils import contactRDFSource


class RDFMT(object):

    def __init__(self, rid, name, mt_type=0, sources=None, subClassOf=None, properties=None, desc=''):
        self.rid = urlparse.quote(rid, safe='/:#-')
        self.sources = [] if sources is None else sources
        self.subClassOf = [] if subClassOf is None else subClassOf
        self.properties = [] if properties is None else properties
        if desc is not None and '^^' in desc:
            desc = desc[:desc.find('^^')]
        if name is not None and '^^' in name:
            name = name[:name.find('^^')]
        self.name = name
        self.desc = desc
        self.mt_type = mt_type

    def to_rdf(self):
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

    def __init__(self, rid, predicate, sources, cardinality=-1, ranges=None, policies=None, label=''):
        self.rid = urlparse.quote(rid, safe='/:#-')
        self.predicate = urlparse.quote(predicate, safe='/:#-')
        self.sources = sources
        self.cardinality = cardinality
        self.ranges = [] if ranges is None else ranges
        self.policies = [] if policies is None else policies

        if label is not None and '^^' in label:
            label = label[:label.find('^^')]
        self.label = label

    def to_rdf(self):
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

    def __init__(self, rid, prange, source, range_type=0, cardinality=-1):
        self.rid = urlparse.quote(rid, safe='/:#-')
        self.prange = prange
        self.source = source
        self.cardinality = cardinality
        self.range_type = range_type

    def to_rdf(self):
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

    def __init__(self, rid, source, cardinality=-1):
        self.rid = urlparse.quote(rid, safe='/:#-')
        self.source = source
        self.cardinality = cardinality

    def to_rdf(self):
        data = ['<' + self.rid + '> a <' + MT_ONTO + 'Source> ',
                '<' + self.rid + '> <' + MT_ONTO + 'datasource> <' + self.source.rid + '> ']
        if self.cardinality != '' and int(self.cardinality) >= 0:
            data.append('<' + self.rid + '> <' + MT_ONTO + 'cardinality> ' + str(self.cardinality))

        return data


class DataSource(object):

    def __init__(self, rid, url, dstype, name=None, desc='', params=None, keywords='', homepage='', version='',
                 organization='', ontology_graph=None, triples=-1):
        self.rid = urlparse.quote(rid, safe='/:#-')
        self.url = url
        if isinstance(dstype, DataSourceType):
            self.dstype = dstype
        else:
            if 'SPARQL_Endpoint' in dstype:
                self.dstype = DataSourceType.SPARQL_ENDPOINT
            elif 'MongoDB' in dstype:
                self.dstype = DataSourceType.MONGODB
            elif 'Neo4j' in dstype:
                self.dstype = DataSourceType.NEO4J
            elif 'SPARK_CSV' in dstype:
                self.dstype = DataSourceType.SPARK_CSV
            elif 'SPARK_XML' in dstype:
                self.dstype = DataSourceType.SPARK_XML
            elif 'SPARK_JSON' in dstype:
                self.dstype = DataSourceType.SPARK_JSON
            elif 'SPARK_TSV' in dstype:
                self.dstype = DataSourceType.SPARK_TSV
            elif 'REST' in dstype:
                self.dstype = DataSourceType.REST_SERVICE
            elif 'LOCAL_CSV' in dstype:
                self.dstype = DataSourceType.LOCAL_CSV
            elif 'LOCAL_TSV' in dstype:
                self.dstype = DataSourceType.LOCAL_TSV
            elif 'LOCAL_JSON' in dstype:
                self.dstype = DataSourceType.LOCAL_JSON
            elif 'LOCAL_XML' in dstype:
                self.dstype = DataSourceType.LOCAL_XML
            elif 'MySQL' in dstype:
                self.dstype = DataSourceType.MYSQL
            else:
                self.dstype = DataSourceType.SPARQL_ENDPOINT

        if name is None:
            self.name = self.url
        else:
            self.name = name.replace('"', "'")

        self.desc = desc
        self.params = {} if params is None else params
        self.keywords = keywords
        self.homepage = homepage
        self.version = version
        self.organization = organization
        self.triples = triples
        self.ontology_graph = ontology_graph

    def isAccessible(self):
        ask = 'ASK {?s ?p ?o}'
        e = self.url
        referer = e
        if self.dstype == DataSourceType.SPARQL_ENDPOINT:
            print('checking endpoint accessibility', e)
            val, c = contactRDFSource(ask, referer)
            if c == -2:
                print(e, '-> is not accessible. Hence, will not be included in the federation!')
            if val:
                return True
            else:
                print(e, '-> is returning empty results. Hence, will not be included in the federation!')

        return False

    def to_rdf(self, update=False):
        data = ['<' + self.rid + '> a <' + MT_ONTO + 'DataSource> ',
                '<' + self.rid + '> <' + MT_ONTO + 'dataSourceType> <' + MT_RESOURCE + 'DatasourceType/' + str(self.dstype.value) + '> ',
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

    def __repr__(self):
        return '{' + \
               '\trid: ' + self.rid +\
               ',\turl: ' + self.url + \
               ',\tdstype: ' + str(self.dstype) + \
               ',\tparams: ' + str(self.params) + \
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
    def from_str(value: str):
        if value is None:
            return None
        try:
            return DataSourceType(value)
        except KeyError:
            return DataSourceType.SPARQL_ENDPOINT


class ACPolicy(object):

    def __init__(self, authorizedBy, operations, authorizedTo, validFrom=None, validUntil=None, desc=''):
        self.authorizedBy = authorizedBy
        self.authorizedTo = authorizedTo
        self.operations = operations
        self.validFrom = validFrom
        self.validUntil = validUntil
        self.desc = desc


class ACOperation(object):

    def __init__(self, name, desc=''):
        self.name = name
        self.desc = desc


class AppUser(object):

    def __init__(self, name, username, passwd=None, params=None):
        self.name = name
        self.username = username
        self.passwd = passwd
        self.params = {} if params is None else params
