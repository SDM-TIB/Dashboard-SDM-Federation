__author__ = "Kemele M. Endris"
from enum import Enum
import urllib.parse as urlparse


class TripleMap(object):
    def __init__(self, rid, ls, subjectmap, predobjmaps):
        self.rid = urlparse.quote(rid, safe="/:#-")
        self.ls = ls
        self.subjectmap = subjectmap
        self.predobjmaps = predobjmaps

    def __repr__(self):
        ret = self.rid + "\n\trml:logicalSource " + str(self.ls)
        ret += "\n\trr:subjectMap " + str(self.subjectmap)
        for po in self.predobjmaps:
            ret += "\n\trr:predicateObjectMap " + str(po)

        return ret[:-1] + "."


class LogicalSource(object):
    def __init__(self, rid, source, source_type, ref_form=None, iterator=None, args={}):
        self.rid = urlparse.quote(rid, safe="/:#-")
        self.source = source
        self.source_type = source_type
        self.ref_form = ref_form
        self.iterator = iterator
        self.args = args

    def __repr__(self):
        ret = "[\n\trml:source <" + self.source + "> . "
        ret += ("\n\trml:iterator \"" + self.iterator + "\" .") if self.iterator is not None else ""
        ret += ("\n\trml:referenceFormulation " + self.ref_form + " .") if self.ref_form is not None else ""
        ret += ("\n\trr:sourcetype \"" + self.source_type.value + "\" .") if self.source_type is not None else ""
        return ret + "\n];"


class RMLTerm(object):
    def __init__(self, rid, constant=None, template=None, reference=None, datatype=None, termtype=None):
        self.rid = urlparse.quote(rid, safe="/:#-")
        self.template = template
        self.constant = constant
        self.reference = reference
        self.datatype = datatype
        self.termtype = termtype


class ConstantTerm(RMLTerm):
    def __repr__(self):
        return "[\n\t rr:constant \"" + self.constant + "\" ;"


class TemplateTerm(RMLTerm):
    def __repr__(self):
        return "\n\t rr:template \"" + self.template + "\" ;"


class ReferenceTerm(RMLTerm):
    def __repr__(self):
        ret =  "\n\trr:reference \"" + self.reference + "\" "
        if self.datatype is not None:
            ret += "; \n\trr:datatype " + self.datatype + " "
        if self.termtype is not None:
            ret += "; \n\trr:termType " + self.termtype + " "
        return ret + "."


class TermMap(object):
    def __init__(self, rid, rmlterm):
        self.rid = urlparse.quote(rid, safe="/:#-")
        self.term = rmlterm


class ObjectReferenceMap(object):
    def __init__(self, rid, subj_map, join_cond=None):
        self.rid = urlparse.quote(rid, safe="/:#-")
        self.subj_map = subj_map
        self.join_cond = join_cond

    def __repr__(self):
        return "\trr:objectMap [ rr:parentTriplesMap" + str(self.subj_map)


class PredicateMap(object):
    def __init__(self, rid, term):
        self.rid = rid
        self.term = term

    def __repr__(self):

        return "\trr:predicateMap [ " + str(self.term)


class ObjectMap(object):
    def __init__(self, rid, term):
        self.rid = rid
        self.term = term

    def __repr__(self):
        return "\trr:objectMap [" + str(self.term)


class SubjectMap(object):
    def __init__(self, rid, rdf_class, term):
        self.rid = urlparse.quote(rid, safe="/:#-")
        self.rdf_class = rdf_class
        self.term_map = term

    def __repr__(self):
        return "[\n\t rr:class " + self.rdf_class + "; " + str(self.term_map) + "];"


class PredicateObjectMap(object):
    def __init__(self, rid, predmap, objmap, obj_reference=None):
        self.rid = urlparse.quote(rid, safe="/:#-")
        self.pred_map = predmap
        self.obj_map = objmap
        self.obj_reference = obj_reference

    def __repr__(self):
        return "[\n\t\t" + str(self.pred_map) + " \n" + str(self.obj_map)

class DataSource(object):
    def __repr__(self):
        return self


class LocalDS(DataSource):
    def __init__(self, rid, path):
        self.rid = urlparse.quote(rid, safe="/:#-")
        self.path = path


class DcatDS(DataSource):
    def __init__(self, rid, name, download_url, media_type=None, format=None, byte_size=-1):
        self.rid = urlparse.quote(rid, safe="/:#-")
        self.name = name
        self.download_url = download_url
        self.media_type = media_type
        self.format = format
        self.byte_size = byte_size


class WebAPIDS(DataSource):
    def __init__(self, rid, template, mapping):
        self.rid = urlparse.quote(rid, safe="/:#-")
        self.template = template
        self.mapping = mapping


class D2RQDS(DataSource):
    def __init__(self, rid, jdbcDSN, jdbcDriver, username, passwd, query=None, version=None):
        self.rid = urlparse.quote(rid, safe="/:#-")
        self.jdbcDSN = jdbcDSN
        self.jdbcDriver = jdbcDriver
        self.username = username
        self.passwd = passwd
        self.query = query
        self.version = version


class SPARQLDS(DataSource):
    def __init__(self, rid, endpoint, supported_lang, result_formats=[], query=None):
        self.rid = urlparse.quote(rid, safe="/:#-")
        self.endpoint = endpoint
        self.supported_lang = supported_lang
        self.result_formats = result_formats
        self.query = query


class CSVOnWebDS(DataSource):
    def __init__(self, rid, url, delimiter, encoding="UTF-8", header=True):
        self.rid = urlparse.quote(rid, safe="/:#-")
        self.url = url
        self.delimiter = delimiter
        self.encoding = encoding
        self.header = header


class MongoDBDS(DataSource):
    def __init__(self, rid, url, username=None, passwd=None):
        self.rid = rid
        self.username = username
        self.password = passwd
        self.url = url


class DataSourceType(Enum):
    SPARQL_ENDPOINT = "SPARQL_Endpoint"
    MONGODB = "MongoDB"
    NEO4J = "Neo4j"
    MYSQL = "MySQL"
    SQLServer = "SQL_Server"
    SPARK = "SPARK"
    LOCAL_FILE = "LOCAL_FILE"
    REST_SERVICE = "REST_Service"