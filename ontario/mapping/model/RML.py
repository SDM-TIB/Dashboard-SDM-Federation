__author__ = "Kemele M. Endris"
from enum import Enum
import urllib.parse as urlparse


class TripleMap(object):
    """
    TripleMap containse three main components:
        1) logical source
        2) subjectmap
        3) predicateobjectmap

        Logical source: sourceID, type, feferenceForm, iterator
        SubjectMap: subject, subjType, rdfclass
        PredicateObjectMap: predicateMap, predtype, objectmap
            ObjectMap: object, objtype, datatype, childcolumn, parentcolumn

    """
    def __init__(self, rid, rdfclass, lsource, subject, subjtype, predobjmaps):
        self.rid = urlparse.quote(rid, safe="/:#-")
        self.lsource = lsource
        self.subject = subject
        self.rdfclass = rdfclass
        self.subjType = subjtype
        self.predobjmaps = predobjmaps

    def __repr__(self):
        ret = self.rid + "\n\trml:logicalSource " + str(self.lsource)
        ret += "\n\trr:subjectMap " + str(self.subject)
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


class TermType(Enum):
    TEMPLATE = "template"
    CONSTANT = "constant"
    REFERENCE = "reference"
    TRIPLEMAP = "triplemap"

