import rdflib
from ontario.rdfmt import MTManager
from ontario.rdfmt.utils import contactRDFSource

__author__ = "Kemele M. Endris"


prefixes = """
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix rml: <http://semweb.mmlab.be/ns/rml#> .
    @prefix ql: <http://semweb.mmlab.be/ns/ql#> .
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    @prefix rev: <http://purl.org/stuff/rev#> .
    @prefix schema: <http://schema.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    @prefix base: <http://tib.de/ontario/mapping#> .
    @prefix iasis: <http://project-iasis.eu/vocab/> .
    @prefix hydra : <http://www.w3.org/ns/hydra/core#> .
"""


class ReferenceMap(object):
    def __init__(self, name, datatype="xsd:string", lang="en"):
        self.name = name
        self.datatype = datatype
        self.lang = lang


class RMLPredicate(object):
    def __init__(self, predicate, refmap, prefix=None, isconstant=False):
        self.predicate = predicate
        self.refmap = refmap
        self.prefix = prefix
        self.isconstant = isconstant

    def __repr__(self):
        return "\t" + self.predicate + " => " + str(self.refmap)


class RMLSubject(object):
    def __init__(self, id, logicalsource, subjecttemplate, subjectclass="rdfs:Class", predicates=[], iterator='$'):
        self.id = id
        self.logicalsource = logicalsource
        if iterator is None or iterator == 'None':
            iterator = '$'
        self.iterator = iterator
        self.subjecttemplate = subjecttemplate
        self.subjectclass = subjectclass
        self.predicates = predicates

    def __repr__(self):
        rep = "<" + self.id + ">  rml:source " + self.logicalsource + "; rr:class " + self.subjectclass + "; rr:iterator: " + self.iterator + "; \n "
        for p in self.predicates:
            rep += str(p) + ";\n"

        return rep[:-2] + ". "

    def __eq__(self, other):
        return self.id == other.id


class RMLMapping(object):

    prefix = "prefix rr: <http://www.w3.org/ns/r2rml#> " \
             "prefix rml: <http://semweb.mmlab.be/ns/rml#> " \
             "prefix ql: <http://semweb.mmlab.be/ns/ql#> " \
             "prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "

    def __init__(self, mapfile, subjectmaps={}):
        self.mapingfile = mapfile
        self.subjectmaps = subjectmaps

    def __repr__(self):
        rep = "Mapping file: " + self.mapingfile + " \n"
        for s in self.subjectmaps:
            rep += str(s) + "\n -------------------------------------------------\n"
        return rep

    def loadAllMappings(self):
        return self.queryMappings()

    def getMapping(self, subjectclass):

        if len(self.subjectmaps) > 0:
            subjj = self.subjectmaps[subjectclass]
            #subjj = [s for s in self.subjectmaps if s.subjectclass == subjectclass]
            return subjj
        else:
            return self.queryMappings(subjectclass)

    def queryMappings(self, subjectclass=None):
        g = rdflib.Graph()
        g.load(self.mapingfile, format='n3')

        subj = "?subjectclass"
        if subjectclass is not None:
            subj = " <" + subjectclass + "> "

        query = self.prefix + " SELECT * " \
                              " WHERE {" \
                              "?s rml:logicalSource ?source. " \
                              "?source rml:source ?sourceuri. " \
                              "?s rr:subjectMap ?smap. " \
                              "?smap rr:template ?subjtemplate. " \
                              "?smap rr:class " + subj + ". " \
                              "?s rr:predicateObjectMap ?pmap. " \
                              "?pmap rr:predicate ?predicate. " \
                              " ?pmap rr:objectMap ?pomap. " \
                              "?pomap rml:reference ?headername " \
                              " OPTIONAL{?source rml:iterator ?iterator } " \
                             " OPTIONAL{?pomap rr:datatype ?datatype } " \
                             " OPTIONAL{?pomap rr:language ?lang}" \
                              " }"
        res = g.query(query)

        for row in res:
            datatype = row.datatype
            if not datatype or len(datatype) == 0:
                datatype = "xsd:string"

            lang = row.lang
            if not lang or len(lang) == 0:
                lang = 'en'

            header = ReferenceMap(str(row.headername), str(datatype), str(lang))
            pred = RMLPredicate("<"+str(row.predicate)+">", header)

            if hasattr(row, 'subjectclass'):# and row.subjectclass:
                subjectclass = row.subjectclass
            if hasattr(row, 'iterator'):
                subject = RMLSubject(str(row.s), str(row.sourceuri), str(row.subjtemplate), "<"+str(subjectclass)+">", [pred], iterator=str(row.iterator))
            else:
                subject = RMLSubject(str(row.s), str(row.sourceuri), str(row.subjtemplate),
                                     "<" + str(subjectclass) + ">", [pred])

            if str(subjectclass) not in self.subjectmaps:
                self.subjectmaps[str(subjectclass)] = [subject]
            else:
                for sub in self.subjectmaps[str(subjectclass)]:
                    if sub.id == str(row.s):
                        sub.predicates.append(pred)

        if subj != "?subjectclass":
            if str(subjectclass) in self.subjectmaps:
                return self.subjectmaps[str(subjectclass)]
            else:
                return {}
        else:
            return self.subjectmaps


class RMLManagerX(object):

    prefix = "prefix rr: <http://www.w3.org/ns/r2rml#> " \
             "prefix rml: <http://semweb.mmlab.be/ns/rml#> " \
             "prefix ql: <http://semweb.mmlab.be/ns/ql#> " \
             "prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "

    def __init__(self, mtendpoint, federation, dsid):
        self.mtmgr = MTManager(mtendpoint, "dba", "dba", federation)
        self.datasource = self.mtmgr.get_data_source(dsid)
        self.mtendpoint = mtendpoint
        self.federation = federation
        self.subjectmaps = {}

    def __repr__(self):
        rep = "Data Source: " + self.datasource.url + " \n"
        for s in self.subjectmaps:
            rep += str(s) + "\n -------------------------------------------------\n"
        return rep

    def loadAllMappings(self):
        return self.queryMappings()

    def getMapping(self, subjectclass):

        if len(self.subjectmaps) > 0:
            subjj = self.subjectmaps[subjectclass]
            #subjj = [s for s in self.subjectmaps if s.subjectclass == subjectclass]
            return subjj
        else:
            return self.queryMappings(subjectclass)

    def queryMappings(self, subjectclass=None):

        subj = "?subjectclass"
        if subjectclass is not None:
            subj = " <" + subjectclass + "> "

        query = self.prefix + " SELECT * " \
                              " WHERE {" \
                              "?s rml:logicalSource ?source. " \
                              "?source rml:source ?sourceuri. " \
                              "?s rr:subjectMap ?smap. " \
                              "?smap rr:template ?subjtemplate. " \
                              "?smap rr:class " + subj + ". " \
                              "?s rr:predicateObjectMap ?pmap. " \
                              "?pmap rr:predicate ?predicate. " \
                              " ?pmap rr:objectMap ?pomap. " \
                              " ?pomap rml:reference ?headername " \
                              " OPTIONAL{?source rml:iterator ?iterator } " \
                             " OPTIONAL{?pomap rr:datatype ?datatype } " \
                             " OPTIONAL{?pomap rr:language ?lang}" \
                              " }"
        res, card = contactRDFSource(query, self.mtendpoint)
        if res is None:
            return {}

        for row in res:
            datatype = "xsd:string"
            if 'datatype' in row:
                datatype = row['datatype']

            lang = 'en'
            if 'lang' in row:
                lang = row['lang']

            header = ReferenceMap(str(row['headername']), str(datatype), str(lang))
            pred = RMLPredicate("<"+str(row['predicate'])+">", header)

            if 'subjectclass'in row:
                subjectclass = row['subjectclass']
            if 'iterator' in row:
                subject = RMLSubject(str(row['s']), str(row['sourceuri']), str(row['subjtemplate']), "<"+str(subjectclass)+">", [pred], iterator=str(row['iterator']))
            else:
                subject = RMLSubject(str(row['s']), str(row['sourceuri']), str(row['subjtemplate']),
                                     "<" + str(subjectclass) + ">", [pred])

            if str(subjectclass) not in self.subjectmaps:
                self.subjectmaps[str(subjectclass)] = [subject]
            else:
                for sub in self.subjectmaps[str(subjectclass)]:
                    if sub.id == str(row['s']):
                        sub.predicates.append(pred)

        if subj != "?subjectclass":
            if str(subjectclass) in self.subjectmaps:
                return self.subjectmaps[str(subjectclass)]
            else:
                return {}
        else:
            return self.subjectmaps


if __name__ == "__main__":
    mapping = RMLManager("http://node2.research.tib.eu:1300/sparql", "http://tib.eu/dsdl/ontario/g/ontariofed", "http://tib.eu/dsdl/ontario/resource/COSMIC")
    mapping.loadAllMappings()
    import pprint
    pprint.pprint(mapping.subjectmaps)