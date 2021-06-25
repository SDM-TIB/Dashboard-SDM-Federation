__author__ = "Kemele M. Endris"
from enum import Enum
import urllib.parse as urlparse
from fedsdm.rdfmt import MTManager
from fedsdm.rdfmt.utils import contactRDFSource
from ontario.mapping.model.RML import *

prefixes = """
    prefix rr: <http://www.w3.org/ns/r2rml#> 
    prefix rml: <http://semweb.mmlab.be/ns/rml#> 
    prefix ql: <http://semweb.mmlab.be/ns/ql#> 
    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
    prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
    prefix rev: <http://purl.org/stuff/rev#> 
    prefix schema: <http://schema.org/> 
    prefix xsd: <http://www.w3.org/2001/XMLSchema#> 
    prefix base: <http://tib.de/ontario/mapping#> 
    prefix iasis: <http://project-iasis.eu/vocab/> 
    prefix hydra: <http://www.w3.org/ns/hydra/core#> 
"""


class RMLManager(object):

    def __init__(self, mtendpoint, federation, dsid):
        self.mtmgr = MTManager(mtendpoint, "dba", "dba", federation)
        self.datasource = self.mtmgr.get_data_source(dsid)
        self.mtendpoint = mtendpoint
        self.federation = federation
        self.subjectmaps = {}

    def loadAll(self, rdfmts=[]):
        if self.datasource is None:
            return []
        filter = ""
        if len(rdfmts) > 0:
            for mt in rdfmts:
                if len(filter)> 0:
                    filter += " || "
                filter += "( ?rdfmt=<" + mt + ">)"

        dsquery = " ?tm rml:logicalSource ?ls . " \
                  " ?ls rml:source <" + self.datasource.rid + "> .  "

        lsquery = dsquery + """                        
                OPTIONAL { ?ls rml:referenceFormulation ?refForm . }  
                OPTIONAL { ?ls rml:iterator ?iterator } 
        """
        smquery = """
            OPTIONAL {
                      ?tm rr:subject ?subject .
                      }
            OPTIONAL {                      
                      ?tm rr:subjectMap ?sm . 
                      ?sm rr:class ?rdfmt . 
                      OPTIONAL { ?sm rr:template ?smtemplate .} 
                      OPTIONAL { ?sm rr:constant ?constsubject .}
                      OPTIONAL { ?sm rml:reference ?smreference .}
                    }
            OPTIONAL {
                      ?tm rr:predicateObjectMap ?pom . 
                      OPTIONAL { ?pom  rr:predicate ?predicate .} 
                      OPTIONAL { ?pom  rr:object ?objconst .}
                      OPTIONAL { 	
                               ?pom rr:predicateMap ?pompm . 	
                               OPTIONAL {?pompm rr:constant ?constpredicate .}	
                               OPTIONAL {?pompm rr:template ?predtemplate . }	
                               OPTIONAL {?pompm rml:reference ?predreference .}	
                              }                     
                      OPTIONAL {
                               ?pom rr:objectMap ?pomobjmap .            
                               OPTIONAL { ?pomobjmap rml:reference ?pomomapreference .}
                               OPTIONAL { ?pomobjmap rr:constant ?constobject .
                                    OPTIONAL { ?pomobjmap rr:class ?pomobjmaprdfmt . }
                               }
                               OPTIONAL { ?pomobjmap rr:template ?predobjmaptemplate . 
                                        OPTIONAL { ?pomobjmap rr:class ?pomobjmaprdfmt . }
                               }
                               OPTIONAL { ?pomobjmap rr:datatype ?pomobjmapdatatype.}
                               OPTIONAL { ?pomobjmap rr:parentTriplesMap ?parentTPM . 	
                                          OPTIONAL{		
                                                 ?pomobjmap rr:joinCondition ?jc .		
                                                 ?jc rr:child ?jcchild .		
                                                 ?jc rr:parent ?jcparent .		
                                                 }		
                                         }	                              
                               }
                   }
        """
        proj = "?tm ?ls ?sm ?pom ?pompm ?pomobjmap ?refForm ?iterator " \
               " ?rdfmt ?subject ?constsubject ?smtemplate ?smreference " \
               " ?predicate ?constpredicate ?predtemplate ?predreference " \
               " ?pomomapreference ?pomobjmapdatatype ?constobject ?objconst " \
               " ?predobjmaptemplate ?parentTPM ?jcchild ?jcparent ?pomobjmaprdfmt "
        if len(filter) > 0:
            filter = " FILTER (" + filter + ") "
        query = prefixes + \
                " SELECT DISTINCT " + proj +" \n " + \
                " WHERE {\n\t GRAPH <" + self.federation + "> {\n\t\t" + \
                lsquery + \
                smquery + filter + \
                " } }"
        # print(query)
        res, card = contactRDFSource(query, self.mtendpoint)
        if res is None:
            return {}
        # import pprint

        results = {}
        for row in res:
            dictresult = {
                "ls": {},
                "subject": "",
                "rdfclass": "",
                "subjtype": "",
                "predConsts": [],
                "predRefs": [],
                "predTempMap": {},
                "predObjMap": {}
            }
            if row['tm'] not in results:
                results[row['tm']] = {row['rdfmt']: dictresult}
            else:
                if row['rdfmt'] in results[row['tm']]:
                    dictresult = results[row['tm']][row['rdfmt']]
                else:
                    results[row['tm']] = {row['rdfmt']: dictresult}

            dictresult['ls'] = {"source": self.datasource.url,
                                "refForm": row['refForm'] if 'refForm' in row else None,
                                "iterator": row['iterator'], "sourceID": self.datasource.rid}

            dictresult['subject'] = row['subject'] if 'subject' in row \
                                                   else row['smtemplate'] if 'smtemplate' in row \
                                                   else row['constsubject'] if 'constsubject' in row \
                                                   else row['smreference'] if 'smreference' in row \
                                                   else None
            if 'smtemplate' in row:
                if len(row['smtemplate'].split('{')) == 2:
                    dictresult['subjectPrefix'] = row['smtemplate'][:row['smtemplate'].find("{")]
                    dictresult['subjectCol'] = row['smtemplate'][row['smtemplate'].find("{")+1:row['smtemplate'].find("}")]
                else:
                    splits = []
                    for sp in row['smtemplate'].split('{'):
                        splits.append(sp)
                    dictresult['subjectPrefix'] = splits
                    dictresult['subjectCol'] = [sp[0] for sp in splits if len(sp) > 1]

            dictresult['subjtype'] = TermType.CONSTANT if 'subject' in row or 'constsubject' in row \
                                                   else TermType.TEMPLATE if 'smtemplate' in row \
                                                   else TermType.REFERENCE if 'smreference' in row \
                                                   else None
            dictresult['rdfclass'] = row['rdfmt']

            predconst = row['predicate'] if 'predicate' in row else row['constpredicate'] if 'constpredicate' in row else None
            predtemp = row['predtemplate'] if 'predtemplate' in row  else None
            predref = row['predreference'] if 'predreference' in row else None

            objconst = row['constobject'] if 'constobject' in row else row['objconst'] if 'objconst' in row else None
            objtemp = row['predobjmaptemplate'] if 'predobjmaptemplate' in row else None
            objref = row['pomomapreference'] if 'pomomapreference' in row else None
            objparentTerm = row['parentTPM'] if 'parentTPM' in row else None

            objdtype = row['pomobjmapdatatype'] if 'pomobjmapdatatype' in row else None
            objrdfclass = row['pomobjmaprdfmt'] if 'pomobjmaprdfmt' in row else None

            predType = TermType.CONSTANT
            pred = None
            if predconst is not None:
                dictresult['predConsts'].append(predconst)
                dictresult['predConsts'] = list(set(dictresult['predConsts']))
                predType = TermType.CONSTANT
                pred = predconst
            elif predtemp is not None:
                dictresult['predTempMap'][predtemp[:predtemp.find('{')]] = predtemp[predtemp.find('{') + 1: predtemp.find('}')]
                predType = TermType.TEMPLATE
                pred = predtemp
            elif predref is not None:
                dictresult['predRefs'].append(predref)
                dictresult['predRefs'] = list(set(dictresult['predRefs']))
                predType = TermType.REFERENCE
                pred = predref

            objtype = TermType.REFERENCE
            obj = None
            if objconst is not None:
                obj = objconst
                objtype = TermType.CONSTANT
            elif objref is not None:
                obj = objref
                objtype = TermType.REFERENCE
            elif objtemp is not None:
                obj = objtemp
                objtype = TermType.TEMPLATE
            elif objparentTerm is not None:
                obj = objparentTerm
                objtype = TermType.TRIPLEMAP

            dictresult['predObjMap'][pred] ={
                "predicate": pred,
                "predType": predType,
                "object": obj,
                "objType": objtype,
                "objrdfclass": objrdfclass,
                "objDataType": objdtype
            }

        return results

    def loadAllMappings(self):
        if self.datasource is None:
            return []

        dsquery = " ?ls rml:source <" + self.datasource.rid + "> .  "

        lsquery = dsquery + """                        
                OPTIONAL { ?ls rml:referenceFormulation ?refForm . }  
                OPTIONAL { ?ls rml:iterator ?iterator } 
        """
        smquery = """
            OPTIONAL {?tm rml:logicalSource ?ls . 
                      ?tm rr:subject ?subject .
                      }
            OPTIONAL {
                      ?tm rml:logicalSource ?ls . 
                      ?tm rr:subjectMap ?sm . 
                      ?sm rr:class ?rdfmt . 
                      OPTIONAL { ?sm rr:template ?smtemplate .} 
                      OPTIONAL { ?sm rr:constant ?constsubject .}
                      OPTIONAL { ?sm rml:reference ?smreference .}

                      ?tm rr:predicateObjectMap ?pom . 
                      OPTIONAL { ?pom  rr:predicate ?predicate .} 
                      OPTIONAL { ?pom  rr:object ?objconst .}
                      OPTIONAL { 	
                               ?pom rr:predicateMap ?pompm . 	
                               OPTIONAL {?pompm rr:constant ?constpredicate .}	
                               OPTIONAL {?pompm rr:template ?predtemplate . }	
                               OPTIONAL {?pompm rml:reference ?predreference .}	
                              }                     
                      OPTIONAL {
                               ?pom rr:objectMap ?pomobjmap .            
                               OPTIONAL { ?pomobjmap rml:reference ?pomomapreference .}
                               OPTIONAL { ?pomobjmap rr:constant ?constobject .}
                               OPTIONAL { ?pomobjmap rr:template ?predobjmaptemplate . }
                               OPTIONAL { ?pomobjmap rr:datatype ?pomobjmapdatatype.}
                               OPTIONAL { ?pomobjmap rr:parentTriplesMap ?parentTPM . 	
                                          OPTIONAL{		
                                                 ?pomobjmap rr:joinCondition ?jc .		
                                                 ?jc rr:child ?jcchild .		
                                                 ?jc rr:parent ?jcparent .		
                                                 }		
                                         }	                              
                               }

                   }
        """
        proj = "?tm ?ls ?sm ?pom ?pompm ?pomobjmap ?refForm ?iterator " \
               " ?rdfmt ?subject ?constsubject ?smtemplate ?smreference " \
               " ?predicate ?constpredicate ?predtemplate ?predreference " \
               " ?pomomapreference ?pomobjmapdatatype ?constobject ?objconst ?predobjmaptemplate ?parentTPM ?jcchild ?jcparent "
        query = prefixes + \
                " SELECT DISTINCT " + proj + " \n " + \
                " WHERE {\n\t GRAPH <" + self.federation + "> {\n\t\t" + \
                lsquery + \
                smquery + \
                " } }"
        # print(query)
        res, card = contactRDFSource(query, self.mtendpoint)
        if res is None:
            return {}
        # import pprint

        results = {}
        for row in res:
            # pprint.pprint(row)
            dictresult = {
                "ls": {},
                "subject": "",
                "rdfclass": "",
                "subjtype": "",
                "predConsts": [],
                "predRefs": [],
                "predTempMap": {},
                "predObjMap": {}
            }
            if row['tm'] not in results:
                results[row['tm']] = {row['rdfmt']: dictresult}
            else:
                if row['rdfmt'] in results[row['tm']]:
                    dictresult = results[row['tm']][row['rdfmt']]
                else:
                    results[row['tm']] = {row['rdfmt']: dictresult}

            dictresult['ls'] = {"source": self.datasource.url, "refForm": row['refForm'] if 'refForm' in row else None,
                                "iterator": row['iterator'], "sourceID": self.datasource.rid}
            # LogicalSource(row['ls'], self.datasource.rid, self.datasource.dstype, row['refForm'] if 'refForm' in row else None)

            dictresult['subject'] = row['subject'] if 'subject' in row \
                else row['smtemplate'] if 'smtemplate' in row \
                else row['constsubject'] if 'constsubject' in row \
                else row['smreference'] if 'smreference' in row \
                else None
            if 'smtemplate' in row:
                dictresult['subjectPrefix'] = row['smtemplate'][:row['smtemplate'].find("{")]
                dictresult['subjectCol'] = row['smtemplate'][
                                           row['smtemplate'].find("{") + 1:row['smtemplate'].find("}")]

            dictresult['subjtype'] = TermType.CONSTANT if 'subject' in row or 'constsubject' in row \
                else TermType.TEMPLATE if 'smtemplate' in row \
                else TermType.REFERENCE if 'smreference' in row \
                else None
            dictresult['rdfclass'] = row['rdfmt']

            predconst = row['predicate'] if 'predicate' in row else row[
                'constpredicate'] if 'constpredicate' in row else None
            predtemp = row['predtemplate'] if 'predtemplate' in row else None
            predref = row['predreference'] if 'predreference' in row else None

            objconst = row['constobject'] if 'constobject' in row else row['objconst'] if 'objconst' in row else None
            objtemp = row['predobjmaptemplate'] if 'predobjmaptemplate' in row else None
            objref = row['pomomapreference'] if 'pomomapreference' in row else None
            objparentTerm = row['parentTPM'] if 'parentTPM' in row else None

            objdtype = row['pomobjmapdatatype'] if 'pomobjmapdatatype' in row else None

            predType = TermType.CONSTANT
            pred = None
            if predconst is not None:
                dictresult['predConsts'].append(predconst)
                dictresult['predConsts'] = list(set(dictresult['predConsts']))
                predType = TermType.CONSTANT
                pred = predconst
            elif predtemp is not None:
                dictresult['predTempMap'][predtemp[:predtemp.find('{')]] = predtemp[
                                                                           predtemp.find('{') + 1: predtemp.find('}')]
                predType = TermType.TEMPLATE
                pred = predtemp
            elif predref is not None:
                dictresult['predRefs'].append(predref)
                dictresult['predRefs'] = list(set(dictresult['predRefs']))
                predType = TermType.REFERENCE
                pred = predref

            objtype = TermType.REFERENCE
            obj = None
            if objconst is not None:
                obj = objconst
                objtype = TermType.CONSTANT
            elif objref is not None:
                obj = objref
                objtype = TermType.REFERENCE
            elif objtemp is not None:
                obj = objtemp
                objtype = TermType.TEMPLATE
            elif objparentTerm is not None:
                obj = objparentTerm
                objtype = TermType.TRIPLEMAP

            dictresult['predObjMap'][pred] = {
                "predicate": pred,
                "predType": predType,
                "object": obj,
                "objType": objtype,
                "objDataType": objdtype
            }

        return results


if __name__ == "__main__":
    mapping = RMLManager("http://localhost:1300/sparql", "http://tib.eu/dsdl/ontario/g/mappingtest", "http://tib.eu/dsdl/ontario/resource/COSMIC-Methylation")
    res = mapping.loadAll()
    import pprint
    pprint.pprint(res)