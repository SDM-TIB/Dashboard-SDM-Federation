XSD = 'http://www.w3.org/2001/XMLSchema#'
"""str: Prefix for XSD; commonly used to specify datatypes."""
RDFS = 'http://www.w3.org/2000/01/rdf-schema#'
"""str: Prefix for RDF Schema; e.g., subclasses, and domain and range of predicates"""
MT_ONTO = 'http://tib.eu/dsdl/ontario/ontology/'
"""str: Prefix for the FedSDM ontology, i.e., federations, datasources, RDF Molecule Templates, etc."""
MT_RESOURCE = 'http://tib.eu/dsdl/ontario/resource/'
"""str: Prefix for resources of FedSDM, i.e., the prefix for actual instances."""

metas = [
    'http://www.w3.org/ns/sparql-service-description',
    'http://www.openlinksw.com/schemas/virtrdf#',
    'http://www.w3.org/2000/01/rdf-schema#',
    'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
    'http://purl.org/dc/terms/Dataset',
    'http://bio2rdf.org/dataset_vocabulary:Endpoint',
    'http://www.w3.org/2002/07/owl#',
    'http://purl.org/goodrelations/',
    'http://www.ontologydesignpatterns.org/ont/d0.owl#',
    'http://www.wikidata.org/',
    'http://dbpedia.org/ontology/Wikidata:',
    'http://dbpedia.org/class/yago/',
    'http://rdfs.org/ns/void#',
    'http://www.w3.org/ns/dcat',
    'http://www.w3.org/2001/vcard-rdf/',
    'http://www.ebusiness-unibw.org/ontologies/eclass',
    'http://bio2rdf.org/bio2rdf.dataset_vocabulary:Dataset',
    'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/',
    'nodeID://'
]
"""Common prefixes of RDF classes and predicates that are not to be included in the metadata of the federation."""
