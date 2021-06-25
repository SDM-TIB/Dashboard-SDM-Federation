
import sys
import os
from flask import Flask, abort, request, make_response, Blueprint
import json
from SPARQLWrapper import SPARQLWrapper, JSON
import logging

bp = Blueprint('lcexplorer', __name__, url_prefix='/lcexplorer')


LIMIT = 10

KG="http://node2.research.tib.eu:19191/sparql"
# KG = os.environ["IASISKG_ENDPOINT"]
# KG="http://10.114.113.14:7171/sparql"
EMPTY_JSON = "{}"

app = Flask(__name__)

############################
#
# Query constants
#
############################


OPENP = "("
CLOSEP = ")"
FILTER = 'FILTER'
CLOSE = "}"
NEWLINE = "\n"
DOT = '.'
SPC = " "
COMA = ","
IN = "in"
VAR = "?"
ANNT = "<http://project-iasis.eu/Annotation/"
GT = ">"
CLOSE_RANK = "} GROUP BY ?idf ORDER BY DESC (?score)"

QUERY_PUBLICATIONS_RANKED = """
SELECT DISTINCT  ?idf COUNT(?c) as ?score SAMPLE(?title) as ?title SAMPLE(?author) as ?author SAMPLE(?journal) as ?journal SAMPLE(?year) as ?year  WHERE {
                            ?s a <http://project-iasis.eu/vocab/Publication>.
                            ?s <http://project-iasis.eu/vocab/title> ?title.
                            ?s <http://project-iasis.eu/vocab/author> ?author.
                            ?s <http://project-iasis.eu/vocab/journal> ?journal.
                            ?s <http://project-iasis.eu/vocab/year> ?year.
                            ?o <http://project-iasis.eu/vocab/annotates>  ?s.
                            ?o <http://project-iasis.eu/vocab/hasAnnotation> ?a.
                            ?o <http://project-iasis.eu/vocab/confidence>  ?c.
                            ?o a <http://project-iasis.eu/vocab/HAS_TOPIC> .
                            ?s <http://project-iasis.eu/vocab/pubmedID> ?idf.
"""

QUERY_DISORDERS_TO_DRUGS = """
SELECT DISTINCT ?drug ?drugLabel WHERE {  ?drug a <http://project-iasis.eu/vocab/Drug>.
                            ?indication <http://project-iasis.eu/vocab/hasIndication>  ?drug.     
                            ?drug <http://project-iasis.eu/vocab/drugLabel> ?drugLabel.
"""

QUERY_BIOMARKERS_TO_DRUGS = """
SELECT DISTINCT ?drug ?drugLabel WHERE {  ?biomarker <http://project-iasis.eu/vocab/hasIndication> ?drug .
                               ?biomarker a <http://project-iasis.eu/vocab/Biomarker>. 
                               ?drug <http://project-iasis.eu/vocab/drugLabel> ?drugLabel.

"""

QUERY_DRUGS_TO_SIDEEFFECTS = """
SELECT DISTINCT ?drugLabel ?sideEffectLabel WHERE {  ?drug a <http://project-iasis.eu/vocab/Drug>.
                                           ?drug <http://project-iasis.eu/vocab/drugLabel> ?drugLabel.
                            ?drug <http://project-iasis.eu/vocab/drug_isRelatedTo_dse>  ?drugSideEffect.
                            ?sideEffect <http://project-iasis.eu/vocab/sideEffect_isRelatedTo_dse> ?drugSideEffect.
                            ?sideEffect <http://project-iasis.eu/vocab/sideEffectLabel> ?sideEffectLabel.
"""

QUERY_DRUGS_TO_DRUGS_INTERACTIONS = """
SELECT DISTINCT ?effectorDrugLabel ?affectdDrugLabel ?effectLabel ?impactLabel WHERE {  
                                           ?interaction <http://project-iasis.eu/vocab/affects> ?effectorDrug.
                                           ?interaction <http://project-iasis.eu/vocab/isAffected> ?affectdDrug.
                                           ?effectorDrug <http://project-iasis.eu/vocab/drugLabel> ?effectorDrugLabel.
                                           ?affectdDrug <http://project-iasis.eu/vocab/drugLabel> ?affectdDrugLabel.
                                           ?interaction <http://project-iasis.eu/vocab/hasEffect> ?effect.
                                           ?effect <http://project-iasis.eu/vocab/effectLabel> ?effectLabel.
                                           ?interaction <http://project-iasis.eu/vocab/hasImpact> ?impact.
                                           ?impact <http://project-iasis.eu/vocab/impactLabel> ?impactLabel.
"""

QUERY_CUI_TO_DRUGS = """
SELECT DISTINCT ?drug ?drugBankID WHERE {
               ?drugCUI <http://project-iasis.eu/vocab/drugCuiID> ?cui.
               ?drugCUI <http://www.w3.org/2002/07/owl#sameAs> ?drug.
               ?drug <http://project-iasis.eu/vocab/drugBankID> ?drugBankID
"""


############################
#
# Query generation
#
############################


def execute_query(query):
    sparql_ins = SPARQLWrapper(KG)
    sparql_ins.setQuery(query)
    sparql_ins.setReturnFormat(JSON)
    return sparql_ins.query().convert()['results']['bindings']


def build_query_with_cuis(cuis):
    query = QUERY_PUBLICATIONS_RANKED
    query += "FILTER(?a in ("
    for cui in cuis:
        query += "<http://project-iasis.eu/Annotation/" + cui + ">,"
    query = query[:-1]
    query += "))} GROUP BY ?idf ORDER BY DESC (?score) LIMIT " + str(LIMIT)
    return query


############################
#
# Processing results
#
############################

def get_publications_results(qresults):
    result = {}
    for cur_result in qresults:
        pub_dicc = {}
        pub_id = cur_result["id"]["value"]
        pub_dicc["title"] = cur_result["title"]["value"]
        pub_dicc["author"] = cur_result["author"]["value"]
        pub_dicc["year"] = cur_result["year"]["value"]
        pub_dicc["journal"] = cur_result["journal"]["value"]
        result[pub_id] = pub_dicc
    return result


def get_publications_ranked_result(qresults, n, limit):
    results = []
    cont = 0
    for cur_result in qresults:
        if (cont >= limit):
            return results
        pub_id = cur_result["idf"]["value"]
        pub_score = cur_result["score"]["value"]
        results.append((pub_id, int(pub_score) / n))
        cont += 1
    return results


def get_publications_ranked_data(lcuis, limit):
    query = build_query_with_cuis(lcuis)
    qresults = execute_query(query)
    return qresults


def disorder2drugs_query(disorders):
    query = QUERY_DISORDERS_TO_DRUGS
    query += "FILTER(?indication in ("
    for cui in disorders:
        query += "<http://project-iasis.eu/Disorder/" + cui + ">,"
    query = query[:-1]
    query += "))}"
    qresults = execute_query(query)
    qresults = [(item['drug']['value'], item['drugLabel']['value']) for item in qresults]

    return qresults


def biomarkers2drugs_query(biomarkers):
    query = QUERY_BIOMARKERS_TO_DRUGS
    query += "FILTER(?biomarker in ("
    for cui in biomarkers:
        query += "<http://project-iasis.eu/Biomarker/" + cui + ">,"
    query = query[:-1]
    query += "))}"
    qresults = execute_query(query)
    qresults = [(item['drug']['value'], item['drugLabel']['value']) for item in qresults]
    return qresults


def drug2sideEffect_query(drugs):
    query = QUERY_DRUGS_TO_SIDEEFFECTS
    query += "FILTER(?drug in ("
    for drug in drugs:
        query += "<" + drug + ">,"
    query = query[:-1]
    query += "))} LIMIT " + str(LIMIT)
    qresults = execute_query(query)
    qresults = [(item['drugLabel']['value'], item['sideEffectLabel']['value']) for item in qresults]
    return qresults


def drug2_interactions_query(drugs):
    query = QUERY_DRUGS_TO_DRUGS_INTERACTIONS
    query += "FILTER(?affectdDrug in ("
    for drug in drugs:
        query += "<" + drug + ">,"
    query = query[:-1]
    query += "))} LIMIT " + str(LIMIT)
    qresults = execute_query(query)
    qresults = [(item['effectorDrugLabel']['value'], item['affectdDrugLabel']['value'], item['effectLabel']['value'],
                 item['impactLabel']['value']) for item in qresults]
    return qresults


def drugsCUI2drugID_query(drugs):
    query = QUERY_CUI_TO_DRUGS
    query += "FILTER(?drugCUI in ("
    for drug in drugs:
        query += "<http://project-iasis.eu/Drug/" + drug + ">,"
    query = query[:-1]
    query += "))} LIMIT " + str(LIMIT)
    qresults = execute_query(query)
    qresults = [("http://project-iasis.eu/Drug/" + item['drugBankID']['value'], item['drug']['value']) for item in
                qresults]
    return qresults


def proccesing_response(input_dicc, limit, ltypes):
    finalResponse = []
    for elem in input_dicc:
        lcuis = input_dicc[elem]
        number_cuis = len(lcuis)

        codicc = {}
        codicc['parameter'] = elem
        #################################################################################3
        if ('sideEffects' in ltypes):
            if elem == 'comorbidities' or elem == 'tumorType':
                drugs = disorder2drugs_query(input_dicc[elem])
            elif elem == 'biomarkers':
                drugs = biomarkers2drugs_query(input_dicc[elem])
            elif elem in ['drugs', 'oncologicalTreatments', 'immunotherapyDrugs', 'tkiDrugs', 'chemotherapyDrugs']:
                drugs = drugsCUI2drugID_query(input_dicc[elem])
            if len(drugs) != 0:
                drug_sideEffects = drug2sideEffect_query([drug[0] for drug in drugs])
                sideEffects = []
                for item in drug_sideEffects:
                    temp_dic = dict()
                    temp_dic['drug'] = item[0]
                    temp_dic['sideEffect'] = item[1]
                    sideEffects.append(temp_dic)
                codicc['sideEffects'] = sideEffects
        ################################################################################
        if ('drugs' in ltypes):
            if elem == 'comorbidities' or elem == 'tumorType':
                drugs = disorder2drugs_query(input_dicc[elem])[:LIMIT]
            elif elem == 'biomarkers':
                drugs = biomarkers2drugs_query(input_dicc[elem])[:LIMIT]
            if len(drugs) != 0:
                codicc['drugs'] = [drug[1] for drug in drugs]
        #####################################################################################
        if ('drugInteractions' in ltypes):
            if elem == 'comorbidities' or elem == 'tumorType':
                drugs = disorder2drugs_query(input_dicc[elem])
            elif elem == 'biomarkers':
                drugs = biomarkers2drugs_query(input_dicc[elem])
            elif elem in ['drugs', 'oncologicalTreatments', 'immunotherapyDrugs', 'tkiDrugs', 'chemotherapyDrugs']:
                drugs = drugsCUI2drugID_query(input_dicc[elem])
            if len(drugs) != 0:
                drug_interactions = drug2_interactions_query([drug[0] for drug in drugs])
                drugInteractions = []
                for item in drug_interactions:
                    temp_dic = dict()
                    temp_dic['effectorDrug'] = item[0]
                    temp_dic['affectdDrug'] = item[1]
                    temp_dic['effect'] = item[2]
                    temp_dic['impact'] = item[3]
                    drugInteractions.append(temp_dic)
                codicc['drugInteractions'] = drugInteractions
        ######################################################################################
        if ('publications' in ltypes):
            pubs = []
            results_pub_ranked = get_publications_ranked_data(lcuis, limit)
            for result in results_pub_ranked:
                pub1 = {}
                pub1['title'] = result["title"]["value"]
                pub1['url'] = "https://www.ncbi.nlm.nih.gov/pubmed/" + result["idf"]["value"]
                pub1['author'] = result['author']["value"]
                pub1['journal'] = result['journal']["value"]
                pub1['year'] = result["year"]["value"]
                pub1['score'] = int(result["score"]["value"]) / number_cuis
                pubs.append(pub1)

            codicc['publications'] = pubs
        ######################################################################################
        finalResponse.append(codicc)
    return finalResponse


def proccesing_list_types(ltype):
    ltype = ltype.replace("[", "")
    ltype = ltype.replace("]", "")
    ltype = ltype.replace(" ", "")
    ltype = ltype.split(",")
    return ltype


@bp.route('/lc_exploration', methods=['POST'])
def run_exploration_api():
    if (not request.json):
        abort(400)
    if 'limit' in request.args:
        limit = int(request.args['limit'])
    else:
        limit = LIMIT
    typed = request.args['type']
    ltypes = proccesing_list_types(typed)
    input_list = request.json
    if len(input_list) == 0:
        r = "{results: 'Error in the input format'}"
    else:
        if len(ltypes) == 0:
            abort(400)
        if ('publications' not in ltypes) and ('sideEffects' not in ltypes) and ('drugInteractions' not in ltypes) and (
                'drugs' not in ltypes) and ('genes' not in ltypes) and ('mutations' not in ltypes):
            abort(400)
        response = proccesing_response(input_list, limit, ltypes)
        r = json.dumps(response, indent=4)
    response = make_response(r, 200)
    response.mimetype = "application/json"
    return response
