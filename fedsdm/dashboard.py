from flask import (
    Blueprint, flash, g, redirect, session, render_template, request, url_for
)
from werkzeug.exceptions import abort

from fedsdm.auth import login_required
from fedsdm.db import get_db
from fedsdm.ui.utils import get_mtconns, get_num_properties, get_num_rdfmts, get_datasources, get_federations

bp = Blueprint('dashboard', __name__, url_prefix='/dashboard')


@bp.route('/')
@login_required
def stats():
    federations = get_federations(g.default_graph)
    g.federations = federations
    if 'fed' in session:
            if session['fed'] not in [f['uri'] for f in federations]:
                del session['fed']
    sourceids = []
    datasources = {}
    rdfmts = 0
    links = 0
    stats = {}
    feds = get_federations(g.default_graph)
    if 'fed' in session:
        if session['fed'] not in [f['uri'] for f in feds]:
            del session['fed']
    for f in feds:
        graph = f['uri']
        dss = get_datasources(graph)
        datasources.update(dss)
        sourceids.extend(list(dss.keys()))
        mts = get_num_rdfmts(graph)
        rdfmts += mts
        lks = get_mtconns(graph)
        links += lks
        stats[f['uri']] = []
        for s in list(dss.keys()):
            nummts = get_num_rdfmts(graph, s)
            datasources[s]['rdfmts'] = nummts
            props = get_num_properties(graph, s)
            datasources[s]['properties'] = props
            linkss = get_mtconns(graph, s)
            datasources[s]['links'] = linkss
            stat = {"rdfmts": nummts,
                    "links": linkss,
                    "triples": datasources[s]['triples'] if 'triples' in datasources[s] else -1,
                    "properties": props,
                    "source": datasources[s]['source']}
            stats[f['uri']].append(stat)

    stat = {
        "rdfmts": rdfmts,
        "sources": len(set(sourceids)),
        "federations": len(feds),
        "links": links}

    datasourcesstat = list(datasources.values())

    g.stats = stats

    return render_template('dashboard/index.html', dsstats=datasourcesstat,  fedstats=stat, federations=g.federations)
