from flask import (
    Blueprint, g, session, render_template
)

from fedsdm.ui.utils import (
    get_mtconns, get_num_properties, get_num_rdfmts, get_datasources, get_federations, get_federation_stats
)

bp = Blueprint('dashboard', __name__, url_prefix='/dashboard')


@bp.route('/')
def stats():
    feds = get_federations()
    g.federations = feds
    if 'fed' in session:
        if session['fed'] not in [f['uri'] for f in feds]:
            del session['fed']
    sourceids = []
    datasources = {}
    rdfmts = 0
    links = 0
    stats = {}

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
    federation_stats = get_federation_stats()

    g.stats = stats

    return render_template('dashboard/index.html', dsStats=datasourcesstat, fedStats=federation_stats, stats=stat)
