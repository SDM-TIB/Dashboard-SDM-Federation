from flask import (
    Blueprint, g, session, render_template
)

from FedSDM.ui.utils import (
    get_mtconns, get_num_properties, get_num_rdfmts, get_datasources, get_federations, get_federation_stats
)

bp = Blueprint('dashboard', __name__, url_prefix='/dashboard')


@bp.route('/')
def get_all_stats():
    feds = get_federations()
    g.federations = feds
    if 'fed' in session:
        if session['fed'] not in [f['uri'] for f in feds]:
            del session['fed']
    source_ids = []
    datasources = {}
    rdfmts = 0
    links = 0
    stats = {}

    for f in feds:
        graph = f['uri']
        dss = get_datasources(graph)
        datasources.update(dss)
        source_ids.extend(list(dss.keys()))
        mts = get_num_rdfmts(graph)
        rdfmts += mts
        lks = get_mtconns(graph)
        links += lks
        stats[f['uri']] = []
        for s in list(dss.keys()):
            num_mts = get_num_rdfmts(graph, s)
            datasources[s]['rdfmts'] = num_mts
            props = get_num_properties(graph, s)
            datasources[s]['properties'] = props
            links_ = get_mtconns(graph, s)
            datasources[s]['links'] = links_
            stat = {
                'rdfmts': num_mts,
                'links': links_,
                'triples': datasources[s]['triples'] if 'triples' in datasources[s] else -1,
                'properties': props,
                'source': datasources[s]['source']
            }
            stats[f['uri']].append(stat)

    stat = {
        'rdfmts': rdfmts,
        'sources': len(set(source_ids)),
        'federations': len(feds),
        'links': links
    }

    datasource_stats = list(datasources.values())
    federation_stats = get_federation_stats()

    g.stats = stats

    return render_template('dashboard/index.html', dsStats=datasource_stats, fedStats=federation_stats, stats=stat)
