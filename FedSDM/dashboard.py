from flask import (
    Blueprint, g, session, render_template
)

from FedSDM.utils import (
    get_num_mt_links, get_num_properties, get_num_rdfmts, get_data_sources, get_federations, get_federation_stats
)

bp = Blueprint('dashboard', __name__, url_prefix='/dashboard')


@bp.route('')
def get_all_stats() -> str:
    """Serves the landing page of FedSDM, i.e., '/dashboard'.

    It makes use of several utility methods in order to get all the different statistics
    for all federations and data sources available to the instance of FedSDM.

    Returns
    -------
    str
        Rendered template of the landing page with all statistics set.

    """
    feds = get_federations()
    g.federations = feds
    if 'fed' in session:
        if session['fed'] not in [f['uri'] for f in feds]:
            del session['fed']
    source_ids = []
    data_sources = {}
    rdfmts = 0
    links = 0

    for f in feds:
        graph = f['uri']
        dss = get_data_sources(graph)
        data_sources.update(dss)
        source_ids.extend(list(dss.keys()))
        mts = get_num_rdfmts(graph)
        rdfmts += mts
        lks = get_num_mt_links(graph)
        links += lks
        for s in list(dss.keys()):
            num_mts = get_num_rdfmts(graph, s)
            data_sources[s]['rdfmts'] = num_mts
            props = get_num_properties(graph, s)
            data_sources[s]['properties'] = props
            links_ = get_num_mt_links(graph, s)
            data_sources[s]['links'] = links_

    stat = {
        'rdfmts': rdfmts,
        'sources': len(set(source_ids)),
        'federations': len(feds),
        'links': links
    }

    data_source_stats = list(data_sources.values())
    federation_stats = get_federation_stats()

    return render_template('dashboard/index.html', dsStats=data_source_stats, fedStats=federation_stats, stats=stat)
