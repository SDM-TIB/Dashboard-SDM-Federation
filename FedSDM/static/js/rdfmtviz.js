$(function() {
    const graph_container = $('#graph'),
          graph_legend = $('#legend'),
          federation_list = $('#federations-list'),
          data_sources = $('#data_sources'),
          mt_details = $('#mt_details'),
          mt_viz = $('#mt_viz'),
          graphArea = document.getElementById('graph'),
          size = d3.scale.pow().exponent(1).domain([1, 100]).range([8, 36]),  // The largest node for each cluster.
          curve = d3.svg.diagonal().projection(function(d) { return [d.x, d.y] });

    data_sources.prop('disabled', true);
    mt_details.prop('disabled', true);
    mt_viz.hide();
    graph_legend.hide();

    let stats = null,
        federation =  federation_list.val(),
        tabVisible = '#home',
        width = graph_container.width(), height = 980, h = 960, w = 760,
        loaded = 0,
        visualized = 0,
        gaLoaded = 0,
        gtable = null,
        linkDistance = 150,
        nCharge = -600,
        nGravity = 0,
        sourcesCard = 0,
        sources = null,
        sourceMT = null,
        focus_node = null,
        highlight_node = null,
        text_center = false,
        outline = false,
        max_score = 1,
        highlight_color = '#A52A2A',
        highlight_trans = 0.1,
        default_link_color = '#888',
        nominal_base_node_size = 8,
        nominal_text_size = 10,
        max_text_size = 24,
        nominal_stroke = 1.5,
        max_stroke = 4.5,
        max_base_node_size = 40,
        min_zoom = 0.1,
        max_zoom = 7,
        sourceNodes = [],  // list of subjects and objects for the DAG
        sourceLinks = [],  // connection link between subject and object -> predicates
        sourceIDs = {}, sourcesNames = {},
        aNodes = [],
        aLinks = [],
        MTCards = { 'All': [] },
        vizType = null,
        data = { nodes: [], links: [] },
        selectedRow = null,
        mNodes = [],
        mLinks = [],
        mSourceNodes = [],
        mSourceLinks = [],
        donut_charts = [],
        expand = {}, // expanded clusters
        net, force,
        canvas = 'graph',
        gSource = null,
        keyc = true, keys = true, keyt = true, keyr = true, keyx = true, keyd = true, keyl = true, keym = true, keyh = true, key1 = true, key2 = true, key3 = true, key0 = true,
        diameter = 500,
        radius = diameter / 2,
        margin = 10,
        circularNode, circularLink;

    window.jsdata = [];

    if (federation != null && federation !== '') { load_data(federation) }

    federation_list.on('change', function() { load_data($(this).val()) });

    function load_data(fed) {
        $('#fedName').html(fed);
        $('#vizFedName').html(fed);
        $('#gaFedName').html(fed);
        data_sources.empty();
        graph_container.empty()
            .html('<h1>Loading...</h1>');
        $('#vizDsName').html('');
        loaded = 0;
        visualized = 0;
        gaLoaded = 0;
        get_rdfmts_stats(fed);
        get_rdfmts(fed);
        get_rdfmts_graph_analysis(fed);
        federation = fed;
    }

    mt_details.on('click', function() {
        $('#list_of_rdfmts').hide();
        mt_details.hide();
        $('#backToTable').show();
        const url = encodeURIComponent(selectedRow[0][2]);
        $.ajax({
            type: 'GET',
            headers: { Accept : 'application/json' },
            url: '/rdfmts/api/mtdetails?mt=' + url + '&fed=' + federation,
            dataType: 'json',
            crossDomain: true,
            success: function(data) {
                console.log('url: ' + url)
                console.log('detail returned: ' + data);
                sources = data.sources;
                mNodes = data.nodes;
                mLinks = data.links;
                mSourcesCard = sources.length;
                for (let i = 0; i < sources.length; i++) {
                    const v = sources[i].id,
                          name  = sources[i].name;
                    sourceIDs[name] = v;
                    sourcesNames[v] = name;
                }
                for (let i = 0; i < mLinks.length; ++i) {
                    let o = mLinks[i];
                    o.source = mNodes[o.source];
                    o.target = mNodes[o.target];
                    if (o.source.datasource === o.target.datasource) {
                        if (o.source.datasource in mSourceLinks) { mSourceLinks[o.source.datasource].push(o) }
                        else { mSourceLinks[o.source.datasource] = [o] }
                    }
                }

                let flatNodes = [];
                $.each(mNodes, function (key, val) {
                    flatNodes.push(val);
                    MTCards['All'].push({ 'label': val.label, 'value': val.weight }); //, 'color': color(val.datasource)
                    if (val.datasource in MTCards) { MTCards[val.datasource].push({ 'label': val.label, 'value': val.weight }) }  //, 'color': color(val.datasource)
                    else { MTCards[val.datasource] = [{ 'label': val.label, 'value': val.weight }] }  // , 'color': color(val.datasource)

                    if (val.datasource in mSourceNodes) { mSourceNodes[val.datasource].push(val) }
                    else { mSourceNodes[val.datasource] = [val] }
                });
                mNodes = flatNodes;
                $("#mt_viz").show();
                draw_details();
            },
            error: function(jqXHR, textStatus) {
                console.log(jqXHR.status);
                console.log(jqXHR.responseText);
                console.log(textStatus);
            }
        });
    });

    function draw_details() {
        data = { nodes: mNodes, links: mLinks };
        mt_viz.html('<h1> Please select data source!</h1>');
        drawRDFMTS(mNodes, mLinks, 'mt_viz');
    }

    $('#backToTable').on('click', function() {
        $('#backToTable').hide();
        mt_viz.hide();
        $('#list_of_rdfmts').show();
        mt_details.show();
    });

    function get_rdfmts_stats(fed) {
        if (fed == null || (fed === federation && loaded === 1)) { return }
        $('#fedName').html(fed);
        $('#vizFedName').html(fed);
        $('#gaFedName').html(fed);
        if (stats == null || stats === 'undefined') {
            let rdfmtsDataTable = $('#rdfmts_data_table');
            rdfmtsDataTable.empty()
                .append('<thead><tr><th>#</th><th>Name</th><th>URI</th><th>Instances</th><th>Num. of Properties</th></tr></thead>');
            stats = rdfmtsDataTable.DataTable({
                order: [[1, 'desc']],
                responsive: true,
                select: true,
                defaultContent: '<i>Not set</i>',
                columnDefs: [
                    { target: 3, render: number_renderer },
                    { target: 4, render: number_renderer }
                ],
                lengthMenu: [ [10, 25, 50, -1], [10, 25, 50, 'All'] ],
                dom: 'Blfrtip',
                buttons: table_buttons('rdfmts_data_table'),
                ajax: '/rdfmts/api/rdfmtstats?graph=' + fed
            });
            loaded = 1;
            let statstable = stats;
            stats.on('select', function(e, dt, type, indexes) {
                selectedRow = statstable.rows(indexes).data().toArray();
                console.log('selected row: ' + selectedRow);
                $('#edit_mt').prop('disabled', false);
                $('#remove_mt').prop('disabled', false);
                mt_details.prop('disabled', false);
                $('#backToTable').hide();
            }).on('deselect', function() {
                $('#edit_mt').prop('disabled', true);
                $('#remove_mt').prop('disabled', true);
                mt_details.prop('disabled', true);
                $('#backToTable').hide();
                selectedRow = null;
            });
        } else {
            console.log('Redrawing table...');
            stats.clear().draw();
            stats.ajax.url('/rdfmts/api/rdfmtstats?graph=' + fed).load();
        }
    }
    function get_rdfmts(fed) {
        if (fed == null || (fed === federation && visualized === 1)) { return }
        $.getJSON('/rdfmts/api/rdfmtstats?graph=' + fed, function(data2) { jsdata = data2 });
        $('#fedName').html(fed);
        $('#gaFedName').html(fed);
        $('#vizFedName').html(fed);

        // list of subjects and objects for the DAG
        $.getJSON('/rdfmts/api/rdfmts?graph=' + fed, function(data) {
            sources = data.sources;
            nodes = data.nodes;
            links = data.links;
            sourcesCard = sources.length;
            max_score = sourcesCard;
            let legend = '',
                data_sources_html = '<li class="datasource"><a href="#" class="datasource" id="source-0">All</a></li><li class="dropdown-divider"></li>' ;
            for (let i = 0; i < sources.length; i++) {
                const v = sources[i].id,
                      name  = sources[i].name;
                sourceIDs[name] = v;
                sourcesNames[v] = name;
                data_sources_html += '<li class="datasource"><a href="#" class="datasource" id="source-' + (i + 1) + '">' + name + '</a></li>';
                legend = legend + '<span style="color:' + color(v) + '"><b>' + name + '</b></span><br/>';
            }
            graph_legend.empty()
                .html(legend);

            $('#ga_data_sources').empty()
                .html(data_sources_html);
            data_sources.empty()
                .html(data_sources_html)
                .prop('disabled', false);
            graph_container.html('<h1> Please select data source!</h1>');
            $('a[class=datasource]').on('click', function() {
                $('#data_sources_btn').val($(this).text());
                if ($(this).text() === 'All') {
                    $('#vizDsName').html('ALL');
                    $('#gaDsName').html('ALL');
                    sourceMT = 'All';
                } else { sourceMT = sourceIDs[$(this).text()]; }
                if (sourceMT) {
                    $('#vizDsName').html($(this).text());
                    $('#gaDsName').html($(this).text());
                } else {
                    $('#vizDsName').html('ALL');
                    $('#gaDsName').html('ALL');
                }
                if (tabVisible === '#analysis') { get_rdfmts_graph_analysis(federation, $(this).text()) }
                else {
                    $('#vizDsName').html($(this).text());
                    $('#gaDsName').html($(this).text());
                    graph_container.empty()
                        .html('<h1> Please select visualization type!</h1>');
                    if (vizType === 'fgraph') { drawSingleSourceRDFMTS(sourceMT, 'force') }
                    else if (vizType === 'cgraph') { drawSingleSourceRDFMTS(sourceMT, 'circular') }
                    else if (vizType === 'donut') { drawDonut(sourceMT) }
                    get_rdfmts_graph_analysis(federation, $(this).text());
                }
            });

            sourceLinks = [];
            for (let i = 0; i < links.length; ++i) {
                let o = links[i];

                o.source = nodes[o.source];
                o.target = nodes[o.target];
                if (o.source == null || o.target == null) { console.log(o) }
                if (o.source.datasource === o.target.datasource) {
                    if (o.source.datasource in sourceLinks) { sourceLinks[o.source.datasource].push(o) }
                    else { sourceLinks[o.source.datasource] = [o] }
                }
            }
            aLinks = links;

            let flatNodes = [];
            sourceNodes = [];
            MTCards = { 'All': [] };
            $.each(nodes, function (key, val) {
                flatNodes.push(val);
                MTCards['All'].push({ 'label': val.label, 'value': val.weight }); //, 'color': color(val.datasource)
                if (val.datasource in MTCards) { MTCards[val.datasource].push({ 'label': val.label, 'value': val.weight }) }  //, 'color': color(val.datasource)
                else { MTCards[val.datasource] = [{ 'label': val.label, 'value': val.weight }] }  // , 'color': color(val.datasource)
                if (val.datasource in sourceNodes) { sourceNodes[val.datasource].push(val) }
                else { sourceNodes[val.datasource] = [val] }
            });
            aNodes = flatNodes;
        });
    }

    function createDonut(title, labels_, data_) {
        const donutBox = document.createElement('DIV'),
              donutWrapper = document.createElement('DIV'),
              donutCanvas = document.createElement('CANVAS'),
              donutLegend = document.createElement('DIV'),
              ul = document.createElement('UL'),
              data = {
                   labels: labels_,
                   datasets: [{ data: data_, backgroundColor: colors }]
              };

        let donut = new Chart(donutCanvas, {
            type: 'doughnut',
            data: data,
            options: {
                cutout: '80%',
                plugins: {
                    legend: { display: false },
                    title: {
                        display: true,
                        text: title,
                        font: { size: 16 }
                    },
                    tooltip: false
                }
            },
            plugins: [{
                id: 'hoverLabel',
                afterDraw: function(chart) {
                    const width = chart.chartArea.width,
                          height = chart.chartArea.height,
                          top = chart.chartArea.top,
                          ctx = chart.ctx;
                    ctx.save();
                    if (chart._active.length > 0) {
                        const idx = chart._active[0].index,
                              numberLabel = chart.config.data.datasets[chart._active[0].datasetIndex].data[idx],
                              color = chart.config.data.datasets[chart._active[0].datasetIndex].backgroundColor[idx];
                        ctx.font = 'bolder 60px Arial';
                        ctx.textAlign = 'center';
                        ctx.textBaseline = 'middle';
                        ctx.fillStyle = color;
                        ctx.fillText(numberWithCommas(numberLabel), width / 2, height / 2 + top);
                    }
                }
            }]
        });

        donutBox.classList.add('donut-box');
        graphArea.appendChild(donutBox);
        donutWrapper.classList.add('donut-wrapper');
        donutBox.appendChild(donutWrapper);
        donutWrapper.appendChild(donutCanvas);
        donutLegend.classList.add('donut-legend');
        donutBox.appendChild(donutLegend);
        donut_charts.push(donut);
        donut.data.labels.forEach((label, index) => {
            const li = document.createElement('LI'),
                  colorBox = document.createElement('SPAN'),
                  p = document.createElement('P'),
                  textNode = document.createTextNode(label);

            colorBox.style.backgroundColor = color(index);
            li.onclick = (click) => {
                click.target.parentNode.classList.toggle('strike');
                donut.toggleDataVisibility(index);
                donut.update();
            };

            p.appendChild(textNode);
            li.appendChild(colorBox);
            li.appendChild(p);
            ul.appendChild(li);
        });
        donutLegend.appendChild(ul);
    }

    function drawDonut(source_mt) {
        donut_charts.forEach(value => value.destroy());
        graph_container.empty();
        graph_legend.hide();
        if (source_mt !== 'All') {
            for (let i = 0; i < jsdata.data.length; i++) {
                for (let j in MTCards[source_mt]) {
                    if (MTCards[source_mt][j].label.includes(jsdata.data[i][1])) { MTCards[source_mt][j].value = jsdata.data[i][3] }
                }
            }
            MTCards[source_mt].sort(function(a, b) { return b.value - a.value });

            let labels_ = [], data_ = [];
            for (let i = 0; i < MTCards[source_mt].length; i++) {
                labels_.push(MTCards[source_mt][i]['label']);
                data_.push(MTCards[source_mt][i]['value']);
            }

            const title = sourcesNames[source_mt];
            createDonut(title, labels_, data_);
        } else {
            $.each(MTCards, function (key, val) {
                for (let i = 0; i < jsdata.data.length; i++) {
                    for (let ii in val) {
                        if (val[ii].label.includes(jsdata.data[i][1])) { val[ii].value = jsdata.data[i][3] }
                    }
                }
                val.sort(function(a, b) { return b.value - a.value });

                let labels_ = [], data_ = [];
                for (let i = 0; i < val.length; i++) {
                    labels_.push(val[i]['label']);
                    data_.push(val[i]['value']);
                }

                const ds_name = sourcesNames[key] ? sourcesNames[key] : federation;
                createDonut(ds_name, labels_, data_);
            });
        }
    }

    $('#stop_force').on('click', function() { if (force) { force.stop() } });
    $('#start_force').on('click', function() {
        if (force) {
            linkDistance += 10;
            force.linkDistance(linkDistance).gravity(0.05).start();
        }
    });
    $('#reset_force').on('click', function() {
        if (force) {
            linkDistance = 150
            const fit = Math.sqrt(aNodes.length / (width * height));
            nCharge = (-1 / fit);
            nGravity = (8 * fit);
            force.linkDistance(linkDistance).gravity(0.05).start();
        }
    });
    $('#graphVizForce').on('click', function() {
        console.log('visible tab for data source selection: ' + tabVisible + ' ' + sourceMT);
        drawSingleSourceRDFMTS(sourceMT, 'force');
        vizType = 'fgraph';
    });
    $('#graphVizCircular').on('click', function() {
        console.log('visible tab for data source selection: ' + tabVisible + ' ' + sourceMT);
        drawSingleSourceRDFMTS(sourceMT, 'circular');
        vizType = 'cgraph';
    });
    $('#donutViz').on('click', function() {
        console.log(sourceMT + ': ' + MTCards);
        drawDonut(sourceMT);
        vizType = 'donut';
    });

    function drawSingleSourceRDFMTS(source, gt) {
        graph_container.empty();
        graph_legend.show();
        console.log('source: ' + source);
        if (source === 'All') {
            if (aLinks.length < 1) { aLinks = [] }
            data = { nodes: aNodes, links: aLinks };
            if (gt === 'force') {
                aNodes.forEach(function(d) { expand[d.datasource] = true });
                drawRDFMTS(aNodes, aLinks);
            } else { drawGraph(data) }
            visualized = 1;
        } else {
            const sNodes = sourceNodes[source];
            console.log('number of nodes:' + sNodes.length);
            // connection link between subject and object -> predicates
            let slinks = sourceLinks[source];
            if (!slinks) { slinks = [] }
            data = { nodes: sNodes, links: slinks };
            if (gt === 'force') {
                sNodes.forEach(function(d) { expand[d.datasource] = true });
                drawRDFMTS(sNodes, slinks);
            } else { drawGraph(data) }
        }
    }

    function nodeID(n) { return n.size ? '_g_' + n.datasource : n.label }

    function linkID(l) {
        const u = nodeID(l.source),
              v = nodeID(l.target);
        return u < v ? u + '|' + v : v + '|' + u;
    }

    function getGroup(n) { return n.datasource }

    // constructs the network to visualize
    function network(data, prev, index, expand) {
        expand = expand || {};
        let gm = {},    // group map
            nm = {},    // node map
            lm = {},    // link map
            gn = {},    // previous group nodes
            gc = {},    // previous group centroids
            nodes = [], // output nodes
            links = []; // output links

        // process previous nodes for reuse or centroid calculation
        if (prev) {
            prev.nodes.forEach(function(n) {
                const i = index(n);
                let o;

                if (n.size > 0) {
                    gn[i] = n;
                    n.size = 0;
                } else {
                    o = gc[i] || (gc[i] = { x: 0, y: 0, count: 0 });
                    o.x += n.x;
                    o.y += n.y;
                    o.count += 1;
                }
            });
        }

        // determine nodes
        for (let k = 0; k < data.nodes.length; ++k) {
            const n = data.nodes[k],
                  i = index(n),
                  l = gm[i] || (gm[i] = gn[i]) || (gm[i] = { datasource: i, size: 0, nodes: [] });

            if (expand[i]) {
                // the node should be directly visible
                nm[n.label] = nodes.length;
                nodes.push(n);
                if (gn[i]) {
                    // place new nodes at cluster location (plus jitter)
                    n.x = gn[i].x + Math.random();
                    n.y = gn[i].y + Math.random();
                }
            } else {
                // the node is part of a collapsed cluster
                if (l.size === 0) {
                    // if new cluster, add to set and position at centroid of leaf nodes
                    nm[i] = nodes.length;
                    nodes.push(l);
                    if (gc[i]) {
                        l.x = gc[i].x / gc[i].count;
                        l.y = gc[i].y / gc[i].count;
                    }
                }
                l.nodes.push(n);
            }
            // always count group size as we also use it to tweak the force graph strengths/distances
            l.size += 1;
            n.group_data = l;
        }

        for (const i in gm) { gm[i].link_count = 0 }

        // determine links
        for (let k = 0; k < data.links.length; ++k) {
            let e = data.links[k],
                u = index(e.source),
                v = index(e.target);
            if (u !== v) {
                gm[u].link_count++;
                gm[v].link_count++;
            }
            u = expand[u] ? nm[e.source.label] : nm[u];
            v = expand[v] ? nm[e.target.label] : nm[v];
            let i = (u < v ? u + '|' + v : v + '|' + u),
                l = lm[i] || (lm[i] = { source: u, target: v, size: 0 });
            l.size += 1;
        }
        for (const i in lm) { links.push(lm[i]) }

        return { nodes: nodes, links: links };
    }

    function drawRDFMTS(nodes, links, divCanvas) {
        console.log('nodes: ' + nodes + '\nlinks:' + links);
        let svg;
        height = 980;
        if (divCanvas == null) {
            graph_container.empty();
            svg = d3.select('#graph').append('svg');
            width = graph_container.width();
            canvas = 'graph';
        } else {
            mt_viz.empty();
            svg = d3.select('#mt_viz').append('svg');
            width = mt_viz.width();
            console.log('Showing visualization...');
            mt_viz.show();
            canvas = 'mtviz';
        }
        let zoom = d3.behavior.zoom().scaleExtent([min_zoom,max_zoom]),
            g = svg.append('g');

        svg.attr('opacity', 1e-6)
            .transition()
            .duration(1000)
            .attr('opacity', 1);

        let toColor = 'fill',
            toWhite = 'stroke';
        if (outline) {
            toColor = 'stroke';
            toWhite = 'fill';
        }

        svg.style('cursor','move');
        let linkedByIndex = {};
        links.forEach(function(d) { linkedByIndex[d.source + ',' + d.target] = true });

        const fit = Math.sqrt(nodes.length / (width * height));
        nGravity = (8 * fit);
        nCharge = (-1 / fit);
        if (force) { force.stop() }
        net = network(data, net, getGroup, expand);
        console.log('network:', net, expand);
        force = d3.layout.force()
            .nodes(net.nodes)
            .links(net.links)
            .linkDistance(function(l) {
                const n1 = l.source,
                      n2 = l.target;
                return divCanvas ? 250 : 200 +
                    Math.min(20 * Math.min((n1.size || (n1.datasource !== n2.datasource ? n1.group_data.size : 0)),
                        (n2.size || (n1.datasource !== n2.datasource ? n2.group_data.size : 0))),
                        -30 +
                        30 * Math.min((n1.link_count || (n1.datasource !== n2.datasource ? n1.group_data.link_count : 0)),
                            (n2.link_count || (n1.datasource !== n2.datasource ? n2.group_data.link_count : 0))),
                        300);
            })
            .linkStrength(2)
            .gravity(0.05)   // 0.05 gravity+charge tweaked to ensure good 'grouped' view (e.g. green group not smack between blue & orange)
            .charge(-600)    // charge is important to turn single-linked groups to the outside
            .friction(0.5)   // friction adjusted to get dampened display
            .size([width,height])
            .start(); //.chargeDistance(1000) .linkDistance(300)

        link = g.selectAll('.link').data(net.links, linkID);
        link.exit().remove();
        link.enter().append('line')
            .attr('class', 'link')
            .attr('x1', function(d) { return d.source.x })
            .attr('y1', function(d) { return d.source.y })
            .attr('x2', function(d) { return d.target.x })
            .attr('y2', function(d) { return d.target.y })
            .style('stroke-width', nominal_stroke)
            .style('stroke', function(d) { return color(d.datasource) });
        node = g.selectAll('.node').data(net.nodes, nodeID);
        node.exit().remove();
        node.enter().append('g')
            .attr('class', function(d) { return 'node' + (d.size ? '' : ' leaf') })
            .attr('cx', function(d) { return d.x })
            .attr('cy', function(d) { return d.y })
            .on('dblclick', function(d) {
                console.log(d.datasource + ': ' + expand[d.datasource])
                expand[d.datasource] = !expand[d.datasource];
                drawRDFMTS(nodes, links, divCanvas);
            })
            .on('mouseover', function(d) { set_highlight(d) })
            .on('mousedown', function(d) {
                d3.event.stopPropagation();
                focus_node = d;
                set_focus(d);
                if (highlight_node === null) { set_highlight(d) }
            })
            .on('mouseout', function() { exit_highlight() });

        node.call(force.drag);

        let ci = 0;
        let circle = node.append('path')
            .attr('d', d3.svg.symbol()
                .size(function(d) { return d.size ? Math.PI * Math.pow(size(65 + d.size > 200 ? 200 : d.size) || nominal_base_node_size,2) : Math.PI * Math.pow(size(25) || nominal_base_node_size,2) })
                .type(function(d) { return d.size ? 'circle' : d.type })
            )
            .style(toColor, function(d) {
                if (divCanvas == null) { return color(d.datasource) }
                else {
                    ci += 1;
                    return color(d.datasource + (ci - 1));
                }
            })
            .style('stroke-width', nominal_stroke)
            .style(toWhite, 'white');

        let text = g.selectAll('.text')
            .data(net.nodes)
            .enter().append('text')
            .attr('dy', '.35em')
            .style('font-size', function(d) { return d.size ? 16 + 'px' : nominal_text_size + 'px' });

        if (text_center) {
            text.text(function (d) { if (d.label) { return d.label } else { return sourcesNames[d.datasource] } })
                .style('text-anchor', 'middle');
        } else {
            text.attr('dx', function() { return (size(65) - size(30) || nominal_base_node_size) })
                .text(function(d) { if (d.label) return  '\u2002'+ d.label; else return '\u2002'+ sourcesNames[d.datasource] });
        }

        d3.select(window).on('mouseup', function() {
            if (focus_node !== null) {
                focus_node = null;
                if (highlight_trans < 1) {
                    circle.style('opacity', 1);
                    text.style('opacity', 1);
                    link.style('opacity', 1);
                }
            }
            if (highlight_node === null) { exit_highlight() }
        });

        zoom.on('zoom', function() {
            let stroke = nominal_stroke;
            if (nominal_stroke * zoom.scale() > max_stroke) { stroke = max_stroke / zoom.scale() }

            link.style('stroke-width', stroke);
            circle.style('stroke-width',stroke);

            let base_radius = nominal_base_node_size;
            if (nominal_base_node_size * zoom.scale() > max_base_node_size) { base_radius = max_base_node_size / zoom.scale() }
            circle.attr('d', d3.svg.symbol()
                .size(function(d) { return d.size ? Math.PI * Math.pow(size(65 + d.size > 200 ? 200 : d.size) * base_radius / nominal_base_node_size || base_radius,2) : Math.PI * Math.pow(size(25) * base_radius / nominal_base_node_size || base_radius,2) })
                .type(function(d) { return d.size ? 'circle' :  d.type })
            );
            if (!text_center) { text.attr('dx', function() { return ((size(65) - size(30)) * base_radius / nominal_base_node_size || base_radius) }) }
            text.style('font-size', function(d) {
                let text_size = nominal_text_size;
                if (d.size) { text_size = 16 }
                if (nominal_text_size * zoom.scale() > max_text_size) { text_size = max_text_size / zoom.scale() }
                return text_size + 'px';
            });
            g.attr('transform', 'translate(' + d3.event.translate + ')scale(' + d3.event.scale + ')');
        });

        svg.call(zoom);

        resize();
        d3.select(window).on('resize', resize).on('keydown', keydown);
        let centroids = {};
        for (let i = 0; i < max_score; i += 3) {
            centroids[i] = { x: 200 * (i/3 + 1), y: 200 };
            centroids[i+1] = { x: 200 * (i/3 + 1), y: 400 };
            centroids[i+2] = { x: 200 * (i/3 + 1), y: 600 };
        }

        force.on('tick', function(e) {
            const k = .1 * e.alpha;

            // push nodes toward their designated focus
            net.nodes.forEach(function(o) {
                if (centroids[o.datasource]) {
                    o.y += (centroids[o.datasource].y - o.y) * k;
                    o.x += (centroids[o.datasource].x - o.x) * k;
                }
            });

            text.forEach(function(o) {
                if (centroids[o.datasource]) {
                    o.y += (centroids[o.datasource].y - o.y) * k;
                    o.x += (centroids[o.datasource].x - o.x) * k;
                }
            });
            link.attr('x1', function(d) { return d.source.x })
                .attr('y1', function(d) { return d.source.y })
                .attr('x2', function(d) { return d.target.x })
                .attr('y2', function(d) { return d.target.y });

            node.attr('cx', function(d) { return d.x })
                .attr('cy', function(d) { return d.y });

            node.attr('transform', function(d) { return 'translate(' + d.x + ',' + d.y + ')' });
            text.attr('transform', function(d) { return 'translate(' + d.x + ',' + d.y + ')' });
        });

        function isConnected(a, b) { return linkedByIndex[a.index + ',' + b.index] || linkedByIndex[b.index + ',' + a.index] || a.index === b.index }

        function hasConnections(a) {
            for (const property in linkedByIndex) {
                let s = property.split(',');
                if ((s[0] === a.index || s[1] === a.index) && linkedByIndex[property]) { return true }
            }
            return false;
        }

        function vis_by_type(type) {
            switch (type) {
                case 'circle': return keyc;
                case 'square': return keys;
                case 'triangle-up': return keyt;
                case 'diamond': return keyr;
                case 'cross': return keyx;
                case 'triangle-down': return keyd;
                default: return true;
            }
        }

        function vis_by_node_score(score) {
            if (isNumber(score)) {
                if (score >= 0.666) { return keyh }
                else if (score >= 0.333) { return keym }
                else if (score >= 0) { return keyl }
            }
            return true;
        }

        function vis_by_link_score(score) {
            if (isNumber(score)) {
                if (score >= 0.666) { return key3 }
                else if (score >= 0.333) { return key2 }
                else if (score >= 0) { return key1 }
            }
            return true;
        }

        function isNumber(n) { return !isNaN(parseFloat(n)) && isFinite(n) }

        function resize() {
            const width = $('#' + canvas).width(), height = 980;
            svg.attr('width', width).attr('height', height);
            force.size([force.size()[0] + (width - w) / zoom.scale(), force.size()[1] + (height - h) / zoom.scale()]).resume();
            w = width;
            h = height;
        }

        function keydown() {
            if (d3.event.keyCode === 32) { force.stop() }
            else if (d3.event.keyCode >= 48 && d3.event.keyCode <= 90 && !d3.event.ctrlKey && !d3.event.altKey && !d3.event.metaKey) {
                switch (String.fromCharCode(d3.event.keyCode)) {
                    case 'C': keyc = !keyc; break;
                    case 'S': keys = !keys; break;
                    case 'T': keyt = !keyt; break;
                    case 'R': keyr = !keyr; break;
                    case 'X': keyx = !keyx; break;
                    case 'D': keyd = !keyd; break;
                    case 'L': keyl = !keyl; break;
                    case 'M': keym = !keym; break;
                    case 'H': keyh = !keyh; break;
                    case '1': key1 = !key1; break;
                    case '2': key2 = !key2; break;
                    case '3': key3 = !key3; break;
                    case '0': key0 = !key0; break;
                }

                link.style('display', function(d) {
                    const flag  = vis_by_type('circle') && vis_by_type('circle') && vis_by_node_score(d.source.datasource) && vis_by_node_score(d.target.datasource) && vis_by_link_score(d.datasource);
                    linkedByIndex[d.source.index + ',' + d.target.index] = flag;
                    return flag ? 'inline' : 'none';
                });
                node.style('display', function(d) { return (key0 || hasConnections(d)) && vis_by_type('circle') && vis_by_node_score(d.datasource) ? 'inline' : 'none' });
                text.style('display', function(d) { return (key0 || hasConnections(d)) && vis_by_type('circle') && vis_by_node_score(d.datasource) ? 'inline' : 'none' });

                if (highlight_node !== null) {
                    if ((key0 || hasConnections(highlight_node)) && vis_by_type('circle') && vis_by_node_score(highlight_node.datasource)) {
                        if (focus_node !== null) { set_focus(focus_node) }
                        set_highlight(highlight_node);
                    } else {
                        exit_highlight();
                    }
                }
            }
        }

        function exit_highlight() {
            highlight_node = null;
            if (focus_node == null) {
                svg.style('cursor', 'move');
                if (highlight_color !== 'white') {
                    circle.style(toWhite, 'white');
                    text.style('font-weight', 'normal');
                    link.style('stroke', function(o) { return (isNumber(o.datasource) && o.datasource >= 0) ? color(o.datasource) : default_link_color });
                }
            }
        }

        function set_focus(d) {
            if (highlight_trans < 1) {
                circle.style('opacity', function(o) { return isConnected(d, o) ? 1 : highlight_trans });
                text.style('opacity', function(o) { return isConnected(d, o) ? 1 : highlight_trans });
                link.style('opacity', function(o) { return o.source.index === d.index || o.target.index === d.index ? 1 : highlight_trans });
            }
        }

        function set_highlight(d) {
            svg.style('cursor', 'pointer');
            if (focus_node !== null) { d = focus_node }
            highlight_node = d;
            highlight_color = color(d.datasource);  // added this to make highlight color same as the color of the node
            if (highlight_color !== 'white') {
                circle.style(toWhite, function(o) { return isConnected(d, o) ? highlight_color : 'white' });
                text.style('font-weight', function(o) { return isConnected(d, o) ? 'bold' : 'normal' });
                link.style('stroke', function(o) { return o.source.index === d.index || o.target.index === d.index ? highlight_color : ((isNumber(o.datasource) && o.datasource >= 0) ? color(o.datasource) : default_link_color) });
            }
        }
    }

    function get_rdfmts_graph_analysis(fed, source){
        if (fed == null || source == null || (fed === federation && source === gSource && gaLoaded === 1)) { return }

        $('#fedName').html(fed);
        $('#vizFedName').html(fed);
        $('#gaFedName').html(fed);
        $('#gaDsName').html(source);
        gSource = source;
        if (gtable == null) {
            gtable = $('#graph-analysis').DataTable({
                responsive: false,
                order: false,
                select: true,
                lengthMenu: [ [10, 25, 50, -1], [10, 25, 50, 'All'] ],
                dom: 'Blfrtip',
                buttons: table_buttons('mt-graph-analysis'),
                ajax: '/rdfmts/api/rdfmtanalysis?graph=' + fed + '&source=' + source
            });
            gaLoaded = 1;
        } else {
            gtable.clear().draw();
            gtable.ajax.url('/rdfmts/api/rdfmtanalysis?graph=' + fed + '&source=' + source).load();
        }
    }

    $('a[data-toggle="tab"]').on('shown.bs.tab', function (e) {
        const target = $(e.target).attr('href')  // activated tab
        if (target === '#visualize') { tabVisible = '#visualize' }
        else if (target === '#analysis') { tabVisible = '#analysis' }
        else { tabVisible = '#home' }
    });

    function addTooltip(circle) {  // generates a tooltip for an SVG circle element based on its ID
        const x = parseFloat(circle.attr('cx')),
              y = parseFloat(circle.attr('cy')),
              r = parseFloat(circle.attr('r')),
              text = circle.attr('id');

        let tooltip = d3.select('#plot')
            .append('text')
            .text(text)
            .attr('x', x)
            .attr('y', y)
            .attr('dy', -r * 2)
            .attr('id', 'tooltip');

        const offset = tooltip.node().getBBox().width / 2;

        if ((x - offset) < -radius) {
            tooltip.attr('text-anchor', 'start')
                .attr('dx', -r);
        } else if ((x + offset) > radius) {
            tooltip.attr('text-anchor', 'end')
                .attr('dx', r);
        } else {
            tooltip.attr('text-anchor', 'middle')
                .attr('dx', 0);
        }
    }

    // Draws an arc diagram for the provided undirected graph
    function drawGraph(graph) {
        let zoom = d3.behavior.zoom().scaleExtent([min_zoom,max_zoom]);
        // create svg image
        graph_container.empty();
        let circumference = 22 * graph.nodes.length;
        let wh = 2 * circumference / Math.PI;
        if (wh < 200) wh = 200;
        if (wh > 1200) wh = 1200;

        diameter = wh;
        let svg = d3.select('#graph').append('svg');
        svg.append('g').classed('chartLayer', true)
            .attr('width', graph_container.width())
            .attr('height', 980);

        radius = wh/2;
        // create plot area within svg image
        let plot = svg.append('g')
            .attr('width', wh)
            .attr('height', wh)
            .attr('id', 'plot')
            .attr('transform', 'translate(' + graph_container.width() / 2 + ', ' + 980 / 2 + ')');

        zoom.on('zoom', function() { plot.attr('transform', 'translate(' + d3.event.translate + ')scale(' + d3.event.scale + ')') });
        svg.call(zoom);

        circleLayout(graph.nodes);  // calculate node positions
        drawCurves(graph.links);  // draw edges first
        drawNodes(graph.nodes);  // draw nodes last
    }

    // Calculates node locations
    function circleLayout(nodes) {
        // sort nodes by group
        nodes.sort(function(a, b) { return a.datasource - b.datasource });

        // used to scale node index to theta value
        const scale = d3.scale.linear()
            .domain([0, nodes.length])
            .range([0, 2 * Math.PI]);

        // Transform polar coordinates into Cartesian coordinates
        nodes.forEach(function(d, i) {
            const theta  = scale(i),
                  radial = radius - margin;
            d.x = radial * Math.sin(theta);
            d.y = radial * Math.cos(theta);
        });
    }

    function dragged(d) {
        d.x = d3.event.x;
        d.y = d3.event.y;
        d3.select(this).attr('cx', d.x).attr('cy', d.y);
        circularLink.filter(function(l) { return l.source === d }).attr('x1', d.x).attr('y1', d.y);
        circularLink.filter(function(l) { return l.target === d }).attr('x2', d.x).attr('y2', d.y);
        circularLink.filter(function(l) { return l.source === d }).attr('d', curve);
        circularLink.filter(function(l) { return l.target === d }).attr('d', curve);
    }

    // Draws nodes with tooltips
    function drawNodes(nodes) {
        circularNode = d3.select('#plot').selectAll('.node')
            .data(nodes)
            .enter()
            .append('circle')
            .attr('class', 'node')
            .attr('id', function(d) { return d.label })
            .attr('cx', function(d) { return d.x })
            .attr('cy', function(d) { return d.y })
            .attr('r', 10)
            .style('fill', function(d) { return color(d.datasource) })
            .on('mouseover', function() { addTooltip(d3.select(this)) })
            .on('mouseout', function() { d3.select('#tooltip').remove() })
            .call(d3.behavior.drag().on('drag', dragged));
    }

    // Draws curved edges between nodes
    function drawCurves(links) {
        circularLink = d3.select('#plot').selectAll('.link')
            .data(links)
            .enter()
            .append('path')
            .attr('class', 'link')
            .style('stroke-width', nominal_stroke)
            .style('stroke', default_link_color)
            .attr('d', curve);
    }
});
