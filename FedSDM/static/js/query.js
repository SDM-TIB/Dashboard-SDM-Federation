$(function() {
    let yasqe = null,
        start = false,
        end = true,
        query = null,
        querytriples = [];
    var vizdata = {nodes: {}, links: []};
    var queryvars = []
    let table = null, selectedRow = null, selectedRowData = [],
        nodes = [], links = [];
    $('#selectfederation').prop('disabled', true);

    let federation = $('#federations-list').val();

    if (federation != null && federation !== '') {
        $('#queryrow').show();
        $('#resultrow').hide();
    } else {
        $('#resultinfo').hide();
        $('#resstatus').hide();
        $('#queryrow').hide();
        $('#resultrow').hide();
    }

    $('#federations-list').on('change', function() {
        federation = $(this).val();
        $('#queryrow').show();
        $('#resultrow').hide();
        if (yasqe == null) {
            initialize_yasqe()
        }
    });

    if (federation !== '') {
        initialize_yasqe()
    }

    function initialize_yasqe() {
        yasqe = YASQE(document.getElementById('yasqe'), {
            // display full query
            viewportMargin: Infinity,
            // grey edit window during query execution
            backdrop: true,
            // modify codemirror tab handling to solely use 2 spaces
            tabSize: 2,
            indentUnit: 2,
            extraKeys: {
                Tab: function(cm) {
                    cm.replaceSelection(new Array(cm.getOption('indentUnit') + 1).join(' '));
                }
            },
            sparql: {
                showQueryButton: true,
                endpoint: '/query/sparql',
                callbacks: {
                    beforeSend: function(jqXHR, setting) {
                        //console.log('federation to query from', federation);
                        $('#resstatus').hide();
                        $('#visualizebtn').hide();
                        $('#showtablebtn').hide();
                        setting.url = '/query/sparql?federation=' + federation + '&query=' + encodeURIComponent(yasqe.getValue());
                        setting.crossDomain = true;
                        setting.data ={'query': yasqe.getValue()};
                        $('#resultinfo').hide();
                        $('#queryresultstable').empty();
                    },
                    success: function(data) {
                        $('#resulttablediv').empty()
                            .append('<table style="width: 100%" class="table table-striped table-bordered table-hover" id="queryresultstable"></table>')

                        if ('error' in data) {
                            $('#resstatus').html('Error:' + data.error);
                            $('#resultrow').show();
                            $('#resstatus').show();
                            $('#resultinfo').show();
                            return true
                        }
                        resdrawn = false;
                        $('#frestime').html(' ' + data.firstResult + ' sec');
                        $('#exetime').html(' ' + data.execTime + ' sec');

                        query = encodeURIComponent(yasqe.getValue());

                        let results = data.result,
                            vars = []
                        if (results.length > 0) {
                            $('#resstatus').hide();
                            $('#resultinfo').show();
                            $('#resultrow').show();

                            vars = data.vars;

                            var theader = '<thead><tr>';
                            var tfooter = '<tfoot><tr>';
                            for (var i = 0; i < vars.length; i++) {
                                theader =  theader + '<th>' + vars[i] + '</th> ';
                                tfooter =  tfooter + '<th>' + vars[i] + '</th> ';
                                queryvars.push(vars[i]);
                            }
                            $('#queryresultstable').append(theader + '</tr></thead>')
                                .append('<tbody></tbody>')
                                .append(tfooter + '</tr></tfoot>');

                            table = $('#queryresultstable').DataTable({
                                responsive: false,
                                select: true,
                                lengthMenu: [ [10, 25, 50, -1], [10, 25, 50, 'All'] ],
                                dom: 'Blfrtip',
                                buttons: table_buttons('sparql-results')
                            });
                            querytriples = data.querytriples;
                            var resmap = {}
                            for (let i = 0; i < results.length; i++) {
                                var row = results[i];
                                var rowml = [];
                                for (var j = 0; j < vars.length; j++) {
                                    var val = row[vars[j]];
                                    if (val.indexOf('^^<') !== -1) {
                                        val = val.substring(0, val.indexOf('^^'));
                                    }
                                    if ('http' === val.substring(0, 4)) {
                                        // rowml.push('<a href="' + val + '"> &lt;' + val + '&gt;</a>');
                                        rowml.push(val);
                                    } else {
                                        rowml.push(val);
                                    }
                                    resmap[vars[j]] = val;
                                }
                                table.row.add(rowml).draw(false);
                                // append_nodes_edges(resmap, querytriples);
                            }
                            table.columns().every(function() {
                                var column = this;
                                var select = $('<select><option value="">All</option></select>')
                                    .appendTo($(column.footer()).empty())
                                    .on('change', function() {
                                        var val = $.fn.dataTable.util.escapeRegex($(this).val());

                                        console.log(val);
//
//                                var ltix = val.indexOf('&lt;');
//                                if (ltix > 0) {
//                                    val = val.substring(ltix + 4, val.indexOf('&gt;'));
//                                }
                                        column.search(val ? '^' + val + '$' : '', true, false).draw();
                                    } );
                                //console.log(column.data().unique());
                                column.data().unique().sort().each(function(d, j) {
                                    var val = d;
                                    var ltix = val.indexOf('&lt;');
                                    if (ltix > 0) {
                                        val = val.substring(ltix+4, val.indexOf('&gt;'));
                                    }
                                    // console.log('data', d, val);
                                    select.append('<option value=' + d + '>' + d + '</option>');
                                } );
                            });
                            table.on('select', function(e, dt, type, indexes) {
                                selectedRow = table.rows( indexes ).data().toArray();

                                selectedRowData = [];
                                for (i in selectedRow[0]) {
                                    var ltix = selectedRow[0][i].indexOf('&lt;');
                                    if (ltix > 0) {
                                        var value = selectedRow[0][i].substring(ltix + 4, selectedRow[0][i].indexOf('&gt;'));
                                        selectedRowData.push(value)
                                    } else {
                                        selectedRowData.push(selectedRow[0][i])
                                    }
                                }

                                //console.log('selected row:', selectedRowData, ltix);

                                $('#addfeedback').prop('disabled', false);
                            }).on('deselect', function(e, dt, type, indexes) {
                                var rowData = table.rows(indexes).data().toArray();
                                $('#addfeedback').prop('disabled', true);
                                selectedRow = null;
                            });
                        } else {
                            $('#resstatus').html('No results found!')
                                .show();
                            $('#resultinfo').show();
                            $('#resultrow').show();
                            response = false;
                            return true;
                        }
                        response = true;
                        $('#stopbutton').prop('disabled', false);
                        show_incremental(vars);
                    } // end of sparql success callback function
                }
            },
            value: 'SELECT DISTINCT ?concept WHERE {\n\t?s a ?concept\n} LIMIT 10'
        });
        query = encodeURIComponent(yasqe.getValue());
        // register our custom autocompleters
        YASQE.registerAutocompleter('customPropertyCompleter', customPropertyCompleter);
        YASQE.registerAutocompleter('customClassCompleter', customClassCompleter);
        // and, to make sure we don't use the other property and class autocompleters, overwrite the default enabled completers
        YASQE.defaults.autocompleters = ['customClassCompleter', 'customPropertyCompleter'];
    }

    $('#visualizebtn').hide()
    $('#visualizebtn').on('click', function() {
        $('#resultgraphdiv').show();
        $('#resulttablediv').hide();
        $('#visualizebtn').hide();
        $('#showtablebtn').show();
        drawresults();
        $('#resultgraphdiv').show();
    });
    $('#showtablebtn').on('click', function() {
        $('#resultgraphdiv').hide();
        $('#resulttablediv').show();
        $('#visualizebtn').show();
        $('#showtablebtn').hide();
    });

    var mnodes = [],
        malinks = []
    mlinks = [];
    var msourcenodes = [],
        msourcelinks = [];
    var mtcards = {'All': []};
    var resdrawn = false;
    function drawresults() {
        if (resdrawn === true) {
            return
        }
        mlinks = vizdata.links;
        mnodes = vizdata.nodes;

        for (var i = 0; i < mlinks.length; ++i) {
            o = mlinks[i];

            o.source = mnodes[o.source];
            o.target = mnodes[o.target];
            //console.log(o);
            if (o.source.datasource === o.target.datasource) {
                if (o.source.datasource in msourcelinks) {
                    msourcelinks[o.source.datasource].push(o);
                } else {
                    msourcelinks[o.source.datasource] = [o];
                }
            }
        }
        malinks = mlinks;

        flatnodes = [];
        $.each(mnodes, function (key, val) {
            flatnodes.push(val);
            mtcards['All'].push({'label': val.label, 'value': val.weight}); //, 'color': color(val.datasource)
            if (val.datasource in mtcards) {
                mtcards[val.datasource].push({'label': val.label, 'value': val.weight}); //, 'color': color(val.datasource)
            } else {
                mtcards[val.datasource] = [{'label': val.label, 'value': val.weight}]; // , 'color': color(val.datasource)
            }
            if (val.datasource in msourcenodes) {
                msourcenodes[val.datasource].push(val);
            } else {
                msourcenodes[val.datasource] = [val]
            }
        });
        mnodes = flatnodes;
        manodes = mnodes ;

        data = {nodes: manodes, links: malinks};
        //console.log('nodes:', manodes)
        //console.log('links:', malinks)
        manodes.forEach(function(d) {
            expand[d.datasource] = true;
        });
        resdrawn = true;
        drawRDFMTS(manodes, malinks, 'mtviz');
    }

    const addfeedbackdialog = $('#feedbackModal');
    const addfeedbackform = addfeedbackdialog.find('form').on('submit', function(event) {
        event.preventDefault();
        addFeedback(true);
    });
    const feedbackdesc =  $('#feedbackdesc'),
          fedbackpreds = $('#fedbackpreds'),
          allfeedbackFields = $([]).add(feedbackdesc).add(fedbackpreds);

    addfeedbackdialog.on('shown.bs.modal', function() {
        feedbackdesc.trigger('focus');
    });
    addfeedbackdialog.on('hidden.bs.modal', function() {
        addfeedbackform[0].reset();
        allfeedbackFields.removeClass('ui-state-error');
        resetTips();
    });
    $('#add-feedback-btn').on('click', function() {
       addFeedback(true);
    });

    function addFeedback(close) {
        allfeedbackFields.removeClass('ui-state-error');
        let valid_pred = checkSelection(fedbackpreds, 'column');
        let valid_desc = checkLength(feedbackdesc, 'description', 2, 500);
        const valid = valid_desc && valid_pred;
        data = {
            'desc': feedbackdesc.val(),
            'pred': fedbackpreds.val(),
            'query': yasqe.getValue(),
            'row': selectedRowData,
            'columns': queryvars
        };
        console.log(data);

        if (valid) {
            $.ajax({
                type: 'POST',
                headers: {
                    Accept : 'application/json'
                },
                url: '/query/feedback?fed=' + federation,
                data: {
                    'desc': feedbackdesc.val(),
                    'pred': fedbackpreds.val(),
                    'query': yasqe.getValue(),
                    'row': selectedRowData,
                    'columns': queryvars
                },
                dataType: 'json',
                crossDomain: true,
                success: function(data, textStatus, jqXHR) {
                    console.log(data);
                    if (data != null && data.length > 0) {
                        // load_query_table(data);
                    } else {
                        $('#validateTips').html('Error while adding feedback!')
                    }
                    // showqueries(datasource);
                },
                error: function(jqXHR, textStatus, errorThrown) {
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                }
            });
        } else {
            close = false;
            console.log('Invalid data...');
        }
        if (close) {
            addfeedbackdialog.modal('hide');
        }
        return valid;
    }

    $('#addfeedback').on('click', function() {
        fedbackpreds.empty()
                    .append('<option value="-1">Select column</option>');
        for (const d in queryvars) {
            fedbackpreds.append('<option value=' + queryvars[d] + '> ' + queryvars[d] + '</option>');
        }
        fedbackpreds.append('<option value="All">All</option>');
    });

    function append_nodes_edges(rowmap, qtripl) {
        for (t in qtripl) {
            t = qtripl[t];
            if (t.s.indexOf('?') === 0) {
                variab = t.s.substring(1, t.s.length);
                s = rowmap[variab];
                setNodeData(s);
            } else {
                s = t.s
                setNodeData(s);
            }
            if (t.p.indexOf('?') === 0) {
                variab = t.p.substring(1, t.p.length);
                //setNodeData(rowmap, variab);
                p = rowmap[variab];
            } else {
                p = t.p;
            }
            if (t.o.indexOf('?') === 0) {
                variab = t.o.substring(1, t.o.length);
                // setNodeData(rowmap, variab);
                o = rowmap[variab];
                setNodeData(o);
            } else {
                o = t.o
                setNodeData(o);
            }
            setEdgeData(s, p, o);
        }
    }

    function setEdgeData(s, p, o) {
        // console.log(s, p, o);
        vizdata.links.push({
            'source': s,
            'target': o,
            'weight': -1,
            'ltype': 'link',
            'type': 'link',
            'pred': p
        });
    }

    function setNodeData(n) {
        vizdata.nodes[n] = {
            'id': n,
            'label': n,
            'datasource': 1,
            'weight': -1,
            'type': 'circle'
        };
    }

    let response = false;
    function show_incremental(vars) {
        if (response === true) {
            // This makes it unable to send a new request
            // unless you get response from last request
            response = false;
            if (shouldstop === false) {
                let req = $.ajax({
                    type: 'GET',
                    url: '/query/nextresult',
                    // headers: {Accept: 'text/csv'},//ask for csv. Simple, and uses less bandwidth
                    success: function(data) {
                        let row = data.result;
                        // console.log('row', row);
                        if (row.length === 0 || row === 'EOF') {
                            // console.log('loop done');
                            resdrawn = false;
                            $('#visualizebtn').show();
                            drawresults();
                            $('#exetime').html(' ' + data.execTime + ' sec');
                            response = false;
                            return
                        }
                        $('#exetime').html(' ' + data.execTime + ' sec');
                        const rowml = [],
                            resmap = {};
                        for (let j = 0; j < vars.length; j++) {
                            let val = row[vars[j]];
                            if (val.indexOf('^^<') !== -1) {
                                val = val.substring(0, val.indexOf('^^'));
                            }
                            if ('http' === val.substring(0, 4)) {
                                // rowml.push('<a href="' + val + '"> &lt;' + val + '&gt;</a>');  TODO: check if this should be added again
                                rowml.push(val);
                            } else {
                                rowml.push(val);
                            }
                            resmap[vars[j]] = val;
                        }

                        table.row.add(rowml).draw(false);
                        append_nodes_edges(resmap, querytriples);

                        table.columns().every(function() {
                            const column = this;
                            const select = $('<select><option value="">All</option></select>')
                                .appendTo($(column.footer()).empty())
                                .on('change', function() {
                                    const val = $.fn.dataTable.util.escapeRegex(
                                        $(this).val()
                                    );
                                    column.search(val ? '^' + val + '$' : '', true, false).draw();
                                });
                            column.data().unique().sort().each(function(d, j) {
                                let val = d;
                                const ltix = val.indexOf('&lt;');
                                if (ltix > 0) {
                                    val = val.substring(ltix+4, val.indexOf('&gt;'));
                                }
                                // console.log('data', d, val);
                                select.append('<option value=' + val + '>' + val + '</option>')
                            });
                        });
                        response = true;
                    }
//                    ,
//                        error: function(jqXHR, textStatus, errorThrown){
//                            console.log(jqXHR.status);
//                            console.log(jqXHR.responseText);
//                            console.log(textStatus);
//                        }
                });
                req.done(function() {
                    // console.log('Request successful!');
                    // This makes it able to send new request on the next interval
                    if (response === true && shouldstop === false) {
                        response = true;
                        show_incremental(vars)
                    } else {
                        shouldstop = false;
                        $('#stopbutton').prop('disabled', true);
                    }
                });
            }
        }
        // setTimeout(,1000);
    }
    let shouldstop = false;
    $('#stopbutton').on('click', function() {
        console.log('stop pressed');
        response = false;
        shouldstop = true;
    });

    /**
     * We use most of the default settings for the property and class autocompletion types. This includes:
     * -  the pre-/post-processing of tokens
     * -  detecting whether we are in a valid autocompletion position
     * -  caching of the suggestion list. These are cached for a period of a month on the client side.
     */
    var getAutocompletionsArrayFromJson = function(result) {
        var completionsArray = [];
        // console.log('parsing');
        // console.log(result);
        result.forEach(function(row) {  // remove first line, as this one contains the projection variable
            // console.log(row);
            if ('type' in row) {
                completionsArray.push(row['type']);  // remove quotes
            } else {
                completionsArray.push(row['property']);  // remove quotes
            }
        });
        return completionsArray;
    }

    var customPropertyCompleter = function(yasqe) {
        // we use several functions from the regular property autocompleter (this way, we don't have to re-define code such as determining whether we are in a valid autocompletion position)
        var returnObj = {
            isValidCompletionPosition: function() { return YASQE.Autocompleters.properties.isValidCompletionPosition(yasqe) },
            preProcessToken: function(token) { return YASQE.Autocompleters.properties.preProcessToken(yasqe, token) },
            postProcessToken: function(token, suggestedString) { return YASQE.Autocompleters.properties.postProcessToken(yasqe, token, suggestedString) }
        };

        // in this case we assume the properties will fit in memory. So, turn on bulk loading, which will make autocompleting a lot faster
        returnObj.bulk = true;
        returnObj.async = true;

        // and, as everything is in memory, enable autoShowing the completions
        returnObj.autoShow = true;

        returnObj.persistent = 'customProperties';  // this will store the sparql results in the client-cache for a month.
        returnObj.get = function(token, callback) {
            // all we need from these parameters is the last one: the callback to pass the array of completions to
            var sparqlQuery = 'SELECT DISTINCT ?property WHERE { ?s ?property ?obj } LIMIT 1000';
            $.ajax({
                data: {query: sparqlQuery},
                url: YASQE.defaults.sparql.endpoint,
                // headers: {Accept: 'text/csv'},//ask for csv. Simple, and uses less bandwidth
                success: function(data) {
                    // console.log(sparqlQuery);
                    // console.log(data);
                    callback(getAutocompletionsArrayFromJson(data.result));
                }
            });
        };
        return returnObj;
    };

    var customClassCompleter = function(yasqe) {
        var returnObj = {
            isValidCompletionPosition: function() { return YASQE.Autocompleters.classes.isValidCompletionPosition(yasqe) },
            preProcessToken: function(token) { return YASQE.Autocompleters.classes.preProcessToken(yasqe, token) },
            postProcessToken: function(token, suggestedString) { return YASQE.Autocompleters.classes.postProcessToken(yasqe, token, suggestedString) }
        };
        returnObj.bulk = true;
        returnObj.async = true;
        returnObj.autoShow = true;
        returnObj.get = function(token, callback) {
            var filters = 'FILTER (!regex(str(?type), "http://www.w3.org/ns/sparql-service-description", "i") && ' +
                ' !regex(str(?type), "http://www.openlinksw.com/schemas/virtrdf#", "i") && ' +
                ' !regex(str(?type), "http://www.w3.org/2000/01/rdf-schema#", "i") && ' +
                ' !regex(str(?type), "http://www.w3.org/1999/02/22-rdf-syntax-ns#", "i") && ' +
                ' !regex(str(?type), "http://purl.org/dc/terms/Dataset", "i") && ' +
                ' !regex(str(?type), "http://www.w3.org/2002/07/owl#", "i") && ' +
                ' !regex(str(?type), "http://rdfs.org/ns/void#", "i") && ' +
                ' !regex(str(?type), "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/", "i") && '+
                ' !regex(str(?type), "nodeID://", "i") ) '
            var sparqlQuery = 'SELECT DISTINCT ?type WHERE{ ?s a ?type. ' + filters + ' } LIMIT 1000';
            $.ajax({
                data: {query: sparqlQuery},
                url: YASQE.defaults.sparql.endpoint,
                // headers: {Accept: 'text/csv'},//ask for csv. Simple, and uses less bandwidth
                success: function(data) {
                    // console.log(sparqlQuery);
                    // console.log(data);
                    callback(getAutocompletionsArrayFromJson(data.result));
                }
            });
        };
        return returnObj;
    };

    $('#classes').on('click', function() {
        yasqe.setValue('SELECT DISTINCT ?c WHERE {\n\t?s a ?c\n}');
    });

    $('#analyticalnumtheoryex').on('click', function() {
        yasqe.setValue('PREFIX schema: <http://schema.org/> \n' +
            'PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n' +
            'SELECT DISTINCT * WHERE {\n' +
            '\t<http://av.tib.eu/resource/video/16439> ?p ?obj .\n' +
            '}  LIMIT 100');
    });

    // TODO: add more example queries here

    function nodeid(n) {
        return n.size ? '_g_' + n.datasource : n.label;
    }

    function linkid(l) {
        var u = nodeid(l.source),
            v = nodeid(l.target);
        return u < v ? u + '|' + v : v + '|' + u;
    }

    function getGroup(n) { return n.datasource; }

    var off = 15,    // cluster hull offset
        expand = {}, // expanded clusters
        net, force, hullg, hull, linkg, nodeg;

    // constructs the network to visualize
    function network(data, prev, index, expand) {
        expand = expand || {};
        var gm = {},    // group map
            nm = {},    // node map
            lm = {},    // link map
            gn = {},    // previous group nodes
            gc = {},    // previous group centroids
            nodes = [], // output nodes
            links = []; // output links

        // process previous nodes for reuse or centroid calculation
        if (prev) {
            prev.nodes.forEach(function(n) {
                var i = index(n), o;

                if (n.size > 0) {
                    gn[i] = n;
                    n.size = 0;
                } else {
                    o = gc[i] || (gc[i] = {x: 0, y: 0, count: 0});
                    o.x += n.x;
                    o.y += n.y;
                    o.count += 1;
                }
            });
        }

        // determine nodes
        for (var k = 0; k < data.nodes.length; ++k) {
            var n = data.nodes[k],
                i = index(n),
                l = gm[i] || (gm[i] = gn[i]) || (gm[i]={datasource: i, size: 0, nodes: []});

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

        for (i in gm) { gm[i].link_count = 0; }

        // determine links
        for (k = 0; k < data.links.length; ++k) {
            var e = data.links[k],
                u = index(e.source),
                v = index(e.target);
            if (u != v) {
                gm[u].link_count++;
                gm[v].link_count++;
            }
            u = expand[u] ? nm[e.source.label] : nm[u];
            v = expand[v] ? nm[e.target.label] : nm[v];
            var i = (u < v ? u + '|' + v : v + '|' + u),
                l = lm[i] || (lm[i] = {source: u, target: v, size: 0});
            l.size += 1;
        }
        for (i in lm) { links.push(lm[i]); }

        return {nodes: nodes, links: links};
    }

    function convexHulls(nodes, index, offset) {
        var hulls = {};

        // create point sets
        for (var k = 0; k < nodes.length; ++k) {
            var n = nodes[k];

            if (n.size) continue;
            var i = index(n),
                l = hulls[i] || (hulls[i] = []);
            l.push([n.x-offset, n.y-offset]);
            l.push([n.x-offset, n.y+offset]);
            l.push([n.x+offset, n.y-offset]);
            l.push([n.x+offset, n.y+offset]);
        }

        // create convex hulls
        var hullset = [];
        for (i in hulls) {
            hullset.push({datasource: i, path: d3.geom.hull(hulls[i])});
        }

        return hullset;
    }
    var curve = d3.svg.line()
        .interpolate('cardinal-closed')
        .tension(.85);
    function drawCluster(d) {
        // console.log('drawcluster', d)
        return curve(d.path); // 0.8
    }
    width = $('#graph').width();
    height = 980;
    var canv = 'graph';

    var width, height;
    var h = 960, w = 760;
    var chartWidth, chartHeight;
    var margin;

    var drag = d3.behavior.drag()
        .origin(function(d) { return d; })
        .on('dragstart', dragstarted)
        .on('drag', dragged)
        .on('dragend', dragended);

    function dragstarted(d) {
        d3.event.sourceEvent.stopPropagation();
        d3.select(this).classed('dragging', true);
    }

    function dragged(d) {
        d3.select(this).attr('cx', d.x = d3.event.x).attr('cy', d.y = d3.event.y);
    }

    function dragended(d) {
        d3.select(this).classed('dragging', false);
    }

    var keyc = true, keys = true, keyt = true, keyr = true, keyx = true, keyd = true, keyl = true, keym = true, keyh = true, key1 = true, key2 = true, key3 = true, key0 = true

    var focus_node = null, highlight_node = null;

    var text_center = false;
    var outline = false;

    var min_score = 0;
    var max_score = 1;
    var highlight_color = '#A52A2A';
    var highlight_trans = 0.1;

    var size = d3.scale.pow().exponent(1)
        .domain([1,100])
        .range([8,36]);
    // The largest node for each cluster.

    var default_node_color = '#ccc';
    //var default_node_color = 'rgb(3,190,100)';
    var default_link_color = '#888';
    var nominal_base_node_size = 8;
    var nominal_text_size = 10;
    var max_text_size = 24;
    var nominal_stroke = 1.5;
    var max_stroke = 4.5;
    var max_base_node_size = 40;
    var min_zoom = 0.1;
    var max_zoom = 7;

    var loaded = 0;
    var vized = 0;
    var stats = null;
    var galoaded = 0;
    var gtable = null;
    var force = null;
    var linkdistance = 150;
    //var charge = -400;
    var nfit = 0;
    var ncharge = -600;
    var ngravity = 0;
    var sourcescard = 0;
    var sources = null;
    var sourcemt = null;
    //list of subjects and objects for the DAG
    var sourcenodes = [];
    //connection link between subject and object ->predicates
    var sourcelinks = [];
    var sourceids = {}, sourcesnames = {};

    var link, circle, text;
    function drawRDFMTS(nodes, links, divcanv) {
        // console.log(nodes, links);
        var svg;
        if(divcanv == null) {
            $('#graph').empty();
            svg = d3.select('#graph').append('svg');
            width = $('#graph').width();
            height = 980;
            canv = 'graph'
        } else {
            $('#graph').empty();
            svg = d3.select('#graph').append('svg');
            width = $('#graph').width();
            height = 980;
            // console.log('showing ...')
            $('#graph').show();
            canv = 'graph'
        }
        var chartLayer = svg.append('g').classed('chartLayer', true);
        var zoom = d3.behavior.zoom().scaleExtent([min_zoom,max_zoom])
        var g = svg.append('g');

        hullg = svg.append('g');
        linkg = svg.append('g');
        nodeg = svg.append('g');

        svg.attr('opacity', 1e-6)
            .transition()
            .duration(1000)
            .attr('opacity', 1);

        var tocolor = 'fill';
        var towhite = 'stroke';
        if (outline) {
            tocolor = 'stroke'
            towhite = 'fill'
        }

        svg.style('cursor', 'move');
        // d3.json('graph.json', function(error, graph) {
        var linkedByIndex = {};
        links.forEach(function(d) {
            linkedByIndex[d.source + ',' + d.target] = true;
        });

        //var ctx = svg.getContext('2d');
        var fit = Math.sqrt(nodes.length / (width * height));
        var charge = (-1 / fit);
        var gravity = (8 * fit);
        ngravity = gravity;
        ncharge = charge;
        if (force) force.stop()
        //data = {nodes:nodes, links:links}
        net = network(data, net, getGroup, expand);
        //console.log(net, expand)
        force = d3.layout.force()
            .nodes(net.nodes)
            .links(net.links)
            .linkDistance(function(l, i) {
                var n1 = l.source, n2 = l.target;
                return divcanv?250:200 +
                    Math.min(20 * Math.min((n1.size || (n1.datasource != n2.datasource ? n1.group_data.size : 0)),
                        (n2.size || (n1.datasource != n2.datasource ? n2.group_data.size : 0))),
                        -30 +
                        30 * Math.min((n1.link_count || (n1.datasource != n2.datasource ? n1.group_data.link_count : 0)),
                            (n2.link_count || (n1.datasource != n2.datasource ? n2.group_data.link_count : 0))),
                        300);
            })
            .linkStrength(function(l, i) { return  2; })
            .gravity(0.05)   // 0.05 gravity+charge tweaked to ensure good 'grouped' view (e.g. green group not smack between blue&orange, ...
            .charge(-600)    // ... charge is important to turn single-linked groups to the outside
            .friction(0.5)   // friction adjusted to get dampened display: less bouncy bouncy ball [Swedish Chef, anyone?]
            .size([width, height])
            .start(); //.chargeDistance(1000) .linkDistance(300)

        link = g.selectAll('.link').data(net.links, linkid);
        link.exit().remove();
        link.enter().append('line')
            .attr('class', 'link')
            .attr('x1', function(d) { return d.source.x; })
            .attr('y1', function(d) { return d.source.y; })
            .attr('x2', function(d) { return d.target.x; })
            .attr('y2', function(d) { return d.target.y; })
            .style('stroke-width', nominal_stroke)
            .style('stroke', function(d) {
                return color(d.datasource);
            });

        var dr = 3;
        node = g.selectAll('.node').data(net.nodes, nodeid);
        node.exit().remove();
        node.enter().append('g')
            .attr('class', function(d) { return 'node' + (d.size ? '' : ' leaf'); })
            //.attr('r', function(d) {console.log(d.size, dr); return d.size ? d.size + dr : 3; })
            .attr('cx', function(d) { return d.x; })
            .attr('cy', function(d) { return d.y; })
            .on('dblclick', function(d) {
                //console.log(d, expand[d.datasource])
                expand[d.datasource] = !expand[d.datasource];
                drawRDFMTS(nodes, links, divcanv);
            })
            //            .on('dblclick.zoom', function(d) {
            //                d3.event.stopPropagation();
            //                var dcx = ($('#graph').width()/2-d.x*zoom.scale());
            //                var dcy = (980/2-d.y*zoom.scale());
            //                zoom.translate([dcx,dcy]);
            //                 g.attr('transform', 'translate(' + dcx + ',' + dcy + ')scale(' + zoom.scale() + ')');
            //
            //           })
            .on('mouseover', function(d) { set_highlight(d); })
            .on('mousedown', function(d) {
                d3.event.stopPropagation();
                focus_node = d;
                set_focus(d)
                if (highlight_node === null)
                    set_highlight(d)
            })
            .on('mouseout', function(d) { exit_highlight(d); });

        node.call(force.drag);

        var ci = 0;
        var circle = node.append('path')
            .attr('d', d3.svg.symbol()
                .size(function(d) {
                    var v = d.size?Math.PI*Math.pow(size(65+d.size>200?200:d.size)||nominal_base_node_size,2):Math.PI*Math.pow(size(25)||nominal_base_node_size,2);
                    return v;}) //size(d.weight)
                .type(function(d) { return d.size? 'circle': d.type; })
            )
            .style(tocolor, function(d) {
                if (divcanv ==null) {
                    return color(d.datasource)
                } else {
                    ci += 1
                    return color(d.datasource + (ci - 1));
                }
            })
            .style('stroke-width', nominal_stroke)
            .style(towhite, 'white');

        var text = g.selectAll('.text')
            .data(net.nodes)
            .enter().append('text')
            .attr('dy', '.35em')
            .style('font-size', function(d){ return d.size ? 16 + 'px' : nominal_text_size + 'px' })

        if (text_center) {
            text.text(function (d) {
                if (d.label) {
                    return d.label;
                } else {
                    return sourcesnames[d.datasource];
                }
            })
                .style('text-anchor', 'middle');
        } else {
            text.attr('dx', function (d) { return (size(65) - size(30) || nominal_base_node_size); })  // size(d.weight)
                .text(function (d) { if (d.label) return '\u2002' + d.label; else return '\u2002' + sourcesnames[d.datasource]; });
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
            if (highlight_node === null)
                exit_highlight();
        });

        zoom.on('zoom', function() {
            var stroke = nominal_stroke;
            if (nominal_stroke * zoom.scale() > max_stroke)
                stroke = max_stroke / zoom.scale();

            link.style('stroke-width', stroke);
            circle.style('stroke-width',stroke);

            var base_radius = nominal_base_node_size;
            if (nominal_base_node_size * zoom.scale() > max_base_node_size)
                base_radius = max_base_node_size / zoom.scale();
            circle.attr('d', d3.svg.symbol()
                .size(function(d) {
                    var v = d.size?Math.PI*Math.pow(size(65+d.size>200?200:d.size)*base_radius/nominal_base_node_size||base_radius,2):Math.PI*Math.pow(size(25)*base_radius/nominal_base_node_size||base_radius,2);
                    return v;}) //size(d.weight)
                .type(function(d) { return d.size ? 'circle' : d.type; })
            );
            //circle.attr('r', function(d) { return (size(d.size) * base_radius / nominal_base_node_size || base_radius); })
            if (!text_center)
                text.attr('dx', function(d) {
                    return ((size(65) - size(30)) * base_radius / nominal_base_node_size || base_radius);
                }); //size(d.weight)

            text.style('font-size', function(d) {
                var text_size = nominal_text_size;
                if (d.size) {
                    text_size = 16;
                }

                if (nominal_text_size * zoom.scale() > max_text_size)
                    text_size = max_text_size / zoom.scale();

                return text_size + 'px'});
            g.attr('transform', 'translate(' + d3.event.translate + ')scale(' + d3.event.scale + ')');
        });

        svg.call(zoom);

        resize();
        //window.focus();
        d3.select(window).on('resize', resize).on('keydown', keydown);
        var centroids = {};
        for (var i = 0; i < max_score; i += 3) {
            centroids[i] = {x: 200 * (i/3 +1), y:200}
            centroids[i+1] = {x: 200 * (i/3+1), y:400}
            centroids[i+2] = {x: 200 * (i/3 +1), y:600}
        }

        force.on('tick', function(e) {
            var k = .1 * e.alpha;
            // updateGroups();

            // Push nodes toward their designated focus.
            net.nodes.forEach(function(o, i) {
                if (centroids[o.datasource]) {
                    o.y += (centroids[o.datasource].y - o.y) * k;
                    o.x += (centroids[o.datasource].x - o.x) * k;
                }
            });

            text.forEach(function(o, i) {
                if (centroids[o.datasource]) {
                    o.y += (centroids[o.datasource].y - o.y) * k;
                    o.x += (centroids[o.datasource].x - o.x) * k;
                }
            });
            link.attr('x1', function(d) { return d.source.x; })
                .attr('y1', function(d) { return d.source.y; })
                .attr('x2', function(d) { return d.target.x; })
                .attr('y2', function(d) { return d.target.y; });

            node.each(printn())
                .attr('cx', function(d) { return d.x; })
                .attr('cy', function(d) { return d.y; });

            node.attr('transform', function(d) { return 'translate(' + d.x + ',' + d.y + ')'; });
            text.attr('transform', function(d) { return 'translate(' + d.x + ',' + d.y + ')'; });

        });
        function printn(alpha) {
            var quadtree = d3.geom.quadtree(nodes);
            return function(d) { };
        }

        function isConnected(a, b) {
            return linkedByIndex[a.index + ',' + b.index] || linkedByIndex[b.index + ',' + a.index] || a.index == b.index;
        }

        function hasConnections(a) {
            for (var property in linkedByIndex) {
                s = property.split(',');
                if ((s[0] == a.index || s[1] == a.index) && linkedByIndex[property])
                    return true;
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
                if (score >= 0.666)
                    return keyh;
                else if (score >= 0.333)
                    return keym;
                else if (score >= 0)
                    return keyl;
            }
            return true;
        }

        function vis_by_link_score(score) {
            if (isNumber(score)) {
                if (score >= 0.666)
                    return key3;
                else if (score >= 0.333)
                    return key2;
                else if (score >= 0)
                    return key1;
            }
            return true;
        }

        function isNumber(n) {
            return !isNaN(parseFloat(n)) && isFinite(n);
        }

        function resize() {
            var width = $('#' + canv).width(), height = 980;
            svg.attr('width', width).attr('height', height);
            force.size([force.size()[0] + (width - w) / zoom.scale(), force.size()[1] + (height - h) / zoom.scale()]).resume();
            w = width;
            h = height;
        }

        function keydown() {
            if (d3.event.keyCode == 32) {
                force.stop();
            }
            else if (d3.event.keyCode>=48 && d3.event.keyCode<=90 && !d3.event.ctrlKey && !d3.event.altKey && !d3.event.metaKey) {
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
                    var flag  = vis_by_type('circle') && vis_by_type('circle') && vis_by_node_score(d.source.datasource) && vis_by_node_score(d.target.datasource) && vis_by_link_score(d.datasource);
                    linkedByIndex[d.source.index + ',' + d.target.index] = flag;
                    return flag ? 'inline' : 'none';
                });
                node.style('display', function(d) {
                    return (key0 || hasConnections(d)) && vis_by_type('circle') && vis_by_node_score(d.datasource) ? 'inline' : 'none';
                });
                text.style('display', function(d) {
                    return (key0 || hasConnections(d)) && vis_by_type('circle') && vis_by_node_score(d.datasource) ? 'inline' : 'none';
                });

                if (highlight_node !== null) {
                    if ((key0 || hasConnections(highlight_node)) && vis_by_type('circle') && vis_by_node_score(highlight_node.datasource)) {
                        if (focus_node !== null)
                            set_focus(focus_node);
                        set_highlight(highlight_node);
                    } else {
                        exit_highlight();
                    }
                }
            }
        }

        function exit_highlight() {
            highlight_node = null;
            if (focus_node === null)
            {
                svg.style('cursor', 'move');
                if (highlight_color != 'white') {
                    circle.style(towhite, 'white');
                    text.style('font-weight', 'normal');
                    link.style('stroke', function(o) {
                        return (isNumber(o.datasource) && o.datasource >= 0) ? color(o.datasource) : default_link_color
                    });
                }
            }
        }

        function set_focus(d){
            if (highlight_trans < 1) {
                circle.style('opacity', function(o) {
                    return isConnected(d, o) ? 1 : highlight_trans;
                });

                text.style('opacity', function(o) {
                    return isConnected(d, o) ? 1 : highlight_trans;
                });

                link.style('opacity', function(o) {
                    return o.source.index == d.index || o.target.index == d.index ? 1 : highlight_trans;
                });
            }
        }

        function set_highlight(d) {
            svg.style('cursor', 'pointer');
            if (focus_node !== null)
                d = focus_node;
            highlight_node = d;
            // added this to make highlight color same as the color of the node
            highlight_color = color(d.datasource);
            if (highlight_color != 'white') {
                circle.style(towhite, function(o) {
                    return isConnected(d, o) ? highlight_color : 'white';
                });
                text.style('font-weight', function(o) {
                    return isConnected(d, o) ? 'bold' : 'normal';
                });
                link.style('stroke', function(o) {
                    return o.source.index == d.index || o.target.index == d.index ? highlight_color : ((isNumber(o.datasource) && o.datasource >= 0) ? color(o.datasource) : default_link_color);
                });
            }
        }
    }
});