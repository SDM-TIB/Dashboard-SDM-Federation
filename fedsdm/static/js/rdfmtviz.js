$(document).ready(function() {


//    $("#federationslist").prop("disabled", true);
    $("#datasources").prop("disabled", true);
    $("#mtdetails").prop('disabled', true);
    $("#mtviz").hide();

    var federation =  $("#federationslist").val();

    var tabvisible = '#home';
    if (federation != null && federation != ""){
        load_data(federation);
    }

    $("#federationslist").change(function(){
        fed = $( this ).val()
        load_data(fed);
    });

    function load_data(fed){

        $("#fedName").html(fed);
        $("#vfedName").html(fed);
        $("#afedName").html(fed);
        $("#datasources").empty();
        $("#graph").empty();
        $("#datasources").empty();
        $("#legend").empty();load_data
        $("#vdsname").html("");
        loaded = 0;
        vized = 0;
        galoaded = 0;
        get_rdfmts_stats(fed);
        $("#graph").html("<h1> Loading ... !</h1>");
        get_rdfmts(fed);
        get_rdfmts_graph_analys(fed);
        federation = fed;

    }
    var width,height;
    var h=960, w =760;
    var chartWidth, chartHeight;
    var margin;

    var drag = d3.behavior.drag()
        .origin(function(d) { return d; })
        .on("dragstart", dragstarted)
        .on("drag", dragged)
        .on("dragend", dragended);

    function dragstarted(d) {
      d3.event.sourceEvent.stopPropagation();
      d3.select(this).classed("dragging", true);
    }

    function dragged(d) {
      d3.select(this).attr("cx", d.x = d3.event.x).attr("cy", d.y = d3.event.y);
    }

    function dragended(d) {
      d3.select(this).classed("dragging", false);
    }

    var keyc = true, keys = true, keyt = true, keyr = true, keyx = true, keyd = true, keyl = true, keym = true, keyh = true, key1 = true, key2 = true, key3 = true, key0 = true

    var focus_node = null, highlight_node = null;

    var text_center = false;
    var outline = false;

    var min_score = 0;
    var max_score = 1;
    var highlight_color = "#A52A2A";
    var highlight_trans = 0.1;

    var size = d3.scale.pow().exponent(1)
      .domain([1,100])
      .range([8,36]);
    // The largest node for each cluster.
    var colors = [
        "#9ACD32",
        "#7B68EE",
        "#622569",
        "#CD5C5C",
        "#800008",
        "#6b5b95",
        "#5b9aa0",
        "#00FF00",
        "#00FFFF",
        "#FFA500",
        "#228B22",
        "#20B2AA",
        "#3CB371",
        "#00CED1",
        "#006400",
        "#2F4F4F",
        "#8B4513",
        "#FF7F50",
        "#483D8B",
        "#BC8F8F",
        "#808080",
        "#9932CC",
        "#FFA07A",
        "#1E90FF",
        "#191970",
        "#800000",
        "#FFD700",
        "#F4A460",
        "#778899",
        "#AFEEEE",
        "#A0522D",
        "#B8860B",
        "#F08080",
        "#BDB76B",
        "#20B2AA",
        "#D2691E",
        "#BA55D3",
        "#800080",
        "#CD5C5C",
        "#FFB6C1",
        "#FF00FF",
        "#FFEFD5",
        "#ADFF2F",
        "#6B8E23",
        "#66CDAA",
        "#8FBC8F",
        "#AFEEEE",
        "#ADD8E6",
        "#6495ED",
        "#4169E1",
        "#BC8F8F",
        "#F4A460",
        "#DAA520",
        "#B8860B",
        "#CD853F",
        "#D2691E",
        "#8B4513",
        "#A52A2A"

    ];
    function color(ig){
       ig = ig // 10 * 10;
       if (ig > 50){
        return  "#ccc";
       }
       return colors[ig];
    }
    var default_node_color = "#ccc";
    //var default_node_color = "rgb(3,190,100)";
    var default_link_color = "#888";
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
    var sourceids = {}, sourcesnames={};

    var anodes = [],
        alinks = [];
    var mtcards = {"All":[]};
    var viztype = null;
    var data={nodes:[], links:[]};

    var selectedRow = null;
    var mnodes = [],
        malinks = []
        mlinks = [];
    var msourcenodes = [],
        msourcelinks = [];
    var mmtcards = {"All":[]};


    $("#mtdetails").click(function(){
        $("#listofrdfmts").hide();
        $("#mtdetails").hide();
        $("#backtotable").show();
        var url = encodeURIComponent(selectedRow[0][2]);
        console.log('url:', url);
        $.ajax({
                type: 'GET',
                headers: {
                    Accept : "application/json"
                },
                url: '/rdfmts/api/mtdetails?mt=' + url + "&fed="+federation,
                dataType: "json",
                crossDomain: true,
                success: function(data, textStatus, jqXHR){
                    console.log(url)
                    console.log('detail returned:', data);
                    sources = data.sources;
                    mnodes = data.nodes;
                    mlinks = data.links;
                    msourcescard = sources.length;
                    for (var i = 0; i<sources.length; i++){
                       var v = sources[i].id;
                       var name  = sources[i].name;
                       sourceids[name] = v;
                       sourcesnames[v] = name;
                       }
                    for (var i=0; i<mlinks.length; ++i) {
                        o = mlinks[i];
                        o.source = mnodes[o.source];
                        o.target = mnodes[o.target];
                        if (o.source.datasource == o.target.datasource){
                            if (o.source.datasource in msourcelinks)
                                msourcelinks[o.source.datasource].push(o);
                            else
                                msourcelinks[o.source.datasource] =[o];

                        }

                   }
                   malinks = mlinks;

                   flatnodes = [];
                   $.each(mnodes, function (key, val) {
                       flatnodes.push(val);
                       mtcards["All"].push({"label":val.label, "value":val.weight}); //, "color": color(val.datasource)
                       if (val.datasource in mtcards){
                            mtcards[val.datasource].push({"label":val.label, "value":val.weight}); //, "color": color(val.datasource)
                       }else{
                            mtcards[val.datasource] = [{"label":val.label, "value":val.weight}]; // , "color": color(val.datasource)
                       }
                       if (val.datasource in msourcenodes){
                           msourcenodes[val.datasource].push(val);
                       }else{
                          msourcenodes[val.datasource] = [val]
                       }
                   });
                   mnodes = flatnodes;
                   manodes = mnodes ;
                   $("#mtviz").show();

                   draw_details();
                   // drawRDFMTS(nodes, links);

                },
                error: function(jqXHR, textStatus, errorThrown){
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                }
            });

    });
    function draw_details(){
       data = {nodes: manodes, links: malinks};
       $("#mtviz").html("<h1> Please select data source!</h1>");

       drawRDFMTS(manodes, malinks, "mtviz");
    }
    $("#backtotable").click(function(){
        $("#backtotable" ).hide();
        $("#mtviz").hide();
        $("#listofrdfmts").show();
        $("#mtdetails" ).show();
    });

    function get_rdfmts_stats(fed){
        if (fed == null || (fed == federation && loaded == 1)){
            console.log("already loaded");
                return
         }
        $("#fedName").html(fed);
        $("#vfedName").html(fed);
        $("#afedName").html(fed);
        if (stats == null || stats == "undefined"){
            $('#rdfmtsdataTables').empty();
            $('#rdfmtsdataTables').append("<thead><tr><th>#</th><th>Name</th><th>URI</th><th>Instances</th><th>Num. of Properties</th></tr></thead>");
            stats = null;
            stats = $('#rdfmtsdataTables').DataTable({
                        order: [[ 1, 'desc' ]],
                        responsive: true,
                        select: true,
                        defaultContent: "<i>Not set</i>",
                        lengthMenu: [ [10, 25, 50, -1], [10, 25, 50, "All"] ],
                        dom: 'Blfrtip',
                        stateSave: true,
                        "bDestroy": true,
                        //"processing": true,
                        //"serverSide": true,
                        buttons: [
                             {
                             text:'copy'
                             },
                             {
                             text:'csv',
                             extend: 'csvHtml5',
                             title: 'rdfmtsdataTables'
                             },
                             {
                             text:'excel',
                             extend: 'excelHtml5',
                             title: 'rdfmtsdataTables'
                             },
                             {
                             text:'pdf',
                             extend: 'pdfHtml5',
                             title: 'rdfmtsdataTables'
                             },
                             {
                             text: 'TSV',
                             extend: 'csvHtml5',
                             fieldSeparator: '\t',
                             extension: '.tsv',
                             title: 'rdfmtsdataTables'
                             }
                        ],
                        ajax: '/rdfmts/api/rdfmtstats?graph=' + fed
                    });
            loaded = 1;
            statstable = stats;
            stats.on( 'select', function ( e, dt, type, indexes ) {
                    selectedRow = statstable.rows( indexes ).data().toArray();
                    console.log("selected row:", selectedRow)
                    $( "#editds" ).prop( "disabled", false );
                    $( "#removeds" ).prop( "disabled", false );
                    $( "#mtdetails" ).prop( "disabled", false );
                    $( "#backtotable" ).hide();
                }).on( 'deselect', function ( e, dt, type, indexes ) {
                    var rowData = statstable.rows( indexes ).data().toArray();
                    $( "#editds" ).prop( "disabled", true );
                    $( "#removeds" ).prop( "disabled", true );
                    $( "#mtdetails" ).prop( "disabled", true );
                    $( "#backtotable" ).hide();
                    selectedRow = null;
                });
            }
        else{
                console.log("redrawing ... table ..");
                stats.clear().draw();
                stats.ajax.url("/rdfmts/api/rdfmtstats?graph=" + fed).load();
            }
    }

    function get_rdfmts(fed){
        if (fed == null || (fed == federation && vized == 1)){
            return
         }
        $("#fedName").html(fed);
        $("#afedName").html(fed);
        $("#vfedName").html(fed);

        var j=0;
        //list of subjects and objects for the DAG

        $.getJSON('/rdfmts/api/rdfmts?graph=' + fed, function(data) {
           sources = data.sources;
           nodes = data.nodes;
           links = data.links;
           sourcescard = sources.length;
           max_score = sourcescard;
           var legend="";
           $("#datasources").empty();
           $("#gadatasources").empty();
           var datasources = '<li class="datasource"><a href="#" class="datasource" id="source-0">All</a></li><li class="divider"></li>' ;
           console.log("number of sources:", sources.length);
           for (var i = 0; i<sources.length; i++){
               var v = sources[i].id;
               var name  = sources[i].name;
               sourceids[name] = v;
               sourcesnames[v] = name;
               datasources += '<li class="datasource"><a href="#" class="datasource"  id="source-'+(i+1)+'">'+ name +'</a></li>'
               legend = legend + '<span style="color:' + color(v) + '"><b>' + name +"</b></span> <br/> ";
            }

            $("#legend").empty();
            $("#legend").html(legend);

            $("#gadatasources").html(datasources);
            $("#datasources").html(datasources);
            $("#datasources").prop("disabled", false)
            $("#graph").html("<h1> Please select data source!</h1>");
            $("a[class=datasource]").click(function(){
                $("#datasourcesbtn").val($(this).text())
                if ($(this).text() == "All"){
                    $("#vdsname").html("ALL");
                    $("#gdsname").html("ALL");
                    source = "All"
                    sourcemt = source;
                }else{
                    source = sourceids[$(this).text()];
                    sourcemt = source;
                }
                if (source){
                    $("#vdsname").html($(this).text());
                    $("#gdsname").html($(this).text());
               } else{
                    $("#vdsname").html("ALL");
                    $("#gdsname").html("ALL");
                }
                if (tabvisible == "#analysis"){
                    get_rdfmts_graph_analys(federation, $(this).text());
                }else{
                    $("#vdsname").html($(this).text());
                    $("#gdsname").html($(this).text());
                    sourcemt = source;
                    $("#graph").empty();
                    $("#graph").html("<h3>Please select Vizualization type</h3>");
                    //drawSingleSourceRDFMTS(sourcemt);
                    if (viztype == 'fgraph'){
                        drawSingleSourceRDFMTS(sourcemt, 'force');
                    }else if (viztype == 'cgraph'){
                        drawSingleSourceRDFMTS(sourcemt, 'circular');
                    }else if (viztype == 'donut'){
                        $("#graph").empty();
                        $("#graph").html('<div id="morris-donut-chart"></div>')
                        console.log(source, mtcards);
                        drawDonut(source);
                    }
                    get_rdfmts_graph_analys(federation, $(this).text());
                }
            });

           for (var i=0; i<links.length; ++i) {
                o = links[i];

                o.source = nodes[o.source];
                o.target = nodes[o.target];
                if (o.source == null || o.target == null){
                    console.log(o)
                }
                if (o.source.datasource == o.target.datasource){
                    if (o.source.datasource in sourcelinks)
                        sourcelinks[o.source.datasource].push(o);
                    else
                        sourcelinks[o.source.datasource] =[o];

                }
//                else{
//                    // add outgoing links
//                    if (o.source.datasource in sourcelinks)
//                        sourcelinks[o.source.datasource].push(o) ;
//                    else
//                        sourcelinks[o.source.datasource] =[o];
//                    // add nodes of outgoing links
//                    if (o.source.datasource in sourcenodes){
//                        sourcenodes[o.source.datasource].push(o.target);
//                        }
//                    else{
//                        sourcenodes[o.source.datasource] =[o.target];
//                    }

//                    // add incoming links
//                    if (o.target.datasource in sourcelinks)
//                        sourcelinks[o.target.datasource].push(o) ;
//                    else
//                        sourcelinks[o.target.datasource] =[o];
//                    // add nodes of incoming links
//                    if (o.target.datasource in sourcenodes){
//                        sourcenodes[o.target.datasource].push(o.source);
//                        }
//                    else{
//                        sourcenodes[o.target.datasource] =[o.source];
//                    }
              //  }
           }
           alinks = links;

           flatnodes = [];
           $.each(nodes, function (key, val) {
               flatnodes.push(val);
               mtcards["All"].push({"label":val.label, "value":val.weight}); //, "color": color(val.datasource)
               if (val.datasource in mtcards){
                    mtcards[val.datasource].push({"label":val.label, "value":val.weight}); //, "color": color(val.datasource)
               }else{
                    mtcards[val.datasource] = [{"label":val.label, "value":val.weight}]; // , "color": color(val.datasource)
               }
               if (val.datasource in sourcenodes){
                   sourcenodes[val.datasource].push(val);
               }else{
                  sourcenodes[val.datasource] = [val]
               }
           });
           nodes = flatnodes;
           anodes = nodes ;
           // drawRDFMTS(nodes, links);
           });


    }

    var showmore = function (key){
       console.log('p[class=legend'+key+']');
       $('p[class=legend'+key+']').toggle();
    };

    function drawDonut(sourcemt){
        if (source != "All"){
            $("#graph").empty();
            $.getJSON('/rdfmts/api/rdfmtstats?graph=' + federation,function(jdata) {
                for (let i=0; i<jdata.data.length; i++) {
                    for (let j in mtcards[sourcemt]) {
                        if( mtcards[sourcemt][j].label.includes(jdata.data[i][1]))
                            mtcards[sourcemt][j].value = jdata.data[i][3];
                    }
                }
                mtcards[sourcemt].sort(function(a, b) {
                    return b.value - a.value;
                });
            });
            $("#graph").append('<div style="float:left"><div id="morris-donut-chart" style="float:left"></div><div  style="margin-top:10px;display:inline-block" id="legendd" class="donut-legend"></div></div>');
                var mtdonut = Morris.Donut({
                        element: 'morris-donut-chart',
                        data: mtcards[sourcemt],
                        resize: true,
                        backgroundColor: '#ccc',
                        labelColor: '#060',
                        colors: colors
                      });
                $('#legendd').append('<span style="color:'+ color(sourcemt) +'"><b><u>'+ sourcesnames[sourcemt] +'</u></b></span>');
                var i = 0;
                mtdonut.options.data.forEach(function(label, j){
                    if (i < 9){
                        var legendItem = $('<span></span>')
                                         .text(label['label'])
                                         .prepend('<i>&nbsp;</i>');
                        legendItem.find('i').css('backgroundColor', mtdonut.options.colors[i]);
                        $('#legendd').append(legendItem);
                   }else if(i == 9){
                        var legendItem = $('<span></span>')
                                     .text(label['label'])
                                     .prepend('<i>&nbsp;</i>');
                        legendItem.find('i').css('backgroundColor', mtdonut.options.colors[i]);
                        $('#legendd').append(legendItem);

                        $('#legendd').append('<p id="showmore'+sourcemt+'">Show more ..</p>');
                        $("#showmore"+sourcemt).click(function (){

                           $('span[class=legend'+sourcemt+']').show();
                           $("#showmore"+sourcemt).hide();
                           $("#showless"+sourcemt).show();
                        });

                   }else{
                        var legendItem = $('<span style="display:none" class="legend'+ sourcemt +'"></span>')
                                             .text(label['label'])
                                             .prepend('<i>&nbsp;</i>');
                            legendItem.find('i').css('backgroundColor', mtdonut.options.colors[i]);
                            $('#legendd' ).append(legendItem);
                   }
                    i += 1;

                  });
                if (i > 9){
                    $('#legendd').append('<p style="display:none" id="showless'+ sourcemt +'">Show less ..</p>');
                    $("#showless"+sourcemt).click(function (){
                       $('span[class=legend'+sourcemt+']').hide();
                       $("#showmore"+sourcemt).show();
                       $("#showless"+sourcemt).hide();
                    });
                }

        }else{
            $("#graph").empty();
            $.each(mtcards, function (key, val) {
                $("#graph").append('<div style="float:left"><div id="morris-donut-chart' + key +'" style="float:left"></div><div style="margin-top:10px;display:inline-block" id="legend'+key +'" class="donut-legend"></div></div>');
                $.getJSON('/rdfmts/api/rdfmtstats?graph=' + federation,function(jdata) {
                    for (let i=0; i<jdata.data.length; i++) {
                        for (let ii in val) {
                            if( val[ii].label.includes(jdata.data[i][1]))
                                val[ii].value = jdata.data[i][3];
                        }
                    }
                    val.sort(function(a, b) {
                        return b.value - a.value;
                    });
                });
                var mtdonut = Morris.Donut({
                        element: 'morris-donut-chart'+key,
                        data: val,
                        resize: true,
                        backgroundColor: '#ccc',
                        labelColor: '#060',
                        colors: colors
                      });
                var dsname = sourcesnames[key]? sourcesnames[key]: federation;
                $('#legend'+key).append('<span style="color:'+ color(key) +'"><b><u>'+ dsname +'</u></b></span>');
                var i = 0;
                mtdonut.options.data.forEach(function(label, j){
                    if (i < 9){
                        var legendItem = $('<span></span>')
                                         .text(label['label'])
                                         .prepend('<i>&nbsp;</i>');
                        legendItem.find('i').css('backgroundColor', mtdonut.options.colors[i]);
                        $('#legend'+key ).append(legendItem);

                    }else if(i == 9){
                        var legendItem = $('<span></span>')
                                     .text(label['label'])
                                     .prepend('<i>&nbsp;</i>');
                        legendItem.find('i').css('backgroundColor', mtdonut.options.colors[i]);
                        $('#legend'+key ).append(legendItem);
                        $('#legend'+key ).append('<p id="showmore'+ key +'">Show more ..</p>');
                        $("#showmore"+key).click(function (){
                           console.log('p[class=legend'+key+']');
                           $('span[class=legend'+key+']').show();
                           $("#showmore"+key).hide();
                           $("#showless"+key).show();
                        });
                   }else{
                        var legendItem = $('<span style="display:none" class="legend'+ key +'"></span>')
                                         .text(label['label'])
                                         .prepend('<i>&nbsp;</i>');
                        legendItem.find('i').css('backgroundColor', mtdonut.options.colors[i]);
                        $('#legend'+key ).append(legendItem);
                    }
                    i += 1;
                  });
                if (i > 9){
                    $('#legend'+key ).append('<p style="display:none" id="showless'+ key +'">Show less ..</p>');
                    $("#showless"+key).click(function (){
                       console.log('p[class=legend'+key+']');
                       $('span[class=legend'+key+']').hide();
                       $("#showless"+key).hide();
                       $("#showmore"+key).show();
                    });
                }
            });

        }
    }

    $("#stopforce").click(function(){
        if (force){
            force.stop()
        }
    });
    $("#startforce").click(function(){
        if (force){
            linkdistance += 10;
            //ncharge -= 10;
            // ngravity -=0.5;
            force.linkDistance(linkdistance).gravity(0.05).start()
        }
    });
    $("#resetforce").click(function(){
        if (force){
            linkdistance = 150
            var fit = Math.sqrt(anodes.length / (width * height));
            var charge = (-1 / fit);
            var gravity = (8 * fit);
            ngravity = gravity;
            ncharge = charge;
            force.linkDistance(linkdistance).gravity(0.05).start()
        }
    });
    $("#graphVizForce").click(function(){
        $("#graph").empty();
        console.log("visible tab for datasource selection:" +tabvisible, sourcemt);
        drawSingleSourceRDFMTS(sourcemt, 'force');
        viztype = "fgraph";
    });

    $("#graphVizCircular").click(function(){
        $("#graph").empty();
        console.log("visible tab for datasource selection:" +tabvisible, sourcemt);
        drawSingleSourceRDFMTS(sourcemt, 'circular');
        viztype = "cgraph";
    });
    $("#donutViz").click(function(){
        $("#graph").empty();
        $("#graph").html('<div id="morris-donut-chart"></div>')
        console.log(source, mtcards);
        drawDonut(source);
        viztype = "donut";
    });


    function drawSingleSourceRDFMTS(source, gt){
        console.log(source);
        if (source == "All"){
            if (alinks.length < 1)
                alinks=[]
            data = {nodes: anodes, links: alinks};
            if (gt == "force"){
                 anodes.forEach(function(d){
                    expand[d.datasource] = true;
                });
                drawRDFMTS(anodes, alinks);
            }else
                drawGraph(data);
            vized = 1;
        }else{
            var snodes = sourcenodes[source];
            console.log("number of nodes:" + snodes.length);
            //connection link between subject and object ->predicates
            var slinks = sourcelinks[source];
            if (!slinks){
                slinks=[]
            }
            data = {nodes: snodes, links: slinks}
            if (gt == "force"){
                 snodes.forEach(function(d){
                    expand[d.datasource] = true;
                });
                drawRDFMTS(snodes, slinks);
            }else
                drawGraph(data);
        }
    }

    function nodeid(n) {
      return n.size ? "_g_"+n.datasource : n.label;
    }

    function linkid(l) {
      var u = nodeid(l.source),
          v = nodeid(l.target);
      return u<v ? u+"|"+v : v+"|"+u;
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
                o = gc[i] || (gc[i] = {x:0,y:0,count:0});
                o.x += n.x;
                o.y += n.y;
                o.count += 1;
              }
            });
          }

          // determine nodes
          for (var k=0; k<data.nodes.length; ++k) {
            var n = data.nodes[k],
                i = index(n),
                l = gm[i] || (gm[i]=gn[i]) || (gm[i]={datasource:i, size:0, nodes:[]});

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
              if (l.size == 0) {
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
          for (k=0; k<data.links.length; ++k) {
              var e = data.links[k],
                  u = index(e.source),
                  v = index(e.target);
              if (u != v) {
                  gm[u].link_count++;
                  gm[v].link_count++;
              }
              u = expand[u] ? nm[e.source.label] : nm[u];
              v = expand[v] ? nm[e.target.label] : nm[v];
              var i = (u<v ? u+"|"+v : v+"|"+u),
                  l = lm[i] || (lm[i] = {source:u, target:v, size:0});
              l.size += 1;
          }
          for (i in lm) { links.push(lm[i]); }

          return {nodes: nodes, links: links};
    }

    function convexHulls(nodes, index, offset) {
      var hulls = {};

      // create point sets
      for (var k=0; k<nodes.length; ++k) {
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
        .interpolate("cardinal-closed")
        .tension(.85);
    function drawCluster(d) {
      console.log("drawcluster", d)
      return curve(d.path); // 0.8
    }
    width = $("#graph").width();
    height = 980;
    var canv = "graph";

    function drawRDFMTS(nodes, links, divcanv){
        console.log('nodes', nodes, 'links', links);
        var svg;
        if(divcanv == null){
            $("#graph").empty();
            svg = d3.select("#graph").append("svg");
            width = $("#graph").width();
            height = 980;
            canv = "graph"
        }else{
            $("#mtviz").empty();
            svg = d3.select("#mtviz").append("svg");
            width = $("#mtviz").width();
            height = 980;
            console.log("showing ..")
            $("#mtviz").show();
            canv = "mtviz"
        }
        var chartLayer = svg.append("g").classed("chartLayer", true);
        var zoom = d3.behavior.zoom().scaleExtent([min_zoom,max_zoom])
        var g = svg.append("g");

        hullg = svg.append("g");
        linkg = svg.append("g");
        nodeg = svg.append("g");

        svg.attr("opacity", 1e-6)
            .transition()
              .duration(1000)
              .attr("opacity", 1);


        var tocolor = "fill";
        var towhite = "stroke";
        if (outline) {
            tocolor = "stroke"
            towhite = "fill"
        }

        svg.style("cursor","move");
        // d3.json("graph.json", function(error, graph) {
        var linkedByIndex = {};
        links.forEach(function(d) {
            linkedByIndex[d.source + "," + d.target] = true;
        });

        //var ctx = svg.getContext("2d");
        var fit = Math.sqrt(nodes.length / (width * height));
        var charge = (-1 / fit);
        var gravity = (8 * fit);
        ngravity = gravity;
        ncharge = charge;
        if (force) force.stop()
        //data = {nodes:nodes, links:links}
        net = network(data, net, getGroup, expand);
        console.log(net, expand)
        force = d3.layout.force()
                  .nodes(net.nodes)
                  .links(net.links)
                  .linkDistance(function(l, i){
                        var n1 = l.source, n2 = l.target;
                        return divcanv?250:200 +
                              Math.min(20 * Math.min((n1.size || (n1.datasource != n2.datasource ? n1.group_data.size : 0)),
                                                     (n2.size || (n1.datasource != n2.datasource ? n2.group_data.size : 0))),
                                   -30 +
                                   30 * Math.min((n1.link_count || (n1.datasource != n2.datasource ? n1.group_data.link_count : 0)),
                                                 (n2.link_count || (n1.datasource != n2.datasource ? n2.group_data.link_count : 0))),
                                   300);
                   })
                  .linkStrength(function(l, i) {
                        return  2;
                        })
                  .gravity(0.05)   // 0.05 gravity+charge tweaked to ensure good 'grouped' view (e.g. green group not smack between blue&orange, ...
                  .charge(-600)    // ... charge is important to turn single-linked groups to the outside
                  .friction(0.5)   // friction adjusted to get dampened display: less bouncy bouncy ball [Swedish Chef, anyone?]
                  .size([width,height])
                  .start(); //.chargeDistance(1000) .linkDistance(300)

        link = g.selectAll(".link").data(net.links, linkid);
        link.exit().remove();
        link.enter().append("line")
            .attr("class", "link")
            .attr("x1", function(d) { return d.source.x; })
            .attr("y1", function(d) { return d.source.y; })
            .attr("x2", function(d) { return d.target.x; })
            .attr("y2", function(d) { return d.target.y; })
            .style("stroke-width", nominal_stroke)
            .style("stroke", function(d) {
                   return color(d.datasource);
            });
//        var link = g.selectAll(".link")
//            .data(links)
//            .enter()
//            .append("line")
//            .attr("class", "link")
//            .style("stroke-width",nominal_stroke)
//            .style("stroke", function(d) {
//                   return color(d.datasource);
//            });
        var dr = 3;
        node = g.selectAll(".node").data(net.nodes, nodeid);
        node.exit().remove();
        node.enter().append("g")
            .attr("class", function(d) {
                   return "node" + (d.size?"":" leaf"); })
            //.attr("r", function(d) {console.log(d.size, dr); return d.size ? d.size + dr : 3; })
            .attr("cx", function(d) { return d.x; })
            .attr("cy", function(d) { return d.y; })
            .on("dblclick", function(d) {
                    console.log(d.datasource, expand[d.datasource])
                    expand[d.datasource] = !expand[d.datasource];
                    drawRDFMTS(nodes, links, divcanv);
            })
//            .on("dblclick.zoom", function(d) {
//                d3.event.stopPropagation();
//                var dcx = ($("#graph").width()/2-d.x*zoom.scale());
//                var dcy = (980/2-d.y*zoom.scale());
//                zoom.translate([dcx,dcy]);
//                 g.attr("transform", "translate("+ dcx + "," + dcy  + ")scale(" + zoom.scale() + ")");
//
//           })
           .on("mouseover", function(d) {
                set_highlight(d);
                })
           .on("mousedown", function(d) {
                d3.event.stopPropagation();
                focus_node = d;
                set_focus(d)
                if (highlight_node === null)
                    set_highlight(d)
                })
           .on("mouseout", function(d) {
                exit_highlight();
           });

            node.call(force.drag);


//        var node = g.selectAll(".node")
//            .data(nodes)
//            .enter().append("g")
//            .attr("class", "node")
//            .call(force.drag);
//
//        node.on("dblclick.zoom", function(d) {
//            d3.event.stopPropagation();
//            var dcx = ($("#graph").width()/2-d.x*zoom.scale());
//	        var dcy = (980/2-d.y*zoom.scale());
//            zoom.translate([dcx,dcy]);
//             g.attr("transform", "translate("+ dcx + "," + dcy  + ")scale(" + zoom.scale() + ")");
//           });

        var ci = 0;
        var circle = node.append("path")
              .attr("d", d3.svg.symbol()
                        .size(function(d) {
                            var v = d.size?Math.PI*Math.pow(size(65+d.size>200?200:d.size)||nominal_base_node_size,2):Math.PI*Math.pow(size(25)||nominal_base_node_size,2);
                            return v;}) //size(d.weight)
                        .type(function(d) { return d.size? 'circle': d.type; })
                   )
              .style(tocolor, function(d) {
                    if (divcanv ==null){
                        return color(d.datasource)
                    }else{
                        ci += 1
                        return color(d.datasource + (ci-1));
                    }
              })
              .style("stroke-width", nominal_stroke)
              .style(towhite, "white");

        var text = g.selectAll(".text")
            .data(net.nodes)
            .enter().append("text")
            .attr("dy", ".35em")
            .style("font-size",function(d){return d.size? 16 + "px": nominal_text_size + "px"})

        if (text_center)
            text.text(function(d) {
             if (d.label) return d.label;
             else {
                return sourcesnames[d.datasource];
                }
            })
            .style("text-anchor", "middle");
        else
            text.attr("dx", function(d) {return (size(65)-size(30)||nominal_base_node_size);}) //size(d.weight)
                .text(function(d) { if (d.label) return  '\u2002'+ d.label; else return '\u2002'+ sourcesnames[d.datasource]; });

//        node.on("mouseover", function(d) {
//                set_highlight(d);
//                })
//            .on("mousedown", function(d) {
//                d3.event.stopPropagation();
//                focus_node = d;
//                set_focus(d)
//                if (highlight_node === null)
//                    set_highlight(d)
//                })
//            .on("mouseout", function(d) {
//                exit_highlight();
//            });

        d3.select(window).on("mouseup", function() {
                if (focus_node!==null){
                    focus_node = null;
                    if (highlight_trans<1){
                        circle.style("opacity", 1);
                        text.style("opacity", 1);
                        link.style("opacity", 1);
                    }
                }
            if (highlight_node === null)
                exit_highlight();
        });

        zoom.on("zoom", function() {
            var stroke = nominal_stroke;
            if (nominal_stroke*zoom.scale()>max_stroke)
                stroke = max_stroke/zoom.scale();

            link.style("stroke-width", stroke);
            circle.style("stroke-width",stroke);

            var base_radius = nominal_base_node_size;
            if (nominal_base_node_size*zoom.scale()>max_base_node_size)
                base_radius = max_base_node_size/zoom.scale();
                circle.attr("d", d3.svg.symbol()
                        .size(function(d) {
                            var v = d.size?Math.PI*Math.pow(size(65+d.size>200?200:d.size)*base_radius/nominal_base_node_size||base_radius,2):Math.PI*Math.pow(size(25)*base_radius/nominal_base_node_size||base_radius,2);
                            return v;}) //size(d.weight)
                        .type(function(d) { return d.size? 'circle':  d.type; })
                   );
            //circle.attr("r", function(d) { return (size(d.size)*base_radius/nominal_base_node_size||base_radius); })
            if (!text_center)
                 text.attr("dx", function(d) {
                    return ((size(65)-size(30))*base_radius/nominal_base_node_size||base_radius);
                }); //size(d.weight)

            text.style("font-size", function(d){
                var text_size = nominal_text_size;
                if (d.size){
                    text_size = 16;
                }

                if (nominal_text_size*zoom.scale()>max_text_size)
                    text_size = max_text_size/zoom.scale();

            return text_size + "px"});
            g.attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");
        });

        svg.call(zoom);

        resize();
        //window.focus();
        d3.select(window).on("resize", resize).on("keydown", keydown);
        var centroids = {};
        for (var i =0; i < max_score; i+=3){
            centroids[i] = {x: 200 * (i/3 +1), y:200}
            centroids[i+1] = {x: 200 * (i/3+1), y:400}
            centroids[i+2] = {x: 200 * (i/3 +1), y:600}
        }


        force.on("tick", function(e) {
            var k = .1 * e.alpha;
            // updateGroups();

             // Push nodes toward their designated focus.
              net.nodes.forEach(function(o, i) {
                if (centroids[o.datasource]){
                    o.y += (centroids[o.datasource].y - o.y) * k;
                    o.x += (centroids[o.datasource].x - o.x) * k;
                }
              });

              text.forEach(function(o, i) {
                if (centroids[o.datasource]){
                    o.y += (centroids[o.datasource].y - o.y) * k;
                    o.x += (centroids[o.datasource].x - o.x) * k;
                }
              });
              link.attr("x1", function(d) { return d.source.x; })
                .attr("y1", function(d) { return d.source.y; })
                .attr("x2", function(d) { return d.target.x; })
                .attr("y2", function(d) { return d.target.y; });

//            link
//                .attr("d", function(d) {
//                     var deltaX = d.target.x - d.source.x,
//                  deltaY = d.target.y - d.source.y,
//                  dist = Math.sqrt(deltaX * deltaX + deltaY * deltaY),
//                  normX = deltaX / dist,
//                  normY = deltaY / dist,
//                  sourcePadding = d.left ? 17 : 12,
//                  targetPadding = d.right ? 17 : 12,
//                  sourceX = d.source.x + (sourcePadding * normX),
//                  sourceY = d.source.y + (sourcePadding * normY),
//                  targetX = d.target.x - (targetPadding * normX),
//                  targetY = d.target.y - (targetPadding * normY);
//              return 'M' + sourceX + ',' + sourceY + 'L' + targetX + ',' + targetY;
//                   });

            node.each(printn())
                .attr("cx", function(d) { return d.x; })
                .attr("cy", function(d) { return d.y; });

            node.attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });
            text.attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });

        });
        function printn(alpha){
            var quadtree = d3.geom.quadtree(nodes);

            return function(d) {
            };
        }

        // select nodes of the group, retrieve its positions
        // and return the convex hull of the specified points
        // (3 points as minimum, otherwise returns null)
//        var polygonGenerator = function(groupId) {
//          var node_coords = node
//            .filter(function(d) { return d.datasource == groupId; })
//            .data()
//            .map(function(d) { return [d.x, d.y]; });
//
//          if (node_coords.length < 3){
//             return null;
//          }
//          return d3.polygonHull(node_coords);
//        };

//          // count members of each group. Groups with less
//          // than 3 member will not be considered (creating
//          // a convex hull need 3 points at least)
//        var groupIds = d3.set(nodes.map(function(n) { return +n.datasource; }))
//            .values()
//            .map( function(groupId) {
//              return {
//                groupId : groupId,
//                count : nodes.filter(function(n) { return +n.datasource == groupId; }).length
//              };
//            })
//            .filter( function(group) { return group.count > 0;})
//            .map( function(group) { return group.groupId; });
//
//        function updateGroups() {
//
//          groupIds.forEach(function(groupId) {
//                //gnodes = nodes.filter(function(d) { return d.datasource == groupId;})
//                polygon = polygonGenerator(groupId);
//                if (polygon !== null){
//                    centroid = d3.polygonCentroid(polygon);
//                    centroids[groupId] = {x: centroid[0], y: centroid[1]};
//                }else{
//                    centroids[groupId] = null;
//                }
//
//          });
//        }


        function isConnected(a, b) {
            return linkedByIndex[a.index + "," + b.index] || linkedByIndex[b.index + "," + a.index] || a.index == b.index;
        }

        function hasConnections(a) {
            for (var property in linkedByIndex) {
                s = property.split(",");
                if ((s[0] == a.index || s[1] == a.index) && linkedByIndex[property])
                    return true;
            }
            return false;
        }

        function vis_by_type(type) {
            switch (type) {
              case "circle": return keyc;
              case "square": return keys;
              case "triangle-up": return keyt;
              case "diamond": return keyr;
              case "cross": return keyx;
              case "triangle-down": return keyd;
              default: return true;
            }
        }

        function vis_by_node_score(score) {
            if (isNumber(score)) {
                if (score>=0.666)
                    return keyh;
                else if (score>=0.333)
                    return keym;
                else if (score>=0)
                    return keyl;
            }
            return true;
        }

        function vis_by_link_score(score){
            if (isNumber(score))  {
                if (score>=0.666)
                    return key3;
                else if (score>=0.333)
                    return key2;
                else if (score>=0)
                    return key1;
            }
            return true;
        }

        function isNumber(n) {
            return !isNaN(parseFloat(n)) && isFinite(n);
        }

        function resize() {
            var width = $("#"+canv).width(), height = 980;
            svg.attr("width", width).attr("height", height);
            force.size([force.size()[0]+(width-w)/zoom.scale(),force.size()[1]+(height-h)/zoom.scale()]).resume();
            w = width;
            h = height;
        }

        function keydown() {
            if (d3.event.keyCode==32) {
                force.stop();
            }
            else if (d3.event.keyCode>=48 && d3.event.keyCode<=90 && !d3.event.ctrlKey && !d3.event.altKey && !d3.event.metaKey) {
                switch (String.fromCharCode(d3.event.keyCode)) {
                    case "C": keyc = !keyc; break;
                    case "S": keys = !keys; break;
                    case "T": keyt = !keyt; break;
                    case "R": keyr = !keyr; break;
                    case "X": keyx = !keyx; break;
                    case "D": keyd = !keyd; break;
                    case "L": keyl = !keyl; break;
                    case "M": keym = !keym; break;
                    case "H": keyh = !keyh; break;
                    case "1": key1 = !key1; break;
                    case "2": key2 = !key2; break;
                    case "3": key3 = !key3; break;
                    case "0": key0 = !key0; break;
                }

                link.style("display", function(d) {
                    var flag  = vis_by_type('circle')&&vis_by_type('circle')&&vis_by_node_score(d.source.datasource)&&vis_by_node_score(d.target.datasource)&&vis_by_link_score(d.datasource);
                    linkedByIndex[d.source.index + "," + d.target.index] = flag;
                    return flag?"inline":"none";
                });
                node.style("display", function(d) {
                    return (key0||hasConnections(d))&&vis_by_type('circle')&&vis_by_node_score(d.datasource)?"inline":"none";
                });
                text.style("display", function(d) {
                 return (key0||hasConnections(d))&&vis_by_type('circle')&&vis_by_node_score(d.datasource)?"inline":"none";
                });

                if (highlight_node !== null) {
                  if ((key0||hasConnections(highlight_node))&&vis_by_type('circle')&&vis_by_node_score(highlight_node.datasource)) {
                       if (focus_node!==null)
                             set_focus(focus_node);
                       set_highlight(highlight_node);
                  }else {
                        exit_highlight();
                  }
                }

            }
        }

        function exit_highlight() {
            highlight_node = null;
            if (focus_node===null)
            {
                svg.style("cursor","move");
                if (highlight_color != "white") {
                  circle.style(towhite, "white");
                  text.style("font-weight", "normal");
                  link.style("stroke", function(o) {
                        return (isNumber(o.datasource) && o.datasource >= 0)? color(o.datasource): default_link_color
                      });
                }

            }
        }

        function set_focus(d){
            if (highlight_trans<1)  {
                circle.style("opacity", function(o) {
                            return isConnected(d, o) ? 1 : highlight_trans;
                        });

                text.style("opacity", function(o) {
                    return isConnected(d, o) ? 1 : highlight_trans;
                });

                link.style("opacity", function(o) {
                    return o.source.index == d.index || o.target.index == d.index ? 1 : highlight_trans;
                });
                }
        }


        function set_highlight(d) {
            svg.style("cursor","pointer");
            if (focus_node !== null)
                d = focus_node;
            highlight_node = d;
            // added this to make highlight color same as the color of the node
            highlight_color = color(d.datasource);
            if (highlight_color != "white") {
                  circle.style(towhite, function(o) {
                        return isConnected(d, o) ? highlight_color : "white";
                        });
                  text.style("font-weight", function(o) {
                        return isConnected(d, o) ? "bold" : "normal";
                        });
                  link.style("stroke", function(o) {
                      return o.source.index == d.index || o.target.index == d.index ? highlight_color : ((isNumber(o.datasource) && o.datasource>=0)?color(o.datasource):default_link_color);
                    });
            }
        }

    }

    var gsource = null
    function get_rdfmts_graph_analys(fed, source){
        if (fed == null || source == null || (fed == federation && source == gsource&& galoaded == 1)){
                return
         }
        $("#fedName").html(fed);
        $("#vfedName").html(fed);
        $("#afedName").html(fed);
        $('#adsname').html(source);
        gsource = source;
        if (gtable == null){
            gtable = $('#graph-analysis').DataTable({
                responsive: false,
                order: false,
                select: true,
                lengthMenu: [ [10, 25, 50, -1], [10, 25, 50, "All"] ],
                dom: 'Blfrtip',
                buttons: [
                     {
                     text:'copy'
                     },
                     {
                     text:'csv',
                     extend: 'csvHtml5',
                     title: 'mt-graph-analysis'
                     },
                     {
                     text:'excel',
                     extend: 'excelHtml5',
                     title: 'mt-graph-analysis'
                     },
                     {
                     text:'pdf',
                     extend: 'pdfHtml5',
                     title: 'mt-graph-analysis'
                     },
                     {
                     text: 'TSV',
                     extend: 'csvHtml5',
                     fieldSeparator: '\t',
                     extension: '.tsv',
                     title: 'mt-graph-analysis'
                     }
                ],
                ajax: '/rdfmts/api/rdfmtanalysis?graph='+fed+"&source="+source
            });
            galoaded = 1;
        }else {
            gtable.clear().draw();
            gtable.ajax.url("/rdfmts/api/rdfmtanalysis?graph=" + fed+"&source="+source).load()
        }
    }

    $('a[data-toggle="tab"]').on('shown.bs.tab', function (e) {
          var target = $(e.target).attr("href") // activated tab
          if (target == "#visualize"){
            //get_rdfmts(federation);
            tabvisible = "#visualize";
          }
          else if (target == "#analysis"){
            //get_rdfmts_graph_analys(federation, "All");
            tabvisible = "#analysis";
          }
          else{
            //get_rdfmts_stats(federation);
            tabvisible = "#home";
          }
    });


    var diameter = 500;
    var radius = diameter / 2;
    var margin = 10;

    // Generates a tooltip for a SVG circle element based on its ID
    function addTooltip(circle) {
        var x = parseFloat(circle.attr("cx"));
        var y = parseFloat(circle.attr("cy"));
        var r = parseFloat(circle.attr("r"));
        var text = circle.attr("id");

        var tooltip = d3.select("#plot")
            .append("text")
            .text(text)
            .attr("x", x)
            .attr("y", y)
            .attr("dy", -r * 2)
            .attr("id", "tooltip");

        var offset = tooltip.node().getBBox().width / 2;

        if ((x - offset) < -radius) {
            tooltip.attr("text-anchor", "start");
            tooltip.attr("dx", -r);
        }
        else if ((x + offset) > (radius)) {
            tooltip.attr("text-anchor", "end");
            tooltip.attr("dx", r);
        }
        else {
            tooltip.attr("text-anchor", "middle");
            tooltip.attr("dx", 0);
        }
    }

    // Draws an arc diagram for the provided undirected graph
    function drawGraph(graph) {
        var zoom = d3.behavior.zoom().scaleExtent([min_zoom,max_zoom])
        // create svg image
        $("#graph").empty();
        var circumference = 0;
        graph.nodes.forEach(function(d, i) {
            circumference += 20+2;
        });
        var wh = 2 * circumference/(Math.PI);
        if (wh < 200) wh = 200;
        if (wh>1200) wh = 1200;

        diameter = wh;
        var svg = d3.select("#graph").append("svg");
        var chartLayer = svg.append("g").classed("chartLayer", true);
        svg.attr("width", $("#graph").width())
            .attr("height", 980);

        // draw border around svg image
        // svg.append("rect")
        //     .attr("class", "outline")
        //     .attr("width", diameter)
        //     .attr("height", diameter);
        radius = wh/2;
        // create plot area within svg image
        var plot = svg.append("g")
            .attr("width", wh)
            .attr("height", wh)
            .attr("id", "plot")
            .attr("transform", "translate(" + $("#graph").width()/2 + ", " + 980/2 + ")");

       zoom.on("zoom", function() {
            plot.attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");
        });
        svg.call(zoom);


        // draw border around plot area
        // plot.append("circle")
        //     .attr("class", "outline")
        //     .attr("r", radius - margin);

        // fix graph links to map to objects instead of indices
//        graph.links.forEach(function(d, i) {
//            d.source = isNaN(d.source) ? d.source : graph.nodes[d.source];
//            d.target = isNaN(d.target) ? d.target : graph.nodes[d.target];
//        });

        // calculate node positions
        circleLayout(graph.nodes);

        // draw edges first
        //drawLinks(graph.links);
        drawCurves(graph.links);

        // draw nodes last
        drawNodes(graph.nodes);



    }

    // Calculates node locations
    function circleLayout(nodes) {
        // sort nodes by group
        nodes.sort(function(a, b) {
            return a.datasource - b.datasource;
        });

        // use to scale node index to theta value
        var scale = d3.scale.linear()
            .domain([0, nodes.length])
            .range([0, 2 * Math.PI]);

        // calculate theta for each node
        nodes.forEach(function(d, i) {
            // calculate polar coordinates
            var theta  = scale(i);
            var radial = radius - margin;

            // convert to cartesian coordinates
            d.x = radial * Math.sin(theta);
            d.y = radial * Math.cos(theta);
        });
    }
    var circularnode, circularlink, circulartext;
    function dragged(d) {
        d.x = d3.event.x, d.y = d3.event.y;
        d3.select(this).attr("cx", d.x).attr("cy", d.y);
        circularlink.filter(function(l) { return l.source === d; }).attr("x1", d.x).attr("y1", d.y);
        circularlink.filter(function(l) { return l.target === d; }).attr("x2", d.x).attr("y2", d.y);
        var curve = d3.svg.diagonal()
            .projection(function(d) { return [d.x, d.y]; });
       circularlink.filter(function(l) { return l.source === d; }).attr("d", curve);
       circularlink.filter(function(l) { return l.target === d; }).attr("d", curve);
      }

    // Draws nodes with tooltips
    function drawNodes(nodes) {
        // used to assign nodes color by group
        // var color = d3.scale.category20();

        circularnode = d3.select("#plot").selectAll(".node")
            .data(nodes)
            .enter()
            .append("circle")
            .attr("class", "node")
            .attr("id", function(d, i) { return d.label; })
            .attr("cx", function(d, i) { return d.x; })
            .attr("cy", function(d, i) { return d.y; })
            .attr("r", 10)
            .style("fill",   function(d, i) { return color(d.datasource); })
            .on("mouseover", function(d, i) { addTooltip(d3.select(this)); })
            .on("mouseout",  function(d, i) { d3.select("#tooltip").remove(); })
            .call(d3.behavior.drag().on("drag", dragged));

    }

    // Draws straight edges betw    een nodes
    function drawLinks(links) {
        circularlink = d3.select("#plot").selectAll(".link")
            .data(links)
            .enter()
            .append("line")
            .attr("class", "link")
            .style("stroke", function(d, i) { return default_link_color; })
            .attr("x1", function(d) { return d.source.x; })
            .attr("y1", function(d) { return d.source.y; })
            .attr("x2", function(d) { return d.target.x; })
            .attr("y2", function(d) { return d.target.y; });
    }

    // Draws curved edges between nodes
    function drawCurves(links) {
        // remember this from tree example?
        var curve = d3.svg.diagonal()
            .projection(function(d) { return [d.x, d.y]; });

        circularlink = d3.select("#plot").selectAll(".link")
            .data(links)
            .enter()
            .append("path")
            .attr("class", "link")
            .style("stroke-width", nominal_stroke)
            .style("stroke", function(d, i) { return default_link_color; })
            .attr("d", curve);
    }








//    var color = d3.scale.linear()
//      .domain([min_score, (min_score+max_score)/2, max_score])
//      .range(["lime", "yellow", "red"]);
//     //d3.scale.category20c();
////     var color = d3.scale.ordinal()
////          .domain(["G", "R", "B"]);
//
//    var color = d3.scale.linear().domain([1,100])
//      .interpolate(d3.interpolateHcl)
//      .range([d3.rgb("#FF0F00"), d3.rgb('#007AFF')]);
    //draw the DAG graph using d3.js
    function drawWhyDAG(nodes, links){

        var width  = 960,
        height = 800,
        // colors = d3.scale.category10();
        colors = d3.scale.ordinal()
          .domain(["G", "R", "B"])
          .range(["#009933", "#FF0000", "#0000FD"]);
        //clear explanation body element
        $('#graph').html("");
        var keyc = true, keys = true, keyt = true, keyr = true, keyx = true, keyd = true, keyl = true, keym = true, keyh = true, key1 = true, key2 = true, key3 = true, key0 = true

        var focus_node = null, highlight_node = null;

        var text_center = false;
        var outline = false;
        var w = w;
        var h = h;
        var min_score = 0;
        var max_score = 1;
        var highlight_color = "blue";
        var highlight_trans = 0.1;
        var colors = d3.scale.linear()
            .domain([min_score, (min_score+max_score)/2, max_score])
            .range(["lime", "yellow", "red"]);
        var size = d3.scale.pow().exponent(1)
          .domain([1,100])
          .range([8,64]);

        var default_node_color = "#ccc";
        //var default_node_color = "rgb(3,190,100)";
        var default_link_color = "#888";
        var nominal_base_node_size = 8;
        var nominal_text_size = 10;
        var max_text_size = 24;
        var nominal_stroke = 1.5;
        var max_stroke = 4.5;
        var max_base_node_size = 36;
        var min_zoom = 0.1;
        var max_zoom = 7;
        var zoom = d3.behavior.zoom().scaleExtent([min_zoom,max_zoom]);
        var svg = d3.select('#graph')
            .append('svg');

            //.attr("preserveAspectRatio", "xMinYMin meet")
            //.attr("viewBox", "0 0 960 500");
        svg.style("cursor","move");

        // init D3 force layout
          var force = d3.layout.force()
              .nodes(nodes)
              .links(links)
              .linkDistance(150)
              .charge(-300)
              .size([w,h])
              .on('tick', tick)
              .start();

          // define arrow markers for graph links
          svg.append('svg:defs').append('svg:marker')
              .attr('id', 'end-arrow')
              .attr('viewBox', '0 -5 10 10')
              .attr('refX', 16)
              .attr('markerWidth', 3)
              .attr('markerHeight', 3)
              .attr('orient', 'auto')
            .append('svg:path')
              .attr('d', 'M0,-5L10,0L0,5')
              .attr('fill', '#000');

          svg.append('svg:defs').append('svg:marker')
              .attr('id', 'start-arrow')
              .attr('viewBox', '0 -5 10 10')
              .attr('refX', 4)
              .attr('markerWidth', 3)
              .attr('markerHeight', 3)
              .attr('orient', 'auto')
            .append('svg:path')
              .attr('d', 'M10,-5L0,0L10,5')
              .attr('fill', '#000');

          // handles to link and node element groups
          var path = svg.append('svg:g').selectAll('path'),
              circle = svg.append('svg:g').selectAll('g');


        //mouse event vars
          var selected_node = null,
             selected_link = null;
        //update force layout (called automatically each iteration)
          function tick() {
            // draw directed edges with proper padding from node centers
            path.attr('d', function(d) {
              var deltaX = d.target.x - d.source.x,
                  deltaY = d.target.y - d.source.y,
                  dist = Math.sqrt(deltaX * deltaX + deltaY * deltaY),
                  normX = deltaX / dist,
                  normY = deltaY / dist,
                  sourcePadding = d.left ? 17 : 12,
                  targetPadding = d.right ? 17 : 12,
                  sourceX = d.source.x + (sourcePadding * normX),
                  sourceY = d.source.y + (sourcePadding * normY),
                  targetX = d.target.x - (targetPadding * normX),
                  targetY = d.target.y - (targetPadding * normY);
              return 'M' + sourceX + ',' + sourceY + 'L' + targetX + ',' + targetY;
            });
            path.attr('id', function(d){
                  return d.id;
            });
            circle.attr('transform', function(d) {
              return 'translate(' + d.x + ',' + d.y + ')';
            });
          }


        //update graph (called when needed)
        //function restart() {
         // path (link) group
         path = path.data(links);

         // update existing links
         path.classed('selected', function(d) { return d === selected_link; })
           .style('marker-start', function(d) { return d.left ? 'url(#start-arrow)' : ''; })
           .style('marker-end', function(d) { return d.right ? 'url(#end-arrow)' : ''; });


         // add new links
         path.enter().append('svg:path')
           .attr('class', 'link')
           .classed('selected', function(d) { return d === selected_link; })
           .style('marker-start', function(d) { return d.left ? 'url(#start-arrow)' : ''; })
           .style('marker-end', function(d) { return d.right ? 'url(#end-arrow)' : ''; });


         var thing = svg.append("svg:g").selectAll("text").data(links)
                    .attr("id", "thing")
                    .style("fill", "navy");

            thing.enter().append("text")
                     .style("font-size", "16px")
                     .attr("dx", 30)
                     .attr("dy", 18)
                .append("textPath")
                   .attr("xlink:href", function(d){return "#" + d.id;})
                    .text(function(d){
                        if(d.pred.lastIndexOf("/") == -1)
                            return d.pred;
                        return d.pred.substring(d.pred.lastIndexOf('/'), d.pred.length);
                    });



         // remove old links
         path.exit().remove();


         // circle (node) group
         // NB: the function arg is crucial here! nodes are known by id, not by index!
         circle = circle.data(nodes, function(d) { return d.id; });

         // update existing nodes (reflexive & selected visual states)
         circle.selectAll('circle')
           .style('fill', function(d) { return  (d === selected_node) ?d3.rgb(colors(d.datasource)).brighter().toString() : colors(d.datasource); })
           .classed('reflexive', function(d) { return d.reflexive; });

         // add new nodes
         var g = circle.enter().append('svg:g');
         var drag = d3.behavior.drag()
            .origin(function(d) { return d; })
            .on("dragstart", dragstarted)
            .on("drag", dragged)
            .on("dragend", dragended);

         g.append('svg:circle')
           .attr('class', 'node')
           .attr('r', 29)
           .style('fill', function(d) { return (d === selected_node) ? d3.rgb(colors(d.datasource)).brighter().toString() : colors(d.datasource); })
           .style('stroke', function(d) { return d3.rgb(colors(d.datasource)).darker().toString(); })
           .classed('reflexive', function(d) { return d.reflexive; });

           var dragcontainer = d3.behavior.drag()
                .on("drag", function(d, i) {
                    d3.select(this).attr("transform", "translate(" + (d.x = d3.event.x) + ","
                    + (d.y = d3.event.y) + ")");
                })
           // .call(drag);

//         function dragged(d) {
//            d.x = d3.event.x, d.y = d3.event.y;
//            d3.select(this).attr("cx", d.x).attr("cy", d.y);
//            path.filter(function(l) { return l.source === d; }).attr("x1", d.x).attr("y1", d.y);
//            path.filter(function(l) { return l.target === d; }).attr("x2", d.x).attr("y2", d.y);
//          }
           //.call(d3.behavior.drag()
           /*
            .on("start", dragstarted)
            .on("drag", dragged)
            .on("end", dragended));*/
            /*function dragstarted(d) {
                console.log("drag start");
              d3.select(this).raise().classed("active", true);
            }

            function dragged(d) {
              d3.select(this).attr("cx", d.x = d3.event.x).attr("cy", d.y = d3.event.y);
            }

            function dragended(d) {
              d3.select(this).classed("active", false);
            }
*/
            function dragstarted(d) {
                d3.select(this).classed("dragging", true);
                if (!d3.event.active)
                    simulation.alphaTarget(0.3).restart();
                d.fx = d.x;
                d.fy = d.y;
            }

            function dragged(d) {
                d.fx = d3.event.x;
                d.fy = d3.event.y;
            }

            function dragended(d) {
            d3.select(this).classed("dragging", false);
                if (!d3.event.active)
                    simulation.alphaTarget(0);
                d.fx = null;
                d.fy = null;
            }


         // show node IDs
         g.append('svg:text')
             .attr('x', 0)
             .attr('y', 0)
             .attr('class', 'id')
             .text(function(d) {
                 if(d.label.lastIndexOf('/') == -1)
                    return d.label;
                 return d.label.substring(d.label.lastIndexOf('/'), d.label.length);
                 });


         // remove old nodes
         circle.exit().remove();

         // set the graph in motion
         force.start();
        /*var node_drag = d3.behavior.drag()
                .on("dragstart", dragstart)
                .on("drag", dragmove)
                .on("dragend", dragend);

        function dragstart(d, i) {
            console.log("drag start");
            force.stop() // stops the force auto positioning before you start dragging
        }

        function dragmove(d, i) {
        console.log("drag move");
            d.px += d3.event.dx;
            d.py += d3.event.dy;
            d.x += d3.event.dx;
            d.y += d3.event.dy;
            tick(); // this is the key to make it work together with updating both px,py,x,y on d !
        }

        function dragend(d, i) {
        console.log("drag end");
            d.fixed = true; // of course set the node to fixed so the force doesn't include the node in its auto positioning stuff
            tick();
            force.resume();
        }
    */

    }


    function getdata(){

       //list of subjects and objects for the DAG
        var nodes = [];
        //connection link between subject and object ->predicates
        var links = [];
        var j=0;
        $('#rdfmtsdataTables tbody').empty();
        $.getJSON('iasiskg-new.json', function(data) {

                var k = 0;
           for(var i in data){
                var node = {};
                var tablerow = [];
                var n =  data[i].rootType;
                var l = data[i].rootType.lastIndexOf("/");
                var name = n.substring(l+1);
                tablerow.push(k+1);
                k = k + 1;
                tablerow.push(name);
                tablerow.push(n);
                tablerow.push(data[i].wrappers[0].cardinality);
                tablerow.push(data[i].predicates.length);
                tablerow.push(data[i].linkedTo.length);

                table.row.add(tablerow).draw( false );
                node.id=j;
                node.label=name;
                node.reflexive = true;
                node.r = 20;
                //check if subject already in nodes list
                var snode = searchNode(nodes, name);
                if(snode == null){
                    nodes.push(node);
                    snode = node;
                }

                j++;
                linkedto = data[i].linkedTo;

                for (lk in linkedto){
                    n =  linkedto[lk];
                    li = n.lastIndexOf("/");
                    lkname = n.substring(li+1);

                    var node2={};
                    node2.id=j;
                    node2.label=lkname;
                    node2.reflexive = true;

                    //check if object already in nodes list
                    var onode = searchNode(nodes, lkname);
                    if(onode == null){
                       nodes.push(node2);
                       onode = node2;
                    }

                    //connects subject - predicate - object -> a single triple
                    var link = {};

                    if (snode != null && onode != null && snode.id != onode.id){
                        link.source = snode;
                        link.pred = "";
                        link.target =onode;
                        link.left = false;
                        link.right = true;
                        link.id="s"+i;
                        //add to connection list array
                        links.push(link);

                    }
                    j++;
                }

            }

            //drawWhyDAG(nodes, links);
            data = {nodes:nodes, links: links};
            //setSize(data);
            drawWhyDAG(nodes, links);
           });
     }

    function main() {
        var range = 100
        var data = {
            nodes:d3.range(0, range).map(function(d){ return {label: "l"+d ,r:~~d3.randomUniform(8, 28)()}}),
            links:d3.range(0, range).map(function(){ return {source:~~d3.randomUniform(range)(), target:~~d3.randomUniform(range)()} })
        }
        console.log(data)
        setSize(data)
        drawChart(data)
    }

    function setSize(data) {
        width = document.querySelector("#graph").clientWidth
        height = document.querySelector("#graph").clientHeight

        margin = {top:0, left:0, bottom:0, right:0 }


        chartWidth = width - (margin.left+margin.right)
        chartHeight = height - (margin.top+margin.bottom)

        svg.attr("width", 860).attr("height", 800)


        chartLayer
            .attr("width", chartWidth)
            .attr("height", chartHeight)
            .attr("transform", "translate("+[margin.left, margin.top]+")")


    }

    function drawChart(data) {

        var simulation = d3.forceSimulation()
            .force("link", d3.forceLink().id(function(d) { return d.index }))
            .force("collide",d3.forceCollide( function(d){return d.r + 8 }).iterations(16) )
            .force("charge", d3.forceManyBody())
            .force("center", d3.forceCenter(chartWidth / 2, chartHeight / 2))
            .force("y", d3.forceY(0))
            .force("x", d3.forceX(0))

        var link = svg.append("g")
            .attr("class", "links")
            .selectAll("line")
            .data(data.links)
            .enter()
            .append("line")
            .attr("stroke", "black")

        var node = svg.append("g")
            .attr("class", "nodes")
            .selectAll("circle")
            .data(data.nodes)
            .enter().append("circle")
            .attr("r", function(d){  return d.r })
            .call(d3.drag()
                .on("start", dragstarted)
                .on("drag", dragged)
                .on("end", dragended));


        var ticked = function() {
            link
                .attr("x1", function(d) { return d.source.x; })
                .attr("y1", function(d) { return d.source.y; })
                .attr("x2", function(d) { return d.target.x; })
                .attr("y2", function(d) { return d.target.y; });

            node
                .attr("cx", function(d) { return d.x; })
                .attr("cy", function(d) { return d.y; });
        }



        function dragstarted(d) {
            if (!d3.event.active) simulation.alphaTarget(0.3).restart();
            d.fx = d.x;
            d.fy = d.y;
        }

        function dragged(d) {
            d.fx = d3.event.x;
            d.fy = d3.event.y;
        }

        function dragended(d) {
            if (!d3.event.active) simulation.alphaTarget(0);
            d.fx = null;
            d.fy = null;
        }
        // define arrow markers for graph links
          svg.append('svg:defs').append('svg:marker')
              .attr('id', 'end-arrow')
              .attr('viewBox', '0 -5 10 10')
              .attr('refX', 16)
              .attr('markerWidth', 3)
              .attr('markerHeight', 3)
              .attr('orient', 'auto')
            .append('svg:path')
              .attr('d', 'M0,-5L10,0L0,5')
              .attr('fill', '#000');

          svg.append('svg:defs').append('svg:marker')
              .attr('id', 'start-arrow')
              .attr('viewBox', '0 -5 10 10')
              .attr('refX', 4)
              .attr('markerWidth', 3)
              .attr('markerHeight', 3)
              .attr('orient', 'auto')
            .append('svg:path')
              .attr('d', 'M10,-5L0,0L10,5')
              .attr('fill', '#000');
        // handles to link and node element groups
          var path = link, // svg.append('svg:g').selectAll('path'),
              circle = node; svg.append('svg:g').selectAll('g');
          //mouse event vars
          var selected_node = null,
             selected_link = null;

           //update force layout (called automatically each iteration)
          function tick() {
            // draw directed edges with proper padding from node centers
            path.attr('d', function(d) {
              var deltaX = d.target.x - d.source.x,
                  deltaY = d.target.y - d.source.y,
                  dist = Math.sqrt(deltaX * deltaX + deltaY * deltaY),
                  normX = deltaX / dist,
                  normY = deltaY / dist,
                  sourcePadding = d.left ? 17 : 12,
                  targetPadding = d.right ? 17 : 12,
                  sourceX = d.source.x + (sourcePadding * normX),
                  sourceY = d.source.y + (sourcePadding * normY),
                  targetX = d.target.x - (targetPadding * normX),
                  targetY = d.target.y - (targetPadding * normY);
              return 'M' + sourceX + ',' + sourceY + 'L' + targetX + ',' + targetY;
            });
            path.attr('id', function(d){
                  return d.id;
            });
            circle.attr('transform', function(d) {
              return 'translate(' + d.x + ',' + d.y + ')';
            });
          }


          //update graph (called when needed)
        //function restart() {
         // path (link) group
         path = simulation.force("link").links(data.links);

         // update existing links
         path.classed('selected', function(d) { return d === selected_link; })
           .style('marker-start', function(d) { return d.left ? 'url(#start-arrow)' : ''; })
           .style('marker-end', function(d) { return d.right ? 'url(#end-arrow)' : ''; });


         // add new links
         path.enter().append('svg:path')
           .attr('class', 'link')
           .classed('selected', function(d) { return d === selected_link; })
           .style('marker-start', function(d) { return d.left ? 'url(#start-arrow)' : ''; })
           .style('marker-end', function(d) { return d.right ? 'url(#end-arrow)' : ''; });


         var thing = svg.append("svg:g").selectAll("text").data(data.links)
                    .attr("id", "thing")
                    .style("fill", "navy");

        thing.enter().append("text")
                 .style("font-size", "16px")
                 .attr("dx", 30)
                 .attr("dy", 18)
                 .append("textPath")
                 .attr("xlink:href", function(d){return "#" + d.id;})
                 .text(function(d){
                    if(d.pred.lastIndexOf("/") == -1)
                        return d.pred;
                    return d.pred.substring(d.pred.lastIndexOf('/'), d.pred.length);
                });



         // remove old links
         path.exit().remove();
         // circle (node) group
         // NB: the function arg is crucial here! nodes are known by id, not by index!
         circle = simulation.nodes(data.nodes).on("tick", tick);
             //circle.data(data.nodes, function(d) { return d.id; });
         var colors = d3.scaleOrdinal().range(d3.schemeCategory20);
         // update existing nodes (reflexive & selected visual states)
         circle.selectAll('circle')
           .style('fill', function(d) { return  (d === selected_node) ?d3.rgb(colors(d.id)).brighter().toString() : colors(d.id); })
           .classed('reflexive', function(d) { return d.reflexive; });

         // add new nodes
         var g = circle.enter().append('svg:g');

         g.append('svg:circle')
           .attr('class', 'node')
           .attr('r', 29)
           .style('fill', function(d) { return (d === selected_node) ? d3.rgb(colors(d.id)).brighter().toString() : colors(d.id); })
           .style('stroke', function(d) { return d3.rgb(colors(d.id)).darker().toString(); })
           .classed('reflexive', function(d) { return d.reflexive; })
           .call(d3.drag()
                .on("start", dragstarted)
                .on("drag", dragged)
                .on("end", dragended));


         // show node IDs
         g.append('svg:text')
             .attr('x', 0)
             .attr('y', 0)
             .attr('class', 'id')
             .text(function(d) {
                 if(d.label.lastIndexOf('/') == -1)
                    return d.label;
                 return d.label.substring(d.label.lastIndexOf('/'), d.label.length);
                 });

         // remove old nodes
         circle.exit().remove();

    }

});
