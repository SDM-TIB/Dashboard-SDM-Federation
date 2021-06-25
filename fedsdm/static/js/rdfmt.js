$(document).ready(function() {

//    $("#federationslist").prop("disabled", true);
    $("#datasources").prop("disabled", true);
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
        $("#legend").empty();
        $("#vdsname").html("");

        get_rdfmts_stats(fed);
        $("#graph").html("<h1> Loading ... !</h1>");
        get_rdfmts(fed);
        get_rdfmts_graph_analys(fed);

        federation = fed;
        loaded = 0;
        vized = 0;
        galoaded = 0;
    }

    function get_rdfmts_stats(fed){
        if (fed == null || (fed == federation)){
            console.log("already loaded");
                return
         }
        $("#fedName").html(fed);
        $("#vfedName").html(fed);
        $("#afedName").html(fed);
        if (stats == null){
            stats = $('#rdfmtsdataTables').DataTable({
                        order: [[ 1, 'desc' ]],
                        responsive: true,
                        select: true,
                        lengthMenu: [ [10, 25, 50, -1], [10, 25, 50, "All"] ],
                        dom: 'Blfrtip',
                        "processing": true,
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

            stats.on( 'select', function ( e, dt, type, indexes ) {
                    selectedRow = stats.rows( indexes ).data().toArray();
                    console.log("selected row:", selectedRow)
                    $( "#editds" ).prop( "disabled", false );
                    $( "#removeds" ).prop( "disabled", false );
                    $( "#mtdetails" ).prop( "disabled", false );
                    $( "#mtdetails" ).text("Show Details");
                    $( "#mtdetails" ).show();
                    $( "#backtotable" ).hide();
                }).on( 'deselect', function ( e, dt, type, indexes ) {
                    var rowData = stats.rows( indexes ).data().toArray();
                    $( "#editds" ).prop( "disabled", true );
                    $( "#removeds" ).prop( "disabled", true );
                    $( "#mtdetails" ).prop( "disabled", true );
                    $( "#mtdetails" ).hide();
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
    var loaded = 0;
    var vized = 0;
    var stats = null;
    var galoaded = 0;
    var gtable = null;
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
           alchemydata = {"edges": links, "nodes": nodes}
           alchemynodetype = []
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
               alchemynodetype.push(name);
               datasources += '<li class="datasource"><a href="#" class="datasource"  id="source-'+(i+1)+'">'+ name +'</a></li>'
               legend = legend + '<span style="color:' + color(v) + '"><b>' + name +"</b></span> <br/> ";
            }

            $("#legend").empty();
            $("#legend").html(legend);

            $("#gadatasources").html(datasources);
            $("#datasources").html(datasources);
            $("#datasources").prop("disabled", false)
            $("#graph").html("<h1> Please select data source!</h1>");

            var config = {
                  dataSource: alchemydata,
                  cluster: true,
                  nodeTypes: {"datasource":alchemynodetype },
                  nodeCaption: "label",
                  rootNodeRadius: 30,
                  showControlDash: true,
                  showStats: true,
                  nodeStats: true,
                  showFilters: true,
                  nodeFilters: true,
                  captionToggle: true,
                  edgesToggle: true,
                  nodesToggle: true,
                  toggleRootNotes: false,
                  zoomControls: true
            };

           alchemy.begin(config);
           });


    }

    function get_rdfmts_graph_analys(fed, source){
        if (fed == null || (fed == federation && galoaded == 1)){
                return
         }
        $("#fedName").html(fed);
        $("#vfedName").html(fed);
        $("#afedName").html(fed);
        if (gtable == null){
            gtable = $('#graph-analysis').DataTable({
                responsive: true,
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



    var showmore = function (key){
       console.log('p[class=legend'+key+']');
       $('p[class=legend'+key+']').toggle();
    };

    function drawDonut(sourcemt){
        if (source != "All"){
            $("#graph").empty();
            $("#graph").append('<div style="float:left"><div id="morris-donut-chart" style="float:left"></div><div  style="margin-top:10px;display:inline-block" id="legendd" class="donut-legend"></div></div>')
                mtcards[sourcemt].sort(function(a, b) {
                    return b.value - a.value;
                });
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
                $("#graph").append('<div style="float:left"><div id="morris-donut-chart' + key +'" style="float:left"></div><div style="margin-top:10px;display:inline-block" id="legend'+key +'" class="donut-legend"></div></div>')
                val.sort(function(a, b) {
                    return b.value - a.value;
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




});