$(document).ready(function() {


    /*
    **********************************************************
    ************ Manage data source Data Table ***************
    **********************************************************
    */

    var federation = $("#federationslist").val(),
        datasource = null;

    var stats=[];
    var statsTable = null;
    var ctx = $("#myChart");
    var myBarChart = null;
    var bsloaded = 0;

    // if federation is set from session, then trigger visualization and management data
    showFederations(federation);

    // Federation list dropdown change event, triggers visualization of statistics and management data
     $("#federationslist").change(function(){
        fed = $( this ).val()
        federation = fed;
        showFederations(federation);
    });

    // check if federation name is set, and show statistics and management data
    function showFederations(fed){
        if (federation != null && federation != ""){
            $("#mfedName").html(federation);
            basic_stat(federation);
            manage(federation);
            if (federation != "All"){
                $("#addds").prop( "disabled", false );
                $("#findalllinks" ).prop( "disabled", false );
            }else{
                $("#addds").prop( "disabled", true );
                $("#findalllinks" ).prop( "disabled", true );
            }
        }else{
            disableButtons();
        }
    }

    // if no federation is selected, then all action buttons will be disabled
    function disableButtons(){
        $("#addds").prop( "disabled", true );
        $( "#editds" ).prop( "disabled", true );
        $( "#removeds" ).prop( "disabled", true );
        $( "#recomputemts" ).prop( "disabled", true );
        $( "#findalllinks" ).prop( "disabled", true );
    }

    // basic statistics and bar chart data
    function basic_stat(fed){
        if (statsTable == null){
            // Construct basic statistics table
            statsTable = $("#basic-statistics").DataTable({
                    order: [[ 1, 'desc' ]],
                    responsive: true,
                    defaultContent: "-1",
                    select: true
                });
            }
        else{
            statsTable.clear().draw();
        }
        var datas = [];
        $.ajax({
            type: 'GET',
            headers: {
                Accept : "application/json"
            },
            url: '/federation/stats',
            data: {"graph": fed},
            dataType: "json",
            crossDomain: true,
            success: function(data, textStatus, jqXHR){
                datas = data.data;
                datasdon = [];
                var bardata = {labels:[], rdfmts:[], triples:[]};
                for (d in datas){
                    rem = [];
                    console.log(datas)
                    rem.push(datas[d].ds);
                    var rdfmts = datas[d].rdfmts;

                    rem.push(rdfmts);
                    var triples = datas[d].triples;
                    if (triples == null){
                        triples = "-1"
                    }

                    rem.push(triples);
                    statsTable.row.add(rem).draw( false );

                    bardata.labels.push(datas[d].ds);
                    rdfmts = Math.log10(rdfmts);
                    bardata.rdfmts.push(rdfmts);
                    triples = Math.log10(triples);
                    bardata.triples.push(triples);
                }

                if (myBarChart == null){
                    myBarChart = new Chart(ctx, {
                    type: 'horizontalBar',
                    data: {
                        labels: bardata.labels,
                        datasets :[{
                                    id: 1,
                                    label: "# of RDF-MTs (log)",
                                    data: bardata.rdfmts,
                                    borderWidth: 1,
                                    backgroundColor: '#6b5b95',
                                },
                                {
                                    id: 2,
                                    label: "# of Triples(log)",
                                    data: bardata.triples,
                                    borderWidth: 1,
                                    backgroundColor: '#d64161',
                                }]
                        },
                    options: {
                            scales: {
                                yAxes: [{
                                    ticks: {
                                        beginAtZero:true
                                          },
                                    gridLines: {
                                         offsetGridLines: true
                                         }
                                      }],
                                xAxes: [{
                                       gridLines: {
                                          offsetGridLines: true
                                       }
                                 }]
                            },
                            legend: {
                                display: true,
                                labels: {
                                    fontColor: 'rgb(25, 99, 132)',
                                    boxWidth: 8

                                }
                            },
                            tooltips: {
                                callbacks: {
                                    label: function(tooltipItem, data) {
                                        var label = data.datasets[tooltipItem.datasetIndex].label || '';

                                        if (label) {
                                            label = label.substring(0, label.indexOf("("));
                                            label += ': ';
                                        }
                                        var xv = Math.pow(10, tooltipItem.xLabel);
                                        label += Math.round(xv * 100) / 100 ;
                                        return label;
                                    }
                                }
                            }
                    }
                });
                }
                else{
                    myBarChart.data.labels=[];
                    myBarChart.data.datasets=[];
                    myBarChart.update();


                    myBarChart.data.labels = bardata.labels;
                    myBarChart.data.datasets = [{
                                                id: 1,
                                                label: "# of RDF-MTs (log)",
                                                data: bardata.rdfmts,
                                                borderWidth: 1,
                                                backgroundColor: '#6b5b95',
                                            },
                                            {
                                                id: 2,
                                                label: "# of Triples(log)",
                                                data: bardata.triples,
                                                borderWidth: 1,
                                                backgroundColor: '#d64161',
                                            }]

                    myBarChart.update();

                }
        },
            error: function(jqXHR, textStatus, errorThrown){
                console.log(jqXHR.status);
                console.log(jqXHR.responseText);
                console.log(textStatus);
            }
        });
        bsloaded = 1;
    }

    var table ;
    var selectedSource = null;

    // basic information about data sources in a given federation
    function manage(fed){
        $("#mfedName").html(fed);
        //Disable buttons before selecting item on the table
        $( "#editds" ).prop( "disabled", true );
        $( "#removeds" ).prop( "disabled", true );
        $( "#recomputemts" ).prop( "disabled", true );

        //Construct data source management data table
        if (table == null){
            mngloaded = 1;
            table = $('#datasources').DataTable({
                order: [[ 1, 'desc' ]],
                responsive: true,
                select: true,
                defaultContent: "<i>Not set</i>",
                ajax: "/federation/datasources?graph=" + fed + "&dstype=All"
            });
            // Dat source table select action
            table.on( 'select', function ( e, dt, type, indexes ) {
                    selectedSource = table.rows( indexes ).data().toArray();
                    $( "#editds" ).prop( "disabled", false );
                    $( "#removeds" ).prop( "disabled", false );
                    $( "#recomputemts" ).prop( "disabled", false );

                }).on( 'deselect', function ( e, dt, type, indexes ) {
                    var rowData = table.rows( indexes ).data().toArray();
                    $( "#editds" ).prop( "disabled", true );
                    $( "#removeds" ).prop( "disabled", true );
                    $( "#recomputemts" ).prop( "disabled", true );
                    selectedSource = null
                });
        }else{
            table.clear().draw();
            $( "#editds" ).prop( "disabled", true );
            $( "#removeds" ).prop( "disabled", true );
            $( "#recomputemts" ).prop( "disabled", true );
            table.ajax.url("/federation/datasources?graph=" + fed).load();
        }
        table.on('draw', function(){
            if (table.column(0).data().length > 0){
                $( "#findlinks" ).prop( "disabled", false );
            }else{
                $( "#findlinks" ).prop( "disabled", true);
            }
        });

    }

    // Add data source click action
     $( "#addds" ).click(function() {
      dialog.dialog("open");
    });

    // Edit data source click action
    $( "#editds" ).click(function() {
      console.log(selectedSource[0][0]);
      $( "#ename" ).val(selectedSource[0][1]);
      $( "#eURL" ).val(selectedSource[0][2]);
      $( "#edstype" ).val(selectedSource[0][3]);
      $( "#elabel" ).val(selectedSource[0][7]);
      $( "#eparams" ).val(selectedSource[0][9]);
      edialog.dialog('open');
    });

    //Remove data source click action
    $('#removeds').click( function () {

        // delete where {<http://ontario.tib.eu/Federation1/datasource/Ensembl-json> ?p ?o}
        $.ajax({
                type: 'GET',
                headers: {
                    Accept : "application/json"
                },
                url: '/federation/api/removeds',
                data: {'ds':selectedSource[0][0], 'fed': fed},
                dataType: "json",
                crossDomain: true,
                success: function(data, textStatus, jqXHR){
                    if (data == true)
                        table.row('.selected').remove().draw( false );
                },
                error: function(jqXHR, textStatus, errorThrown){
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                }
            });

        $( "#editds" ).prop( "disabled", true );
        $( "#removeds" ).prop( "disabled", true );
        $( "#recomputemts" ).prop( "disabled", true );
    });

    // Create Mappings click action
    $('#recomputemts').click( function () {
        console.log(selectedSource[0][0]);
          $.ajax({
                type: 'GET',
                headers: {
                    Accept : "application/json"
                },

                url: '/federation/api/recreatemts?fed=' + encodeURIComponent(federation) +"&datasource=" + encodeURIComponent(selectedSource[0][0]),
                data: {'query':'all'},
                dataType: "json",
                crossDomain: true,
                success: function(data, textStatus, jqXHR){
                    if (data != null && data.status == 1){
                        alert("Recreating RDF-MTs for "+ selectedSource[0][0] + " is underway ...");
                    }else{
                        alert("Cannot start the process. Please check if there are data sources in this federation.");
                    }
                },
                error: function(jqXHR, textStatus, errorThrown){
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                }
            });
    });

    $('#findlinks').click( function () {
        console.log(selectedSource[0][0]);
          $.ajax({
                type: 'GET',
                headers: {
                    Accept : "application/json"
                },

                url: '/federation/api/findlinks?fed=' + encodeURIComponent(federation) +"&datasource=" + encodeURIComponent(selectedSource[0][0]),
                data: {'query':'all'},
                dataType: "json",
                crossDomain: true,
                success: function(data, textStatus, jqXHR){
                    if (data != null && data.status == 1){
                        alert("Finding links in progress ...");
                    }else{
                        alert("Cannot start the process. Please check if there are data sources in this federation.");
                    }
                },
                error: function(jqXHR, textStatus, errorThrown){
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                }
            });
    });

    $('#findalllinks').click( function () {
          $.ajax({
                type: 'GET',
                headers: {
                    Accept : "application/json"
                },

                url: '/federation/api/findlinks?fed=' + encodeURIComponent(federation),
                dataType: "json",
                crossDomain: true,
                success: function(data, textStatus, jqXHR){
                    if (data != null && data.status == 1){
                        alert("Finding links in progress ...");
                    }else{
                        alert("Cannot start the process. Please check if there are data sources in this federation.");
                    }
                },
                error: function(jqXHR, textStatus, errorThrown){
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                }
            });
    });

    /*
    ***************************************************
    ***** Dialog management functions *****************
    ***************************************************
    */
    var dialog, edialog, form,
    // From http://www.whatwg.org/specs/web-apps/current-work/multipage/states-of-the-type-attribute.html#e-mail-state-%28type=email%29
    name =     $("#name" ),
    desc =     $("#desc" ),
    dstype =   $("#dstype"),
    URL =      $("#URL" ),
    params =   $("#params" ),
    keywords =     $("#keywords" ),
    organization = $("#organization" ),
    homepage =     $("#homepage" ),
    version =      $("#version" ),

    allFields = $( [] ).add( name ).add( desc ).add( dstype ).add( URL ).add( params ).add( keywords ).add( organization ).add( homepage ).add( version ),
    tips = $( ".validateTips" );

     var crnfdialog;   
    
    dialog = $( "#add-form" ).dialog({
              autoOpen: false,
              height: 800,
              width: 550,
              modal: true,
              classes: {
                  "ui-dialog": "ui-corner-all"
              },
              buttons: [{
                text: "Finish",
                    click:addDataSource,
                    class: "btn btn-success"
                },{
                    text: "Continue",
                    click:saveAndMore,
                    class: "btn btn-primary"
                },{
                    text: "Cancel",
                    click: function() {
                        dialog.dialog( "close" );
                        },
                    class: "btn btn-danger"
                }
              ],
              close: function() {
                form[0].reset();
                allFields.removeClass("ui-state-error" );
              }
         });

    form = dialog.find("form" ).on("submit", function( event ) {
          event.preventDefault();
          addDataSource(true);
     });    
    
     $( "#CreateNewFed" ).click(function() {
        crnfdialog.dialog("open");
    });
    
    
    crnfdialog = $( "#my-form" ).dialog({
        autoOpen: false,
        height: 550,
        width: 550,
        modal: true,
        classes: {
            "ui-dialog": "ui-corner-all"
        },
        buttons: [{
            text: "Create",
            click: createnewfederation,
            class: "btn btn-success"
        },{
            text: "Cancel",
            click: function() {
                crnfdialog.dialog( "close" );
            },
            class: "btn btn-danger"
        }
        ],
        close: function() {
            form[0].reset();
            allFields.removeClass("ui-state-error" );
        }
    });

    function addDataSource(close) {
          var valid = true;
          allFields.removeClass( "ui-state-error" );

          valid = valid && checkLength( name, "name", 2, 169 );
          valid = valid && checkLength( URL, "url", 6, 100 );
          //valid = valid && checkRegexp(name, /^[a-z]([0-9a-z_\s])+$/i, "Data source should consist of a-z, 0-9, underscores, spaces and must begin with a letter." );
          //valid = valid && checkRegexp( URL, emailRegex, "eg. ui@jquery.com" );

          if(valid ) {
            $.ajax({
                type: 'POST',
                headers: {
                    Accept : "application/json"
                },
                url: '/federation/addsource?fed=' + federation,
                data: { 'name':name.val(),
                        "url":URL.val(),
                        'dstype':dstype.val(),
                        'keywords':keywords.val(),
                        'params': params.val(),
                        'desc': desc.val(),
                        'version':version.val(),
                        'homepage':homepage.val(),
                        'organization':organization.val()
                },
                dataType: "json",
                crossDomain: true,
                success: function(data, textStatus, jqXHR){
                    if (data != null && data.length > 0){
                        manage(federation);
                    }else{
                        $('#validateTips').html("Error while adding data source to the federation!")
                    }
                    table.clear().draw();
                    alert('The new Data source was added.');
                    table.ajax.url("/federation/datasources?graph=" + federation).load();
                },
                error: function(jqXHR, textStatus, errorThrown){
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                }
            });
          }else{
            name.addClass( "ui-state-error" )
            URL.addClass( "ui-state-error" )
            console.log("Invalid data....");
          }
          if (close){
            dialog.dialog( "close" );
            return valid;
          }
    }

    function saveAndMore(){
        addDataSource(false);
        form[0].reset();
        allFields.removeClass("ui-state-error" );
    }

    edialog = $( "#editdsdialog" ).dialog({
              autoOpen: false,
              height: 800,
              width: 700,
              modal: true,
              classes: {
                  "ui-dialog": "highlight"
              },
              buttons: {
                "Update Data Source": updateDS,
                Cancel: function() {
                   edialog.dialog( "close" );
                }
              },
              close: function() {
                form[ 0 ].reset();
                allFields.removeClass( "ui-state-error" );
              }
        });
    function updateDS() {
       var  name =     $("#ename" ),
            desc =     $("#edesc" ),
            dstype =   $("#edstype"),
            URL =      $("#eURL" ),
            params =   $("#eparams" ),
            keywords =     $("#ekeywords" ),
            organization = $("#eorganization" ),
            homepage =     $("#ehomepage" ),
            version =      $("#eversion" ),
          allFields = $( [] ).add( name ).add( desc ).add( dstype ).add( URL ).add( params ).add( keywords ).add( organization ).add( homepage ).add( version ),
          tips = $( ".validateTips" );

       var valid = true;
       allFields.removeClass( "ui-state-error" );
       if ( valid ) {

            table.row('.selected').remove().draw( false );
            table.row.add([ name.val(), desc.val(), dstype.val(), URL.val(), params.val(),,,,,]).draw( false );
            $( "#editds" ).prop( "disabled", true );
            $( "#removeds" ).prop( "disabled", true );
            $( "#createmapping" ).prop( "disabled", true );
             edialog.dialog( "close" );
           }
       return valid;
    }

    function updateTips( t ) {
          tips.text( t ).addClass( "ui-state-highlight" );
          setTimeout(function() {
                tips.removeClass( "ui-state-highlight", 1500 );
          }, 500 );
     }

    function checkLength( o, n, min, max ) {
          if ( o.val().length > max || o.val().length < min ) {
            o.addClass( "ui-state-error" );
            updateTips( "Length of " + n + " must be between " +
              min + " and " + max + "." );
            return false;
          } else {
            return true;
          }
    }

    function checkRegexp( o, regexp, n ) {
          if ( !( regexp.test( o.val() ) ) ) {
            o.addClass( "ui-state-error" );
            updateTips( n );
            return false;
          } else {
            return true;
          }
    }

    var federation = null,
        datasource = null;
    function createnewfederation() {
        var name = $('#name').val();
        var desc = $('#description').val();
        console.log(name + " " + desc);
        if (name != null && name != '' && name.length > 0){
            $.ajax({
                type: 'POST',
                headers: {
                    Accept : "application/json"
                },
                url: '/federation/create',
                data: {'name':name, 'description':desc},

                crossDomain: true,
                success: function(data, textStatus, jqXHR){
                    console.log(data);
                    if (data != null && data.length > 0){
                        alert('The new data federation was successfully created!');
                        federation = data;
                        $("#fedName").html(name);
                        $('#newfedform').hide();
                        crnfdialog.dialog( "close" );
                        manage(federation);
                        // what to do next?

                    }else{
                        $('#errormsg').html("Error while creating the new federation! Please enter a valid name (var name).")
                    }
                },
                error: function(jqXHR, textStatus, errorThrown){
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                }
            });
        }
        if (name == null || name == '' || name.length <= 0) {
            alert('The Name field should not be empty.\nPlease insert a name in the Name field.');
        }
        return false
    }
    
    /*
    *********************************************************
    *********** Visualize sample graph **********************
    *********************************************************
    */

    function visualize_sample(){




    }

    var colors = [
        "#6b5b95",
        "#feb236",
        "#d64161",
        "#ff7b25",
        "#b2ad7f",
        "#878f99",
        "#86af49",
        "#a2b9bc",
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
});
