$(document).ready(function() {
    /*
    **********************************************************
    ************ Manage data source Data Table ***************
    **********************************************************
    */

    $('#datasourcerow').show();

    $("#showcollectionsbtn").prop("disabled", true);


    var selectedDatasourceRow = null,
        selectedDataSourceID=null;
    var selectedDatabase=null, selectedDatabaseName=null;
    var selectedCollection=null, selectedCollectionName=null;
    var collectionstable = null;
    var federation = $("#federationslist").val(),
        datasource = null;

    if (federation != null && federation != ""){
        $("#mfedName").html(federation);
        show_datasource(federation);
    }

    $("#federationslist").change(function(){
        fed = $( this ).val();
        if (fed == null || fed == ""){
            return false;
        }
        $("#mfedName").html(fed);
        show_datasource(fed);
        federation = fed;
    });
    var loadedsoruces = 0;
    var datasourcetable = null;
    function show_datasource(fed){
        if (loadedsoruces == 1 && datasourcetable){
            datasourcetable.destroy();
            $("#availabledatasources").empty();
        }
        if (datasourcetable == null){

            // Construct basic statistics table
            datasourcetable = $("#availabledatasources").DataTable({
                    order: [[ 1, 'desc' ]],
                    responsive: true,
                    select: true,
                    dom: 'Blfrtip',
                    processing: true,
                    buttons: [
                             {
                             text:'copy'
                             },
                             {
                             text:'csv',
                             extend: 'csvHtml5',
                             title: fed.replace('/', '_')
                             },
                             {
                             text:'excel',
                             extend: 'excelHtml5',
                             title: fed.replace('/', '_')
                             },
                             {
                             text:'pdf',
                             extend: 'pdfHtml5',
                             title: fed.replace('/', '_')
                             },
                             {
                             text: 'TSV',
                             extend: 'csvHtml5',
                             fieldSeparator: '\t',
                             extension: '.tsv',
                             title: fed.replace('/', '_')
                             }],
                    ajax: "/datasources?graph=" + fed + "&dstype=All"
                });
            // Dat source table select action
            var dstale = datasourcetable;
            datasourcetable.on( 'select', function ( e, dt, type, indexes ) {
                    selectedDatasourceRow = dstale.rows( indexes ).data().toArray();
                    selectedDataSourceID = selectedDatasourceRow[0][0];
                    selectedDatabaseName = null;
                    selectedCollectionName = null;
                    $('#showcollectionsbtn').prop('disabled', false)
                }).on( 'deselect', function ( e, dt, type, indexes ) {
                    // var rowData = stats.rows( indexes ).data().toArray();
                    selectedDatasourceRow = null;
                    selectedDataSourceID = null;
                    selectedDatabaseName = null;
                    selectedCollectionName = null;
                    $('#showcollectionsbtn').prop('disabled', true)
                });
                loadedsoruces = 1
        }
        else{
            datasourcetable.clear().draw();
            $('#showcollectionsbtn').prop('disabled', true);
            selectedDatasourceRow = null;
            selectedDataSourceID = null;
            selectedDatabaseName = null;
            selectedCollectionName = null;
            datasourcetable.ajax.url("/datasources?graph=" + fed+ "&dstype=All").load();
        }
    }

    $('#showcollectionsbtn').click(function(){
        $("#federationlistrow").hide();
        $("#datasourcerow").hide();

        $("#collectionsrow").show();
        $("#datasourcename").html(selectedDatasourceRow[0][1]);
        $("#dsname").html(selectedDatasourceRow[0][1]);
        show_databases();
    });

    $("#backtodstablebtn").click(function(){
        $("#collectionsrow").hide();

        $("#federationlistrow").show();
        $("#datasourcerow").show();

        $("#dsname").html();
        $("#datasourcename").html();
    });

    function show_databases(){
        if (collectionstable == null){
        }else{
            collectionstable.destroy();
            $('#collsinfotable').empty();
        }
        collectionstable = $("#collsinfotable").DataTable({
                    order: [[ 1, 'document' ]],
                    responsive: true,
                    destroy: true,
                    select: true,
                    dom: 'Blfrtip',
                    processing: true,
                    buttons: [
                             {
                             text:'copy'
                             },
                             {
                             text:'csv',
                             extend: 'csvHtml5',
                             title: selectedDatasourceRow[0][1]
                             },
                             {
                             text:'excel',
                             extend: 'excelHtml5',
                             title: selectedDatasourceRow[0][1]
                             },
                             {
                             text:'pdf',
                             extend: 'pdfHtml5',
                             title: selectedDatasourceRow[0][1]
                             },
                             {
                             text: 'TSV',
                             extend: 'csvHtml5',
                             fieldSeparator: '\t',
                             extension: '.tsv',
                             title: selectedDatasourceRow[0][1]
                             }],
                    defaultContent: "<i>Not set</i>",
                    ajax: "/api/get_ds_collections?fed=" + encodeURIComponent(federation)
                                             + "&ds=" + encodeURIComponent(selectedDataSourceID),
                    columns: [
                        { "data": "document", "title": "Document"},
                        { "data": "db", "title": "Database"},
                        { "data": "count" , "title": "Size"}
                    ]
                });
        // Dat source table select action
        collectionstable.on( 'select', function ( e, dt, type, indexes ) {
                selectedDatabase = collectionstable.rows( indexes ).data().toArray();
                $('#createmappingbtn').prop('disabled', false);
                selectedDatabaseName = selectedDatabase[0]['document'];
                selectedCollectionName = null;
            }).on( 'deselect', function ( e, dt, type, indexes ) {
                selectedDatabase = null;
                selectedDatabaseName = null;
                selectedCollectionName = null;
                $('#createmappingbtn').prop('disabled', true)
            });

    }

    $('#createmappingbtn').click(function(){
         $("#collectionsrow").hide();
         $("#mappingrow").show();
         $("#docname").html(selectedDatabaseName);
         show_samples();
    });

    $("#backtocollectionstablebtn").click(function(){
        $("#mappingrow").hide();
        $("#collectionsrow").show();

    });

    var docs = null;
    var selectedDocRow = null;
    var columnNames = [];
    var subjectMaps = {};
    function show_samples(){
        $("#mappingtextarea").val("");
        $("#mappingtextarea").prop('disabled', true);
        $("#selectpredobjbtn").prop('disabled', true);
        $.ajax({
            type: 'GET',
            headers: {
                Accept : "application/json"
            },
            url: "/api/get_columns_names?fed=" + encodeURIComponent(federation)
                                + "&ds=" + encodeURIComponent(selectedDatasourceRow[0][0])
                                + "&dbname=" + encodeURIComponent(selectedDatabase[0]['db'])
                                + "&collname=" + encodeURIComponent(selectedDatabaseName)
                                + "&dstype=" + encodeURIComponent(selectedDatasourceRow[0][3]),
            dataType: "json",
            crossDomain: true,
            success: function(data, textStatus, jqXHR){
                html = "<tr>";
                conlumns = [];
                var columnNamesHtml = "";
                columnNamesHtml += '<option value="">--select column name--</option>';
                for (col in data.columns){
                    col=data.columns[col];
                    conlumns.push({ "data": col.data, "title":col.title })
                    columnNamesHtml += '<option value="'+col.title+'">'+col.title+'</option>';
                    html += "<th>" + col.title + "</th>";
                }
                html += "</tr>"
                $("#sampledoctableheader").html(html);
                $("#columnNames").html(columnNamesHtml);
                $("#predobjref").html(columnNamesHtml);

                if (docs == null){
                }else{
                    docs.destroy();
                    // $('#sampledoctable').empty();
                }
                docs = $("#sampledoctable").DataTable({
                            order: [[ 1, 'desc' ]],
                            destroy: true,
                            responsive: true,
                            select: true,
                            dom: 'Blfrtip',
                            processing: true,
                            buttons: [
                             {
                             text:'copy'
                             },
                             {
                             text:'csv',
                             extend: 'csvHtml5',
                             title: selectedDatabaseName
                             },
                             {
                             text:'excel',
                             extend: 'excelHtml5',
                             title: selectedDatabaseName
                             },
                             {
                             text:'pdf',
                             extend: 'pdfHtml5',
                             title: selectedDatabaseName
                             },
                             {
                             text: 'TSV',
                             extend: 'csvHtml5',
                             fieldSeparator: '\t',
                             extension: '.tsv',
                             title: selectedDatabaseName
                             }],
                            data: data.data,
                            defaultContent: "<i>Not set</i>",
                            columns: conlumns
                        });
                // Dat source table select action
                docs.on( 'select', function ( e, dt, type, indexes ) {
                        selectedDocRow = docs.rows( indexes ).data().toArray();
                    }).on( 'deselect', function ( e, dt, type, indexes ) {
                        selectedDocRow = null;
                    });

            },
            error: function(jqXHR, textStatus, errorThrown){
                console.log("Error while showing sample", jqXHR)
                console.log(jqXHR.status);
                console.log(jqXHR.responseText);
                console.log(textStatus);
            }
        });

        $.ajax({
            type: 'GET',
            headers: {
                Accept : "application/json"
            },
            url: "/api/get_mapping?fed=" + encodeURIComponent(federation)
                                + "&ds=" + encodeURIComponent(selectedDatasourceRow[0][0])
                                + "&dbname=" + encodeURIComponent(selectedDatabase[0]['db'])
                                + "&collname=" + encodeURIComponent(selectedDatabaseName)
                                + "&dstype=" + encodeURIComponent(selectedDatasourceRow[0][3]),
            dataType: "json",
            crossDomain: true,
            success: function(data, textStatus, jqXHR){
                if (data.data!=null && data.data != ""){
                    var mapareahtml = ""
                    oldsubjectRML = data.data;
                    console.log(data.data);
                    for (s in data.data){
                        mapareahtml += data.data[s];
                    }
                    $("#mappingtextarea").val("");
                    $("#mappingtextarea").val(rmlprefs+mapareahtml);
                    $("#selectpredobjbtn").prop('disabled', true);

                    activehtml = $("#activeSubjectID").html();
                    oldsubjectMaps = data.subjmap;
                }
            },
            error: function(jqXHR, textStatus, errorThrown){
                console.log("Error while getting existing mapping ..", jqXHR)
                console.log(jqXHR.status);
                console.log(jqXHR.responseText);
                console.log(textStatus);
            }
        });

    }
     /*
    ***************************************************
    ***** Dialog management functions *****************
    ***************************************************
    */

    $("#selectsubjcol").click(function(){
        $("#mappingtextarea").prop('disabled', true);
        var lbl = selectedDatabaseName.replace(" ", "_");
        var dbp = selectedDatasourceRow[0][1].replace(" ", "_");
        $("#subjtemp").val(selectedDataSourceID + "/"+ dbp + '/' + lbl +"/resource/")
        dialog.dialog("open");
    });

    $("#columnNames").change(function(){
        var lbl = selectedDatabaseName.replace(" ", "_");
        var dbp = selectedDatasourceRow[0][1].replace(" ", "_");
        $("#subjtemp").val(selectedDataSourceID + "/"+dbp + '/' + lbl + "/resource/{"+ $( this ).val().replace(" ", "_") +"}");
    });

    var dialog, edialog, form,
        colnames = $("#columnNames"),
        temp  = $("#subjtemp"),
        subjclass = $("#subjclass");
    var subjectID = null;
    var activeSubjectID = null;
    var subjectRML = {};
    var oldsubjectRML = {};

    dialog = $("#subjcol-dialog").dialog({
                                      autoOpen: false,
                                      height: 500,
                                      width: 700,
                                      modal: true,
                                      classes: {
                                          "ui-dialog": "highlight"
                                      },
                                      buttons: {
                                        "Ok": enableMappingArea,
                                        Cancel: function() {
                                          dialog.dialog( "close" );
                                        }
                                      },
                                      close: function() {
                                        form[0].reset();
                                        //allFields.removeClass("ui-state-error" );
                                      }
                             });

    form = dialog.find("form" ).on("submit", function( event ) {
              event.preventDefault();
              enableMappingArea(true);
         });


    function enableMappingArea(close) {
        var mappingtextarea = $("#mappingtextarea").val();

        $("#mappingtextarea").val("");
        var rml = "";
        var lbl = selectedDatabaseName.replace(" ", "_");
        var db = selectedDataSourceName.replace(" ", "_");
        var mappref = ""
        var lsid = $.md5(db + lbl + "-" + colnames.val().replace(" ", "") + selectedDataSourceID);
        var smid = $.md5(db + lbl + "-" + colnames.val().replace(" ", "") + selectedDataSourceID + temp.val() + subjclass.val());

        subjectID = "omap:" + db + '_' + lbl + '_' + $.md5(subjclass.val()) + "-" + colnames.val().replace(" ", "")
        rml += "\n" + subjectID + "\n"+
                  "rml:logicalSource omap:" + lsid + ".\n" +
                  " \t omap:" + lsid + " rml:source <" + selectedDataSourceID +"> ;\n" +
                  " \t\t rml:referenceFormulation ql:Cypher ;\n" +
                  " \t\t rml:iterator \"node." + lbl +"\".\n" +
                subjectID +
                  "  rr:subjectMap omap:"+ smid +" .\n" +
                  "   \t omap:"+ smid +" rr:template \"" + temp.val() +"\" ;\n " +
                  "   \t\t rr:class <" + subjclass.val() + "> .\n "

        subjectMaps[colnames.val()+"-"+subjclass.val()] = subjectID;
        subjectRML[subjectID] = rml;
        activeSubjectID = subjectID;
        for (s in subjectRML){
            if (s == subjectID){
                continue;
            }
            rml += subjectRML[s]
        }
        for (s in oldsubjectRML){
            if (s == subjectID){
                continue;
            }
            rml += oldsubjectRML[s]
        }
        $("#mappingtextarea").val(rmlprefs + rml);
        $("#mappingtextarea").prop('disabled', true);
        $("#selectpredobjbtn").prop('disabled', false);

        activehtml = $("#activeSubjectID").html();
        activehtml += '<option value="' + subjectID +'">' + colnames.val()+"-"+subjclass.val() +'</option>';
        $("#activeSubjectID").html(activehtml);
        $("#activeSubjectID").prop('disabled', false);
        $("#activeSubjectID").val(activeSubjectID);


     dialog.dialog( "close" );
     return true;
    }
    $("#activeSubjectID").change(function(){
        activeSubjectID = $(this).val();
        $("#selectedactiveSubjID").html($(this));
    });

    $("#selectpredobjbtn").click(function(){
        var parentMaps = '<option value="">--select parent triple map --</option>';
        for (t in subjectMaps){
            parentMaps += '<option value="' + subjectMaps[t] +'">' + t +'</option>';
        }
        for (t in oldsubjectMaps){
            parentMaps += '<option value="' + oldsubjectMaps[t] +'">' + t +'</option>';
        }

        $("#predobjparenttmap").html(parentMaps);
        edialog.dialog('open');
    });


    edialog = $("#predobj-dialog" ).dialog({
                                          autoOpen: false,
                                          height: 500,
                                          width: 700,
                                          modal: true,
                                          classes: {
                                              "ui-dialog": "highlight"
                                          },
                                          buttons: {
                                            "Ok": addPredObjMap,
                                            Cancel: function() {
                                               edialog.dialog( "close" );
                                            }
                                          },
                                          close: function() {
                                            form[ 0 ].reset();
                                            //allFields.removeClass( "ui-state-error" );
                                          }
                                        });
    function addPredObjMap() {
        var rml = "";
        var  predicate =  $("#predicate"),
            refe =     $("#predobjref"),
            parenttripm = $("#predobjparenttmap" );

        if (parenttripm.val() == "" && refe.val() == ""){
            parenttripm.addClass( "ui-state-error" );
            refe.addClass( "ui-state-error" );
            return false;
        }
        var lbl = selectedLabel.replace(" ", "_");
        var db = selectedDataSourceName.replace(" ", "_");

        parenttripm.removeClass( "ui-state-error" );
        refe.removeClass( "ui-state-error" );

        var predobid = $.md5(db+lbl + "-" + colnames.val().replace(" ", "") + selectedDataSourceID + temp.val() + subjclass.val()+predicate.val());
        rml += "\n" + activeSubjectID + " rr:predicateObjectMap omap:"+ predobid +" . \n" +
               "omap:"+ predobid +" rr:predicate   <" + predicate.val() +"> ;\n";
        if (parenttripm.val() != ""){
            var ombid = $.md5(db+lbl + "-" + colnames.val().replace(" ", "") + selectedDataSourceID + temp.val() + subjclass.val()+predicate.val()+parenttripm.val());
            rml += "\t rr:objectMap omap:"+ ombid +" .\n" +
                  "omap:"+ ombid +" rr:parentTriplesMap " + parenttripm.val() +" . \n"

        }else{
            var ombid = $.md5(db+lbl + "-" + colnames.val().replace(" ", "") + selectedDataSourceID + temp.val() + subjclass.val()+predicate.val()+refe.val());
            rml += "  \t rr:objectMap omap:"+ ombid +"  .\n" +
                  " omap:"+ ombid +" rml:reference \"" + refe.val() +" \" . \n"
        }
        subjectRML[activeSubjectID] += rml

        var mapareahtml = ""
        for (s in subjectRML){
            mapareahtml += subjectRML[s];
        }
        for (s in oldsubjectRML){
            mapareahtml += oldsubjectRML[s];
        }

        $("#mappingtextarea").val("");
        $("#mappingtextarea").val(rmlprefs+mapareahtml);

        edialog.dialog( "close" );
        return true;
    }

    var rmlprefs = " PREFIX rr: <http://www.w3.org/ns/r2rml#> \n" +
                  " PREFIX rml: <http://semweb.mmlab.be/ns/rml#>  \n" +
                  " PREFIX omap: <http://tib.eu/dsdl/ontario/mapping#>  \n" +
                  " PREFIX ql: <http://semweb.mmlab.be/ns/ql#> \n" +
                  " PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n" +
                  " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  \n" +
                  " PREFIX rev: <http://purl.org/stuff/rev#>  \n" +
                  " PREFIX schema: <http://schema.org/>  \n" +
                  " PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>  \n" +
                  " PREFIX base: <http://tib.de/ontario/mapping#>  \n" +
                  " PREFIX iasis: <http://project-iasis.eu/vocab/>  \n" +
                  " PREFIX hydra: <http://www.w3.org/ns/hydra/core#>  \n"


});