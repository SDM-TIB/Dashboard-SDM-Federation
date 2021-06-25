$(document).ready(function() {


    var federation = $("#federationslist").val();
    $("#selectfederation").prop("disabled", true);
    var loaded = 0;

    if (federation != null && federation != ""){
        load_table(federation);
    }

    $("#federationslist").change(function(){
        federation = $( this ).val();
        load_table(federation);
    });


    var table = null;
    function load_table(){

        //Disable buttons before selecting item on the table
        $( "#editis" ).prop( "disabled", true );
        $( "#issuedetails" ).prop( "disabled", true );

        //Construct data source management data table
        if (table == null){
            loaded = 1;
            table = $('#reportedissues').DataTable({
                // order: [[ 1, 'desc' ]],
//                "columnDefs": [
//                            {
//                                "Federation": [ 0 ],
//                                "visible": false,
//                                "searchable": false
//                            }
//                 ],
                responsive: true,
                select: true,
                defaultContent: "<i>Not set</i>",
                ajax: '/feedback/issues?fed=' + federation
            });

            var issuetable = table;
            table.on( 'select', function ( e, dt, type, indexes ) {
                    selectedRow = issuetable.rows( indexes ).data().toArray();
                    $( "#editis" ).prop( "disabled", false );
                    $( "#issuedetails" ).prop( "disabled", false );
                }).on( 'deselect', function ( e, dt, type, indexes ) {
                    var rowData = issuetable.rows( indexes ).data().toArray();
                    $( "#editis" ).prop( "disabled", true );
                    $( "#issuedetails" ).prop( "disabled", true );
                    selectedRow = null;
                }).on('dblclick', function(e, dt, type, indexes){
                    var rowData = issuetable.rows( indexes ).data().toArray();
                    console.log('report id', rowData[0][0]);
                    $.ajax({
                            type: 'GET',
                            headers: {
                                Accept : "application/json"
                            },
                            url: '/feedback/details?id=' + rowData[0][0],
                            dataType: "json",
                            crossDomain: true,
                            success: function(data, textStatus, jqXHR){
                                console.log('details ', data);
                                $("#user").val(rowData[0][2]);
                                $("#query").val(rowData[0][3]);
                                $("#desc").val(rowData[0][4]);
                                $("#status").val(rowData[0][5]);

                                $("#var").val(data['var']);
                                $("#pred").val(data['pred']);
                                var obj = JSON.parse(data['row']);
                                var pretty = JSON.stringify(obj, undefined, 4);
                                $("#rowjson").val(pretty);
                                feedbackdialog.dialog("open");
                            },
                            error: function(jqXHR, textStatus, errorThrown){
                                console.log(jqXHR.status);
                                console.log(jqXHR.responseText);
                                console.log(textStatus);
                            }
                    });
                });
        }else{
            table.clear().draw();
            $( "#editis" ).prop( "disabled", true );
            $( "#issuedetails" ).prop( "disabled", true );
            table.ajax.url('/feedback/issues?fed=' + federation).load();
        }

        table.on('draw', function(){
        });

    }



    var feedbackform=null;
    var feedbackdialog=null;
    var feedbackdesc =  $("#feedbackdesc" ),
        fedbackpreds = $("#fedbackpreds"),
        tips = $( ".validateTips" );

    feedbackdialog = $("#feedback-dialog" ).dialog({
              autoOpen: false,
              height: 400,
              width: 550,
              modal: true,
              classes: {
                  "ui-dialog": "ui-corner-all"
              },
              close: function() {
                feedbackform[0].reset();
                allfeedbackFields.removeClass("ui-state-error" );
              }
         });

    $( "#issuedetails" ).click(function(){

        $.ajax({
                type: 'GET',
                headers: {
                    Accept : "application/json"
                },
                url: '/feedback/details?id=' + selectedRow[0][0],
                dataType: "json",
                crossDomain: true,
                success: function(data, textStatus, jqXHR){
                    console.log('details ', data);
                    $("#user").val(selectedRow[0][2]);
                    $("#query").val(selectedRow[0][3]);
                    $("#desc").val(selectedRow[0][4]);
                    $("#status").val(selectedRow[0][5]);

                    $("#var").val(data['var']);
                    $("#pred").val(data['pred']);
                    $("#rowjson").val(data['row']);
                    feedbackdialog.dialog("open");
                },
                error: function(jqXHR, textStatus, errorThrown){
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                }
        });

//        $("#fedbackpreds").empty();
//        $("#fedbackpreds").append('<option value="-1">Select column</option>');
//        for (d in queryvars){
//            $("#fedbackpreds").append('<option value=' + queryvars[d] + '> '+ queryvars[d] + '</option>');
//        }
//        $("#fedbackpreds").append('<option value="All">All</option>');
//        //

    });

});