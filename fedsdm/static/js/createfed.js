$(document).ready(function() {

    var federation = null,
        datasource = null;


    $("#createnewfederation").click(function(){
        var name = $('#name').val();
        var desc = $('#description').val();
        console.log(name + " " + desc);
        if (name != null && name !== '' && name.length > 0){
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
                        manage(federation);
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
        if (name == null || name === '' || name.length <= 0) {
            alert('The Name field should not be empty.\nPlease insert a name in the Name field.');
        }
        return false
    });

    var table = null;
    var selectedRow = null;

    function manage(fed){
        $('#datasourcestable').show();

        //Disable buttons before selecting item on the table
        $( "#editds" ).prop( "disabled", true );
        $( "#removeds" ).prop( "disabled", true );
        $( "#createmapping" ).prop( "disabled", true );

        //Construct data source management data table
        if (table == null){
            table = $('#datasources').DataTable({
                order: [[ 1, 'desc' ]],
                responsive: true,
                select: true,
                defaultContent: "<i>Not set</i>",
                ajax: "/federation/datasources?graph=" + fed
                });
            
            // Dat source table select action
            table.on( 'select', function ( e, dt, type, indexes ) {
                    selectedRow = table.rows( indexes ).data().toArray();
                    $( "#editds" ).prop( "disabled", false );
                    $( "#removeds" ).prop( "disabled", false );
                    $( "#createmapping" ).prop( "disabled", false );

                }).on( 'deselect', function ( e, dt, type, indexes ) {
                    var rowData = table.rows( indexes ).data().toArray();
                    $( "#editds" ).prop( "disabled", true );
                    $( "#removeds" ).prop( "disabled", true );
                    $( "#createmapping" ).prop( "disabled", true );

                    selectedRow = null
                });

        }else{
            table.clear().draw();
            table.ajax.url("/federation/api/datasources?graph=" + fed).load()
        }
    }

    // Add data source click action
     $( "#addds" ).click(function() {
      dialog.dialog("open");

    });

    // Edit data source click action
    $( "#editds" ).click(function() {
      console.log(selectedRow[0][0]);
      $( "#ename" ).val(selectedRow[0][1]);
      $( "#eURL" ).val(selectedRow[0][2]);
      $( "#edstype" ).val(selectedRow[0][3]);
      $( "#elabel" ).val(selectedRow[0][7]);
      $( "#eparams" ).val(selectedRow[0][9]);
      edialog.dialog('open');

    });

    //Remove data source click action
    $('#removeds').click( function () {
        table.row('.selected').remove().draw( false );
        $( "#editds" ).prop( "disabled", true );
        $( "#removeds" ).prop( "disabled", true );
        $( "#createmapping" ).prop( "disabled", true );
    });

    // Create Mappings click action
    $('#createmapping').click( function () {
          window.location = "/federation/api/mappings"
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

    dialog = $( "#add-form" ).dialog({
                                      autoOpen: false,
                                      height: 800,
                                      width: 700,
                                      modal: true,
                                      classes: {
                                          "ui-dialog": "highlight"
                                      },
                                      buttons: {
                                        "Finish": addDataSource,
                                        "Continue": saveAndMore,
                                        Cancel: function() {
                                          dialog.dialog( "close" );
                                        }
                                      },
                                      close: function() {
                                        form[0].reset();
                                        allFields.removeClass("ui-state-error" );
                                      }
                             });

    form = dialog.find("form" ).on("submit", function( event ) {
              event.preventDefault();
              addDataSource(true);
         });

    function addDataSource(close) {
          var valid = true;
          allFields.removeClass( "ui-state-error" );

          valid = valid && checkLength( name, "name", 2, 16 );
          valid = valid && checkLength( URL, "url", 6, 80 );
          valid = valid && checkLength( desc, "desc", 2, 116 );
          //valid = valid && checkRegexp( name, /^[a-z]([0-9a-z_\s])+$/i, "Data source should consist of a-z, 0-9, underscores, spaces and must begin with a letter." );
          //valid = valid && checkRegexp( URL, emailRegex, "eg. ui@jquery.com" );

          if ( valid ) {
            $.ajax({
                type: 'POST',
                headers: {
                    Accept : "application/json"
                },
                url: '/federation/api/addsource?fed=' + federation,
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
                    console.log(data);
                    if (data != null && data.length > 0){
                        manage(federation);
                    }else{
                        $('#validateTips').html("Error while adding data source to the federation!")
                    }
                    table.clear().draw();
                    table.ajax.url("/federation/api/datasources?graph=" + federation).load()
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

});
