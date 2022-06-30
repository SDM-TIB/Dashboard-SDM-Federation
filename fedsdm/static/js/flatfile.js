$(function() {
    /*
    **********************************************************
    ************ Manage data source Data Table ***************
    **********************************************************
    */
    $('#federationlistrow').show();
    $('#datasourcerow').show();
    $('#createmapping').show();
    $('#backtotable').hide();
    $('#mappingrow').hide();

    $('#createmapping').prop('disabled', true);
    $('#activeSubjectID').prop('disabled', true);

    $('#sampledoctablediv').hide();

    var federation = null,
        datasource = null;

    $.ajax({
        type: 'GET',
        headers: {
            Accept : 'application/json'
        },
        url: '/api/federations',
        data: {'query':'all'},
        dataType: 'json',
        crossDomain: true,
        success: function(data, textStatus, jqXHR) {
            html = '<option value="All" selected>Please select federation</option>'
            for (f in data) {
                html += '<option value="'+data[f]+'">'+data[f]+'</option>'
            }
            html += '<option value="All">All</option> '
            $('#federations-list').html(html);
            $('#federations-list').prop('disabled', false);

        },
        error: function(jqXHR, textStatus, errorThrown) {
            console.log('ERROR: get federations list')
            console.log(jqXHR.status);
            console.log(jqXHR.responseText);
            console.log(textStatus);
        }
    });

    $('#federations-list').on('change', function() {
        fed = $(this).val()
        $('#mfedName').html(fed);
        basic_stat(fed);
        federation = fed;
    });

    var stats = null;
    var selectedRow = null, selectedDataSourceID=null,
        selectedDatabaseName = null;
    function basic_stat(fed) {
        if (stats == null) {
            // Construct basic statistics table
            stats = $('#availabledatasources').DataTable({
                order: [[1, 'desc']],
                responsive: true,
                select: true,
                ajax: '/api/datasources?graph=' + fed + '&dstype=CSV_TSV'
            });
            // Dat source table select action
            stats.on('select', function(e, dt, type, indexes) {
                selectedRow = stats.rows(indexes).data().toArray();
                selectedDataSourceID = selectedRow[0][0];
                selectedDatabaseName = selectedRow[0][1];
                $('#createmapping').prop('disabled', false)
            }).on('deselect', function (e, dt, type, indexes) {
                var rowData = stats.rows(indexes).data().toArray();
                selectedRow = null;
                selectedDataSourceID = null;
                selectedDatabaseName = null;
                $('#createmapping').prop('disabled', true)
            });
        } else {
            stats.clear().draw();
            $('#createmapping').prop('disabled', true);
            selectedRow = null;
            selectedDataSourceID = null;
            selectedDatabaseName = null;
            stats.ajax.url('/api/datasources?graph=' + fed + '&dstype=CSV_TSV').load();
        }
    }

    $('#createmapping').on('click', function() {
        $('#datasourcerow').hide();
        $('#createmapping').hide();
        $('#federationlistrow').hide();
        $('#backtotable').show();
        $('#mappingrow').show();
        $('#dsname').html(selectedRow[0][1]);
        $('#sampledoctablediv').show();
        $('#startmapping').prop('disabled', true);
        show_samples();
    });
    $('#backtotable').on('click', function() {
        $('#federationlistrow').show();
        $('#datasourcerow').show();
        $('#createmapping').show();
        $('#backtotable').hide();
        $('#mappingrow').hide();
        $('#sampledoctable').hide();
    });

    var docs = null;
    var selectedDocRow = null;
    var columnNames = [];
    var subjectMaps = {};
    function show_samples() {
        $('#mappingtextarea').val('');
        $('#mappingtextarea').prop('disabled', true);
        $('#selectpredobjbtn').prop('disabled', true);
        $('#docname').html(selectedDatabaseName);
        $.ajax({
            type: 'GET',
            headers: {
                Accept : 'application/json'
            },
            url: '/api/get_columns_names?fed=' + encodeURIComponent(federation)
                + '&ds=' + encodeURIComponent(selectedRow[0][0])
                + '&dbname=' + encodeURIComponent(selectedRow[0][2])
                + '&collname='
                + '&dstype=' + encodeURIComponent(selectedRow[0][3]),
            dataType: 'json',
            crossDomain: true,
            success: function(data, textStatus, jqXHR) {
                html = '<tr>';
                conlumns = [];
                var columnNamesHtml = '';
                columnNamesHtml += '<option value="">--select column name--</option>';
                for (col in data.columns){
                    col=data.columns[col];
                    conlumns.push({ 'data': col.data })
                    columnNamesHtml += '<option value="'+col.title+'">'+col.title+'</option>';
                    html += '<th>' + col.title + '</th>';
                }
                html += '</tr>'
                $('#sampledoctableheader').html(html);
                $('#columnNames').html(columnNamesHtml);
                $('#predobjref').html(columnNamesHtml);
                console.log(data);
                console.log(conlumns);

                docs = $('#sampledoctable').DataTable({
                    order: [[1, 'desc']],
                    responsive: true,
                    select: true,
                    data: data.data,
                    columns: conlumns
                });
                // Data source table select action
                docs.on('select', function(e, dt, type, indexes) {
                    selectedDocRow = docs.rows(indexes).data().toArray();
                    //$('#startmapping').prop('disabled', false);

                }).on('deselect', function(e, dt, type, indexes) {
                    selectedDocRow = null
                    //$('#startmapping').prop('disabled', true);
                });
            },
            error: function(jqXHR, textStatus, errorThrown) {
                console.log('Error while showing sample', jqXHR)
                console.log(jqXHR.status);
                console.log(jqXHR.responseText);
                console.log(textStatus);
            }
        });

        $.ajax({
            type: 'GET',
            headers: {
                Accept : 'application/json'
            },
            url: '/api/get_mapping?fed=' + encodeURIComponent(federation)
                + '&ds=' + encodeURIComponent(selectedRow[0][0])
                + '&dbname=' + encodeURIComponent(selectedRow[0][2])
                + '&collname='
                + '&dstype=' + encodeURIComponent(selectedRow[0][3]),
            dataType: 'json',
            crossDomain: true,
            success: function(data, textStatus, jqXHR) {
                console.log(data);
                if (data.data!=null && data.data != '') {
                    $('#mappingtextarea').prop('disabled', false);
                    $('#mappingtextarea').val(data.data);
                    $('#selectpredobjbtn').prop('disabled', true);
                }
            },
            error: function(jqXHR, textStatus, errorThrown) {
                console.log('Error while getting existing mapping ..', jqXHR)
                console.log(jqXHR.status);
                console.log(jqXHR.responseText);
                console.log(textStatus);
            }
        });
    }

    $('#selectsubjcol').on('click', function() {
        $('#mappingtextarea').prop('disabled', true);
        var db = selectedDatabaseName.replace(' ', '_').replace(' ', '_').replace(' ', '_');
        $('#subjtemp').val(selectedDataSourceID + '/' + db + '/resource/')
        dialog.dialog('open');
    });

    $('#columnNames').on('change', function() {
        var db = selectedDatabaseName.replace(' ', '_').replace(' ', '_').replace(' ', '_');
        $('#subjtemp').val(selectedDataSourceID + '/' + db + '/resource/{' + $(this).val().replace(' ', '_') + '}');
    });

    $('#resetMappingBtn').on('click', function() {
        rml = ''
        $('#mappingtextarea').val('')
        subjectMaps = {};
        columnNames = [];
        subjectID = null;
        $('#selectpredobjbtn').prop('disabled', true);
    });

    $('#saveMappingBtn').on('click', function() {
        var mapareahtml = ''
        for (s in subjectRML) {
            mapareahtml += subjectRML[s];
        }
        $.ajax({
            type: 'POST',
            headers: {
                Accept : 'application/json'
            },
            url: '/api/savemapping?fed=' + encodeURIComponent(federation)
                + '&ds=' + encodeURIComponent(selectedRow[0][0]),
            data : {'mapping': mapareahtml, 'prefix': rmlprefs},
            dataType: 'json',
            crossDomain: true,
            success: function(data, textStatus, jqXHR) {
                console.log(data);
                rml = '';
                docs = null;
                selectedDocRow = null;
                columnNames = [];
                subjectMaps = {};
                selectedCollection = null;
                $('#startmapping').show();
                $('#startmapping').show();
                $('#sampledoctablediv').hide();
                alert('Mapping saved!')
            },
            error: function(jqXHR, textStatus, errorThrown) {
                console.log('Error while showing sample', jqXHR)
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
        colnames = $('#columnNames'),
        temp  = $('#subjtemp'),
        subjclass = $('#subjclass');
    var subjectID = null;
    var activeSubjectID = null;
    var subjectRML = {};
    dialog = $('#subjcol-dialog').dialog({
        autoOpen: false,
        height: 500,
        width: 700,
        modal: true,
        classes: {
            'ui-dialog': 'highlight'
        },
        buttons: {
            'Ok': enableMappingArea,
            Cancel: function() {
                dialog.dialog('close');
            }
        },
        close: function() {
            form[0].reset();
            //allFields.removeClass('ui-state-error');
        }
    });

    form = dialog.find('form' ).on('submit', function(event) {
        event.preventDefault();
        enableMappingArea(true);
    });
    var rml = '';
    function enableMappingArea(close) {
        var mappingtextarea = $('#mappingtextarea').val();
        $('#mappingtextarea').val('');
        var rml = '';
        var db = selectedDatabaseName.replace(' ', '_');
        var mappref = ''
        var lsid = $.md5(db + '-' + colnames.val().replace(' ', '') + selectedDataSourceID);
        var smid = $.md5(db + '-' + colnames.val().replace(' ', '') + selectedDataSourceID + temp.val() + subjclass.val());

        subjectID = 'omap:' + db  + '_' + $.md5(subjclass.val()) + '-' + colnames.val().replace(' ', '')
        rml += '\n' + subjectID + '\n'+
            'rml:logicalSource omap:' + lsid + '.\n' +
            ' \t omap:' + lsid + ' rml:source <' + selectedDataSourceID + '> ;\n' +
            ' \t\t rml:referenceFormulation ql:TSV ;\n' +
            ' \t\t rml:iterator "*".\n' +
            subjectID +
            '  rr:subjectMap omap:' + smid + ' .\n' +
            '   \t omap:' + smid + ' rr:template "' + temp.val() + '" ;\n ' +
            '   \t\t rr:class <' + subjclass.val() + '> .\n '

        subjectMaps[colnames.val() + '-' + subjclass.val()] = subjectID;
        subjectRML[subjectID] = rml;
        activeSubjectID = subjectID;
        $('#mappingtextarea').val(rmlprefs + rml);
        $('#mappingtextarea').prop('disabled', true);
        $('#selectpredobjbtn').prop('disabled', false);

        activehtml = $('#activeSubjectID').html();
        activehtml += '<option value="' + subjectID +'">' + colnames.val()+ '-' +subjclass.val() +'</option>';
        $('#activeSubjectID').html(activehtml);
        $('#activeSubjectID').prop('disabled', false);
        $('#activeSubjectID').val(activeSubjectID);

        dialog.dialog('close');
        return true;
    }
    $('#activeSubjectID').on('change', function() {
        activeSubjectID = $(this).val();
        $('#selectedactiveSubjID').html($(this));
    });

    $('#selectpredobjbtn').on('click', function() {
        var parentMaps = '<option value="">--select parent triple map --</option>';
        for (t in subjectMaps) {
            parentMaps += '<option value="' + subjectMaps[t] +'">' + t +'</option>';
        }
        $('#predobjparenttmap').html(parentMaps);
        edialog.dialog('open');
    });

    edialog = $('#predobj-dialog').dialog({
        autoOpen: false,
        height: 500,
        width: 700,
        modal: true,
        classes: {
            'ui-dialog': 'highlight'
        },
        buttons: {
            'Ok': addPredObjMap,
            Cancel: function() {
                edialog.dialog( 'close' );
            }
        },
        close: function() {
            form[0].reset();
            //allFields.removeClass( 'ui-state-error' );
        }
    });
    function addPredObjMap() {
        var rml = '';
        var predicate = $('#predicate'),
            refe = $('#predobjref'),
            parenttripm = $('#predobjparenttmap');

        if (parenttripm.val() == '' && refe.val() == ''){
            parenttripm.addClass('ui-state-error');
            refe.addClass('ui-state-error');
            return false;
        }
        var db = selectedDatabaseName.replace(' ', '_');

        parenttripm.removeClass('ui-state-error');
        refe.removeClass('ui-state-error');

        var predobid = $.md5(db + '-' + colnames.val().replace(' ', '') + selectedDataSourceID + temp.val() + subjclass.val() + predicate.val());
        rml += '\n' + activeSubjectID + ' rr:predicateObjectMap omap:'+ predobid +' . \n' +
            'omap:'+ predobid +' rr:predicate   <' + predicate.val() +'> ;\n';
        if (parenttripm.val() != '') {
            var ombid = $.md5(db + '-' + colnames.val().replace(' ', '') + selectedDataSourceID + temp.val() + subjclass.val() + predicate.val() + parenttripm.val());
            rml += '\t rr:objectMap omap:' + ombid + ' .\n' +
                'omap:' + ombid + ' rr:parentTriplesMap ' + parenttripm.val() + ' . \n'
        } else {
            var ombid = $.md5(db + '-' + colnames.val().replace(' ', '') + selectedDataSourceID + temp.val() + subjclass.val() + predicate.val() + refe.val());
            rml += '  \t rr:objectMap omap:' + ombid + '  .\n' +
                ' omap:' + ombid + ' rml:reference "' + refe.val() + ' \" . \n'
        }
        subjectRML[activeSubjectID] += rml

        var mapareahtml = ''
        for (s in subjectRML) {
            mapareahtml += subjectRML[s];
        }
        $('#mappingtextarea').val('');
        $('#mappingtextarea').val(rmlprefs+mapareahtml);

        edialog.dialog('close');
        return true;
    }

    var rmlprefs = ' PREFIX rr: <http://www.w3.org/ns/r2rml#> \n' +
        ' PREFIX rml: <http://semweb.mmlab.be/ns/rml#>  \n' +
        ' PREFIX omap: <http://tib.eu/dsdl/ontario/mapping#>  \n' +
        ' PREFIX ql: <http://semweb.mmlab.be/ns/ql#> \n' +
        ' PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n' +
        ' PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  \n' +
        ' PREFIX rev: <http://purl.org/stuff/rev#>  \n' +
        ' PREFIX schema: <http://schema.org/>  \n' +
        ' PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>  \n' +
        ' PREFIX base: <http://tib.de/ontario/mapping#>  \n' +
        ' PREFIX iasis: <http://project-iasis.eu/vocab/>  \n' +
        ' PREFIX hydra: <http://www.w3.org/ns/hydra/core#>  \n'
});
