$(function() {
    /*
    **********************************************************
    ************ Manage data source Data Table ***************
    **********************************************************
    */
    var tabvisible = '#home';

    var federation = null;
    $.ajax({
        type: 'GET',
        headers: {
            Accept : 'application/json'
        },
        url: 'api/federations',
        data: {'query': 'all'},
        dataType: 'json',
        crossDomain: true,
        success: function(data, textStatus, jqXHR) {
            html = '';
            for (f in data) {
                html += '<li class="fed"><a href="#" class="fed" id="fed-' + f + '">'+ data[f]+'</a></li>'
            }
            html += '<li class="divider"></li><li class="fed"><a href="#" class="fed" id="fed-' + (f+1) + '">All</a></li>'
            $('#federations-list').html(html);
            $('a[class=fed]').on('click', function() {
                fed = $(this).text()
                $('#fedName').html(fed);
                $('#mfedName').html(fed);
                if (tabvisible == '#home') {
                    basic_stat(fed);
                } else {
                    manage(fed);
                }
                federation = fed;
            });
        },
        error: function(jqXHR, textStatus, errorThrown) {
            console.log(jqXHR.status);
            console.log(jqXHR.responseText);
            console.log(textStatus);
        }
    });

    var stats = null;
    var table;
    var selectedRow = null;

    var bsloaded = 0;
    function basic_stat(fed) {
        console.log(fed);
        if (fed == null || fed == federation && bsloaded == 1) {
            return
        }
        if (stats == null) {
            // Construct basic statistics table
            stats = $('#basic-statistics').DataTable({
                order: [[1, 'desc']],
                responsive: true,
                select: true
            });
        } else {
            stats.clear().draw();
        }
        datas = [];
        $.ajax({
            type: 'GET',
            headers: {
                Accept : 'application/json'
            },
            url: 'api/dsstats',
            data: {'graph': fed},
            dataType: 'json',
            crossDomain: true,
            success: function(data, textStatus, jqXHR) {
                datas = data;
                datasdon = [];
                for (d in datas){
                    rem = [];
                    rem.push(datas[d].ds);
                    var rdfmts = datas[d].rdfmts;
                    if (rdfmts.indexOf('^^') != -1) {
                        rdfmts = rdfmts.substring(0, rdfmts.indexOf('^^'));
                    }
                    rem.push(rdfmts);
                    var triples = datas[d].triples;
                    if (triples.indexOf('^^') != -1) {
                        triples = triples.substring(0, triples.indexOf('^^'));
                    }
                    rem.push(triples);
                    stats.row.add(rem).draw( false );
                }

                $('#morris-bar-chart').empty();
                Morris.Donut({
                    element: 'morris-bar-chart',
                    data: datas,
                    resize: true
                });
                Morris.Bar({
                    element: 'morris-bar-chart',
                    data: datas,
                    xkey: 'ds',
                    ykeys: ['rdfmts', 'triples'],
                    labels: ['#RDF-MTs', '#Triples'],
                    hideHover: 'auto',
                    gridTextSize: 10,
                    xLabelAngle: 5,
                    barRatio: 0.4,
                    resize: true
                });
            },
            error: function(jqXHR, textStatus, errorThrown) {
                console.log(jqXHR.status);
                console.log(jqXHR.responseText);
                console.log(textStatus);
            }
        });
        bsloaded = 1;
    }

    $('a[data-toggle="tab"]').on('shown.bs.tab', function (e) {
        var target = $(e.target).attr('href') // activated tab
        if (target == '#manage') {
            tabvisible = target;
            manage(federation);
        } else {
            tabvisible = '#home';
            basic_stat(federation);
        }
    });

    var mngloaded = 0;
    function manage(fed) {
        $('#fedName').html(fed);
        $('#mfedName').html(fed);
        if (fed == federation && mngloaded == 1) {
            return
        }
        //Disable buttons before selecting item on the table
        $('#editds').prop('disabled', true);
        $('#removeds').prop('disabled', true);
        $('#createmapping').prop('disabled', true);

        //Construct data source management data table
        if (table == null) {
            mngloaded = 1;
            table = $('#datasources').DataTable({
                order: [[1, 'desc']],
                responsive: true,
                select: true,
//                columnDefs: [
//                        {
//                            'targets': [ 0 ],
//                            'visible': false,
//                            'searchable': true
//                        },
//                        {
//                            'targets': [ 4 ],
//                            'visible': false
//                        },
//                        {
//                            'targets': [ 8 ],
//                            'visible': false,
//                            'searchable': false
//                        },
//                        {
//                            'targets': [ 9 ],
//                            'visible': false,
//                            'searchable': false
//                        }
//                    ],
                ajax: 'api/datasources?graph=' + fed

            });
            //            table.column( 0 ).visible( false );
            //            table.column( 4 ).visible( false);
            //            table.column( 6 ).visible( false );
            //            table.column( 9 ).visible( false );
            //            table.draw( false );
            // Dat source table select action
            table.on('select', function (e, dt, type, indexes) {
                selectedRow = table.rows( indexes ).data().toArray();
                $('#editds').prop('disabled', false);
                $('#removeds').prop('disabled', false);
                $('#createmapping').prop('disabled', false);
            }).on('deselect', function (e, dt, type, indexes) {
                var rowData = table.rows( indexes ).data().toArray();
                $('#editds').prop('disabled', true);
                $('#removeds').prop('disabled', true);
                $('#createmapping').prop('disabled', true);

                selectedRow = null
            });
        } else {
            table.clear().draw();
            table.ajax.url('api/datasources?graph=' + fed).load()
        }
    }

    // Add data source click action
    $('#addds').on('click', function() {
        dialog.dialog('open');
    });

    // Edit data source click action
    $('#editds').on('click', function() {
        console.log(selectedRow[0][0]);
        $('#ename').val(selectedRow[0][1]);
        $('#eURL').val(selectedRow[0][2]);
        $('#edstype').val(selectedRow[0][3]);
        $('#elabel').val(selectedRow[0][7]);
        $('#eparams').val(selectedRow[0][9]);
        edialog.dialog('open');

    });

    //Remove data source click action
    $('#removeds').on('click', function () {
        table.row('.selected').remove().draw(false);
        $('#editds').prop('disabled', true);
        $('#removeds').prop('disabled', true);
        $('#createmapping').prop('disabled', true);
    });

    // Create Mappings click action
    $('#createmapping').on('click', function () {
        window.location = 'api/mappings'
    });

    /*
    ***************************************************
    ***** Dialog management functions *****************
    ***************************************************
    */
    var dialog, edialog, form,
        // From http://www.whatwg.org/specs/web-apps/current-work/multipage/states-of-the-type-attribute.html#e-mail-state-%28type=email%29
        name = $('#name'),
        label = $('#label'),
        dstype = $('#dstype'),
        URL = $('#URL'),
        params = $('#params'),
        allFields = $([]).add(name).add(label).add(dstype).add(URL).add(params);

    dialog = $('#dialog-form').dialog({
        autoOpen: false,
        height: 600,
        width: 650,
        modal: true,
        classes: {
            'ui-dialog': 'highlight'
        },
        buttons: {
            'Add New Data Source': addUser,
            Cancel: function() {
                dialog.dialog('close');
            }
        },
        close: function() {
            form[0].reset();
            allFields.removeClass('ui-state-error');
        }
    });
    function updateDS() {
        var name = $('#ename'),
            label = $('#elabel'),
            dstype = $('#edstype'),
            URL = $('#eURL'),
            params = $('#eparams'),
            allFields = $([]).add(name).add(label).add(dstype).add(URL).add(params),
            tips = $('.validateTips');

        var valid = true;
        allFields.removeClass('ui-state-error');

        if (valid) {
            table.row('.selected').remove().draw(false);
            table.row.add([name.val(), label.val(), dstype.val(), URL.val(), params.val(),,,,,]).draw(false);
            $('#editds').prop('disabled', true);
            $('#removeds').prop('disabled', true);
            $('#createmapping').prop('disabled', true);
            edialog.dialog('close');
        }
        return valid;
    }

    edialog = $('#editdsdialog').dialog({
        autoOpen: false,
        height: 600,
        width: 650,
        modal: true,
        classes: {
            'ui-dialog': 'highlight'
        },
        buttons: {
            'Update Data Source': updateDS,
            Cancel: function() {
                edialog.dialog('close');
            }
        },
        close: function() {
            form[0].reset();
            allFields.removeClass('ui-state-error');
        }
    });

    form = dialog.find('form').on('submit', function(event) {
        event.preventDefault();
        addUser();
    });

    function addUser() {
        var valid = true;
        allFields.removeClass('ui-state-error');

        valid = valid && checkLength(name, 'name', 2, 16);
        valid = valid && checkLength(URL, 'url', 6, 80);
        valid = valid && checkLength(label, 'label', 2, 16);

        valid = valid && checkRegexp(name, /^[a-z]([0-9a-z_\s])+$/i, 'Username may consist of a-z, 0-9, underscores, spaces and must begin with a letter.');
        //valid = valid && checkRegexp(URL, emailRegex, 'eg. ui@jquery.com');

        if (valid) {
            table.row.add([name.val(), label.val(), dstype.val(), URL.val(), params.val()]).draw(false);
            dialog.dialog('close');
        }
        return valid;
    }
});
