$(function() {
    let federation = $('#federations-list').val(),
        table = null,
        feedbackdialog = $('#detailsModal');
    $('#selectfederation').prop('disabled', true);

    if (federation != null && federation !== '') {
        load_table(federation);
    }

    $('#federations-list').on('change', function() {
        federation = $(this).val();
        load_table(federation);
    });

    function load_table() {
        //Disable buttons before selecting item on the table
        $('#editis').prop('disabled', true);
        $('#issuedetails').prop('disabled', true);

        //Construct data source management data table
        if (table == null) {
            table = $('#reportedissues').DataTable({
                order: [[1, 'desc']],
                responsive: true,
                select: true,
                defaultContent: '<i>Not set</i>'
            });

            var issuetable = table;
            table.on('select', function(e, dt, type, indexes) {
                selectedRow = issuetable.rows(indexes).data().toArray();
                $('#editis').prop('disabled', false);
                $('#issuedetails').prop('disabled', false);
            }).on('deselect', function(e, dt, type, indexes) {
                var rowData = issuetable.rows(indexes).data().toArray();
                $('#editis').prop('disabled', true);
                $('#issuedetails').prop('disabled', true);
                selectedRow = null;
            }).on('dblclick', function(e, dt, type, indexes) {
                var rowData = issuetable.rows( indexes ).data().toArray();
                console.log('report id', rowData[0][0]);
                $.ajax({
                    type: 'GET',
                    headers: {
                        Accept : 'application/json'
                    },
                    url: '/feedback/details?id=' + rowData[0][0],
                    dataType: 'json',
                    crossDomain: true,
                    success: function(data) {
                        console.log('details ', data);
                        $('#user').val(rowData[0][2]);
                        $('#query').val(rowData[0][3]);
                        $('#desc').val(rowData[0][4]);
                        $('#status').val(rowData[0][5]);

                        $('#var').val(data['var']);
                        $('#pred').val(data['pred']);
/*                        var obj = JSON.parse(data['row']);
                        var pretty = JSON.stringify(obj, undefined, 4);
                        $('#rowjson').val(pretty); */
                        $('#rowjson').val(JSON.stringify(data['row'], undefined, 4));
                        feedbackdialog.modal('show');
                    },
                    error: function(jqXHR, textStatus) {
                        console.log(jqXHR.status);
                        console.log(jqXHR.responseText);
                        console.log(textStatus);
                    }
                });
            });
        } else {
            table.clear().draw();
            $('#editis').prop('disabled', true);
            $('#issuedetails').prop('disabled', true);
        }

        $.ajax({
                type: 'GET',
                    headers: {
                    Accept : 'application/json'
                },
                url: '/feedback/issues?fed=' + federation,
                crossDomain: true,
                success: function(data) {
                    const datas = data.data;
                    for (const d in datas) {
                        let row = [
                            datas[d].id,
                            datas[d].fed,
                            datas[d].user,
                            datas[d].query,
                            datas[d].desc,
                            datas[d].status,
                            datas[d].created
                        ];
                        table.row.add(row).draw(false);
                    }
                },
                error: function(jqXHR, textStatus) {
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                }
            })
    }

    $('#issuedetails').on('click', function() {
        $.ajax({
            type: 'GET',
            headers: {
                Accept : 'application/json'
            },
            url: '/feedback/details?id=' + selectedRow[0][0],
            dataType: 'json',
            crossDomain: true,
            success: function(data) {
                console.log('details', data);
                $('#user').val(selectedRow[0][2]);
                $('#query').val(selectedRow[0][3]);
                $('#desc').val(selectedRow[0][4]);
                $('#status').val(selectedRow[0][5]);

                $('#var').val(data['var']);
                $('#pred').val(data['pred']);
                $('#rowjson').val(JSON.stringify(data['row'], undefined, 4));
            },
            error: function(jqXHR, textStatus) {
                console.log(jqXHR.status);
                console.log(jqXHR.responseText);
                console.log(textStatus);
            }
        });
        // $('#fedbackpreds').empty();
        // $('#fedbackpreds').append('<option value="-1">Select column</option>');
        // for (d in queryvars) {
        //     $('#fedbackpreds').append('<option value=' + queryvars[d] + '> ' + queryvars[d] + '</option>');
        // }
        // $('#fedbackpreds').append('<option value="All">All</option>');
    });
});
