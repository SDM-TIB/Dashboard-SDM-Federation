/*!
 * --------------------------------------------------------
 * FedSDM: feedback.js
 * Provides the functionality of displaying reported issues
 * --------------------------------------------------------
 */

$(function() {
    const federationList = $('#federations-list'),
        editIssue = $('#edit_issue'),
        detailsIssue = $('#issue_details');
    let federation = federationList.val(),
        table = null,
        feedbackDialog = $('#detailsModal'),
        selectedRow = null;

    if (federation != null && federation !== '') {
        load_table(federation);
    }

    federationList.on('change', function() {
        federation = $(this).val();
        if (federation != null && federation !== '') {
            load_table(federation);
        }
    });

    // Loads the issue details and opens a dialog showing the details of the specific issue
    function get_issue_details(issueData) {
        $.ajax({
            type: 'GET',
            headers: {
                Accept : 'application/json'
            },
            url: '/feedback/details?iid=' + issueData[0][0],
            dataType: 'json',
            crossDomain: true,
            success: function(data) {
                console.log('details ', data);
                $('#user').val(issueData[0][2]);
                $('#query').val(issueData[0][3]);
                $('#desc').val(issueData[0][4]);
                $('#status').val(issueData[0][5]);

                $('#var').val(data['var']);
                $('#pred').val(data['pred']);
                $('#rowJSON').val(JSON.stringify(data['row'], undefined, 4));
                feedbackDialog.modal('show');
            },
            error: function(jqXHR, textStatus) {
                console.log(jqXHR.status);
                console.log(jqXHR.responseText);
                console.log(textStatus);
            }
        });
    }

    // Loads the data for all reported issues and populates a table with the received information.
    // Also adds event handlers to the table, e.g., double-clicking on a row opens a dialog with
    // more detailed information about the issue.
    function load_table(federation) {
        $('#fedName').html(federation);
        //Disable buttons before selecting item on the table
        editIssue.prop('disabled', true);
        detailsIssue.prop('disabled', true);

        //Construct data source management data table
        if (table == null) {
            table = $('#reported_issues').DataTable({
                order: [[1, 'desc']],
                responsive: true,
                select: true,
                defaultContent: '<i>Not set</i>'
            });

            let issueTable = table;
            table.on('select', function(e, dt, type, indexes) {
                selectedRow = issueTable.rows(indexes).data().toArray();
                editIssue.prop('disabled', false);
                detailsIssue.prop('disabled', false);
            }).on('deselect', function() {
                editIssue.prop('disabled', true);
                detailsIssue.prop('disabled', true);
                selectedRow = null;
            }).on('dblclick', 'tbody tr', function() {
                const rowData = [issueTable.row(this).data()];
                console.log('report id', rowData[0][0]);
                get_issue_details(rowData);
            });
        } else {
            table.clear().draw();
            editIssue.prop('disabled', true);
            detailsIssue.prop('disabled', true);
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

    // Event handler for 'on click' of the 'issue details' button.
    // Opens a dialog with more detailed information about the selected issue.
    detailsIssue.on('click', function() { get_issue_details(selectedRow); });
});
