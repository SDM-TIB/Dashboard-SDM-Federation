/*!
 * --------------------------------------------------------
 * FedSDM: feedback.js
 * Provides the functionality of displaying reported issues
 * --------------------------------------------------------
 */

const federationList = $('#federations-list'),
    editIssue = $('#edit_issue'),
    detailsIssue = $('#issue_details');
let federation = federationList.val(),
    table = null,
    feedbackDialog = $('#detailsModal'),
    selectedRow = null;

if (federation != null && federation !== '') { load_table(federation); }

federationList.on('change', function() {
    federation = $(this).val();
    if (federation != null && federation !== '') { load_table(federation); }
    else { clear_table(); }
});

// Loads the issue details and opens a dialog showing the details of the specific issue
function get_issue_details(issueData) {
    fetch('/feedback/details?iid=' + issueData[0][0], { headers: { Accept: 'application/json' } })
        .then(res => res.json())
        .then(data => {
            console.log('details ', data);
            $('#user').val(issueData[0][2]);
            $('#query').val(issueData[0][3]);
            $('#desc').val(issueData[0][4]);
            $('#status').val(issueData[0][5]);

            $('#var').val(data['var']);
            $('#pred').val(data['pred']);
            $('#rowJSON').val(JSON.stringify(data['row'], undefined, 4));
            feedbackDialog.modal('show');
        })
        .catch(err => console.error(err));
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
            select: { style: 'single' },
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
    } else { clear_table(); }

    fetch('/feedback/issues?fed=' + federation, { headers: { Accept: 'application/json' } })
        .then(res => res.json())
        .then(data => data.data)
        .then(data => {
            for (const d in data) {
                let row = [
                    data[d].id,
                    data[d].fed,
                    data[d].user,
                    data[d].query,
                    data[d].desc,
                    data[d].status,
                    data[d].created
                ];
                table.row.add(row).draw(false);
            }
        })
        .catch(err => console.error(err));
}

// Clears the table with the issue details.
function clear_table() {
    table.clear().draw();
    editIssue.prop('disabled', true);
    detailsIssue.prop('disabled', true);
}

// Event handler for 'on click' of the 'issue details' button.
// Opens a dialog with more detailed information about the selected issue.
detailsIssue.on('click', function() { get_issue_details(selectedRow); });
