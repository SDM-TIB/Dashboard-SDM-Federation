const federationList = $('#federations-list');
let federation = federationList.val(),
    queryResultsTable = $('#query_result_table'),
    yasqe = null,
    queryTriples = [],
    queryVars = [],
    table = null, selectedRow = null, selectedRowData = [],
    response = false,
    shouldStop = false;

function initialize_ui() {
    if (federation != null && federation !== '') {
        $('#query_row').show();
        $('#result_row').hide();
        if (yasqe == null) { initialize_yasqe() }
    } else {
        $('#result_info').hide();
        $('#result_status').hide();
        $('#query_row').hide();
        $('#result_row').hide();
    }
}

federationList.on('change', function() {
    federation = $(this).val();
    initialize_ui();
});

function query_result_renderer(data) {
    const val = data['value'];
    if (data['type'] === 'uri') { return '<a href="' + val + '">' + val + '</a>' }
    else if (data['type'] === 'typed-literal') { return val + '<sup class="gray">' + data['datatype'].replace('http://www.w3.org/2001/XMLSchema#', ' xsd:') + '</sup>' }
    else { return val }
}

// Register custom completers once at module load, before any Yasqe instance is created.
// forkAutocompleter() inherits isValidCompletionPosition / preProcessToken /
// postProcessSuggestion from the built-in completers and only replaces get()
// with a call to the project's own endpoint, passing the current federation.
Yasqe.forkAutocompleter('property', {
    name: 'customPropertyCompleter',
    bulk: true,
    autoShow: true,
    persistenceId: function() { return 'customProperties_' + federation; },
    get: function() {
        return new Promise(function(resolve, reject) {
            $.ajax({
                data: { federation: federation },
                url: window.location.origin + '/query/properties',
                success: function(data) { resolve(data.result || []); },
                error: function(xhr, status, err) { reject(err); }
            });
        });
    }
});

Yasqe.forkAutocompleter('class', {
    name: 'customClassCompleter',
    bulk: true,
    autoShow: true,
    persistenceId: function() { return 'customClasses_' + federation; },
    get: function() {
        return new Promise(function(resolve, reject) {
            $.ajax({
                data: { federation: federation },
                url: window.location.origin + '/query/classes',
                success: function(data) { resolve(data.result || []); },
                error: function(xhr, status, err) { reject(err); }
            });
        });
    }
});

function initialize_yasqe() {
    yasqe = new Yasqe(document.getElementById('yasqe'), {
        showQueryButton: true,
        tabSize: 2,
        indentUnit: 2,
        autocompleters: ['variables', 'prefixes', 'customPropertyCompleter', 'customClassCompleter'],
        extraKeys: { Tab: function(cm) { cm.replaceSelection(new Array(cm.getOption('indentUnit') + 1).join(' ')) } },
        requestConfig: {
            endpoint: window.location.origin + '/query/sparql',
            method: 'GET'
        },
        value: 'SELECT DISTINCT ?concept WHERE {\n\t?s a ?concept\n} LIMIT 10'
    });

    yasqe.on('queryBefore', function(instance) {
        instance.config.requestConfig.endpoint = window.location.origin + '/query/sparql?federation=' + encodeURIComponent(federation);
        $('#result_status').hide();
        $('#btnVisualize').hide();
        $('#btnShowTable').hide();
        $('#result_info').hide();
        queryResultsTable.empty();
    });

    yasqe.on('queryResponse', function(instance, queryResp) {
        if (queryResp instanceof Error) {
            $('#result_row').show();
            $('#result_info').show();
            $('#result_status').html('Error: ' + queryResp.message).show();
            return;
        }

        let data;
        try {
            data = JSON.parse(queryResp.content);
        } catch(e) {
            $('#result_row').show();
            $('#result_info').show();
            $('#result_status').html('Error: could not parse response').show();
            return;
        }

        $('#result_table_div').empty()
            .append('<table style="width: 100%" class="table table-striped table-bordered table-hover" id="query_result_table"></table>');
        queryResultsTable = $('#query_result_table');

        if ('error' in data) {
            $('#result_row').show();
            $('#result_info').show();
            $('#result_status').html('Error: ' + data.error).show();
            return;
        }

        $('#time_first').html(' ' + data.time_first + ' sec');
        $('#time_total').html(' ' + data.time_total + ' sec');

        const results = data.result,
            vars = data.vars;

        if (results.length > 0) {
            $('#result_status').hide();
            $('#result_info').show();
            $('#result_row').show();

            let tableHeader = '<thead><tr>',
                tableFooter = '<tfoot><tr>';
            for (let i = 0; i < vars.length; i++) {
                tableHeader += '<th>' + vars[i] + '</th> ';
                tableFooter += '<th>' + vars[i] + '</th> ';
                queryVars.push(vars[i]);
            }
            queryResultsTable.append(tableHeader + '</tr></thead>')
                .append('<tbody></tbody>')
                .append(tableFooter + '</tr></tfoot>');

            table = queryResultsTable.DataTable({
                responsive: false,
                select: true,
                lengthMenu: [ [10, 25, 50, -1], [10, 25, 50, 'All'] ],
                dom: 'Blfrtip',
                buttons: table_buttons('sparql-results'),
                columnDefs: [{ targets: '_all', render: query_result_renderer }]
            });
            queryTriples = data.query_triples;
            for (let i = 0; i < results.length; i++) {
                let row = results[i], row_ml = [];
                for (let j = 0; j < vars.length; j++) { row_ml.push(row[vars[j]]) }
                table.row.add(row_ml).draw(false);
            }
            table.columns().every(function() {
                let column = this,
                    select = $('<select><option value="">All</option></select>')
                        .appendTo($(column.footer()).empty())
                        .on('change', function() {
                            let val = $.fn.dataTable.util.escapeRegex($(this).val());
                            column.search(val ? '^' + val + '$' : '', true, false).draw();
                        });
                column.data().unique().sort().each(function(d) {
                    select.append('<option value=' + d + '>' + d + '</option>');
                });
            });
            table.on('select', function(e, dt, type, indexes) {
                selectedRow = table.rows(indexes).data().toArray();
                selectedRowData = [];
                for (let i in selectedRow[0]) { selectedRowData.push(selectedRow[0][i]['value']) }
                $('#add_feedback').prop('disabled', false);
            }).on('deselect', function() {
                $('#add_feedback').prop('disabled', true);
                selectedRow = null;
            });

            response = true;
            $('#btnStop').prop('disabled', false);
            show_incremental(vars);
        } else {
            $('#result_status').html('No results found!').show();
            $('#result_info').show();
            $('#result_row').show();
            response = false;
        }
    });
}

const addFeedbackDialog = $('#feedbackModal');
const addFeedbackForm = addFeedbackDialog.find('form').on('submit', function(event) {
    event.preventDefault();
    addFeedback(true);
});
const feedbackDesc = $('#feedbackDesc'),
      feedbackPredicates = $('#feedbackPredicates'),
      allFeedbackFields = $([]).add(feedbackDesc).add(feedbackPredicates);

addFeedbackDialog.on('shown.bs.modal', function() { feedbackDesc.trigger('focus'); });
addFeedbackDialog.on('hidden.bs.modal', function() {
    addFeedbackForm[0].reset();
    allFeedbackFields.removeClass('ui-state-error');
    resetTips();
});
$('#add-feedback-btn').on('click', function() { addFeedback(true) });

async function addFeedback(close) {
    allFeedbackFields.removeClass('ui-state-error');
    let valid = checkSelection(feedbackPredicates, 'column') && checkLength(feedbackDesc, 'description', 2, 500);
    console.log({'desc': feedbackDesc.val(), 'pred': feedbackPredicates.val(), 'query': yasqe.getValue(), 'row': selectedRowData, 'columns': queryVars});

    if (valid) {
        let data = new FormData();
        data.append('fed', federation);
        data.append('desc', feedbackDesc.val());
        data.append('pred', feedbackPredicates.val());
        data.append('query', yasqe.getValue());
        data.append('row[]', selectedRowData);
        data.append('columns[]', queryVars);
        console.log(data);

        valid = await fetch('/query/feedback', {
                method: 'POST',
                headers: { Accept: 'application/text' },
                body: data
            })
            .then(res => res.text())
            .then(data => {
                console.log(data);
                if (data === null || data.length === 0) {
                    $('#validateTips').html('Error while adding feedback!');
                    return false;
                }
                return true;
            })
            .catch(err => console.log(err));
    } else {
        close = false;
        console.log('Invalid data...');
    }
    if (valid && close) { addFeedbackDialog.modal('hide') }
    return valid;
}

$('#add_feedback').on('click', function() {
    feedbackPredicates.empty()
        .append('<option value="-1">Select column</option>');
    for (const d in queryVars) { feedbackPredicates.append('<option value=' + queryVars[d] + '> ' + queryVars[d] + '</option>') }
    feedbackPredicates.append('<option value="All">All</option>');
});

async function show_incremental(vars) {
    // No new request can be sent unless a response from the last request was received
    while (response === true && shouldStop === false) {
        response = false;
        await fetch('/query/nextresult')
            .then(res => res.json())
            .then(data => {
                let row = data.result;
                let elemTimeTotal = $('#time_total');
                if (row.length === 0 || row === 'EOF') {
                    $('#btnVisualize').show();
                    elemTimeTotal.html(' ' + data.time_total + ' sec');
                    return;
                }
                elemTimeTotal.html(' ' + data.time_total + ' sec');
                const row_ml = [];
                for (let j = 0; j < vars.length; j++) { row_ml.push(row[vars[j]]) }
                table.row.add(row_ml).draw(false);

                table.columns().every(function() {
                    const column = this;
                    const select = $('<select><option value="">All</option></select>')
                        .appendTo($(column.footer()).empty())
                        .on('change', function() {
                            const val = $.fn.dataTable.util.escapeRegex($(this).val());
                            column.search(val ? '^' + val + '$' : '', true, false).draw();
                        });
                    column.data().unique().sort().each(function(d) {
                        const val = d['value'];
                        select.append('<option value=' + val + '>' + val + '</option>');
                    });
                });
                response = true;
            })
            .catch(err => console.log(err));
    }
    shouldStop = false;
    $('#btnStop').prop('disabled', true);
}

$('#btnStop').on('click', function() {
    console.log('stop pressed');
    response = false;
    shouldStop = true;
});

$('#classes').on('click', function() { yasqe.setValue('SELECT DISTINCT ?c WHERE {\n\t?s a ?c\n}') });

$('#analyticalnumtheoryex').on('click', function() {
    yasqe.setValue('PREFIX schema: <http://schema.org/> \n' +
        'PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n' +
        'SELECT DISTINCT * WHERE {\n' +
        '\t<http://av.tib.eu/resource/video/16439> ?p ?obj .\n' +
        '}  LIMIT 100');
});

// TODO: add more example queries here

initialize_ui();
