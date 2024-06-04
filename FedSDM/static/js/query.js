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

function initialize_yasqe() {
    // register our custom autocompleters
    YASQE.registerAutocompleter('customPropertyCompleter', customPropertyCompleter);
    YASQE.registerAutocompleter('customClassCompleter', customClassCompleter);
    // and, to make sure we don't use the other property and class autocompleters, overwrite the default enabled completers
    YASQE.defaults.autocompleters = ['customClassCompleter', 'customPropertyCompleter'];
    yasqe = YASQE(document.getElementById('yasqe'), {
        viewportMargin: Infinity,  // display full query
        backdrop: true,            // grey edit window during query execution
        tabSize: 2,                // modify codemirror tab handling to solely use 2 spaces
        indentUnit: 2,
        extraKeys: { Tab: function(cm) { cm.replaceSelection(new Array(cm.getOption('indentUnit') + 1).join(' ')) } },
        sparql: {
            showQueryButton: true,
            endpoint: '/query/sparql',
            callbacks: {
                beforeSend: function(jqXHR, setting) {
                    $('#result_status').hide();
                    $('#btnVisualize').hide();
                    $('#btnShowTable').hide();
                    setting.url = '/query/sparql?federation=' + federation + '&query=' + encodeURIComponent(yasqe.getValue());
                    setting.crossDomain = true;
                    setting.data ={ 'query': yasqe.getValue() };
                    $('#result_info').hide();
                    queryResultsTable.empty();
                },
                success: function(data) {
                    $('#result_table_div').empty()
                        .append('<table style="width: 100%" class="table table-striped table-bordered table-hover" id="query_result_table"></table>');
                    queryResultsTable = $('#query_result_table');

                    if ('error' in data) {
                        $('#result_row').show();
                        $('#result_info').show();
                        $('#result_status').html('Error: ' + data.error)
                            .show();
                        return true;
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
                            tableHeader =  tableHeader + '<th>' + vars[i] + '</th> ';
                            tableFooter =  tableFooter + '<th>' + vars[i] + '</th> ';
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
                            let row = results[i],
                                row_ml = [];
                            for (let j = 0; j < vars.length; j++) {
                                const entry = row[vars[j]];
                                row_ml.push(entry);
                            }
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
                            } );
                        });
                        table.on('select', function(e, dt, type, indexes) {
                            selectedRow = table.rows( indexes ).data().toArray();
                            selectedRowData = [];
                            for (let i in selectedRow[0]) { selectedRowData.push(selectedRow[0][i]['value']) }
                            $('#add_feedback').prop('disabled', false);
                        }).on('deselect', function() {
                            $('#add_feedback').prop('disabled', true);
                            selectedRow = null;
                        });
                    } else {
                        $('#result_status').html('No results found!')
                            .show();
                        $('#result_info').show();
                        $('#result_row').show();
                        response = false;
                        return true;
                    }
                    response = true;
                    $('#btnStop').prop('disabled', false);
                    show_incremental(vars);
                } // end of sparql success callback function
            }
        },
        value: 'SELECT DISTINCT ?concept WHERE {\n\t?s a ?concept\n} LIMIT 10'
    });
}

const addFeedbackDialog = $('#feedbackModal');
const addFeedbackForm = addFeedbackDialog.find('form').on('submit', function(event) {
    event.preventDefault();
    addFeedback(true);
});
const feedbackDesc =  $('#feedbackDesc'),
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
        data.append('fed', federation)
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
    if (response === true) {
        // No new request can be sent unless a response from the last request was received
        response = false;
        if (shouldStop === false) {
            await fetch('/query/nextresult')
                .then(res => res.json())
                .then(data => {
                    let row = data.result;
                    let elemTimeTotal = $('#time_total');
                    if (row.length === 0 || row === 'EOF') {
                        $('#btnVisualize').show();
                        elemTimeTotal.html(' ' + data.time_total + ' sec');
                        response = false;
                        return;
                    }
                    elemTimeTotal.html(' ' + data.time_total + ' sec');
                    const row_ml = [];
                    for (let j = 0; j < vars.length; j++) {
                        const entry = row[vars[j]];
                        row_ml.push(entry);
                    }

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

            if (response === true && shouldStop === false) {
                response = true;
                await show_incremental(vars);
            } else {
                shouldStop = false;
                $('#btnStop').prop('disabled', true);
            }
        }
    }
}

$('#btnStop').on('click', function() {
    console.log('stop pressed');
    response = false;
    shouldStop = true;
});

/**
 * We use most of the default settings for the property and class autocompletion types. This includes:
 * -  the pre-/post-processing of tokens
 * -  detecting whether we are in a valid autocompletion position
 * -  caching of the suggestion list. These are cached for a period of a month on the client side.
 */
var getAutocompletionsArrayFromJson = function(result) {
    let completionsArray = [];
    result.forEach(function(row) {  // remove first line, as this one contains the projection variable
        if ('type' in row) { completionsArray.push(row['type']) }  // remove quotes
        else { completionsArray.push(row['property']) }  // remove quotes
    });
    return completionsArray;
}

var customPropertyCompleter = function(yasqe) {
    // we use several functions from the regular property autocompleter (this way, we don't have to re-define code such as determining whether we are in a valid autocompletion position)
    var returnObj = {
        isValidCompletionPosition: function() { return YASQE.Autocompleters.properties.isValidCompletionPosition(yasqe) },
        preProcessToken: function(token) { return YASQE.Autocompleters.properties.preProcessToken(yasqe, token) },
        postProcessToken: function(token, suggestedString) { return YASQE.Autocompleters.properties.postProcessToken(yasqe, token, suggestedString) }
    };

    // in this case we assume the properties will fit in memory. So, turn on bulk loading, which will make autocompleting a lot faster
    returnObj.bulk = true;
    returnObj.async = true;

    // and, as everything is in memory, enable autoShowing the completions
    returnObj.autoShow = true;

    returnObj.persistent = 'customProperties';  // this will store the sparql results in the client-cache for a month.
    returnObj.get = function(token, callback) {
        // all we need from these parameters is the last one: the callback to pass the array of completions to
        var sparqlQuery = 'SELECT DISTINCT ?property WHERE { ?s ?property ?obj } LIMIT 1000';
        $.ajax({
            data: { query: sparqlQuery },
            url: YASQE.defaults.sparql.endpoint,
            // headers: { Accept: 'text/csv' },  //ask for csv. Simple, and uses less bandwidth
            success: function(data) {
                // console.log(sparqlQuery);
                // console.log(data);
                callback(getAutocompletionsArrayFromJson(data.result));
            }
        });
    };
    return returnObj;
};

var customClassCompleter = function(yasqe) {
    let returnObj = {
        isValidCompletionPosition: function() { return YASQE.Autocompleters.classes.isValidCompletionPosition(yasqe) },
        preProcessToken: function(token) { return YASQE.Autocompleters.classes.preProcessToken(yasqe, token) },
        postProcessToken: function(token, suggestedString) { return YASQE.Autocompleters.classes.postProcessToken(yasqe, token, suggestedString) }
    };
    returnObj.bulk = true;
    returnObj.async = true;
    returnObj.autoShow = true;
    returnObj.get = function(token, callback) {
        const filters = 'FILTER (!regex(str(?type), "http://www.w3.org/ns/sparql-service-description", "i") && ' +
            ' !regex(str(?type), "http://www.openlinksw.com/schemas/virtrdf#", "i") && ' +
            ' !regex(str(?type), "http://www.w3.org/2000/01/rdf-schema#", "i") && ' +
            ' !regex(str(?type), "http://www.w3.org/1999/02/22-rdf-syntax-ns#", "i") && ' +
            ' !regex(str(?type), "http://purl.org/dc/terms/Dataset", "i") && ' +
            ' !regex(str(?type), "http://www.w3.org/2002/07/owl#", "i") && ' +
            ' !regex(str(?type), "http://rdfs.org/ns/void#", "i") && ' +
            ' !regex(str(?type), "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/", "i") && '+
            ' !regex(str(?type), "nodeID://", "i") ) '
        const sparqlQuery = 'SELECT DISTINCT ?type WHERE { ?s a ?type . ' + filters + ' } LIMIT 1000';
        $.ajax({
            data: { query: sparqlQuery },
            url: YASQE.defaults.sparql.endpoint,
            // headers: { Accept: 'text/csv' },  //ask for csv. Simple, and uses less bandwidth
            success: function(data) {
                // console.log(sparqlQuery);
                // console.log(data);
                callback(getAutocompletionsArrayFromJson(data.result));
            }
        });
    };
    return returnObj;
};

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
