/*!
 * ------------------------------------------------------------------------------------------------
 * FedSDM: federations.js
 * Loads statistics about the federations and data sources; creates tables and bar charts with them
 * ------------------------------------------------------------------------------------------------
 */

const federationList = $('#federations-list'),
      button_add_source = $('#add_ds'),
      button_edit_source = $('#edit_ds'),
      button_remove_source = $('#remove_ds'),
      button_recompute_mts = $('#recompute_mts'),
      button_links = $('#find_links'),
      button_all_links = $('#find_all_links');
let statsTable = null,
    sourceStatsChart = null,
    table = null,
    selectedSource = null;
const prefix = 'http://ontario.tib.eu/federation/g/';

// Federation list dropdown change event, triggers visualization of statistics and management data.
federationList.on('change', function() {
    federation = $(this).val();
    showFederations(federation);
});

let statsTableFed = null;
// Populates the table with the statistics about all available federations.
federationOverview = function(feds) {
    if (statsTableFed == null) {
        statsTableFed = $('#federations-statistics').DataTable({
            order: [[ 1, 'desc' ]],
            responsive: false,
            defaultContent: '-1',
            select: true,
            dom: 'lfrtip',
            columnDefs: [
                { target: 1, render: number_renderer },
                { target: 2, render: number_renderer },
                { target: 3, render: number_renderer },
                { target: 4, render: number_renderer },
                { target: 5, render: number_renderer }
            ]
        });
    } else { statsTableFed.clear().draw() }

    for (let fed of feds) {
        let rem = [];
        rem.push(fed['name']);
        rem.push(fed['sources']);
        rem.push(fed['triples']);
        rem.push(fed['rdfmts']);
        rem.push(fed['properties']);
        rem.push(fed['links']);
        statsTableFed.row.add(rem).draw(false);
    }
}

// Check if federation name is set, and show statistics and management data.
function showFederations(federation) {
    if (federation != null && federation !== '') {
        $('#fedName').html(federation);
        basic_stat(federation);
        manage(federation);
        if (federation !== 'All'){
            button_add_source.prop('disabled', false);
            button_all_links.prop('disabled', false);
        } else {
            button_add_source.prop('disabled', true);
            button_all_links.prop('disabled', true);
        }
    } else {
        disableButtons();
        if (table != null) { table.clear().draw() }
        if (statsTable != null) { statsTable.clear().draw() }
        if (sourceStatsChart != null) {
            sourceStatsChart.data.labels = [];
            sourceStatsChart.data.datasets = [];
            sourceStatsChart.update();
        }
    }
}

// If no data source is selected, some action buttons will be disabled.
function set_disabled_prop_ds_buttons(disabled) {
    button_edit_source.prop('disabled', disabled);
    button_remove_source.prop('disabled', disabled);
    button_recompute_mts.prop('disabled', disabled);
    button_links.prop('disabled', disabled);
}

// If no federation is selected, then all action buttons will be disabled.
function disableButtons() {
    button_add_source.prop('disabled', true);
    button_all_links.prop('disabled', true);
    set_disabled_prop_ds_buttons(true);
}

// Turns an array with the information about the data sources, i.e., the number ot triples and
// the number of RDF Molecule Templates, into the representation for a bar chart using Chart.js.
function sourceStatsToBarChart(data) {
    return [
        {
            id: 1,
            label: '# of Triples(log)',
            data: data.triples,
            borderWidth: 1,
            backgroundColor: colorNumberTriples,
        }, {
            id: 2,
            label: '# of RDF-MTs (log)',
            data: data.rdfmts,
            borderWidth: 1,
            backgroundColor: colorNumberMolecules,
        }
    ];
}

// Loads the basic statistics about data sources in a given federation and populates the table and bar chart.
function basic_stat(fed) {
    if (statsTable == null) {
        // Construct basic statistics table
        statsTable = $('#basic-statistics').DataTable({
            order: [[ 1, 'desc' ]],
            responsive: false,
            defaultContent: '-1',
            columnDefs: [
                { target: 1, render: number_renderer },
                { target: 2, render: number_renderer }
            ],
            select: true,
            dom: 'lfrtip'
        });
    } else { statsTable.clear().draw() }

    fetch('/federation/stats?graph=' + fed, { headers: { Accept: 'application/json' } })
        .then(res => res.json())
        .then(data => data.data)
        .then(data => {
            let barData = { labels: [], rdfmts: [], triples: [] };
            for (const d in data) {
                let rem = [];
                rem.push(data[d].ds);
                let rdfmts = data[d].rdfmts;

                rem.push(rdfmts);
                let triples = data[d].triples;
                if (triples == null) { triples = '-1' }

                rem.push(triples);
                statsTable.row.add(rem).draw(false);

                barData.labels.push(data[d].ds);
                rdfmts = log10(rdfmts);
                barData.rdfmts.push(rdfmts);
                triples = log10(triples);
                barData.triples.push(triples);
            }

            $('#sourceStatsChartContainer').height(62 + 70 * barData.labels.length);
            if (sourceStatsChart == null) {
                sourceStatsChart = new Chart($('#sourceStatsChart'), {
                    type: 'bar',
                    data: {
                        labels: barData.labels,
                        datasets: sourceStatsToBarChart(barData)
                    },
                    options: chartOptions
                });
            } else {
                sourceStatsChart.data.labels = [];
                sourceStatsChart.data.datasets = [];
                sourceStatsChart.update();
                sourceStatsChart.data.labels = barData.labels;
                sourceStatsChart.data.datasets = sourceStatsToBarChart(barData);
                sourceStatsChart.update();
            }
        })
        .catch(err => console.error(err));
}

// Sets up the management tab with information about data sources in a given federation.
// Additionally, adds a 'on select' method to the table.
function manage(fed) {
    $('#fedName').html(fed);
    // disable buttons before selecting item on the table
    set_disabled_prop_ds_buttons(true);

    // construct data source management data table
    if (table == null) {
        table = $('#data-sources').DataTable({
            order: [[ 1, 'desc' ]],
            responsive: false,
            select: true,
            dom: 'lfrtip',
            defaultContent: '<i>Not set</i>',
            columnDefs: [{ target: 0, visible: false, searchable: false }],
            ajax: '/federation/datasources?graph=' + federation
        });
        // data source table select action
        table.on('select', function(e, dt, type, indexes) {
            selectedSource = table.rows(indexes).data().toArray();
            set_disabled_prop_ds_buttons(false);
        }).on('deselect', function() {
            set_disabled_prop_ds_buttons(true);
            selectedSource = null;
        });
    } else {
        selectedSource = null;
        table.clear().draw();
        set_disabled_prop_ds_buttons(true);
        table.ajax.url('/federation/datasources?graph=' + fed).load();
    }
    table.on('draw', function() {
        if (table.column(0).data().length > 0) { button_all_links.prop('disabled', false) }
        else { button_all_links.prop('disabled', true) }
    });
}

// Edit data source click action
button_edit_source.on('click', function() {
    $('#edit_name').val(selectedSource[0][1]);
    $('#edit_URL').val(selectedSource[0][2]);
    $('#edit_ds_type').val(selectedSource[0][3]);
    $('#edit_keywords').val(selectedSource[0][4].trim());
    $('#edit_homepage').val(selectedSource[0][5].trim());
    $('#edit_organization').val(selectedSource[0][6].trim());
    $('#edit_label').val(selectedSource[0][7].trim());
    $('#edit_version').val(selectedSource[0][8].trim());
    $('#edit_params').val(selectedSource[0][9].trim());
    $('#edit_types').val(selectedSource[0][10].trim());
});

// Remove data source click action
button_remove_source.on('click', function() {
    // TODO: The backend is not yet implemented
    fetch('/federation/api/removeds?ds=' + encodeURIComponent(selectedSource[0][0]) + '&fed=' + encodeURIComponent(federation), { headers: { Accept: 'application/json' } })
        .then(res => res.json())
        .then(data => { console.log(data); if (data === true) { table.row('.selected').remove().draw(false) } })
        .catch(err => console.error(err));
    set_disabled_prop_ds_buttons(true);
});

// Recompute RDF Molecule Templates click action
button_recompute_mts.on('click', function() {
    fetch('/federation/api/recreatemts?fed=' + encodeURIComponent(federation) + '&datasource=' + encodeURIComponent(selectedSource[0][0]) + '&query=all', { headers: { Accept: 'application/json' } })
        .then(res => res.json())
        .then(data => {
            if (data != null && data.status === 1) { alert('Recreating RDF-MTs for ' + selectedSource[0][0] + ' is underway...') }
            else { alert('Cannot start the process. Please check if there are data sources in this federation.') }
        })
        .catch(err => console.error(err));
});

// Find links click action
button_links.on('click', function() {
    fetch('/federation/api/findlinks?fed=' + encodeURIComponent(federation) + '&datasource=' + encodeURIComponent(selectedSource[0][0]), { headers: { Accept: 'application/json' } })
        .then(res => res.json())
        .then(data => {
            if (data != null && data.status === 1) { alert('Finding links in progress...') }
            else { alert('Cannot start the process. Please check if there are data sources in this federation.') }
        })
        .catch(err => console.error(err));
});

// Find all links click action
button_all_links.on('click', function() {
    fetch('/federation/api/findlinks?fed=' + encodeURIComponent(federation), { headers: { Accept: 'application/json' } })
        .then(res => res.json())
        .then(data => {
            if (data != null && data.status === 1) { alert('Finding links in progress...') }
            else { alert('Cannot start the process. Please check if there are data sources in this federation.') }
        })
        .catch(err => console.error(err));
});

// Constants used in the management dialog functions
const fedModal = $('#federationModal'),
      addSourceModal = $('#addSourceModal'),
      editSourceModal = $('#editSourceModal'),
      newFedForm = fedModal.find('form'),
      addSourceForm = addSourceModal.find('form'),
      editSourceForm = editSourceModal.find('form'),
      name = $('#name'),
      desc = $('#desc'),
      ds_type = $('#ds_type'),
      URL = $('#URL'),
      params = $('#params'),
      types = $('#types'),
      keywords = $('#keywords'),
      organization = $('#organization'),
      homepage = $('#homepage'),
      version = $('#version'),
      allFields = $([]).add(name).add(desc).add(ds_type).add(URL).add(params).add(types).add(keywords).add(organization).add(homepage).add(version),
      edit_name = $('#edit_name'),
      edit_desc = $('#edit_desc'),
      edit_ds_type = $('#edit_ds_type'),
      edit_URL = $('#edit_URL'),
      edit_params = $('#edit_params'),
      edit_types = $('#edit_types'),
      edit_keywords = $('#edit_keywords'),
      edit_organization = $('#edit_organization'),
      edit_homepage = $('#edit_homepage'),
      edit_version = $('#edit_version'),
      allFieldsEdit = $([]).add(edit_name).add(edit_desc).add(edit_ds_type).add(edit_URL).add(edit_params).add(edit_types).add(edit_keywords).add(edit_organization).add(edit_homepage).add(edit_version),
      fedName = $('#fed_new_name'),
      fedDesc = $('#description'),
      fedPublic = $('#fed_new_public'),
      allFieldsFed = $([]).add(fedName).add(fedDesc).add(fedPublic);

newFedForm.on('submit', function(event) {
    event.preventDefault();
    createNewFederation(true);
});
fedModal.on('shown.bs.modal', function() { fedName.trigger('focus') });
fedModal.on('hidden.bs.modal', function() {
    newFedForm[0].reset();
    allFieldsFed.removeClass('ui-state-error');
    resetTips();
});
$('#create-fed-btn-create').on('click', function() { createNewFederation(true) });

addSourceForm.on('submit', function(event) {
    event.preventDefault();
    addDataSource(true);
});
addSourceModal.on('shown.bs.modal', function() { name.trigger('focus') });
addSourceModal.on('hidden.bs.modal', function() {
    addSourceForm[0].reset();
    allFields.removeClass('ui-state-error');
    resetTips();
});
$('#add-source-btn-finish').on('click', function() { addDataSource(true) });
$('#add-source-btn-more').on('click', function() { saveAndMore() });

editSourceForm.on('submit', function(event) {
    event.preventDefault();
    updateDS(true);
});
editSourceModal.on('shown.bs.modal', function() { edit_desc.trigger('focus') });
editSourceModal.on('hidden.bs.modal', function() {
    editSourceForm[0].reset();
    allFieldsEdit.removeClass('ui-state-error');
    resetTips();
});
$('#edit-source-btn').on('click', function() { updateDS(true) });

// Adds a new data source using the FedSDM API. If the parameter 'close' is true, then the dialog will be closed
// after adding the new source. Otherwise, the dialog stays open in order to add another source to the federation.
async function addDataSource(close) {
    resetTips();
    allFields.removeClass('ui-state-error');
    let valid = checkLength(name, 'name', 2, 169) && checkLength(URL, 'URL', 6, 100);
    //valid = valid && checkRegexp(name, /^[a-z]([0-9a-z_\s])+$/i, 'Data source should consist of a-z, 0-9, underscores, spaces and must begin with a letter.' );
    //valid = valid && checkRegexp( URL, emailRegex, 'eg. ui@jquery.com' );
    if (valid) {
        let data = new FormData();
        data.append('fed', federation);
        data.append('name', name.val());
        data.append('url', URL.val());
        data.append('dstype', ds_type.val());
        data.append('keywords', keywords.val());
        data.append('params', params.val());
        data.append('types', types.val());
        data.append('desc', desc.val());
        data.append('version', version.val());
        data.append('homepage', homepage.val());
        data.append('organization', organization.val());
        valid = await fetch('/federation/addsource', {
                method: 'POST',
                headers: { Accept: 'application/json' },
                body: data
            })
            .then(res => res.json())
            .then(data => {
                if (data.status >= 0) { manage(federation) }
                else {
                    close = false;
                    updateTips('Error while adding data source to the federation!');
                    return false;
                }
                table.clear().draw();
                table.ajax.url('/federation/datasources?graph=' + federation).load();
                if (close) { addSourceModal.modal('hide') }
                return true;
            })
            .catch(err => console.log(err));
    }
    return valid;
}

// Submits the data to add a new data source, keeps the dialog open, and resets the form elements after a
// successful request so that the user can add a second data source without reopening the dialog.
function saveAndMore() {
    addDataSource(false).then(res => {
        if (res) {
            addSourceForm[0].reset();
            allFields.removeClass('ui-state-error');
        }
    });
}

// Validates the form elements in the edit data source dialog and sends the request for updating the source
// using the FedSDM API. On success, the dialog is closes. On fail, an error message will be shown.
function updateDS(close) {
    resetTips();
    allFieldsEdit.removeClass('ui-state-error');
    let eid = selectedSource[0][0];
    const valid = checkLength(edit_name, 'name', 2, 169) && checkLength(edit_URL, 'URL', 6, 100);
    if (valid) {
        table.row('.selected').remove().draw(false);
        table.row.add([eid, edit_name.val(), edit_URL.val(), edit_ds_type.val(), edit_keywords.val(), edit_homepage.val(), edit_organization.val(), edit_desc.val(), edit_version.val(), edit_params.val(),]).draw(false);
        button_edit_source.prop('disabled', true);
        button_remove_source.prop('disabled', true);

        let data = new FormData();
        data.append('fed', federation);
        data.append('id', eid);
        data.append('name', edit_name.val().trim());
        data.append('url', edit_URL.val().trim());
        data.append('dstype', edit_ds_type.val().trim());
        data.append('keywords', edit_keywords.val().trim());
        data.append('params', edit_params.val().trim());
        data.append('types', edit_types.val().trim());
        data.append('desc', edit_desc.val().trim());
        data.append('version', edit_version.val().trim());
        data.append('homepage', edit_homepage.val().trim());
        data.append('organization', edit_organization.val().trim());
        fetch('/federation/editsource', {
                method: 'POST',
                headers: { Accept: 'application/json' },
                body: data
            })
            .then(res => res.json())
            .then(data => {
                if (data.status >= 0) { manage(federation); }
                else {
                    close = false;
                    updateTips('Error while editing data source!');
                }
                table.clear().draw();
                table.ajax.url('/federation/datasources?graph=' + federation).load();
                if (close) { editSourceModal.modal('hide') }
            })
            .catch(err => console.log(err));
    }
    return valid;
}

// Sends the request to add a new federation to the FedSDM API. If the parameter 'close' is true, the
// dialog will be closes after the creation of the new federation. Otherwise, the dialog stays open so
// that the user can create another federation without reopening the dialog.
function createNewFederation(close) {
    resetTips();
    const name = fedName.val().toString(),
          desc = fedDesc.val().toString(),
          valid = checkLength(fedName, 'name', 2, 169);
    console.log(name + ' ' + desc);
    if (valid) {
        let data = new FormData();
        data.append('name', name);
        data.append('description', desc);
        data.append('is_public', fedPublic.is(':checked'))
        fetch('/federation/create', {
                method: 'POST',
                headers: { Accept: 'text/plain' },
                body: data
            })
            .then(res => res.text())
            .then(data => {
                console.log(data);
                if (data !== null && data.length > 0) {
                    federation = data;
                    $('#fedName').html(name);
                    // select new federation and go to the 'manage data sources' tab
                    federation = prefix + name.replaceAll(' ', '-');
                    federationList.append('<option value=' + federation + ' selected>' + name + '</option>');
                    showFederations(federation);
                    $('#maincontent a[href="#manage"]').tab('show');
                    // TODO: Update the federation stats table
                    if (close) { fedModal.modal('hide') }
                } else { updateTips('Error while creating the new federation! Please try again later!'); }
            })
            .catch(err => console.log(err));
    }
    return valid;
}
