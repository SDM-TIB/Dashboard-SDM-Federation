$(function() {
    /*
    **********************************************************
    ************ Manage data source Data Table ***************
    **********************************************************
    */
    const federationList = $('#federations-list'),
          button_add_source = $('#addds'),
          button_edit_source = $('#editds'),
          button_remove_source = $('#removeds'),
          button_recompute_mts = $('#recomputemts'),
          button_links = $('#findlinks'),
          button_all_links = $('#findalllinks');
    let federation = federationList.val(),
        statsTable = null,
        sourceStatsChart = null,
        bsloaded = 0,
        table = null,
        selectedSource = null;
    const prefix = 'http://ontario.tib.eu/federation/g/';

    // if federation is set from session, then trigger visualization and management data
    showFederations(federation);

    // Federation list dropdown change event, triggers visualization of statistics and management data
    federationList.on('change', function() {
        federation = $(this).val();
        showFederations(federation);
    });

    let statsTablefed = null;
    window.federationOverview = function(feds) {
        if (statsTablefed == null) {
            statsTablefed = $('#federations-statistics').DataTable({
                order: [[ 1, 'desc' ]],
                responsive: false,
                defaultContent: '-1',
                select: true,
                columnDefs: [
                    { target: 1, render: number_renderer },
                    { target: 2, render: number_renderer },
                    { target: 3, render: number_renderer },
                    { target: 4, render: number_renderer },
                    { target: 5, render: number_renderer }
                ]
            });
        } else {
            statsTablefed.clear().draw();
        }

        for (let i in feds) {  // in JavaScript this will return the index and not the element
            let fed = feds[i],
                rem = [];
            rem.push(fed['name']);
            rem.push(fed['sources']);
            rem.push(fed['triples']);
            rem.push(fed['rdfmts']);
            rem.push(fed['properties']);
            rem.push(fed['links']);
            statsTablefed.row.add(rem).draw(false);
        }
    }

    // check if federation name is set, and show statistics and management data
    function showFederations(federation) {
        if (federation != null && federation !== '') {
            $('#mfedName').html(federation);
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
            if (table != null) {
                table.clear().draw();
            }
            if (statsTable != null) {
                statsTable.clear().draw();
            }
            if (sourceStatsChart != null) {
                sourceStatsChart.data.labels = [];
                sourceStatsChart.data.datasets = [];
                sourceStatsChart.update();
            }
        }
    }

    // if no data source is selected, some action buttons will be disabled
    function set_disabled_prop_ds_buttons(disabled) {
        button_edit_source.prop('disabled', disabled);
        button_remove_source.prop('disabled', disabled);
        button_recompute_mts.prop('disabled', disabled);
    }

    // if no federation is selected, then all action buttons will be disabled
    function disableButtons() {
        button_add_source.prop('disabled', true);
        button_links.prop('disabled', true)
        button_all_links.prop('disabled', true);
        set_disabled_prop_ds_buttons(true)
    }

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
        ]
    }

    // basic statistics and bar chart data
    function basic_stat(fed) {
        if (statsTable == null) {
            // Construct basic statistics table
            statsTable = $('#basic-statistics').DataTable({
                order: [[ 1, 'desc' ]],
                responsive: false,
                defaultContent: '-1',
                columnDefs: [
                    {
                        target: 1,
                        render: number_renderer
                    },
                    {
                        target: 2,
                        render: number_renderer
                    }
                ],
                select: true
            });
        } else {
            statsTable.clear().draw();
        }
        let datas = [];
        $.ajax({
            type: 'GET',
            headers: {
                Accept : 'application/json'
            },
            url: '/federation/stats',
            data: {'graph': fed},
            dataType: 'json',
            crossDomain: true,
            success: function(data) {
                datas = data.data;
                let bardata = {labels:[], rdfmts:[], triples:[]};
                for (const d in datas) {
                    let rem = [];
                    //console.log(datas)
                    rem.push(datas[d].ds);
                    let rdfmts = datas[d].rdfmts;

                    rem.push(rdfmts);
                    let triples = datas[d].triples;
                    if (triples == null) {
                        triples = '-1'
                    }

                    rem.push(triples);
                    statsTable.row.add(rem).draw( false );

                    bardata.labels.push(datas[d].ds);
                    rdfmts = log10(rdfmts);
                    bardata.rdfmts.push(rdfmts);
                    triples = log10(triples);
                    bardata.triples.push(triples);
                }

                if (sourceStatsChart == null) {
                    sourceStatsChart = new Chart($('#sourceStatsChart'), {
                        type: 'bar',
                        data: {
                            labels: bardata.labels,
                            datasets: sourceStatsToBarChart(bardata)
                        },
                        options: chartOptions
                    });
                } else {
                    sourceStatsChart.data.labels = [];
                    sourceStatsChart.data.datasets = [];
                    sourceStatsChart.update();
                    sourceStatsChart.data.labels = bardata.labels;
                    sourceStatsChart.data.datasets = sourceStatsToBarChart(bardata)
                    sourceStatsChart.update();
                }
            },
            error: function(jqXHR, textStatus) {
                console.log(jqXHR.status);
                console.log(jqXHR.responseText);
                console.log(textStatus);
            }
        });
        bsloaded = 1;
    }

    // basic information about data sources in a given federation
    function manage(fed) {
        $('#mfedName').html(fed);
        //Disable buttons before selecting item on the table
        set_disabled_prop_ds_buttons(true);

        //Construct data source management data table
        if (table == null) {
            table = $('#datasources').DataTable({
                order: [[ 1, 'desc' ]],
                responsive: false,
                select: true,
                defaultContent: '<i>Not set</i>',
                columnDefs: [{ target: 0, visible: false, searchable: false }],
                ajax: '/federation/datasources?graph=' + federation + '&dstype=All'
            });
            // Data source table select action
            table.on('select', function(e, dt, type, indexes) {
                selectedSource = table.rows(indexes).data().toArray();
                set_disabled_prop_ds_buttons(false);
            }).on('deselect', function() {
                set_disabled_prop_ds_buttons(true);
                selectedSource = null
            });
        } else {
            table.clear().draw();
            set_disabled_prop_ds_buttons(true);
            table.ajax.url('/federation/datasources?graph=' + fed).load();
        }
        table.on('draw', function() {
            if (table.column(0).data().length > 0) {
                button_links.prop('disabled', false);
            } else {
                button_links.prop('disabled', true);
            }
        });
    }

    // Edit data source click action
    button_edit_source.on('click', function() {
        $('#ename').val(selectedSource[0][1]);
        $('#eURL').val(selectedSource[0][2]);
        $('#edstype').val(selectedSource[0][3]);
        $('#ekeywords').val(selectedSource[0][4].trim());
        $('#ehomepage').val(selectedSource[0][5].trim());
        $('#eorganization').val(selectedSource[0][6].trim());
        $('#elabel').val(selectedSource[0][7].trim());
        $('#eversion').val(selectedSource[0][8].trim());
        $('#eparams').val(selectedSource[0][9].trim());
    });

    //Remove data source click action
    button_remove_source.on('click', function() {
        // delete where {<http://ontario.tib.eu/Federation1/datasource/Ensembl-json> ?p ?o}
        $.ajax({
            type: 'GET',
            headers: {
                Accept : 'application/json'
            },
            url: '/federation/api/removeds',
            data: {'ds': selectedSource[0][0], 'fed': fed},
            dataType: 'json',
            crossDomain: true,
            success: function(data) {
                if (data === true)
                    table.row('.selected').remove().draw(false);
            },
            error: function(jqXHR, textStatus) {
                console.log(jqXHR.status);
                console.log(jqXHR.responseText);
                console.log(textStatus);
            }
        });

        set_disabled_prop_ds_buttons(true);
    });

    // Create Mappings click action
    button_recompute_mts.on('click', function() {
        console.log(selectedSource[0][0]);
        $.ajax({
            type: 'GET',
            headers: {
                Accept : 'application/json'
            },
            url: '/federation/api/recreatemts?fed=' + encodeURIComponent(federation) + '&datasource=' + encodeURIComponent(selectedSource[0][0]),
            data: {'query': 'all'},
            dataType: 'json',
            crossDomain: true,
            success: function(data) {
                if (data != null && data.status === 1) {
                    alert('Recreating RDF-MTs for '+ selectedSource[0][0] + ' is underway ...');
                } else {
                    alert('Cannot start the process. Please check if there are data sources in this federation.');
                }
            },
            error: function(jqXHR, textStatus) {
                console.log(jqXHR.status);
                console.log(jqXHR.responseText);
                console.log(textStatus);
            }
        });
    });

    button_links.on('click', function() {
        console.log(selectedSource[0][0]);
        $.ajax({
            type: 'GET',
            headers: {
                Accept : 'application/json'
            },
            url: '/federation/api/findlinks?fed=' + encodeURIComponent(federation) + '&datasource=' + encodeURIComponent(selectedSource[0][0]),
            data: {'query': 'all'},
            dataType: 'json',
            crossDomain: true,
            success: function(data) {
                if (data != null && data.status === 1) {
                    alert('Finding links in progress ...');
                } else {
                    alert('Cannot start the process. Please check if there are data sources in this federation.');
                }
            },
            error: function(jqXHR, textStatus) {
                console.log(jqXHR.status);
                console.log(jqXHR.responseText);
                console.log(textStatus);
            }
        });
    });

    button_all_links.on('click', function() {
        $.ajax({
            type: 'GET',
            headers: {
                Accept : 'application/json'
            },
            url: '/federation/api/findlinks?fed=' + encodeURIComponent(federation),
            dataType: 'json',
            crossDomain: true,
            success: function(data) {
                if (data != null && data.status === 1) {
                    alert('Finding links in progress ...');
                } else {
                    alert('Cannot start the process. Please check if there are data sources in this federation.');
                }
            },
            error: function(jqXHR, textStatus) {
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
    const fedModal = $('#federationModal'),
          addSourceModal = $('#addSourceModal'),
          editSourceModal = $('#editSourceModal'),
          crnfform = fedModal.find('form'),
          form = addSourceModal.find('form'),
          eform = editSourceModal.find('form'),
          name = $('#name'),
          desc = $('#desc'),
          dstype = $('#dstype'),
          URL = $('#URL'),
          params = $('#params'),
          keywords = $('#keywords'),
          organization = $('#organization'),
          homepage = $('#homepage'),
          version = $('#version'),
          allFields = $([]).add(name).add(desc).add(dstype).add(URL).add(params).add(keywords).add(organization).add(homepage).add(version),
          ename = $('#ename'),
          edesc = $('#edesc'),
          edstype = $('#edstype'),
          eURL = $('#eURL'),
          eparams = $('#eparams'),
          ekeywords = $('#ekeywords'),
          eorganization = $('#eorganization'),
          ehomepage = $('#ehomepage'),
          eversion = $('#eversion'),
          allFieldsEdit = $([]).add(ename).add(edesc).add(edstype).add(eURL).add(eparams).add(ekeywords).add(eorganization).add(ehomepage).add(eversion),
          fedName = $('#namecf'),
          fedDesc = $('#description'),
          allFieldsFed = $([]).add(fedName).add(fedDesc);

    crnfform.on('submit', function(event) {
        event.preventDefault();
        createnewfederation(true);
    });
    fedModal.on('shown.bs.modal', function() {
        fedName.trigger('focus');
    });
    fedModal.on('hidden.bs.modal', function() {
        crnfform[0].reset();
        allFieldsFed.removeClass('ui-state-error');
        resetTips();
    });
    $('#create-fed-btn-create').on('click', function() {
       createnewfederation(true);
    });

    form.on('submit', function(event) {
        event.preventDefault();
        addDataSource(true);
    });
    addSourceModal.on('shown.bs.modal', function() {
        name.trigger('focus');
    });
    addSourceModal.on('hidden.bs.modal', function() {
        form[0].reset();
        allFields.removeClass('ui-state-error');
        resetTips();
    });
    $('#add-source-btn-finish').on('click', function() {
       addDataSource(true);
    });
    $('#add-source-btn-more').on('click', function() {
       saveAndMore();
    });

    eform.on('submit', function(event) {
        event.preventDefault();
        updateDS();
    });
    editSourceModal.on('shown.bs.modal', function() {
        edesc.trigger('focus');
    });
    editSourceModal.on('hidden.bs.modal', function() {
        eform[0].reset();
        allFieldsEdit.removeClass('ui-state-error');
        resetTips();
    });
    $('#edit-source-btn').on('click', function() {
       updateDS();
    });

    function addDataSource(close) {
        resetTips();
        allFields.removeClass('ui-state-error');
        const validName = checkLength(name, 'name', 2, 169);
        const validURL = checkLength(URL, 'URL', 6, 100);
        const valid = validName && validURL;
        //valid = valid && checkRegexp(name, /^[a-z]([0-9a-z_\s])+$/i, 'Data source should consist of a-z, 0-9, underscores, spaces and must begin with a letter.' );
        //valid = valid && checkRegexp( URL, emailRegex, 'eg. ui@jquery.com' );
        if (valid) {
            $.ajax({
                type: 'POST',
                headers: {
                    Accept : 'application/json'
                },
                url: '/federation/addsource?fed=' + federation,
                data: {
                    'name': name.val(),
                    'url': URL.val(),
                    'dstype': dstype.val(),
                    'keywords': keywords.val(),
                    'params': params.val(),
                    'desc': desc.val(),
                    'version': version.val(),
                    'homepage': homepage.val(),
                    'organization': organization.val()
                },
                dataType: 'json',
                crossDomain: true,
                success: function(data) {
                    if (data != null && data.length > 0) {
                        manage(federation);
                    } else {
                        $('#validateTips').html('Error while adding data source to the federation!')
                    }
                    table.clear().draw();
                    table.ajax.url('/federation/datasources?graph=' + federation).load();
                },
                error: function(jqXHR, textStatus) {
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                }
            });
        } else {
            close = false;
            console.log('Invalid data...');
        }
        if (close) {
            addSourceModal.modal('hide');
        }
        return valid;
    }

    function saveAndMore() {
        let valid = addDataSource(false);
        if (valid) {
            form[0].reset();
            allFields.removeClass('ui-state-error');
        }
    }

    function updateDS() {
        resetTips();
        allFieldsEdit.removeClass('ui-state-error');
        let eid = selectedSource[0][0];
        const validName = checkLength(ename, 'name', 2, 169);
        const validURL = checkLength(eURL, 'URL', 6, 100);
        const valid = validName && validURL;
        if (valid) {
            table.row('.selected').remove().draw(false);
            table.row.add([eid, ename.val(), eURL.val(), edstype.val(), ekeywords.val(), ehomepage.val(), eorganization.val(), edesc.val(), eversion.val(), eparams.val(),]).draw(false);
            button_edit_source.prop('disabled', true);
            button_remove_source.prop('disabled', true);
            $('#createmapping').prop('disabled', true);
            $.ajax({
                type: 'POST',
                headers: {
                    Accept : 'application/json'
                },
                url: '/federation/editsource?fed=' + federation,
                data: {
                    'id': eid,
                    'name': ename.val().trim(),
                    'url': eURL.val().trim(),
                    'dstype': edstype.val().trim(),
                    'keywords': ekeywords.val().trim(),
                    'params': eparams.val().trim(),
                    'desc': edesc.val().trim(),
                    'version': eversion.val().trim(),
                    'homepage': ehomepage.val().trim(),
                    'organization': eorganization.val().trim()
                },
                dataType: 'json',
                crossDomain: true,
                success: function(data) {
                    if (data != null && data.length > 0) {
                        manage(federation);
                        console.log(data);
                    } else {
                        $('#validateTips').html('Error while editing data source!')
                    }
                    table.clear().draw();
                    table.ajax.url('/federation/datasources?graph=' + federation).load();
                },
                error: function(jqXHR, textStatus) {
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                }
            });
            editSourceModal.modal('hide');
        }
        return valid;
    }

    function createnewfederation(close) {
        resetTips();
        let name = fedName.val();
        let desc = fedDesc.val();
        console.log(name + ' ' + desc);
        const valid = checkLength(fedName, 'name', 2, 169);
        if (valid) {
            $.ajax({
                type: 'POST',
                headers: {
                    Accept : 'application/json'
                },
                url: '/federation/create',
                data: {'name': name, 'description': desc},
                crossDomain: true,
                success: function(data) {
                    console.log(data);
                    if (data != null && data.length > 0) {
                        federation = data;
                        $('#fedName').html(name);
                        $('#newfedform').hide();
                        // select new federation and go to the 'manage data sources' tab
                        federation = prefix + name;
                        federationList.append('<option value=' + federation + ' selected>' + name + '</option>');
                        showFederations(federation);
                        let aTab = '#manage';
                        if (aTab) {
                            $('#maincontent a[href="' + aTab + '"]').tab('show');
                        }
                    } else {
                        close = false;
                        $('#errormsg').html('Error while creating the new federation! Please enter a valid name (var name).')
                    }
                },
                error: function(jqXHR, textStatus) {
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                }
            });
        } else {
            close = false;
            console.log('Invalid data...');
        }
        if (close) {
            fedModal.modal('hide');
        }
        return valid;
    }
});
