$(document).ready(function() {
    /*
    **********************************************************
    ************ Manage data source Data Table ***************
    **********************************************************
    */
    let federation = $("#federations-list").val();

    let statsTable = null;
    let ctx = $("#myChart");
    let myBarChart = null;
    let bsloaded = 0;
    let prefix = 'http://ontario.tib.eu/federation/g/';

    // if federation is set from session, then trigger visualization and management data
    showFederations(federation);

    // Federation list dropdown change event, triggers visualization of statistics and management data
    $("#federations-list").change(function() {
        federation = $(this).val();
        showFederations(federation);
    });

    let statsTablefed = null;
    window.federationOverview = function(feds, nf, nd, nl, nm, np, nk) {
        if (statsTablefed == null) {
            statsTablefed = $("#federations-statistics").DataTable({
                order: [[ 1, 'desc' ]],
                responsive: false,
                defaultContent: "-1",
                select: true
            });
        } else {
            statsTablefed.clear().draw();
        }
        var datas = [];
        $.ajax({
            type: 'GET',
            headers: {
                Accept : "application/json"
            },
            url: '/federation/stats',
            data: {"graph": federation},
            dataType: "json",
            crossDomain: true,
            success: function(data, textStatus, jqXHR) {
                datas = feds;
                var rdata = {labels:[], rdfmts:[], triples:[]};
                for (d in nf) {
                    rem = [];
                    rem.push(nf[d]);
                    rem.push(nd[d]);
                    rem.push(nl[d]);
                    rem.push(nm[d]);
                    rem.push(np[d]);
                    rem.push(nk[d]);
                    statsTablefed.row.add(rem).draw(false);
                }
            },
            error: function(jqXHR, textStatus, errorThrown) {
                console.log(jqXHR.status);
                console.log(jqXHR.responseText);
                console.log(textStatus);
            }
        });
    }

    // check if federation name is set, and show statistics and management data
    function showFederations(federation) {
        if (federation != null && federation !== "") {
            $("#mfedName").html(federation);
            basic_stat(federation);
            manage(federation);
            if (federation !== "All"){
                $("#addds").prop("disabled", false);
                $("#findalllinks").prop("disabled", false);
            } else {
                $("#addds").prop("disabled", true);
                $("#findalllinks").prop("disabled", true);
            }
        } else {
            disableButtons();
        }
    }

    // if no federation is selected, then all action buttons will be disabled
    function disableButtons(){
        $("#addds").prop("disabled", true);
        $("#editds").prop("disabled", true);
        $("#removeds").prop("disabled", true);
        $("#recomputemts").prop("disabled", true);
        $("#findlinks").prop("disabled", true)
        $("#findalllinks").prop("disabled", true);
    }

    // basic statistics and bar chart data
    function basic_stat(fed) {
        if (statsTable == null) {
            // Construct basic statistics table
            statsTable = $("#basic-statistics").DataTable({
                order: [[ 1, 'desc' ]],
                responsive: false,
                defaultContent: "-1",
                select: true
            });
        } else {
            statsTable.clear().draw();
        }
        let datas = [];
        $.ajax({
            type: 'GET',
            headers: {
                Accept : "application/json"
            },
            url: '/federation/stats',
            data: {"graph": fed},
            dataType: "json",
            crossDomain: true,
            success: function(data, textStatus, jqXHR) {
                datas = data.data;
                datasdon = [];
                let bardata = {labels:[], rdfmts:[], triples:[]};
                for (d in datas) {
                    rem = [];
                    //console.log(datas)
                    rem.push(datas[d].ds);
                    let rdfmts = datas[d].rdfmts;

                    rem.push(rdfmts);
                    let triples = datas[d].triples;
                    if (triples == null) {
                        triples = "-1"
                    }

                    rem.push(triples);
                    statsTable.row.add(rem).draw( false );

                    bardata.labels.push(datas[d].ds);
                    rdfmts = Math.log10(rdfmts);
                    bardata.rdfmts.push(rdfmts);
                    triples = Math.log10(triples);
                    bardata.triples.push(triples);
                }

                if (myBarChart == null) {
                    myBarChart = new Chart(ctx, {
                        type: 'horizontalBar',
                        data: {
                            labels: bardata.labels,
                            datasets :[
                                {
                                    id: 1,
                                    label: "# of RDF-MTs (log)",
                                    data: bardata.rdfmts,
                                    borderWidth: 1,
                                    backgroundColor: '#6b5b95',
                                }, {
                                    id: 2,
                                    label: "# of Triples(log)",
                                    data: bardata.triples,
                                    borderWidth: 1,
                                    backgroundColor: '#d64161',
                                }]
                        },
                        options: {
                            scales: {
                                yAxes: [{
                                    ticks: {
                                        beginAtZero:true
                                    },
                                    gridLines: {
                                        offsetGridLines: true
                                    }
                                }],
                                xAxes: [{
                                    gridLines: {
                                        offsetGridLines: true
                                    }
                                }]
                            },
                            legend: {
                                display: true,
                                labels: {
                                    fontColor: 'rgb(25, 99, 132)',
                                    boxWidth: 8

                                }
                            },
                            tooltips: {
                                callbacks: {
                                    label: function(tooltipItem, data) {
                                        let label = data.datasets[tooltipItem.datasetIndex].label || '';
                                        if (label) {
                                            label = label.substring(0, label.indexOf("("));
                                            label += ': ';
                                        }
                                        let xv = Math.pow(10, tooltipItem.xLabel);
                                        label += Math.round(xv * 100) / 100 ;
                                        return label;
                                    }
                                }
                            }
                        }
                    });
                } else {
                    myBarChart.data.labels = [];
                    myBarChart.data.datasets = [];
                    myBarChart.update();

                    myBarChart.data.labels = bardata.labels;
                    myBarChart.data.datasets = [
                        {
                            id: 1,
                            label: "# of RDF-MTs (log)",
                            data: bardata.rdfmts,
                            borderWidth: 1,
                            backgroundColor: '#6b5b95',
                        }, {
                            id: 2,
                            label: "# of Triples(log)",
                            data: bardata.triples,
                            borderWidth: 1,
                            backgroundColor: '#d64161',
                        }]

                    myBarChart.update();
                }
            },
            error: function(jqXHR, textStatus, errorThrown) {
                console.log(jqXHR.status);
                console.log(jqXHR.responseText);
                console.log(textStatus);
            }
        });
        bsloaded = 1;
    }

    var table;
    let selectedSource = null;

    // basic information about data sources in a given federation
    function manage(fed) {
        $("#mfedName").html(fed);
        //Disable buttons before selecting item on the table
        $("#editds").prop("disabled", true);
        $("#removeds").prop("disabled", true);
        $("#recomputemts").prop("disabled", true);

        //Construct data source management data table
        if (table == null) {
            mngloaded = 1;
            table = $('#datasources').DataTable({
                order: [[ 1, 'desc' ]],
                responsive: true,
                select: true,
                defaultContent: "<i>Not set</i>",
                ajax: "/federation/datasources?graph=" + federation + "&dstype=All"
            });
            // Dat source table select action
            table.on('select', function(e, dt, type, indexes) {
                selectedSource = table.rows(indexes).data().toArray();
                $("#editds").prop("disabled", false);
                $("#removeds").prop("disabled", false);
                $("#recomputemts").prop("disabled", false);
            }).on('deselect', function(e, dt, type, indexes) {
                let rowData = table.rows(indexes).data().toArray();
                $("#editds" ).prop("disabled", true);
                $("#removeds" ).prop("disabled", true);
                $("#recomputemts" ).prop("disabled", true);
                selectedSource = null
            });
        } else {
            table.clear().draw();
            $("#editds").prop("disabled", true);
            $("#removeds").prop("disabled", true);
            $("#recomputemts").prop("disabled", true);
            table.ajax.url("/federation/datasources?graph=" + fed).load();
        }
        table.on('draw', function() {
            if (table.column(0).data().length > 0) {
                $("#findlinks").prop("disabled", false);
            } else {
                $("#findlinks").prop("disabled", true);
            }
        });
    }

    // Add data source click action
    $("#addds").click(function() {
        dialog.dialog("open");
    });

    // Edit data source click action
    $("#editds").click(function() {
        $("#ename").val(selectedSource[0][1]);
        $("#eURL").val(selectedSource[0][2]);
        $("#edstype").val(selectedSource[0][3]);
        $("#ekeywords").val(selectedSource[0][4].trim());
        $("#ehomepage").val(selectedSource[0][5].trim());
        $("#eorganization").val(selectedSource[0][6].trim());
        $("#elabel").val(selectedSource[0][7].trim());
        $("#eversion").val(selectedSource[0][8].trim());
        $("#eparams").val(selectedSource[0][9].trim());
        edialog.dialog('open');
    });

    //Remove data source click action
    $('#removeds').click(function() {
        // delete where {<http://ontario.tib.eu/Federation1/datasource/Ensembl-json> ?p ?o}
        $.ajax({
            type: 'GET',
            headers: {
                Accept : "application/json"
            },
            url: '/federation/api/removeds',
            data: {'ds':selectedSource[0][0], 'fed': fed},
            dataType: "json",
            crossDomain: true,
            success: function(data, textStatus, jqXHR) {
                if (data == true)
                    table.row('.selected').remove().draw(false);
            },
            error: function(jqXHR, textStatus, errorThrown) {
                console.log(jqXHR.status);
                console.log(jqXHR.responseText);
                console.log(textStatus);
            }
        });

        $("#editds").prop("disabled", true);
        $("#removeds").prop("disabled", true);
        $("#recomputemts").prop("disabled", true);
    });

    // Create Mappings click action
    $('#recomputemts').click(function() {
        console.log(selectedSource[0][0]);
        $.ajax({
            type: 'GET',
            headers: {
                Accept : "application/json"
            },
            url: '/federation/api/recreatemts?fed=' + encodeURIComponent(federation) +"&datasource=" + encodeURIComponent(selectedSource[0][0]),
            data: {'query':'all'},
            dataType: "json",
            crossDomain: true,
            success: function(data, textStatus, jqXHR) {
                if (data != null && data.status == 1) {
                    alert("Recreating RDF-MTs for "+ selectedSource[0][0] + " is underway ...");
                } else {
                    alert("Cannot start the process. Please check if there are data sources in this federation.");
                }
            },
            error: function(jqXHR, textStatus, errorThrown) {
                console.log(jqXHR.status);
                console.log(jqXHR.responseText);
                console.log(textStatus);
            }
        });
    });

    $('#findlinks').click(function() {
        console.log(selectedSource[0][0]);
        $.ajax({
            type: 'GET',
            headers: {
                Accept : "application/json"
            },
            url: '/federation/api/findlinks?fed=' + encodeURIComponent(federation) +"&datasource=" + encodeURIComponent(selectedSource[0][0]),
            data: {'query':'all'},
            dataType: "json",
            crossDomain: true,
            success: function(data, textStatus, jqXHR) {
                if (data != null && data.status === 1) {
                    alert("Finding links in progress ...");
                } else {
                    alert("Cannot start the process. Please check if there are data sources in this federation.");
                }
            },
            error: function(jqXHR, textStatus, errorThrown) {
                console.log(jqXHR.status);
                console.log(jqXHR.responseText);
                console.log(textStatus);
            }
        });
    });

    $('#findalllinks').click(function() {
        $.ajax({
            type: 'GET',
            headers: {
                Accept : "application/json"
            },
            url: '/federation/api/findlinks?fed=' + encodeURIComponent(federation),
            dataType: "json",
            crossDomain: true,
            success: function(data, textStatus, jqXHR) {
                if (data != null && data.status == 1) {
                    alert("Finding links in progress ...");
                } else {
                    alert("Cannot start the process. Please check if there are data sources in this federation.");
                }
            },
            error: function(jqXHR, textStatus, errorThrown) {
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
    let dialog, edialog, form, eform,
        // From http://www.whatwg.org/specs/web-apps/current-work/multipage/states-of-the-type-attribute.html#e-mail-state-%28type=email%29
        name = $("#name"),
        desc = $("#desc"),
        dstype = $("#dstype"),
        URL = $("#URL"),
        params = $("#params"),
        keywords = $("#keywords"),
        organization = $("#organization"),
        homepage = $("#homepage"),
        version = $("#version"),
        allFields = $([]).add(name).add(desc).add(dstype).add(URL).add(params).add(keywords).add(organization).add(homepage).add(version),
        tips = $(".validateTips"),
        crnfdialog;

    dialog = $("#add-form").dialog({
        autoOpen: false,
        height: 800,
        width: 550,
        modal: true,
        classes: {
            "ui-dialog": "ui-corner-all"
        },
        buttons: [{
            text: "Finish",
            click:addDataSource,
            class: "btn btn-success"
        },{
            text: "Continue",
            click:saveAndMore,
            class: "btn btn-primary"
        },{
            text: "Cancel",
            click: function() { dialog.dialog("close"); },
            class: "btn btn-danger"
        }],
        close: function() {
            form[0].reset();
            allFields.removeClass("ui-state-error" );
        }
    });

    form = dialog.find("form").on("submit", function(event) {
        event.preventDefault();
        addDataSource(true);
    });

    $("#CreateNewFed").click(function() {
        crnfdialog.dialog("open");
    });
    $("#AddFed").click(function() {
        crnfdialog.dialog("open");
    });

    crnfdialog = $("#my-form").dialog({
        autoOpen: false,
        height: 550,
        width: 550,
        modal: true,
        classes: {
            "ui-dialog": "ui-corner-all"
        },
        buttons: [{
            text: "Create",
            click: createnewfederation,
            class: "btn btn-success"
        },{
            text: "Cancel",
            click: function() { crnfdialog.dialog("close"); },
            class: "btn btn-danger"
        }],
        close: function() {
            form[0].reset();
            allFields.removeClass("ui-state-error");
        }
    });

    function addDataSource(close) {
        let valid = true;
        allFields.removeClass("ui-state-error");
        valid = valid && checkLength(name, "name", 2, 169);
        valid = valid && checkLength(URL, "url", 6, 100);
        //valid = valid && checkRegexp(name, /^[a-z]([0-9a-z_\s])+$/i, "Data source should consist of a-z, 0-9, underscores, spaces and must begin with a letter." );
        //valid = valid && checkRegexp( URL, emailRegex, "eg. ui@jquery.com" );
        if (valid) {
            $.ajax({
                type: 'POST',
                headers: {
                    Accept : "application/json"
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
                dataType: "json",
                crossDomain: true,
                success: function(data, textStatus, jqXHR) {
                    if (data != null && data.length > 0) {
                        manage(federation);
                    } else {
                        $('#validateTips').html("Error while adding data source to the federation!")
                    }
                    table.clear().draw();
                    table.ajax.url("/federation/datasources?graph=" + federation).load();
                },
                error: function(jqXHR, textStatus, errorThrown) {
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                }
            });
        } else {
            close = false;
            name.addClass("ui-state-error");
            URL.addClass("ui-state-error");
            console.log("Invalid data...");
        }
        if (close) {
            dialog.dialog("close");
        }
        return valid;
    }

    function saveAndMore() {
        let valid = addDataSource(false);
        if (valid) {
            form[0].reset();
            allFields.removeClass("ui-state-error");
        }
    }

    edialog = $("#editdsdialog").dialog({
        autoOpen: false,
        height: 800,
        width: 700,
        modal: true,
        classes: {
            "ui-dialog": "ui-corner-all"
        },
        buttons: [{
            text: "Update Data Source",
            click: updateDS,
            class: "btn btn-success"
        },{
            text: "Cancel",
            click: function() { edialog.dialog("close"); },
            class: "btn btn-danger"
        }],
        close: function() {
            eform[0].reset();
            allFields.removeClass("ui-state-error");
        }
    });

    eform = edialog.find("form").on("submit", function(event) {
        event.preventDefault();
        updateDS();
    });

    function updateDS() {
        let ename = $("#ename"),
            edesc = $("#elabel"),
            edstype = $("#edstype"),
            eURL = $("#eURL"),
            eparams = $("#eparams"),
            ekeywords = $("#ekeywords"),
            eorganization = $("#eorganization"),
            ehomepage = $("#ehomepage"),
            eversion = $("#eversion"),
            eid = selectedSource[0][0],
            allFields = $([]).add(name).add(desc).add(dstype).add(URL).add(params).add(keywords).add(organization).add(homepage).add(version),
            tips = $(".validateTips");
        let valid = true;
        allFields.removeClass("ui-state-error");
        valid = valid && checkLength(ename, "name", 2, 169);
        valid = valid && checkLength(eURL, "url", 6, 100);
        if (valid) {
            table.row('.selected').remove().draw(false);
            table.row.add([eid, ename.val(), eURL.val(), edstype.val(), ekeywords.val(), ehomepage.val(), eorganization.val(), edesc.val(), eversion.val(), eparams.val(),]).draw(false);
            $("#editds").prop("disabled", true);
            $("#removeds").prop("disabled", true);
            $("#createmapping").prop("disabled", true);
            $.ajax({
                type: 'POST',
                headers: {
                    Accept : "application/json"
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
                dataType: "json",
                crossDomain: true,
                success: function(data, textStatus, jqXHR) {
                    if (data != null && data.length > 0) {
                        manage(federation);
                        console.log(data);
                    } else {
                        $('#validateTips').html("Error while editing data source!")
                    }
                    table.clear().draw();
                    table.ajax.url("/federation/datasources?graph=" + federation).load();
                },
                error: function(jqXHR, textStatus, errorThrown) {
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                }
            });
            edialog.dialog("close");
        }
        return valid;
    }

    function updateTips(t) {
        tips.text(t).addClass("ui-state-highlight");
        setTimeout(function() {
            tips.removeClass("ui-state-highlight", 1500);
        }, 500 );
    }

    function checkLength(o, n, min, max) {
        if (o.val().length > max || o.val().length < min) {
            o.addClass("ui-state-error");
            updateTips("Length of " + n + " must be between " + min + " and " + max + "." );
            return false;
        } else {
            return true;
        }
    }

    function checkRegexp(o, regexp, n) {
        if (!regexp.test(o.val())) {
            o.addClass("ui-state-error");
            updateTips(n);
            return false;
        } else {
            return true;
        }
    }

    function createnewfederation() {
        let name = $('#namecf').val();
        let desc = $('#description').val();
        console.log(name + " " + desc);
        if (name != null && name !== '' && name.length > 0) {
            $.ajax({
                type: 'POST',
                headers: {
                    Accept : "application/json"
                },
                url: '/federation/create',
                data: {'name': name, 'description': desc},
                crossDomain: true,
                success: function(data, textStatus, jqXHR) {
                    console.log(data);
                    if (data != null && data.length > 0) {
                        federation = data;
                        $("#fedName").html(name);
                        $('#newfedform').hide();
                        crnfdialog.dialog( "close" );
                        // select new federation and go to the 'manage data sources' tab
                        federation = prefix + name;
                        $("#federations-list").append('<option value=' + federation + ' selected>' + name + '</option>');
                        showFederations(federation);
                        let aTab = '#manage';
                        if (aTab) {
                            $('#maincontent a[href="' + aTab + '"]').tab('show');
                        }
                    } else {
                        $('#errormsg').html("Error while creating the new federation! Please enter a valid name (var name).")
                    }
                },
                error: function(jqXHR, textStatus, errorThrown) {
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
    }
});
