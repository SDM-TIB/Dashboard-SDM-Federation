$(document).ready(function() {
    /*
    *************************************************************************
    ******* Load statistics about data sources, RDF-MTs and links ***********
    *************************************************************************
    */
    $("#federation-summary").hide();
    $("#data-summary").hide();
    $("#summary-row").hide();
    $("#content-row").hide();
    let dataSummaryChart = null,
        federationSummaryChart = null;
    var nfeds = [];
    var nds = [];
    var ntpl = [];
    var nftm = [];
    var nprp = [];
    var nlnk = [];

    const chartOptions = {
        scales: {
            yAxes: [{
                ticks: {
                    beginAtZero:true
                },
                gridLines: {
                    offsetGridLines: true
                }
            }]
        },
        legend: {
            display: true,
            labels: {
                fontColor: colorChartLabels,
                boxWidth: 8
            }
        },
        tooltips: {
            callbacks: {
                label: function(tooltipItem, data) {
                    let label = data.datasets[tooltipItem.datasetIndex].label || "";
                    if (label) {
                        label = label.substring(0, label.indexOf("(") - 1);
                        label += ': ';
                    }
                    label += Math.round(Math.pow(10, tooltipItem.xLabel) * 100) / 100 ;
                    return label;
                }
            }
        }
    }

    window.setFederation = function(dataSources, nf, nd, nl, nm, np, nk) {
        if (dataSources != null) {
            $("#summary-row").show();
            $('#content-row').show();

            let dsData = {labels: [], rdfmts: [], links: [], properties: [], triples: []};
            for (let i in dataSources) {  // in JavaScript this will return the index and not the element
                let ds = dataSources[i];
                dsData.labels.push(ds.source);
                dsData.rdfmts.push(Math.log10(ds.rdfmts));
                dsData.triples.push(Math.log10(ds.triples));
                dsData.properties.push(Math.log10(ds.properties));
                dsData.links.push(Math.log10(ds.links))
                if (i > 9) {
                    break;
                }
            }
            dataSummaryChart = new Chart($("#data-summary-chart"), {
                type: 'horizontalBar',
                data: {
                    labels: dsData.labels,
                    datasets : [
                        {
                            id: 1,
                            label: "# of Triples (log)",
                            data: dsData.triples,
                            borderWidth: 1,
                            backgroundColor: colorNumberTriples
                        }, {
                            id: 2,
                            label: "# of RDF-MTs (log)",
                            data: dsData.rdfmts,
                            borderWidth: 1,
                            backgroundColor: colorNumberMolecules
                        }, {
                            id: 3,
                            label: "# of Properties (log)",
                            data: dsData.properties,
                            borderWidth: 1,
                            backgroundColor: colorNumberProperties
                        }, {
                            id: 4,
                            label: "# of Links (log)",
                            data: dsData.links,
                            borderWidth: 1,
                            backgroundColor: colorNumberLinks
                        }
                    ]
                },
                options: chartOptions
            });

            nfeds = nf;
            nds = nd;
            for(let [i, v] of nds.entries()) {
                let bb = parseInt(v);
                nds[i] = Math.log10(bb);
            }
            ntpl = nl;
            for(let [i, v] of ntpl.entries()) {
                let bb = parseInt(v);
                ntpl[i] = Math.log10(bb);
            }
            nftm = nm;
            for(let [i, v] of nftm.entries()) {
                let bb = parseInt(v);
                nftm[i] = Math.log10(bb);
            }
            nprp = np;
            for(let [i, v] of nprp.entries()) {
                let bb = parseInt(v);
                nprp[i] = Math.log10(bb);
            }
            nlnk = nk;
            for(let [i, v] of nlnk.entries()) {
                let bb = parseInt(v);
                nlnk[i] = Math.log10(bb);
            }
            federationSummaryChart = new Chart($("#federation-summary-chart"), {
                type: 'horizontalBar',
                data: {
                    labels: nfeds,
                    datasets : [
                        {
                            id: 1,
                            label: "# of Data Sources (log)",
                            data: nds,
                            borderWidth: 1,
                            backgroundColor: colorNumberSources
                        }, {
                            id: 2,
                            label: "# of Triples (log)",
                            data: ntpl,
                            borderWidth: 1,
                            backgroundColor: colorNumberTriples
                        }, {
                            id: 3,
                            label: "# of RDF-MTs (log)",
                            data: nftm,
                            borderWidth: 1,
                            backgroundColor: colorNumberMolecules
                        }, {
                            id: 4,
                            label: "# of Properties (log)",
                            data: nprp,
                            borderWidth: 1,
                            backgroundColor: colorNumberProperties
                        }, {
                            id: 5,
                            label: "# of Links (log)",
                            data: nlnk,
                            borderWidth: 1,
                            backgroundColor: colorNumberLinks
                        }
                    ]
                },
                options: chartOptions
            });
        }
        $("#federation-summary").show();
        $("#data-summary").show();
    }

    window.setStats = function(stats) {
        $("#totaldatasources").html(stats.sources);
        $("#totalrdfmts").html(stats.rdfmts);
        $("#totallinksbnrdfmts").html(stats.links);
        $("#totalfederations").html(stats.federations);
    }
});