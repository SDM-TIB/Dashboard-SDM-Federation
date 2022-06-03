$(document).ready(function() {
    /*
    *************************************************************************
    ******* Load statistics about data sources, RDF-MTs and links ***********
    *************************************************************************
    */
    $("#data-summary").hide();
    $("#summaryrow").hide();
    $("#contentrow").hide();
    var datas = [];
    let dataSummaryChart = null,
        federationSummaryChart = null;
    var nfeds = [];
    var nds = [];
    var ntpl = [];
    var nftm = [];
    var nprp = [];
    var nlnk = [];

    const tooltip = {
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

    window.setFederation = function(federations, nf, nd, nl, nm, np, nk) {
        if (federations != null) {
            $("#summaryrow").show();
            $('#contentrow').show();

            let barData = {labels: [], rdfmts: [], links: [], properties: [], triples: []},
                addedLabels = [];
            for (let i in federations) {
                let r = federations[i];
                barData.labels.push(r.source);
                barData.rdfmts.push(Math.log10(r.rdfmts));
                barData.triples.push(Math.log10(r.triples));
                barData.properties.push(Math.log10(r.properties));
                barData.links.push(Math.log10(r.links))
                if (r.source in addedLabels) {
                    console.log(r.source  + "was added already" + r.rdfmts);
                } else {
                    datas.push({label: r.source, value: r.rdfmts});
                    addedLabels.push(r.source);
                }
                if (i > 9) {
                    break;
                }
            }
            if (dataSummaryChart == null) {
                dataSummaryChart = new Chart($("#data-summary-chart"), {
                    type: 'horizontalBar',
                    data: {
                        labels: barData.labels,
                        datasets : [
                            {
                                id: 4,
                                label: "# of Triples (log)",
                                data: barData.triples,
                                borderWidth: 1,
                                backgroundColor: "#b2ad7f"
                            }, {
                                id: 1,
                                label: "# of RDF-MTs (log)",
                                data: barData.rdfmts,
                                borderWidth: 1,
                                backgroundColor: "#6b5b95"
                            }, {
                                id: 2,
                                label: "# of Properties (log)",
                                data: barData.properties,
                                borderWidth: 1,
                                backgroundColor: "#feb236"
                            }, {
                                id: 3,
                                label: "# of Links (log)",
                                data: barData.links,
                                borderWidth: 1,
                                backgroundColor:"#d64161"
                            }
                        ]
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
                            }]
                        },
                        legend: {
                            display: true,
                            title: "Summary of sample data sources",
                            labels: {
                                fontColor: "rgb(25, 99, 132)",
                                boxWidth: 8
                            }
                        },
                        tooltips: tooltip
                    }
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
                                label: "# of data sources (log)",
                                data: nds,
                                borderWidth: 1,
                                backgroundColor: "#169649"
                            }, {
                                id: 2,
                                label: "# of Triples (log)",
                                data: ntpl,
                                borderWidth: 1,
                                backgroundColor: "#b2ad7f"
                            }, {
                                id: 3,
                                label: "# of RDF-MTs (log)",
                                data: nftm,
                                borderWidth: 1,
                                backgroundColor: "#6b5b95"
                            }, {
                                id: 4,
                                label: "# of Properties (log)",
                                data: nprp,
                                borderWidth: 1,
                                backgroundColor: "#feb236"
                            }, {
                                id: 5,
                                label: "# of Links (log)",
                                data: nlnk,
                                borderWidth: 1,
                                backgroundColor:"#d64161"
                            }
                        ]
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
                        },
                        legend: {
                            display: true,
                            title: "Summary of sample data sources",
                            labels: {
                                fontColor: "rgb(25, 99, 132)",
                                boxWidth: 8

                            }
                        },
                        tooltips: tooltip
                    }
                });
            } else {
                dataSummaryChart.data.labels = [];
                dataSummaryChart.data.datasets = [];
                dataSummaryChart.update();

                dataSummaryChart.data.labels = barData.labels;
                dataSummaryChart.data.datasets = [
                    {
                        id: 4,
                        label: "# of Triples (log)",
                        data: barData.triples,
                        borderWidth: 1,
                        backgroundColor: "#b2ad7f"
                    }, {
                        id: 1,
                        label: "# of RDF-MTs (log)",
                        data: barData.rdfmts,
                        borderWidth: 1,
                        backgroundColor: "#6b5b95"
                    }, {
                        id: 2,
                        label: "# of Properties (log)",
                        data: barData.properties,
                        borderWidth: 1,
                        backgroundColor: "#feb236"
                    }, {
                        id: 3,
                        label: "# of Links (log)",
                        data: barData.links,
                        borderWidth: 1,
                        backgroundColor:"#d64161"
                    }
                ]
                dataSummaryChart.update();
                federationSummaryChart.update();
            }
            Morris.Donut({
                element: 'ds-dist-chart',
                data: datas,
                backgroundColor: "#ccc",
                labelColor: "#060",
                colors: colors,
                resize: true
            });
        }
        $("#data-summary").show();
    }

    window.setStats = function(stats) {
        $("#totaldatasources").html(stats.sources);
        $("#totalrdfmts").html(stats.rdfmts);
        $("#totallinksbnrdfmts").html(stats.links);
        $("#totalfederations").html(stats.federations);
    }
});