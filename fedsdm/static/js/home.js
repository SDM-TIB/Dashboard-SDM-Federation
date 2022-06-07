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

    window.setFederation = function(dataSources, federations) {
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

            let fedData = {labels: [], sources: [], rdfmts: [], links: [], properties: [], triples: []};
            for (let i in federations) {  // in JavaScript this will return the index and not the element
                let fed = federations[i];
                fedData.labels.push(fed.name);
                fedData.sources.push(Math.log10(fed.sources));
                fedData.rdfmts.push(Math.log10(fed.rdfmts));
                fedData.links.push(Math.log10(fed.links));
                fedData.links.push(Math.log10(fed.properties));
                fedData.triples.push(Math.log10(fed.triples))
            }
            federationSummaryChart = new Chart($("#federation-summary-chart"), {
                type: 'horizontalBar',
                data: {
                    labels: fedData.labels,
                    datasets : [
                        {
                            id: 1,
                            label: "# of Data Sources (log)",
                            data: fedData.sources,
                            borderWidth: 1,
                            backgroundColor: colorNumberSources
                        }, {
                            id: 2,
                            label: "# of Triples (log)",
                            data: fedData.triples,
                            borderWidth: 1,
                            backgroundColor: colorNumberTriples
                        }, {
                            id: 3,
                            label: "# of RDF-MTs (log)",
                            data: fedData.rdfmts,
                            borderWidth: 1,
                            backgroundColor: colorNumberMolecules
                        }, {
                            id: 4,
                            label: "# of Properties (log)",
                            data: fedData.properties,
                            borderWidth: 1,
                            backgroundColor: colorNumberProperties
                        }, {
                            id: 5,
                            label: "# of Links (log)",
                            data: fedData.links,
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
        $("#total-data-sources").html(stats.sources);
        $("#total-rdfmts").html(stats.rdfmts);
        $("#total-links-rdfmts").html(stats.links);
        $("#total-federations").html(stats.federations);
    }
});