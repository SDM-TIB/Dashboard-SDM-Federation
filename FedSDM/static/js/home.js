/*!
 * ---------------------------------------------------------------------------------------------
 * FedSDM: home.js
 * Loads statistics about the federations, data sources, RDF-MTs, etc. and creates the bar charts
 * ---------------------------------------------------------------------------------------------
 */

$(function() {
    const federationSummary = $('#federation-summary'),
          dataSummary = $('#data-summary'),
          summaryRow =$('#summary-row'),
          contentRow = $('#content-row');
    federationSummary.hide();
    dataSummary.hide();
    summaryRow.hide();
    contentRow.hide();
    let dataSummaryChart = null,
        federationSummaryChart = null;

    // Given the statistics about the data sources as well as the federations,
    // this function populates the bar charts of the landing page.
    // It is called from the HTML template since that is the place where the data is available.
    // The chart options defined in utils.js as well as colors defined in colors.js are used.
    // The surrounding HTML needs to import those scripts first.
    window.setFederation = function(dataSources, federations) {
        if (dataSources != null && federations != null) {
            summaryRow.show();
            contentRow.show();
            federationSummary.show();
            dataSummary.show();

            let dsData = { labels: [], rdfmts: [], links: [], properties: [], triples: [] };
            for (let i in dataSources) {  // in JavaScript this will return the index and not the element
                let ds = dataSources[i];
                dsData.labels.push(ds.source);
                dsData.rdfmts.push(log10(ds.rdfmts));
                dsData.triples.push(log10(ds.triples));
                dsData.properties.push(log10(ds.properties));
                dsData.links.push(log10(ds.links));
                if (i > 9) { break }
            }
            dataSummary.height(62 + 110 * dsData.labels.length);
            dataSummaryChart = new Chart($('#data-summary-chart'), {
                type: 'bar',
                data: {
                    labels: dsData.labels,
                    datasets : [
                        {
                            id: 1,
                            label: '# of Triples (log)',
                            data: dsData.triples,
                            borderWidth: 1,
                            backgroundColor: colorNumberTriples
                        }, {
                            id: 2,
                            label: '# of RDF-MTs (log)',
                            data: dsData.rdfmts,
                            borderWidth: 1,
                            backgroundColor: colorNumberMolecules
                        }, {
                            id: 3,
                            label: '# of Properties (log)',
                            data: dsData.properties,
                            borderWidth: 1,
                            backgroundColor: colorNumberProperties
                        }, {
                            id: 4,
                            label: '# of Links (log)',
                            data: dsData.links,
                            borderWidth: 1,
                            backgroundColor: colorNumberLinks
                        }
                    ]
                },
                options: chartOptions
            });

            let fedData = { labels: [], sources: [], rdfmts: [], links: [], properties: [], triples: [] };
            for (let i in federations) {  // in JavaScript this will return the index and not the element
                let fed = federations[i];
                fedData.labels.push(fed.name);
                fedData.sources.push(log10(fed.sources));
                fedData.rdfmts.push(log10(fed.rdfmts));
                fedData.links.push(log10(fed.links));
                fedData.properties.push(log10(fed.properties));
                fedData.triples.push(log10(fed.triples));
            }
            federationSummary.height(62 + 130 * fedData.labels.length);
            federationSummaryChart = new Chart($('#federation-summary-chart'), {
                type: 'bar',
                data: {
                    labels: fedData.labels,
                    datasets : [
                        {
                            id: 1,
                            label: '# of Data Sources (log)',
                            data: fedData.sources,
                            borderWidth: 1,
                            backgroundColor: colorNumberSources
                        }, {
                            id: 2,
                            label: '# of Triples (log)',
                            data: fedData.triples,
                            borderWidth: 1,
                            backgroundColor: colorNumberTriples
                        }, {
                            id: 3,
                            label: '# of RDF-MTs (log)',
                            data: fedData.rdfmts,
                            borderWidth: 1,
                            backgroundColor: colorNumberMolecules
                        }, {
                            id: 4,
                            label: '# of Properties (log)',
                            data: fedData.properties,
                            borderWidth: 1,
                            backgroundColor: colorNumberProperties
                        }, {
                            id: 5,
                            label: '# of Links (log)',
                            data: fedData.links,
                            borderWidth: 1,
                            backgroundColor: colorNumberLinks
                        }
                    ]
                },
                options: chartOptions
            });
        }
    }

    // Given the main statistics about the federations, this function populates the cards of the landing page.
    // It is called from the HTML template since that is the place where the data is available.
    window.setStats = function(stats) {
        $('#total-data-sources').html(stats.sources);
        $('#total-rdfmts').html(stats.rdfmts);
        $('#total-links-rdfmts').html(stats.links);
        $('#total-federations').html(stats.federations);
    }
});
