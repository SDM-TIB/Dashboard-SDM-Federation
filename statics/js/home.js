$(document).ready(function() {

    // get total number of datasets
    /*
    *************************************************************************
    ******* Load statistics about data sources, RDF-MTs and links ***********
    *************************************************************************
    */
    $("#datasummary").hide();
    $("#summaryrow").hide();
    $.ajax({
                type: 'GET',
                headers: {
                    Accept : "application/json"
                },

                url: 'api/metastat',
                data: {'query':'all'},
                dataType: "json",
                crossDomain: true,
                success: function(data, textStatus, jqXHR){
                    $("#totaldatasources").html(data.sources);
                    $("#totalrdfmts").html(data.rdfmts);
                    $("#totallinksbnrdfmts").html(data.links);
                    $("#totalfederations").html(data.federations);
                    console.log(data.federations);
                    $("#summaryrow").show();
                },
                error: function(jqXHR, textStatus, errorThrown){
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                    // $('#tblRes thead tr').remove();
                    // $('#tblRes tbody tr').remove();
                    //infoError("Querying fialed: "+textStatus);

                }
     });


    // $("#rdfmt-dist-chart").empty();
    var datas = [];
    var ctx = $("#rdfmt-dist-chart");
    var myBarChart = null;

    $.ajax({
                type: 'GET',
                headers: {
                    Accept : "application/json"
                },

                url: 'api/dschart',
                data: {"graph": "All"},
                dataType: "json",
                crossDomain: true,
                success: function(data, textStatus, jqXHR){
                    var bardata = {labels:[], rdfmts:[], links:[], properties:[], triples:[]};
                    var i = 0;
                    var addedlabels = [];
                    for (d in data){
                        var r = data[d];
                        bardata.labels.push(r.source);
                        var rdfmts = Math.log10(r.rdfmts);
                        bardata.rdfmts.push(rdfmts);
                        var triples = Math.log10(r.triples);
                        bardata.triples.push(triples);
                        var properties = Math.log10(r.properties);
                        bardata.properties.push(properties);
                        var links = Math.log10(r.links);
                        bardata.links.push(links)
                        i += 1;
                        if (r.source in addedlabels){
                            console.log(r.source  + "was added already" + r.rdfmts);
                        }else{
                            datas.push({label:r.source, value:r.rdfmts});
                            addedlabels.push(r.source);
                        }
                        if (i>9){
                            break;
                        }


                    }
                    if (myBarChart == null){
                        myBarChart = new Chart(ctx, {
                        type: 'horizontalBar',

                        data: {
                            labels: bardata.labels,
                            datasets : [
                                    {
                                        id: 4,
                                        label: "# of Triples (log)",
                                        data: bardata.triples,
                                        borderWidth: 1,
                                        backgroundColor: "#b2ad7f"
                                    },
                                    {
                                        id: 1,
                                        label: "# of RDF-MTs (log)",
                                        data: bardata.rdfmts,
                                        borderWidth: 1,
                                        backgroundColor: "#6b5b95"
                                    },
                                    {
                                        id: 2,
                                        label: "# of Properties (log)",
                                        data: bardata.properties,
                                        borderWidth: 1,
                                        backgroundColor: "#feb236"
                                    },
                                    {
                                        id: 3,
                                        label: "# of Links (log)",
                                        data: bardata.links,
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
                                        fontColor: 'rgb(25, 99, 132)',
                                        boxWidth: 8

                                    }
                                },
                                tooltips: {
                                    callbacks: {
                                        label: function(tooltipItem, data) {
                                            var label = data.datasets[tooltipItem.datasetIndex].label || '';

                                            if (label) {
                                                label = label.substring(0, label.indexOf("("));
                                                label += ': ';
                                            }
                                            var xv = Math.pow(10, tooltipItem.xLabel);
                                            label += Math.round(xv * 100) / 100 ;
                                            return label;
                                        }
                                    }
                                }
                        }
                    });
                    }
                    else{
                    myBarChart.data.labels=[];
                    myBarChart.data.datasets=[];
                    myBarChart.update();

                    myBarChart.data.labels = bardata.labels;
                    myBarChart.data.datasets = [
                                    {
                                        id: 4,
                                        label: "# of Triples (log)",
                                        data: bardata.triples,
                                        borderWidth: 1,
                                        backgroundColor: "#b2ad7f"
                                    },
                                    {
                                        id: 1,
                                        label: "# of RDF-MTs (log)",
                                        data: bardata.rdfmts,
                                        borderWidth: 1,
                                        backgroundColor: "#6b5b95"
                                    },
                                    {
                                        id: 2,
                                        label: "# of Properties (log)",
                                        data: bardata.properties,
                                        borderWidth: 1,
                                        backgroundColor: "#feb236"
                                    },
                                    {
                                        id: 3,
                                        label: "# of Links (log)",
                                        data: bardata.links,
                                        borderWidth: 1,
                                        backgroundColor:"#d64161"
                                    }


                                    ]
                    myBarChart.update();


                }
                console.log(datas);
                Morris.Donut({
                    element: 'ds-dist-chart',
                    data: datas,
                    backgroundColor: '#ccc',
                    labelColor: '#060',
                    colors: colors,
                    resize: true
                });
                $("#datasummary").show();
                },
                error: function(jqXHR, textStatus, errorThrown){
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                }
     });

    var colors = [

        "#622569",
        "#CD5C5C",
        "#800008",
        "#6b5b95",
        "#5b9aa0",
        "#00FF00",
        "#00FFFF",
        "#FFA500",
        "#228B22",
        "#20B2AA",
        "#3CB371",
        "#00CED1",
        "#006400",
        "#2F4F4F",
        "#6b5b95",
        "#feb236",
        "#d64161",
        "#ff7b25",
        "#b2ad7f",
        "#878f99",
        "#86af49",
        "#a2b9bc",
        "#9ACD32",
        "#7B68EE",
        "#8B4513",
        "#FF7F50",
        "#483D8B",
        "#BC8F8F",
        "#808080",
        "#9932CC",
        "#FFA07A",
        "#1E90FF",
        "#191970",
        "#800000",
        "#FFD700",
        "#F4A460",
        "#778899",
        "#AFEEEE",
        "#A0522D",
        "#B8860B",
        "#F08080",
        "#BDB76B",
        "#20B2AA",
        "#D2691E",
        "#BA55D3",
        "#800080",
        "#CD5C5C",
        "#FFB6C1",
        "#FF00FF",
        "#FFEFD5",
        "#ADFF2F",
        "#6B8E23",
        "#66CDAA",
        "#8FBC8F",
        "#AFEEEE",
        "#ADD8E6",
        "#6495ED",
        "#4169E1",
        "#BC8F8F",
        "#F4A460",
        "#DAA520",
        "#B8860B",
        "#CD853F",
        "#D2691E",
        "#8B4513",
        "#A52A2A"

    ];
});