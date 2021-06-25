$(document).ready(function() {
        var table = 0;
        var nodes = [],
            links = [];
        $("#selectfederation").prop("disabled", true);
        var federation = null;
         $("#resultinfo").hide();
         $("#resstatus").hide();
         $("#queryrow").hide();
         $("#resultrow").hide();
        $.ajax({
                type: 'GET',
                headers: {
                    Accept : "application/json"
                },
                url: 'api/federations',
                data: {'query':'all'},
                dataType: "json",
                crossDomain: true,
                success: function(data, textStatus, jqXHR){

                    html = '<option value="All" selected>Please select federation</option>'
                    for (f in data){
                        html += '<option value="'+data[f]+'">'+data[f]+'</option>'
                    }
                    html += '<option value="All">All</option> '
                    $("#selectfederation").html(html);
                    $("#selectfederation").prop("disabled", false);
                },
                error: function(jqXHR, textStatus, errorThrown){
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                }
            });

        $("#selectfederation").change(function(){
            console.log($( this ).val());
            fed = $(this).val();
            federation = fed;
            $("#queryrow").show();
            $("#resultrow").hide();
        });

        //finally, initialize YASQE
        var yasqe = YASQE(document.getElementById("yasqe"), {
            // display full query
            viewportMargin: Infinity,
            // grey edit window during query execution
            backdrop: 99,
            // modify codemirror tab handling to solely use 2 spaces
            tabSize: 2,
            indentUnit: 2,
            extraKeys: {
             Tab: function (cm) {
                var spaces = new Array(cm.getOption("indentUnit") + 1).join(" ");
                cm.replaceSelection(spaces);
              }
            },
            sparql: {
                showQueryButton: true,
                endpoint: "sparql"
            }


        });

        YASQE.defaults.sparql.showQueryButton = true;
        YASQE.defaults.value = "SELECT DISTINCT ?concept\n WHERE {\n\t?s a ?concept\n} LIMIT 10";
        YASQE.defaults.sparql.endpoint = "sparql";
        yasqe.options.sparql.callbacks.beforeSend = function(jqXHR, setting){
                console.log('federation to query from',federation);
                $("#resstatus").hide();
                setting.url = "sparql?federation=" +federation + "&query=" + encodeURIComponent(yasqe.getValue());
                setting.crossDomain = true;
                setting.data ={"query": yasqe.getValue()};
                $("#resultinfo").hide();
                $('#dataTables-example').empty();
        }

        var start=false, end = true;
        var vars = [], query=encodeURIComponent(yasqe.getValue());

        yasqe.options.sparql.callbacks.success =  function(data){
            console.log('success');
            console.log("get values for:", yasqe.getValue());
            $("#results").empty();
            $("#results").append('<table width="100%" class="table display table-striped table-bordered table-hover" id="dataTables-example"></table>')
            console.log('data:', data);

            if ('error' in data){
                $("#resstatus").html("Error:" + data.error);
                $("#resultrow").show();
                $("#resstatus").show();
                $("#resultinfo").show();
                return true
            }

            $("#frestime").html(" " + data.firstResult + " sec");
            $("#exetime").html(" " + data.execTime + " sec");
            console.log(data.execTime);

            query=encodeURIComponent(yasqe.getValue());

            var results = data.result;
            if (results.length > 0){
               $("#resstatus").hide();
               $("#resultinfo").show();
               $("#resultrow").show();

                vars = data.vars;
                var theader = "<thead><tr>";
                for (var i=0; i<vars.length;i++){
                    theader =  theader + "<th>" + vars[i] + "</th> ";
                }
                $("#dataTables-example").append(theader + "</tr></thead> ");


                table =  $('#dataTables-example').DataTable({
                    responsive: true,
                    select: true,
                    lengthMenu: [ [10, 25, 50, -1], [10, 25, 50, "All"] ],
                    dom: 'Blfrtip',
                    buttons: [
                         {
                         text:'copy'
                         },
                         {
                         text:'csv',
                         extend: 'csvHtml5',
                         title: 'sparql-results'
                         },
                         {
                         text:'excel',
                         extend: 'excelHtml5',
                         title: 'sparql-results'
                         },
                         {
                         text:'pdf',
                         extend: 'pdfHtml5',
                         title: 'sparql-results'
                         },
                         {
                         text: 'TSV',
                         extend: 'csvHtml5',
                         fieldSeparator: '\t',
                         extension: '.tsv',
                         title: 'sparql-results'
                         }
                    ]
                });

                for (var i=0; i<results.length; i++){
                    var row=results[i];
                    var rowml = [];
                    for (var j=0; j <vars.length; j++){
                        var val = row[vars[j]];
                        if (val.indexOf("^^<") != -1){
                            val = val.substring(0, val.indexOf("^^"));
                        }
                        if('http' == val.substring(0, 4) ){
                            rowml.push("<a href=\"" + val + "\"> &lt;" + val + "&gt;</a>");
                        }else{
                            rowml.push(val);
                        }
                    }
                    table.row.add(rowml).draw( false );
                }

            }else{
                $("#resstatus").html("No results found!");
                $("#resstatus").show();
                $("#resultinfo").show();
                $("#resultrow").show();
                response = false;
                return true;
            }
            console.log("calling getnext");
            response = true;
            show_incremental();
        }; // end of sparql success callback function

        var response = false;
        function show_incremental(){
            if(response == true){
                // This makes it unable to send a new request
                // unless you get response from last request
                response = false;
                var req = $.ajax({
                    type:"get",
                    url: "nextresult",
                    // headers: {Accept: "text/csv"},//ask for csv. Simple, and uses less bandwidth
                    success: function(data) {
                        var row=data.result;
                        console.log('row', row);
                        if (row.length == 0 || row == "EOF"){
                            console.log("loop done");
                            $("#exetime").html(" " + data.execTime + " sec");
                            response = false;
                            return
                        }
                        $("#exetime").html(" " + data.execTime + " sec");
                        var rowml = [];
                        for (var j=0; j <vars.length; j++){
                            var val = row[vars[j]];
                            if (val.indexOf("^^<") != -1){
                                val = val.substring(0, val.indexOf("^^"));
                            }
                            if('http' == val.substring(0, 4) ){

                                rowml.push("<a href=\"" + val + "\"> &lt;" + val + "&gt;</a>");
                            }else{
                                rowml.push(val);
                            }
                        }
                        table.row.add(rowml).draw( false );
                        response = true;
                    }
//                    ,
//                        error: function(jqXHR, textStatus, errorThrown){
//                            console.log(jqXHR.status);
//                            console.log(jqXHR.responseText);
//                            console.log(textStatus);
//                        }
                });
                    req.done(function(){
                        console.log("Request successful!");
                        // This makes it able to send new request on the next interval
                        if (response)
                            {response = true;
                              show_incremental()
                            }
                    });

            }

            // setTimeout(,1000);
        }


        /**
         * We use most of the default settings for the property and class autocompletion types. This includes:
         * -  the pre/post processing of tokens
         * -  detecting whether we are in a valid autocompletion position
         * -  caching of the suggestion list. These are cached for a period of a month on the client side.
         */

        var getAutocompletionsArrayFromJson = function(result) {
            var completionsArray = [];
            console.log('parsing');
            console.log(result);
            result.forEach(function(row) {//remove first line, as this one contains the projection variable
            console.log(row);
                if ('type' in row)
                    completionsArray.push(row['type']);//remove quotes
                else{
                    completionsArray.push(row['property']);//remove quotes
                }
            });
            return completionsArray;
        }


        var customPropertyCompleter = function(yasqe) {
            //we use several functions from the regular property autocompleter (this way, we don't have to re-define code such as determining whether we are in a valid autocompletion position)
            var returnObj = {
                isValidCompletionPosition: function(){return YASQE.Autocompleters.properties.isValidCompletionPosition(yasqe)},
                preProcessToken: function(token) {return YASQE.Autocompleters.properties.preProcessToken(yasqe, token)},
                postProcessToken: function(token, suggestedString) {return YASQE.Autocompleters.properties.postProcessToken(yasqe, token, suggestedString)}
            };

            //In this case we assume the properties will fit in memory. So, turn on bulk loading, which will make autocompleting a lot faster
            returnObj.bulk = true;
            returnObj.async = true;

            //and, as everything is in memory, enable autoShowing the completions
            returnObj.autoShow = true;

            returnObj.persistent = "customProperties";//this will store the sparql results in the client-cache for a month.
            returnObj.get = function(token, callback) {
                //all we need from these parameters is the last one: the callback to pass the array of completions to
                var sparqlQuery = "SELECT DISTINCT ?property WHERE {?s ?property ?obj } limit 1000";
                $.ajax({
                    data: {query: sparqlQuery},
                    url: YASQE.defaults.sparql.endpoint,
                    // headers: {Accept: "text/csv"},//ask for csv. Simple, and uses less bandwidth
                    success: function(data) {
                        console.log(sparqlQuery);
                        console.log(data);
                        callback(getAutocompletionsArrayFromJson(data.result));
                    }
                });
            };
            return returnObj;
        };
        //now register our new autocompleter
        YASQE.registerAutocompleter('customPropertyCompleter', customPropertyCompleter);


        //excellent, now do the same for the classes
        var customClassCompleter = function(yasqe) {
            var returnObj = {
                isValidCompletionPosition: function(){return YASQE.Autocompleters.classes.isValidCompletionPosition(yasqe)},
                preProcessToken: function(token) {return YASQE.Autocompleters.classes.preProcessToken(yasqe, token)},
                postProcessToken: function(token, suggestedString) {return YASQE.Autocompleters.classes.postProcessToken(yasqe, token, suggestedString)}
            };
            returnObj.bulk = true;
            returnObj.async = true;
            returnObj.autoShow = true;
            returnObj.get = function(token, callback) {
                var filters ='filter (!regex(str(?type), "http://www.w3.org/ns/sparql-service-description", "i") && ' +
                     ' !regex(str(?type), "http://www.openlinksw.com/schemas/virtrdf#", "i") && ' +
                     ' !regex(str(?type), "http://www.w3.org/2000/01/rdf-schema#", "i") && ' +
                     ' !regex(str(?type), "http://www.w3.org/1999/02/22-rdf-syntax-ns#", "i") && ' +
                     ' !regex(str(?type), "http://purl.org/dc/terms/Dataset", "i") && ' +
                     ' !regex(str(?type), "http://www.w3.org/2002/07/owl#", "i") && ' +
                     ' !regex(str(?type), "http://rdfs.org/ns/void#", "i") && ' +
                     ' !regex(str(?type), "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/", "i") && '+
                     ' !regex(str(?type), "nodeID://", "i") ) '
                var sparqlQuery = "SELECT DISTINCT ?type WHERE{ ?s a ?type. " + filters + " } Limit 1000";
                $.ajax({
                    data: {query: sparqlQuery},
                    url: YASQE.defaults.sparql.endpoint,
                    // headers: {Accept: "text/csv"},//ask for csv. Simple, and uses less bandwidth
                    success: function(data) {
                        console.log(sparqlQuery);
                        console.log(data);
                        callback(getAutocompletionsArrayFromJson(data.result));
                    }
                });
            };
            return returnObj;
        };
        YASQE.registerAutocompleter('customClassCompleter', customClassCompleter);

        //And, to make sure we don't use the other property and class autocompleters, overwrite the default enabled completers
        YASQE.defaults.autocompleters = ['customClassCompleter', 'customPropertyCompleter'];


        var iasisq = "PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n "+
                        "PREFIX iasis:    <http://project-iasis.eu/vocab/>\n "+
                        "PREFIX   owl:                 <http://www.w3.org/2002/07/owl#>\n "+
                        "PREFIX drugbank: <http://bio2rdf.org/drugbank_vocabulary:>          \n "+
                        "SELECT DISTINCT ?mutation ?transcript ?proteinName ?drug ?drug1\n "+
                        "WHERE {        \n "+
                        "     ?mutation    rdf:type                              iasis:Mutation .\n "+
                        "     ?mutation    iasis:mutation_somatic_status        'Confirmed somatic variant'.\n "+
                        "     ?mutation    iasis:mutation_isLocatedIn_transcript ?transcript .\n "+
                        "     ?transcript  iasis:translates_as                    ?protein .      \n "+
                        "      ?drug        iasis:drug_interactsWith_protein       ?protein .\n "+
                        "     ?protein     iasis:label                            ?proteinName .\n "+
                        "     ?drug        iasis:label                            'docetaxel' .\n "+
                        "     ?drug        iasis:externalLink                             ?drug1 .\n "+
                        "      ?drug1         drugbank:transporter     ?transporter .\n "+
                        "      ?transporter   drugbank:gene-name       ?proteinName .\n "+
                        "} LIMIT 10"

        var bouncerquery =
                            "SELECT * WHERE {\n "+
                            "    ?s <http://project-iasis.eu/vocab/egfr_mutated> ?egfrmut .\n "+
                            "    ?s <http://project-iasis.eu/vocab/gender> ?gender.\n "+
                            "    ?s <http://project-iasis.eu/vocab/smoking> \"1\"^^<http://www.w3.org/2001/XMLSchema#integer>  .\n "+
                            "    ?s <http://project-iasis.eu/vocab/stage> ?stage.\n "+
                            "    ?s <http://project-iasis.eu/vocab/biopsy> ?biopsy .\n "+
                            "    ?biopsy <http://project-iasis.eu/vocab/mutation_aa> ?mutation .\n "+
                            "    ?biopsy <http://project-iasis.eu/vocab/resultsDate> ?resultdate.\n "+
                            "    ?cmut <http://project-iasis.eu/vocab/mutation_cds> ?mutcds .\n "+
                            "    ?cmut <http://project-iasis.eu/vocab/mutation_aa> ?mutation .\n "+
                            "    ?cmut <http://project-iasis.eu/vocab/is_located> ?gene. \n "+
                            "    ?gene <http://project-iasis.eu/vocab/gene_name> \"EGFR\" .\n "+
                            "    ?gene <http://project-iasis.eu/vocab/accession_number> ?acc_num .\n "+
                            "    } \n "+
                            "    LIMIT 10"
        $("#people").click(function(){
            yasqe.setValue(iasisq);
        });
        $("#lungcancer").click(function(){
            yasqe.setValue(bouncerquery);
        });


 });