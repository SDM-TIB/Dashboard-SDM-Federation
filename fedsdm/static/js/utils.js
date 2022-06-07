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

let tips = $(".validateTips");

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
