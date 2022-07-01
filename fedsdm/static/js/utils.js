const chartOptions = {
    indexAxis: 'y',
    scales: {
        y: {
            beginAtZero: true,
            grid: {
                offset: true
            }
        }
    },
    animations: {
        numbers: {
            type: 'number',
            properties: ['x']
        }
    },
    plugins: {
        legend: {
            display: true,
            labels: {
                fontColor: colorChartLabels,
                boxWidth: 12
            }
        },
        tooltip: {
            callbacks: {
                label: function(context) {
                    let label = context.dataset.label || '';
                    if (label) {
                        label = label.substring(0, label.indexOf('(') - 1);
                        label += ': ';
                    }
                    const value = Math.round(Math.pow(10, context.parsed.x) * 100) / 100
                    label += parseInt(value.toString(), 10);
                    return label;
                }
            }
        }
    }
}

let tips = $('.validateTips');

function log10(value) {
    return parseInt(value) === 1 ? 0.1 : Math.log10(value)
}

function numberWithCommas(value) {
    return value.toString().replace(/\B(?<!\.\d*)(?=(\d{3})+(?!\d))/g, ",");
}

function updateTips(t) {
    tips.text(t).addClass('ui-state-highlight');
    setTimeout(function() {
        tips.removeClass('ui-state-highlight', 1500, 'swing');
    }, 500 );
}

function checkLength(o, n, min, max) {
    if (o.val().length > max || o.val().length < min) {
        o.addClass('ui-state-error');
        updateTips('Length of ' + n + ' must be between ' + min + ' and ' + max + '.' );
        return false;
    } else {
        return true;
    }
}

function checkRegexp(o, regexp, n) {
    if (!regexp.test(o.val())) {
        o.addClass('ui-state-error');
        updateTips(n);
        return false;
    } else {
        return true;
    }
}
