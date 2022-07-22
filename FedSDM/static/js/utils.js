const chartOptions = {
    indexAxis: 'y',
    maintainAspectRatio: false,
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
                    label += numberWithCommas(parseInt(value.toString(), 10));
                    return label;
                }
            }
        }
    }
}

function table_buttons(title) {
    return [
        {
            text: 'Copy',
            extend: 'copyHtml5',
            title: title
        }, {
            text: 'Excel',
            extend: 'excelHtml5',
            title: title
        }, {
            text: 'CSV',
            extend: 'csvHtml5',
            title: title
        }, {
            text: 'TSV',
            extend: 'csvHtml5',
            fieldSeparator: '\t',
            extension: '.tsv',
            title: title
        }, {
            text: 'PDF',
            extend: 'pdfHtml5',
            title: title
        }
    ];
}

const number_renderer = DataTable.render.number(',', '.', 0);

let tips = $('.validateTips');

function log10(value) {
    return parseInt(value) === 1 ? 0.1 : Math.log10(value)
}

function numberWithCommas(value) {
    return value.toString().replace(/\B(?<!\.\d*)(?=(\d{3})+(?!\d))/g, ",");
}

function resetTips() {
    tips.removeClass('ui-state-error')
        .text('Some fields are required.');
}

function updateTips(t) {
    const current_text = tips.text()
    console.log("current: " + current_text + "\tnew: " + t);
    if (current_text.includes('Some fields are required.')) {
        tips.text(t);
    } else {
        const str_ary = current_text.split('.');
        const str_set = [...new Set(str_ary)];
        const text = str_set.join('.\n')
        tips.text(text + t);
        tips.html(tips.html().replace(/\n/g,'<br>'))
    }

    tips.addClass('ui-state-highlight');
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

function checkSelection(o, n) {
    if (o.val() === '-1') {
        o.addClass('ui-state-error');
        updateTips('Select an option for ' + n + '.');
        return false;
    } else {
        return true;
    }
}
