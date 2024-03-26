/*!
 * -------------------------------------------------------------------------------
 * FedSDM: utils.js
 * Provides some utility methods and variables; mainly to reduce code duplication.
 * -------------------------------------------------------------------------------
 */

// Options used for bar charts created with chart.js.
// Also takes care of the tooltips.
const chartOptions = {
    indexAxis: 'y',
    maintainAspectRatio: false,
    scales: {
        y: {
            beginAtZero: true,
            grid: { offset: true }
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
            labels: { boxWidth: 12 }
        },
        tooltip: {
            callbacks: {
                label: function(context) {
                    let label = context.dataset.label || '';
                    if (label) { label = label.substring(0, label.indexOf('(') - 1) + ': ' }
                    const value = Math.round(Math.pow(10, context.parsed.x) * 100) / 100;
                    label += numberWithCommas(parseInt(value.toString(), 10));
                    return label;
                }
            }
        }
    }
}

// The buttons of DataTables, like copy and export to CSV, TSV, PDF.
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

// DataTable number renderer to add a thousand separator.
const number_renderer = DataTable.render.number(',', '.', 0);

// The DOM element where error messages from validating the input form will be shown.
let tips = $('.validateTips');

// A function computing log10. However, log10(1) will return 0.1 instead of 0 so that it does not disappear in the charts.
function log10(value) { return parseInt(value) === 1 ? 0.1 : Math.log10(value) }

// Converts any number to its corresponding string value including a comma as thousands separator.
function numberWithCommas(value) { return value.toString().replace(/\B(?<!\.\d*)(?=(\d{3})+(?!\d))/g, ",") }

// Removes the error state from the input form validating error message DOM element and resets its text.
function resetTips() { tips.removeClass('ui-state-error').text('Some fields are required.') }

// Updates the input form validating error message DOM element with the new error message.
function updateTips(t) {
    const current_text = tips.text();
    console.log("current: " + current_text + "\tnew: " + t);
    if (current_text.includes('Some fields are required.')) { tips.text(t) }
    else {
        const str_ary = current_text.split('.');
        const str_set = [...new Set(str_ary)];
        const text = str_set.join('.\n')
        tips.text(text + t);
        tips.html(tips.html().replace(/\n/g,'<br>'));
    }

    tips.addClass('ui-state-highlight');
    tips.addClass('ui-state-error');
    setTimeout(function() { tips.removeClass('ui-state-highlight', 1500, 'swing') }, 500 );
}

// Checks whether the length of the value of o is inbetween min and max.
// n is the input form argument for which the input value's length is checked.
function checkLength(o, n, min, max) {
    if (o.val().length > max || o.val().length < min) {
        o.addClass('ui-state-error');
        updateTips('Length of ' + n + ' must be between ' + min + ' and ' + max + '.');
        return false;
    } else { return true }
}

// Checks whether the input value o of the input form argument n satisfies the regular expression regexp.
function checkRegexp(o, regexp, n) {
    if (!regexp.test(o.val())) {
        o.addClass('ui-state-error');
        updateTips(n);
        return false;
    } else { return true }
}

// Checks whether o is a valid selection option of input form argument n.
// It assumes o is the index of the selected option and that invalid / placeholder have an index value of -1.
function checkSelection(o, n) {
    if (o.val() === '-1') {
        o.addClass('ui-state-error');
        updateTips('Select an option for ' + n + '.');
        return false;
    } else { return true }
}
