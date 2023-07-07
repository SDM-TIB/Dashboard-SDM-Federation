/*!
 * --------------------------------------------------------------
 * FedSDM: colors.js
 * Provides constants with colors used throughout the application
 * --------------------------------------------------------------
 */

// A list with many colors. It is meant to be used in charts, so that the n-th dataset always uses the same color.
const colors = [
    '#9ACD32',
    '#7B68EE',
    '#622569',
    '#CD5C5C',
    '#800008',
    '#6b5b95',
    '#5b9aa0',
    '#00FF00',
    '#00FFFF',
    '#FFA500',
    '#228B22',
    '#20B2AA',
    '#3CB371',
    '#00CED1',
    '#006400',
    '#2F4F4F',
    '#8B4513',
    '#FF7F50',
    '#483D8B',
    '#BC8F8F',
    '#808080',
    '#9932CC',
    '#FFA07A',
    '#1E90FF',
    '#191970',
    '#800000',
    '#FFD700',
    '#F4A460',
    '#778899',
    '#AFEEEE',
    '#A0522D',
    '#B8860B',
    '#F08080',
    '#BDB76B',
    '#20B2AA',
    '#D2691E',
    '#BA55D3',
    '#800080',
    '#CD5C5C',
    '#FFB6C1',
    '#FF00FF',
    '#FFEFD5',
    '#ADFF2F',
    '#6B8E23',
    '#66CDAA',
    '#8FBC8F',
    '#AFEEEE',
    '#ADD8E6',
    '#6495ED',
    '#4169E1',
    '#BC8F8F',
    '#F4A460',
    '#DAA520',
    '#B8860B',
    '#CD853F',
    '#D2691E',
    '#8B4513',
    '#A52A2A'
];

// The color to be used for labels of charts.
const colorChartLabels = '#196384';

// The color to be used for the associated dataset in the statistics bar charts.
const colorNumberTriples = '#B2AD7F',
      colorNumberMolecules = '#6B5B95',
      colorNumberProperties = '#FEB236',
      colorNumberLinks = '#D64161',
      colorNumberSources = '#169649';

// Returns the color at position idx in the list of colors. If idx exceeds the limit, #CCC will be returned.
function color(idx) {
    if (idx > colors.length - 1) { return  '#CCC' }
    return colors[idx];
}
