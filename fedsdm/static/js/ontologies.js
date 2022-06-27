$(function() {
    $('#editds').prop('disabled', true);
    $('#removeds').prop('disabled', true);

    var table = $('#dataTables-example').DataTable({
        order: [[1, 'desc']],
        responsive: true,
        select: true
    });

    var stats = $('#basic-statistics').DataTable({
        order: [[1, 'desc']],
        responsive: true,
        select: true
    });

    var dialog, edialog, form,
        // From http://www.whatwg.org/specs/web-apps/current-work/multipage/states-of-the-type-attribute.html#e-mail-state-%28type=email%29
        name = $('#name'),
        label = $('#label'),
        dstype = $('#dstype'),
        URL = $('#URL'),
        params = $('#params'),
        allFields = $([]).add(name).add(label).add(dstype).add(URL).add(params);

    function addUser() {
        var valid = true;
        allFields.removeClass('ui-state-error');

        valid = valid && checkLength(name, 'name', 2, 16);
        valid = valid && checkLength(URL, 'url', 6, 80);
        valid = valid && checkLength(label, 'label', 2, 16);

        valid = valid && checkRegexp(name, /^[a-z]([0-9a-z_\s])+$/i, 'Username may consist of a-z, 0-9, underscores, spaces and must begin with a letter.');
        //valid = valid && checkRegexp(URL, emailRegex, 'eg. ui@jquery.com');

        if (valid) {
            table.row.add([name.val(), label.val(), dstype.val(), URL.val(), params.val()]).draw(false);
            dialog.dialog('close');
        }
        return valid;
    }

    dialog = $('#dialog-form').dialog({
        autoOpen: false,
        height: 600,
        width: 650,
        modal: true,
        classes: {
            'ui-dialog': 'highlight'
        },
        buttons: {
            'Add New Data Source': addUser,
            Cancel: function() {
                dialog.dialog('close');
            }
        },
        close: function() {
            form[0].reset();
            allFields.removeClass('ui-state-error');
        }
    });
    function updateDS() {
        var name = $('#ename'),
            label = $('#elabel'),
            dstype = $('#edstype'),
            URL = $('#eURL'),
            params = $('#eparams'),
            allFields = $([]).add(name).add(label).add(dstype).add(URL).add(params),
            tips = $('.validateTips');

        var valid = true;
        allFields.removeClass('ui-state-error');

        if (valid) {
            table.row('.selected').remove().draw(false);
            table.row.add([name.val(), label.val(), dstype.val(), URL.val(), params.val()]).draw(false);
            $('#editds').prop('disabled', true);
            $('#removeds').prop('disabled', true);
            edialog.dialog('close');
        }
        return valid;
    }

    edialog = $('#editdsdialog').dialog({
        autoOpen: false,
        height: 600,
        width: 650,
        modal: true,
        classes: {
            'ui-dialog': 'highlight'
        },
        buttons: {
            'Update Data Source': updateDS,
            Cancel: function() {
                $('#editds').prop('disabled', true);
                $('#removeds').prop('disabled', true);
                edialog.dialog('close');
            }
        },
        close: function() {
            form[0].reset();
            $('#editds').prop('disabled', true);
            $('#removeds').prop('disabled', true);
            allFields.removeClass('ui-state-error');
        }
    });

    form = dialog.find('form').on('submit', function(event) {
        event.preventDefault();
        addUser();
    });

    $('#addds').on('click', function() {
        dialog.dialog('open');
    });

    var selectedRow = null;
    table
        .on('select', function(e, dt, type, indexes) {
            selectedRow = table.rows(indexes).data().toArray();
            $('#editds').prop('disabled', false);
            $('#removeds').prop('disabled', false);

        } )
        .on('deselect', function(e, dt, type, indexes) {
            var rowData = table.rows(indexes).data().toArray();
            $('#editds').prop('disabled', true);
            $('#removeds').prop('disabled', true);
            selectedRow = null
        } );

    $('#editds').on('click', function() {
        console.log(selectedRow[0][0]);
        $('#ename').val(selectedRow[0][0]);
        $('#elabel').val(selectedRow[0][1]);
        $('#edstype').val(selectedRow[0][2]);
        $('#eURL').val(selectedRow[0][3]);
        $('#eparams').val(selectedRow[0][4]);
        edialog.dialog('open');

    });

    $('#removeds').on('click', function () {
        table.row('.selected').remove().draw(false);
        $('#editds').prop('disabled', true);
        $('#removeds').prop('disabled', true);
    });
});