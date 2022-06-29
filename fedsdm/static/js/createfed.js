$(function() {
    let federation = null;

    $('#create-new-federation').on('click', function() {
        let name = $('#name').val(),
            desc = $('#description').val();
        console.log(name + ' ' + desc);
        if (name != null && name !== '' && name.length > 0) {
            $.ajax({
                type: 'POST',
                headers: {
                    Accept : 'application/json'
                },
                url: '/federation/create',
                data: {'name': name, 'description': desc},
                crossDomain: true,
                success: function(data) {
                    console.log(data);
                    if (data != null && data.length > 0) {
                        alert('The new data federation was successfully created!');
                        federation = data;
                        $('#new-fed-form').hide();
                    } else {
                        $('#errormsg').html('Error while creating the new federation! Please enter a valid name (var name).')
                    }
                },
                error: function(jqXHR, textStatus) {
                    console.log(jqXHR.status);
                    console.log(jqXHR.responseText);
                    console.log(textStatus);
                }
            });
        }
        if (name == null || name === '' || name.length <= 0) {
            alert('The Name field should not be empty.\nPlease insert a name in the Name field.');
        }
        return false
    });
});
