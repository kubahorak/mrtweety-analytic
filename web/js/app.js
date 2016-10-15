
function exec() {

    var hashtagList = $('#hashtag-list');

    $.getJSON( "analytic.json", function( data ) {

        var items = data.items.map(function (item) {
            return item.hashtag;
        });

        hashtagList.empty();

        if (items.length) {
            var content = '<li>' + items.join('</li><li>') + '</li>';
            var list = $('<ul />').html(content);
            hashtagList.append(list);
        }

    });

    setTimeout(exec, 5000);
}

$(document).ready(function() {
    exec();
    setTimeout(exec, 5000);
});
