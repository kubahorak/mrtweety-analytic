
function exec() {

    var hashtagList = $('#hashtag-list');

    $.getJSON( "analytic.json", function( data ) {

        var items = data.items.map(function (item) {
            return item.hashtag;
        });

        hashtagList.empty();

        var content = "";
        for (i = 0; i < items.length; ++i) {
            var item = items[i];
            var link = "<a href='https://twitter.com/hashtag/" + item + "'>" + item + "</a>";
            content += "<li class='list-group-item'>" + link + "</li>";
        }
        
        if (items.length) {
            var list = $('<ul />').addClass('list-group').html(content);
            hashtagList.append(list);
        }

    });

    setTimeout(exec, 5000);
}

$(document).ready(function() {
    exec();
    setTimeout(exec, 5000);
});
