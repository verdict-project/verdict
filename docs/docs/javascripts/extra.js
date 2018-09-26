document.addEventListener("DOMContentLoaded", function() {
    var nav = document.getElementsByClassName("md-nav");
    for(var i = 0; i < nav.length; i++) {
        if (nav.item(i).getAttribute("data-md-level")) {
            nav.item(i).style.display = 'block';
            nav.item(i).style.overflow = 'visible';
        }
    }

    var nav = document.getElementsByClassName("md-nav__toggle");
    for(var i = 0; i < nav.length; i++) {
       nav.item(i).checked = true;
    }
});
