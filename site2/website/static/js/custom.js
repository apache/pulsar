// Turn off ESLint for this file because it's sent down to users as-is.
/* eslint-disable */
window.addEventListener('load', function() {

  // setup apache menu items in nav bar
  const community = document.querySelector("a[href='#community']").parentNode;
  const communityMenu = 
    '<li>' +
    '<a id="community-menu" href="#">Community</a>' + 
    '<div id="community-dropdown" class="hide">' +
      '<ul id="community-dropdown-items">' +
        '<li><a href="/contact">Contant</a></li>' +
        '<li><a href="/events">Events</a></li>' +
        '<li><a href="https://twitter.com/Apache_Pulsar">Twitter</a></li>' +
        '<li><a href="https://github.com/apache/incubator-pulsar/wiki">Wiki</a></li>' +
        '<li><a href="https://github.com/apache/incubator-pulsar/issues">Issue tracking</a></li>' +
        '<li><a href="/resources">Resources</a></li>' +
        '<li><a href="/team">Team</a></li>' +
      '</ul>' +
    '</div>' +
    '</li>';

  community.innerHTML = communityMenu;

  const communityMenuItem = document.getElementById("community-menu");
  const communityDropDown = document.getElementById("community-dropdown");
  communityMenuItem.addEventListener("click", function(event) {
    event.preventDefault();

    if (communityDropDown.className == 'hide') {
      communityDropDown.className = 'visible';
    } else {
      communityDropDown.className = 'hide';
    }
  });

  // setup apache menu items in nav bar
  const apache = document.querySelector("a[href='#apache']").parentNode;
  const apacheMenu = 
    '<li>' +
    '<a id="apache-menu" href="#">Apache</a>' + 
    '<div id="apache-dropdown" class="hide">' +
      '<ul id="apache-dropdown-items">' +
        '<li><a href="https://www.apache.org/">Foundation</a></li>' +
        '<li><a href="https://www.apache.org/licenses/">License</a></li>' +
        '<li><a href="https://www.apache.org/foundation/sponsorship.html">Sponsorship</a></li>' +
        '<li><a href="https://www.apache.org/foundation/thanks.html">Thanks</a></li>' +
        '<li><a href="https://www.apache.org/security">Security</a></li>' +
      '</ul>' +
    '</div>' +
    '</li>';

  apache.innerHTML = apacheMenu;

  const apacheMenuItem = document.getElementById("apache-menu");
  const apacheDropDown = document.getElementById("apache-dropdown");
  apacheMenuItem.addEventListener("click", function(event) {
    event.preventDefault();

    if (apacheDropDown.className == 'hide') {
      apacheDropDown.className = 'visible';
    } else {
      apacheDropDown.className = 'hide';
    }
  }); 

});
