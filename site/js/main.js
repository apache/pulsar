const scrollOffset = 90;

const tocifyOptions = {
  context: '.docs-container article',
  selectors: 'h2,h3,h4',
  smoothScroll: true,
  scrollTo: 90,
  extendPage: true,
  theme: 'jqueryui',
  hashGenerator: function(txt, elem) {
    return txt.replace(/ /g, '').replace(/\n/g, '') + "-" + Math.random().toString(36).substring(7);
  }
}

function restApiTabs() {
  var allCards = $('.card-text');
  allCards.hide();

  $('.card').click(function() {
    var tile = $(this).children('.card-text');
    tile.show();
    $(this).click(function() {
      allCards.hide();
    });
  });
}

function tableOfContents() {
  $("#toc").tocify(tocifyOptions);
}

function popovers() {
  $('[data-toggle="popover"]').popover({
    html: true,
    placement: 'top'
  });

  $('.popover-dismiss').popover({
    trigger: 'focus'
  });
}

$(function() {
  popovers();
  tableOfContents();
  restApiTabs();
});
