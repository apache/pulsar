/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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

function sidebarExpand() {
  function openAccordionElement(el) {
    var selectedElement = `#${el}`;
    $(selectedElement).collapse('show');
    $('aside.sidebar-nav').scrollTo(selectedElement);
  }

  function addElement(el) {
    LS['pulsar-sidebar-selected'] = el;
  }

  function clearElements() {
    LS['pulsar-sidebar-selected'] = null;
  }

  var LS = window.localStorage;
  var selected = LS['pulsar-sidebar-selected'] || null;

  console.log("Initial selected: " + selected);

  if (selected != null) {
    openAccordionElement(selected);
  }

  var tabs = $("[id^='collapse']");

  tabs.on('shown.bs.collapse', function() {
    addElement(this.id);
  });

  tabs.on('hidden.bs.collapse', function() {
    clearElements();
  });
}

$(function() {
  sidebarExpand();
  popovers();
  tableOfContents();
  restApiTabs();
});
