$(document).ready(function() {

    /**
     * TODO: Refactor with intent toward pure functions. Mutation of state can lead to bugs and difficult debugging.
     */

    var toc = navData.toc;
    var flatToc = navData.flatToc.reverse();

    function collectNodes(tocMap) {
        var tocNodes = {};
        tocMap.map(function(node, index) {
            var sectionNode = $('#' + node.section);
            var tocSubsections = {};
            tocItem = {section: sectionNode};
            var subsectionNodes;
            if (node.subsections) {
                subsectionNodes = (collectNodes(node.subsections));
                tocItem.subsections = subsectionNodes;
            }
            tocNodes[node.section] = tocItem;
        });
        return tocNodes;
    }
    var tocItems = collectNodes(toc);

    function collectNodesFlat(tocMap, obj) {
        var collect = obj || {};
        tocMap.map(function(node, index) {
            var sectionNode = $('#' + node.section);
            tocItem = {section: sectionNode};
            if (node.subsections) {
                subsectionNodes = (collectNodesFlat(node.subsections, collect));
            }
            collect[node.section] = sectionNode;
        });
        return collect;
    }
    var tocFlat = collectNodesFlat(toc);

    var prevSectionToken;
    var prevSubsectionToken;
    var activeTokensObj = {};

    function scrollActions(scrollPosition) {
        var activeSection = checkNodePositions(toc, tocFlat, scrollPosition);
        var activeSubSection,
            prevL1Nav,
            currL1Nav,
            prevL2Nav,
            currL2Nav;

        // No active section - return existing activeTokensObj (may be empty)
        if (!activeSection) {
            return activeTokensObj;
        }

        /**
         * This block deals with L1Nav sections
         */

        // If no previous token, set previous to current active and show L1Nav
        if (!prevSectionToken) {
            prevSectionToken = activeSection.token;
            currL1Nav = getNavNode(activeSection.token);
            currL1Nav.show('fast');
        } 
        // If active is not the same as previous, hide previous L1Nav and show current L1Nav; set previous to current
        else if (activeSection.token !== prevSectionToken) {
            prevL1Nav = getNavNode(prevSectionToken);
            currL1Nav = getNavNode(activeSection.token);
            prevL1Nav.hide('fast');
            currL1Nav.show('fast');
            prevSectionToken = activeSection.token;
        }

        /**
         * This block deals with L2Nav subsections
         */

        // If there is a subsections array and it has a non-zero length, set active subsection
        if (activeSection.subsections && activeSection.subsections.length !== 0) {
            activeSubSection = checkNodePositions(activeSection.subsections, tocFlat, scrollPosition);
            if (activeSubSection) {
                if (!prevSubsectionToken) {
                    prevSubsectionToken = activeSubSection.token;
                    currL2Nav = getNavNode(activeSubSection.token);
                    currL2Nav.show('fast');
                } else if (activeSubSection.token !== prevSubsectionToken) {
                    prevL2Nav = getNavNode(prevSubsectionToken);
                    currL2Nav = getNavNode(activeSubSection.token);
                    prevL2Nav.hide('fast');
                    currL2Nav.show('fast');
                    prevSubsectionToken = activeSubSection.token;
                }
            } else {
                prevL2Nav = getNavNode(prevSubsectionToken);
                prevL2Nav.hide('fast');
                prevSubsectionToken = null;
            }
        }
        activeTokensObj.L1 = prevSectionToken;
        activeTokensObj.L2 = prevSubsectionToken;
        return activeTokensObj;
    }

    /**
     * Checks for active elements by scroll position
     */

    var prevElemToken;
    var activeElemToken;

    function checkActiveElement(items, scrollPosition) {
        var offset = 50;
        var offsetScroll = scrollPosition + offset;
        var visibleNode;
        for (var i = 0; i < items.length; i++) {
            var token = items[i];
            var node = getHeadingNode(token);
            if (offsetScroll >= node.offset().top) {
                activeElemToken = token;
            }
        }
        if (!prevElemToken) {
            getNavElemNode(activeElemToken).addClass('selected');
            prevElemToken = activeElemToken;
            return;
        }
        if (activeElemToken !== prevElemToken) {
            getNavElemNode(prevElemToken).removeClass('selected');
            getNavElemNode(activeElemToken).addClass('selected');
            prevElemToken = activeElemToken;
        }
        return activeElemToken;
    }

    function getHeadingNode(token) {
        return $('#' + token);
    }

    function getNavNode(token) {
        return $('#' + token + '-nav');
    }

    function getNavElemNode(token) {
        return $('#sidebar-wrapper > ul a[href="#' + token + '"]');
    }

    function checkNodePositions(nodes, flatNodeMap, scrollPosition) {
        var activeNode;
        for (var i = 0; i < nodes.length; i++) {
            var item = nodes[i];
            var node = flatNodeMap[item.section];
            var nodeTop = node.offset().top - 50;
            if (scrollPosition >= nodeTop) {
                activeNode = {token: item.section, node: node};

                if (item.subsections) {
                    activeNode.subsections = item.subsections;
                }
                break;
            }
        }
        return activeNode;
    }

    function scrollToNav(token) {
        setTimeout(function() {
            var scrollPosition = $(window).scrollTop();
            var activeSectionTokens = scrollActions(scrollPosition);
            var activeElemToken = checkActiveElement(flatToc, scrollPosition);
            var navNode = $('#sidebar-wrapper > ul a[href="#' + token + '"]');
            $('#sidebar-wrapper').scrollTo(navNode, {duration: 'fast', axis: 'y'});
        }, 200);
    }

    $(window).on('hashchange', function(event) {
        var scrollPosition = $(window).scrollTop();
        var activeSectionTokens = scrollActions(scrollPosition);
        var activeElemToken = checkActiveElement(flatToc, scrollPosition);
        var scrollToken = activeSectionTokens.L2 ? activeSectionTokens.L2 : activeSectionTokens.L1;
        scrollToNav(scrollToken);
        var token = location.hash.slice(1);
    });

    var scrollPosition = $(window).scrollTop();
    scrollActions(scrollPosition);
    checkActiveElement(flatToc, scrollPosition);
    // TODO: prevent scroll on sidebar from propogating to window
    $(window).on('scroll', function(event) {
        var scrollPosition = $(window).scrollTop();
        var activeSectionTokens = scrollActions(scrollPosition);
        var activeElemToken = checkActiveElement(flatToc, scrollPosition);
    });
});
