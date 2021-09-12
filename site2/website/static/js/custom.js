// Turn off ESLint for this file because it's sent down to users as-is.
/* eslint-disable */

window.addEventListener('load', function () {
    let restApiVersions = null;
    var url = "../../../../swagger/restApiVersions.json";
    var request = new XMLHttpRequest();
    request.open("get", url, false);
    request.send(null);
    restApiVersions = JSON.parse(request.responseText);

    // setup apache menu items in nav bar
    const apache = document.querySelector("a[href='#apache']").parentNode;
    const apacheMenu =
        '<li>' +
        '<a id="apache-menu" href="#">Apache <span style="font-size: 0.75em">&nbsp;▼</span></a>' +
        '<div id="apache-dropdown" class="hide">' +
        '<ul id="apache-dropdown-items">' +
        '<li><a href="https://www.apache.org/" target="_blank" >Foundation &#x2750</a></li>' +
        '<li><a href="https://www.apache.org/licenses/" target="_blank">License &#x2750</a></li>' +
        '<li><a href="https://www.apache.org/foundation/sponsorship.html" target="_blank">Sponsorship &#x2750</a></li>' +
        '<li><a href="https://www.apache.org/foundation/thanks.html" target="_blank">Thanks &#x2750</a></li>' +
        '<li><a href="https://www.apache.org/security" target="_blank">Security &#x2750</a></li>' +
        '</ul>' +
        '</div>' +
        '</li>';

    apache.innerHTML = apacheMenu;

    const apacheMenuItem = document.getElementById("apache-menu");
    const apacheDropDown = document.getElementById("apache-dropdown");
    apacheMenuItem.addEventListener("click", function (event) {
        event.preventDefault();

        if (apacheDropDown.className == 'hide') {
            apacheDropDown.className = 'visible';
        } else {
            apacheDropDown.className = 'hide';
        }
    });

    const href = document.querySelector('a[href="/en/versions"]');
    let version = href.textContent;

    if (version === 'next') {
        version = 'master'
    }

    let adminApiVersion = ''
    if (restApiVersions[version][0]['fileName'].indexOf('swagger') == 0) {
        adminApiVersion = restApiVersions[version][0]['version']
    } else {
        adminApiVersion = restApiVersions[version][1]['version']
    }
    let functionApiVersion = ''
    if (restApiVersions[version][0]['fileName'].indexOf('swaggerfunctions') >= 0) {
        functionApiVersion = restApiVersions[version][0]['version']
    } else {
        functionApiVersion = restApiVersions[version][1]['version']
    }
    let sourceApiVersion = ''
    if (restApiVersions[version][0]['fileName'].indexOf('swaggersource') >= 0) {
        sourceApiVersion = restApiVersions[version][0]['version']
    } else {
        sourceApiVersion = restApiVersions[version][1]['version']
    }
    let sinkApiVersion = ''
    if (restApiVersions[version][0]['fileName'].indexOf('swaggersink') >= 0) {
        sinkApiVersion = restApiVersions[version][0]['version']
    } else {
        sinkApiVersion = restApiVersions[version][1]['version']
    }
    let packagesApiVersion = ''
    if (restApiVersions[version][0]['fileName'].indexOf('swaggerpackages') >= 0) {
        packagesApiVersion = restApiVersions[version][0]['version']
    } else {
        packagesApiVersion = restApiVersions[version][1]['version']
    }
    // setup rest api menu items in nav bar
    const restapis = document.querySelector("a[href='#restapis']").parentNode;
    const restapisMenu =
        '<li>' +
        '<a id="restapis-menu" href="#restapis">REST APIs <span style="font-size: 0.75em">&nbsp;▼</span></a>' +
        '<div id="restapis-dropdown" class="hide">' +
        '<ul id="restapis-dropdown-items">' +
        '<li><a href="/admin-rest-api?version=' + version + '&apiversion=' + adminApiVersion + '">Admin REST API </a></li>' +
        '<li><a href="/functions-rest-api?version=' + version + '&apiversion=' + functionApiVersion + '">Functions </a></li>' +
        '<li><a href="/source-rest-api?version=' + version + '&apiversion=' + sourceApiVersion + '">Sources </a></li>' +
        '<li><a href="/sink-rest-api?version=' + version + '&apiversion=' + sinkApiVersion + '">Sinks </a></li>' +
        '<li><a href="/packages-rest-api?version=' + version + '&apiversion=' + packagesApiVersion + '">Packages </a></li>' +
        '</ul>' +
        '</div>' +
        '</li>';

    restapis.innerHTML = restapisMenu;

    const restapisMenuItem = document.getElementById("restapis-menu");
    const restapisDropDown = document.getElementById("restapis-dropdown");
    restapisMenuItem.addEventListener("click", function (event) {
        event.preventDefault();

        if (restapisDropDown.className == 'hide') {
            restapisDropDown.className = 'visible';
        } else {
            restapisDropDown.className = 'hide';
        }
    });

    // setup cli menu items in nav bar
    const cli = document.querySelector("a[href='#cli']").parentNode;
    const cliMenu =
        '<li>' +
        '<a id="cli-menu" href="#">CLI <span style="font-size: 0.75em">&nbsp;▼</span></a>' +
        '<div id="cli-dropdown" class="hide">' +
        '<ul id="cli-dropdown-items">' +
        '<li><a href="/pulsar-admin-cli?version=' + version + '">Pulsar Admin</a></li>' +
        '<li><a href="/pulsar-client-cli?version=' + version + '">Pulsar Client</a></li>' +
        '<li><a href="/pulsar-perf-cli?version=' + version + '">Pulsar Perf</a></li>' +
        '<li><a href="/pulsar-cli?version=' + version + '">Pulsar</a></li>' +
      '</ul>' +
        '</div>' +
        '</li>';

    cli.innerHTML = cliMenu;

    const cliMenuItem = document.getElementById("cli-menu");
    const cliDropDown = document.getElementById("cli-dropdown");
    cliMenuItem.addEventListener("click", function (event) {
        event.preventDefault();

        if (cliDropDown.className == 'hide') {
            cliDropDown.className = 'visible';
        } else {
            cliDropDown.className = 'hide';
        }
    });

    function button(label, ariaLabel, icon, className) {
        const btn = document.createElement('button');
        btn.classList.add('btnIcon', className);
        btn.setAttribute('type', 'button');
        btn.setAttribute('aria-label', ariaLabel);
        btn.innerHTML =
            '<div class="btnIcon__body">' +
            icon +
            '<strong class="btnIcon__label">' +
            label +
            '</strong>' +
            '</div>';
        return btn;
    }

    function addButtons(codeBlockSelector, btn) {
        document.querySelectorAll(codeBlockSelector).forEach(function (code) {
            code.parentNode.appendChild(btn.cloneNode(true));
        });
    }

    const copyIcon =
        '<svg width="12" height="12" viewBox="340 364 14 15" xmlns="http://www.w3.org/2000/svg"><path fill="currentColor" d="M342 375.974h4v.998h-4v-.998zm5-5.987h-5v.998h5v-.998zm2 2.994v-1.995l-3 2.993 3 2.994v-1.996h5v-1.995h-5zm-4.5-.997H342v.998h2.5v-.997zm-2.5 2.993h2.5v-.998H342v.998zm9 .998h1v1.996c-.016.28-.11.514-.297.702-.187.187-.422.28-.703.296h-10c-.547 0-1-.452-1-.998v-10.976c0-.546.453-.998 1-.998h3c0-1.107.89-1.996 2-1.996 1.11 0 2 .89 2 1.996h3c.547 0 1 .452 1 .998v4.99h-1v-2.995h-10v8.98h10v-1.996zm-9-7.983h8c0-.544-.453-.996-1-.996h-1c-.547 0-1-.453-1-.998 0-.546-.453-.998-1-.998-.547 0-1 .452-1 .998 0 .545-.453.998-1 .998h-1c-.547 0-1 .452-1 .997z" fill-rule="evenodd"/></svg>';

    addButtons(
        '.hljs',
        button('Copy', 'Copy code to clipboard', copyIcon, 'btnClipboard')
    );

    const clipboard = new ClipboardJS('.btnClipboard', {
        target: function (trigger) {
            return trigger.parentNode.querySelector('code');
        },
    });

    clipboard.on('success', function (event) {
        event.clearSelection();
        const textEl = event.trigger.querySelector('.btnIcon__label');
        textEl.textContent = 'Copied';
        setTimeout(function () {
            textEl.textContent = 'Copy';
        }, 2000);
    });

    // setup input of api version
    let pathName = window.location.pathname;
    let apiVersionList = [];
    restApiVersions[version].forEach(ele => {
        let hasRestApi = ele.fileName.find(h => h == 'swagger');
        let hasFunctions = ele.fileName.find(h => h == 'swaggerfunctions');
        let hasSink = ele.fileName.find(h => h == 'swaggersink');
        let hasSource = ele.fileName.find(h => h == 'swaggersource');

        if ( pathName.indexOf('/admin-rest-api') >= 0 && hasRestApi ) {
            apiVersionList.push(ele);
        }
        if ( pathName.indexOf('/functions-rest-api') >= 0 && hasFunctions ) {
            apiVersionList.push(ele);
        }
        if ( pathName.indexOf('/sink-rest-api') >= 0 && hasSink ) {
            apiVersionList.push(ele);
        }
        if ( pathName.indexOf('/source-rest-api') >= 0 && hasSource ) {
            apiVersionList.push(ele);
        }
    });

    const wrapperDiv = document.querySelectorAll('.wrapper')[1];
    
    if (pathName.indexOf('/admin-rest-api') >= 0
        || pathName.indexOf('/functions-rest-api') >= 0
        || pathName.indexOf('/source-rest-api') >= 0
        || pathName.indexOf('/sink-rest-api') >= 0
        || pathName.indexOf('/packages-rest-api') >= 0) {
        wrapperDiv.innerHTML = `<select name="apiVersion" class="version_select" id="version_select">
                                    ${apiVersionList.map(v => `<option value="${v.version}">${v.version}</option>`).join("")}
                                </select>`;

    } else {
        return;
    }


    const versionSelect = document.querySelectorAll('.version_select')[0];
    let aversion = '';
    if (!window.location.search.split('&')[1]) {
        aversion = apiVersionList[0];
    } else {
        aversion = window.location.search.split('&')[1].split('=')[1];
    }
    let versionOption = versionSelect.querySelectorAll('option');
    versionOption.forEach(ele => {
        if (ele.value == aversion) {
            ele.selected = true;
        } else {
            ele.selected = false;
        }
    });

    versionSelect.addEventListener('change', (e) => {
        console.log(window.location);
        let optionValue = e.target.value;
        let changeValueDom = document.createElement('a');
        changeValueDom.href = pathName + '?version=' + version + '&apiversion=' + optionValue;
        this.document.querySelectorAll('body')[0].appendChild(changeValueDom);
        changeValueDom.click();

    })

});
