function getSwaggerByVersion(){
    var params = window.location.search
    var pathName = window.location.pathname
    params = params.replace('?', '')
    const paramsList = params.split('&')
    var version = 'master'
    var apiversion = '';
    for (var i in paramsList) {
        var param = paramsList[i].split('=')
        if (param[0] === 'version') {
            version = param[1]
        }
        if (param[0] === 'apiversion') {
            apiversion = param[1]
        }
    }
    var href = document.querySelector('a[href="/en/versions"]');
    href.innerHTML = '<h3>' + version + '</h3>'
    var restApiHref = document.querySelectorAll("#restapis-dropdown-items a")
    for (var i = 0; i < restApiHref.length; i++) {
        var tempHref = restApiHref[i].href
        var path = tempHref.split('version')
        var newVersionPath = path[0] + 'version=' + version
        restApiHref[i].href = newVersionPath
    }

    if (version !== 'master') {
        var versions = version.split('.')
        var majorVersion = parseInt(versions[0])
        var minorVersion = parseInt(versions[1])
        if (majorVersion < 2) {
            version = '2.3.0'
        } else if (minorVersion < 3) {
            version = '2.3.0'
        }
    }

    
    const wrapper = document.querySelector('.pageContainer .wrapper')
    const redoc = document.createElement('redoc');


    if (pathName.indexOf('admin-rest-api') >= 0) {
        redoc.setAttribute('spec-url', '/swagger/' + version + '/' + apiversion  + '/swagger.json')
    } else if (pathName.indexOf('functions-rest-api') >= 0) {
        redoc.setAttribute('spec-url', '/swagger/' + version + '/' + apiversion  + '/swaggerfunctions.json')
    } else if (pathName.indexOf('source-rest-api') >= 0) {
        redoc.setAttribute('spec-url', '/swagger/' + version + '/' + apiversion  + '/swaggersource.json')
    } else if (pathName.indexOf('sink-rest-api') >= 0) {
        redoc.setAttribute('spec-url', '/swagger/' + version + '/' + apiversion  + '/swaggersink.json')
    } else if (pathName.indexOf('packages-rest-api' >= 0)) {
        redoc.setAttribute('spec-url', '/swagger/' + version + '/' + apiversion  + '/swaggerpackages.json')
    }
    redoc.setAttribute('lazy-rendering', 'true')
    const redocLink = document.createElement('script');
    redocLink.setAttribute('src', 'https://rebilly.github.io/ReDoc/releases/latest/redoc.min.js')
    const script = document.querySelector('.pageContainer .wrapper script')

    wrapper.insertBefore(redoc, script)
    wrapper.insertBefore(redocLink, script)

}
window.onload=getSwaggerByVersion
