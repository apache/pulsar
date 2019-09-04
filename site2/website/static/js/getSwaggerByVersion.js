function getSwaggerByVersion(){
    var params = window.location.search
    var pathName = window.location.pathname
    params = params.replace('?', '')
    const paramsList = params.split('&')
    var version = 'master'
    for (var i in paramsList) {
        var param = paramsList[i].split('=')
        if (param[0] === 'version') {
            version = param[1]
        }
    }
    const wrapper = document.querySelector('.pageContainer .wrapper')
    const redoc = document.createElement('redoc');
    if (pathName.indexOf('admin-rest-api') >= 0) {
        redoc.setAttribute('spec-url', '/swagger/' + version + '/swagger.json')
    } else if (pathName.indexOf('functions-rest-api') >= 0) {
        redoc.setAttribute('spec-url', '/swagger/' + version + '/swaggerfunctions.json')
    } else if (pathName.indexOf('source-rest-api') >= 0) {
        redoc.setAttribute('spec-url', '/swagger/' + version + '/swaggersource.json')
    } else if (pathName.indexOf('sink-rest-api') >= 0) {
        redoc.setAttribute('spec-url', '/swagger/' + version + '/swaggersink.json')
    }
    redoc.setAttribute('lazy-rendering', 'true')
    const redocLink = document.createElement('script');
    redocLink.setAttribute('src', 'https://rebilly.github.io/ReDoc/releases/latest/redoc.min.js')
    const script = document.querySelector('.pageContainer .wrapper script')

    wrapper.insertBefore(redoc, script)
    wrapper.insertBefore(redocLink, script)

}
window.onload=getSwaggerByVersion
