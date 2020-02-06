function getCliByVersion(){
    var params = window.location.search
    var latestVersion = document.getElementById("latestVersion").textContent
    params = params.replace('?', '')
    const paramsList = params.split('&')
    var version = 'master'
    for (var i in paramsList) {
        var param = paramsList[i].split('=')
        if (param[0] === 'version') {
            version = param[1]
        }
    }

    if (version === "master") {
        var latestVersionSplit = latestVersion.split('.')
        version = parseInt(latestVersionSplit[0]) + "." + (parseInt(latestVersionSplit[1]) + 1) + ".0"
    }
    var versions = version.split('.')
    var majorVersion = parseInt(versions[0])
    var minorVersion = parseInt(versions[1])
    if ((majorVersion == 2 && minorVersion <= 5) || majorVersion === 1) {
        if (version === latestVersion) {
            window.location.href = "/docs/en/pulsar-admin"
            return
        } else {
            window.location.href = "/docs/en/" + version + "/pulsar-admin"
            return
        }
    } else {
        window.location.href = "http://pulsar.apache.org/tools/pulsar-admin/" + version + "-SNAPSHOT"
        return
    }
}
window.onload=getCliByVersion
