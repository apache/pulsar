function getCliByVersion(){
    var params = window.location.search
    var latestVersion = document.getElementById("latestVersion").textContent
    var clientModule = document.getElementById("clientModule").textContent
    if (!clientModule) {
        clientModule = "pulsar-admin"
    }
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
    var minMinorVersion = 5
    var referenceLink = "/pulsar-admin"
    if (clientModule === "pulsar-client") {
        minMinorVersion = 7
        referenceLink = "/reference-cli-tools/#pulsar-client"
    }
    if (clientModule === "pulsar-admin") {
        if ((majorVersion == 2 && minorVersion <= minMinorVersion) || majorVersion === 1) {
            if (version === latestVersion) {
                window.location.href = "/docs/en" + referenceLink
                return
            } else {
                window.location.href = "/docs/en/" + version + referenceLink
                return
            }
        } else {
            version = parseInt(versions[0]) + "." + parseInt(versions[1]) + ".0"
            window.location.href = "http://pulsar.apache.org/tools/ + " + clientModule + "/" + version + "-SNAPSHOT"
            return
        }
    }
}
window.onload=getCliByVersion
