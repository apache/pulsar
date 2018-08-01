const replace = require('replace-in-file');

const fs = require('fs')

const CWD = process.cwd()
const siteConfig = require(`${CWD}/siteConfig.js`);
const docsDir = `${CWD}/build/${siteConfig.projectName}/docs`


function getVersions() {
  try {
    return JSON.parse(require('fs').readFileSync(`${CWD}/versions.json`, 'utf8'));
  } catch (error) {
    //console.error(error)
    console.error('no versions found defaulting to 2.1.0')
  }
  return ['2.1.0']
}

function downloadPageUrl() {
  return `${siteConfig.baseUrl}download`
}

function binaryReleaseUrl(version) {
  return `http://www.apache.org/dyn/closer.cgi/incubator/pulsar/pulsar-${version}/apache-pulsar-${version}-bin.tar.gz`
}

function connectorReleaseUrl(version) {
  return `http://www.apache.org/dyn/closer.cgi/incubator/pulsar/pulsar-${version}/apache-pulsar-io-connectors-${version}-bin.tar.gz`
}

function rpmReleaseUrl(version, type) {
  rpmVersion = version.replace('incubating', '1_incubating');
  return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=incubator/pulsar/pulsar-${version}/RPMS/apache-pulsar-client${type}-${rpmVersion}.x86_64.rpm`
}

function debReleaseUrl(version, type) {
  return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=incubator/pulsar/pulsar-${version}/DEB/apache-pulsar-client${type}.deb`
}

function doReplace(options) {
  replace(options)
    .then(changes => {
      if (options.dry) {
        console.log('Modified files:');
        console.log(changes.join('\n'))
      }
    })
    .catch(error => {
      console.error('Error occurred:', error);
    });
}


const versions = getVersions();

const latestVersion = versions[0];

const from = [
  /{{pulsar:version_number}}/g,
  /{{pulsar:version}}/g, 
  /pulsar:binary_release_url/g,
  /pulsar:connector_release_url/g,
  /pulsar:download_page_url/g,
  /{{pulsar:rpm:client}}/g,
  /{{pulsar:rpm:client-debuginfo}}/g,
  /{{pulsar:rpm:client-devel}}/g,
  /{{pulsar:deb:client}}/g,
  /{{pulsar:deb:client-devel}}/g,
];

const options = {
  files: [
    `${docsDir}/*.html`, 
    `${docsDir}/**/*.html`
  ],
  ignore: versions.map(v => `${docsDir}/${v}/**/*`), // TODO add next and assets
  from: from,
  to: [
    `${latestVersion}`, 
    `${latestVersion}-incubating`, 
    binaryReleaseUrl(`${latestVersion}-incubating`), 
    connectorReleaseUrl(`${latestVersion}-incubating`), 
    downloadPageUrl(),
    rpmReleaseUrl(`${latestVersion}-incubating`, ""),
    rpmReleaseUrl(`${latestVersion}-incubating`, "-debuginfo"),
    rpmReleaseUrl(`${latestVersion}-incubating`, "-devel"),
    debReleaseUrl(`${latestVersion}-incubating`, ""),
    debReleaseUrl(`${latestVersion}-incubating`, "-dev"),
  ],
  dry: false
};

doReplace(options);

// TODO activate and test when first version of docs are cut
// replaces versions
for (v of versions) {
  if (v === latestVersion) {
    continue
  }
  const opts = {
    files: [
      `${docsDir}/${v}/*.html`, 
      `${docsDir}/${v}/**/*.html`
    ],
    from: from,
    to: [
      `${v}`, 
      `${v}-incubating`, 
      binaryReleaseUrl(`${v}-incubating`),
      connectorReleaseUrl(`${v}-incubating`),
      downloadPageUrl(),
      rpmReleaseUrl(`${v}-incubating`, ""),
      rpmReleaseUrl(`${v}-incubating`, "-debuginfo"),
      rpmReleaseUrl(`${v}-incubating`, "-devel"),
      debReleaseUrl(`${v}-incubating`, ""),
      debReleaseUrl(`${v}-incubating`, "-dev"),
    ],
    dry: true
  };
  doReplace(opts);
}