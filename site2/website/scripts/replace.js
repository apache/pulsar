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

function pulsarRepoUrl() {
  return siteConfig.githubUrl;
}

function binaryReleaseUrl(version) {
  return `http://www.apache.org/dyn/closer.cgi/incubator/pulsar/pulsar-${version}/apache-pulsar-${version}-bin.tar.gz`
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
  /{{pulsar:version}}/g, 
  /pulsar:binary_release_url/g,
  /pulsar:download_page_url/g,
  /pulsar:repo_url/g
];


const options = {
  files: [
    `${docsDir}/*.html`, 
    `${docsDir}/**/*.html`
  ],
  ignore: versions.map(v => `${docsDir}/${v}/**/*`), // TODO add next and assets
  from: from,
  to: [
    `${latestVersion}-incubating`, 
    binaryReleaseUrl(`${latestVersion}-incubating`), 
    downloadPageUrl(),
    pulsarRepoUrl()
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
      `${v}-incubating`, 
      binaryReleaseUrl(`${v}-incubating`),
      downloadPageUrl(),
      pulsarRepoUrl()
    ],
    dry: true
  };
  doReplace(opts);
}

