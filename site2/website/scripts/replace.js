const replace = require('replace-in-file');

const fs = require('fs')

const CWD = process.cwd()
const siteConfig = require(`${CWD}/siteConfig.js`);
const docsDir = `${CWD}/build/ApachePulsar/docs`


function getVersions() {
  try {
    return JSON.parse(require('fs').readFileSync(`${CWD}/versions.json`, 'utf8'));
  } catch (error) {
    console.error(error)
  }
  return ['2.1.0']
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

const options = {
  files: [
    `${docsDir}/*.html`, 
    `${docsDir}/**/*.html`
  ],
  ignore: versions.map(v => `${docsDir}/${v}/**/*`), // TODO add next and assets
  from: [/pulsar:version/g, /pulsar:binary_release_url/g],
  to: [`${latestVersion}-incubating`, binaryReleaseUrl(`${latestVersion}-incubating`)],
  dry: false
};

doReplace(options);

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
    from: [/pulsar:version/g, /pulsar:binary_release_url/g],
    to: [`${v}-incubating`, binaryReleaseUrl(`${v}-incubating`)],
    dry: true
  };
  doReplace(opts);
}

