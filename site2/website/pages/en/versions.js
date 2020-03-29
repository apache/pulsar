
const React = require('react');

const CompLibrary = require('../../core/CompLibrary');
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

const CWD = process.cwd();

const translate = require('../../server/translate.js').translate;

const siteConfig = require(`${CWD}/siteConfig.js`);
// versions post docusaurus
const versions = require(`${CWD}/versions.json`);
// versions pre docusaurus
const oldversions = require(`${CWD}/oldversions.json`);

function Versions(props) {
    const latestStableVersion = versions[0];
    const repoUrl = `https://github.com/${siteConfig.organizationName}/${
      siteConfig.projectName
    }`;
    return (
    <div className="pageContainer">
      <Container className="mainContainer documentContainer postContainer">
        <div className="post">
          <header className="postHeader">
            <h1>{siteConfig.title} <translate>Versions</translate></h1>
          </header>
          <h3 id="latest"><translate>Latest Stable Version</translate></h3>
          <p><translate>Latest stable release of Apache Pulsar.</translate></p>
          <table className="versions">
            <tbody>
              <tr>
                <th>{latestStableVersion}</th>
                <td>
                  <a
                    href={`${siteConfig.baseUrl}docs/${props.language}/standalone`}>
                    <translate>
                    Documentation
                    </translate>
                  </a>
                </td>
                <td>
                  <a href={`${siteConfig.baseUrl}release-notes#${latestStableVersion}`}>
                    <translate>
                    Release Notes
                    </translate>
                  </a>
                </td>
              </tr>
            </tbody>
          </table>
          <h3 id="rc"><translate>Latest Version</translate></h3>
          <translate>
          Here you can find the latest documentation and unreleased code.
          </translate>
          <table className="versions">
            <tbody>
              <tr>
                <th>master</th>
                <td>
                  <a
                    href={`${siteConfig.baseUrl}docs/${props.language}/next/standalone`}>
                    <translate>Documentation</translate>
                  </a>
                </td>
                <td>
                  <a href={repoUrl}><translate>Source Code</translate></a>
                </td>
              </tr>
            </tbody>
          </table>
          <h3 id="archive"><translate>Past Versions</translate></h3>
          <p>
          <translate>
            Here you can find documentation for previous versions of Apache Pulsar.
          </translate>
          </p>
          <table className="versions">
            <tbody>
              {versions.map(
                version =>
                  version !== latestStableVersion && (
                    <tr key={version}>
                      <th>{version}</th>
                      <td>
                        <a
                          href={`${siteConfig.baseUrl}docs/${props.language}/${version}/standalone`}>
                          <translate>Documentation</translate>
                        </a>
                      </td>
                      <td>
                        <a href={`${siteConfig.baseUrl}release-notes#${version}`}>
                          <translate>Release Notes</translate>
                        </a>
                      </td>
                    </tr>
                  )
              )}
              {oldversions.map(
                version =>
                  version !== latestStableVersion && (
                    <tr key={version}>
                      <th>{version}</th>
                      <td>
                        <a
                          href={`${siteConfig.baseUrl}docs/v${version}/getting-started/LocalCluster/`}>
                          <translate>Documentation</translate>
                        </a>
                      </td>
                      <td>
                        <a href={`${siteConfig.baseUrl}release-notes#${version}`}>
                          <translate>Release Notes</translate>
                        </a>
                      </td>
                    </tr>
                  )
              )}
            </tbody>
          </table>
        </div>
      </Container>
    </div>
  );
}

Versions.title = 'Versions';

module.exports = Versions;
