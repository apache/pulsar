
const React = require('react');

const CompLibrary = require('../../core/CompLibrary');
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

const CWD = process.cwd();

const siteConfig = require(`${CWD}/siteConfig.js`);
// versions post docusaurus
// const versions = require(`${CWD}/versions.json`);
// versions pre docusaurus
const oldversions = require(`${CWD}/oldversions.json`);

function Versions(props) {
    const latestStableVersion = oldversions[0];
    const repoUrl = `https://github.com/${siteConfig.organizationName}/${
      siteConfig.projectName
    }`;
    return (
    <div className="pageContainer">
      <Container className="mainContainer documentContainer postContainer">
        <div className="post">
          <header className="postHeader">
            <h1>{siteConfig.title} Versions</h1>
          </header>
          <h3 id="latest">Latest Stable Version</h3>
          <p>Latest stable release of Apache Pulsar.</p>
          <table className="versions">
            <tbody>
              <tr>
                <th>{latestStableVersion}</th>
                <td>
                  <a
                    href={`${siteConfig.url}/docs/v${latestStableVersion}/getting-started/LocalCluster/`}>
                    Documentation
                  </a>
                </td>
                <td>
                  <a href={`${siteConfig.baseUrl}release-notes#${latestStableVersion}`}>
                    Release Notes
                  </a>
                </td>
              </tr>
            </tbody>
          </table>
          <h3 id="rc">Latest Version</h3>
          Here you can find the latest documentation and unreleased code.
          <table className="versions">
            <tbody>
              <tr>
                <th>master</th>
                <td>
                  <a
                    href={`${siteConfig.baseUrl}docs/standalone`}>
                    Documentation
                  </a>
                </td>
                <td>
                  <a href={repoUrl}>Source Code</a>
                </td>
              </tr>
            </tbody>
          </table>
          <h3 id="archive">Past Versions</h3>
          <p>
            Here you can find documentation for previous versions of Apache Pulsar.
          </p>
          <table className="versions">
            <tbody>
              {oldversions.map(
                version =>
                  version !== latestStableVersion && (
                    <tr key={version}>
                      <th>{version}</th>
                      <td>
                        <a
                          href={`${siteConfig.url}/docs/v${version}/getting-started/LocalCluster/`}>
                          Documentation
                        </a>
                      </td>
                      <td>
                        <a href={`${siteConfig.baseUrl}/release-notes#${version}`}>
                          Release Notes
                        </a>
                      </td>
                    </tr>
                  )
              )}
            </tbody>
          </table>
          <p>
            You can find past versions of this project on{' '}
            <a href={`${repoUrl}/releases`}>GitHub</a> or download from{' '}
            <a href={`${siteConfig.baseUrl}download`}>Apache</a>.
          </p>
        </div>
      </Container>
    </div>
  );
}

Versions.title = 'Versions';

module.exports = Versions;
