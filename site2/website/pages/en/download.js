const React = require('react');

const CompLibrary = require('../../core/CompLibrary');
const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

const CWD = process.cwd();

const siteConfig = require(`${CWD}/siteConfig.js`);
const releases = require(`${CWD}/releases.json`);

const archiveRootUrl = siteConfig.archiveRootUrl;


function archiveUrl(version, type) {
  return `${archiveRootUrl}/pulsar-${version}/apache-pulsar-${version}-${type}.tar.gz`
}

class Download extends React.Component {
  render() {
    const latestRelease = releases[0];

    const latestVersion = `${latestRelease}-incubating`
    const latestArchiveUrl = archiveUrl(latestVersion, 'bin');
    const latestSrcArchiveUrl = archiveUrl(latestVersion, 'src')

    const releaseInfo = releases.map(r => {
      const version = `${r}-incubating`;
      return {
        version: version,
        binArchiveUrl: archiveUrl(version, 'bin'),
        srcArchiveUrl: archiveUrl(version, 'src')
      }
    });

    return (
      <div className="pageContainer">
        <Container className="mainContainer documentContainer postContainer">
          <div className="post">
            <header className="postHeader">
              <h1>Apache Pulsar downloads</h1>
              <hr />
            </header>
            <h2 id="latest">Current version (Stable) {latestVersion}</h2>
            <table className="versions" style={{width:'100%'}}>
              <thead>
                <tr>
                  <th>Release</th>
                  <th>Link</th>
                  <th>Crypto files</th>
                </tr>
              </thead>
              <tbody>
                <tr key={'binary'}>
                  <th>Binary</th>
                  <td>
                    <a href={latestArchiveUrl}>pulsar-{latestVersion}-bin.tar.gz</a>
                  </td>
                  <td>
                    <a href={`${latestArchiveUrl}.asc`}>asc</a>, 
                    <a href={`${latestArchiveUrl}.sha1`}>sha1</a>, 
                    <a href={`${latestArchiveUrl}.sha512`}>sha512</a>
                  </td>
                </tr>
                <tr key={'source'}>
                  <th>Source</th>
                  <td>
                    <a href={latestSrcArchiveUrl}>pulsar-{latestVersion}-src.tar.gz</a>
                  </td>
                  <td>
                    <a href={`${latestSrcArchiveUrl}.asc`}>asc</a>, 
                    <a href={`${latestSrcArchiveUrl}.sha1`}>sha1</a>, 
                    <a href={`${latestSrcArchiveUrl}.sha512`}>sha512</a>
                  </td>
                </tr>
              </tbody>
            </table>

            <h2>Release Integrity</h2>
            <MarkdownBlock>
              You must [verify](https://www.apache.org/info/verification.html) the integrity of the downloaded files.
              We provide OpenPGP signatures for every release file. This signature should be matched against the
              [KEYS](https://www.apache.org/dist/incubator/pulsar/KEYS) file which contains the OpenPGP keys of
              Pulsar's Release Managers. We also provide `MD5` and `SHA-512` checksums for every release file.
              After you download the file, you should calculate a checksum for your download, and make sure it is
              the same as ours.
            </MarkdownBlock>


            <h2>Release notes</h2>
            <MarkdownBlock>
              [Release notes](/release-notes) for all Pulsar's versions
            </MarkdownBlock>

            <h2>Getting started</h2>
            <MarkdownBlock>
              Once you've downloaded a Pulsar release, instructions on getting up and running with a standalone cluster 
              that you can run on your laptop can be found in the [Run Pulsar locally](/docs/standalone) tutorial.
            </MarkdownBlock>
            <p>
              If you need to connect to an existing Pulsar cluster or instance using an officially supported client, 
              see the client docs for these languages:
            </p>
            <table className="clients">
              <thead>
                <tr>
                  <th>Client guide</th>
                  <th>API docs</th>
                </tr>
              </thead>
              <tbody>
                <tr key={'java'}>
                  <td><a href={'docs/client-libraries-java'}>The Pulsar java client</a></td>
                  <td>The Pulsar java client</td>
                </tr>
                <tr key={'go'}>
                  <td><a href={'docs/client-libraries-go'}>The Pulsar go client</a></td>
                  <td>The Pulsar go client</td>
                </tr>
                <tr key={'python'}>
                  <td><a href={'docs/client-libraries-python'}>The Pulsar python client</a></td>
                  <td>The Pulsar python client</td>
                </tr>
                <tr key={'cpp'}>
                  <td><a href={'docs/client-libraries-cpp'}>The Pulsar C++ client</a></td>
                  <td>The Pulsar C++ client</td>
                </tr>
              </tbody>
            </table>

            <h2 id="archive">Older releases</h2>
            <table className="versions">
              <thead>
                <tr>
                  <th>Release</th>
                  <th>Binary</th>
                  <th>Source</th>
                  <th>Release notes</th>
                </tr>
              </thead>
              <tbody>
                {releaseInfo.map(
                  info =>
                    info.version !== latestVersion && (
                      <tr key={info.version}>
                        <th>{info.version}</th>
                        <td>
                          <a href={info.binArchiveUrl}>pulsar-{info.version}-bin-tar.gz</a>
                          &nbsp;
                          (<a href={`${info.binArchiveUrl}.asc`}>asc</a>,&nbsp;
                            <a href={`${info.binArchiveUrl}.sha1`}>sha1</a>,&nbsp;
                            <a href={`${info.binArchiveUrl}.sha512`}>sha512</a>)
                        </td>
                        <td>
                          <a href={info.srcArchiveUrl}>pulsar-{info.version}-bin-tar.gz</a>
                          &nbsp;
                          (<a href={`${info.srcArchiveUrl}.asc`}>asc</a>,&nbsp;
                            <a href={`${info.srcArchiveUrl}.sha1`}>sha1</a>,&nbsp;
                            <a href={`${info.srcArchiveUrl}.sha512`}>sha512</a>)
                        </td>
                        <td>
                          <a href={''}>Release Notes</a>
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
}

module.exports = Download;
