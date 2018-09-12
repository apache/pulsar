const React = require('react');

const CompLibrary = require('../../core/CompLibrary');
const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

const CWD = process.cwd();

const siteConfig = require(`${CWD}/siteConfig.js`);

const releaseNotes = require('fs').readFileSync(`${CWD}/release-notes.md`, 'utf8')

class ReleaseNotes extends React.Component {
  render() {

    return (
      <div className="pageContainer">
        <Container className="mainContainer documentContainer postContainer">
          <div className="post">
            <header className="postHeader">
              <h1>Apache Pulsar Release Notes</h1>
              <hr />
            </header>
            <MarkdownBlock>
              {releaseNotes}
            </MarkdownBlock>
          </div>
        </Container>
      </div>
    );
  }
}

module.exports = ReleaseNotes;
