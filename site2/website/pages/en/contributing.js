const React = require('react');

const CompLibrary = require('../../core/CompLibrary');
const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

const CWD = process.cwd();

const siteConfig = require(`${CWD}/siteConfig.js`);

const contributing = require('fs').readFileSync(`${CWD}/contributing.md`, 'utf8')

class Contributing extends React.Component {
  render() {

    return (
      <div className="pageContainer">
        <Container className="mainContainer documentContainer postContainer">
          <div className="post">
            <header className="postHeader">
              <h1>Contributing to Apache Pulsar</h1>
              <hr />
            </header>
            <MarkdownBlock>
              {contributing}
            </MarkdownBlock>
          </div>
        </Container>
      </div>
    );
  }
}

module.exports = Contributing;
