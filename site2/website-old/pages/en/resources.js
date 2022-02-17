

const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');
const Container = CompLibrary.Container;

const CWD = process.cwd();

const translate = require('../../server/translate.js').translate;

const siteConfig = require(`${CWD}/siteConfig.js`);
const resources = require(`${CWD}/data/resources.js`)

class Resources extends React.Component {
  render() {
    let language = this.props.language || '';
    

    return (
      <div className="docMainWrapper wrapper">
        <Container className="mainContainer documentContainer postContainer">
          <div className="post">
            <header className="postHeader">
              <h1><translate>Resources</translate></h1>
              <hr />
            </header>
            
            <h2><translate>Articles</translate></h2>
            <table className="versions">
              <thead>
                <tr>
                  <th><translate>Forum</translate></th>
                  <th><translate>Date</translate></th>
                  <th><translate>Link</translate></th>
                </tr>
              </thead>
              <tbody>
                {resources.articles.map(
                  (a, i) => (
                    <tr key={i}>
                      <td><a href={a.forum_link}>{a.forum}</a></td>
                      <td>{a.date}</td>
                      <td><a href={a.link}>{a.title}</a></td>
                    </tr>
                  )
                )}
              </tbody>
            </table>

            <h2><translate>Presentations</translate></h2>
            <table className="versions">
              <thead>
                <tr>
                  <th><translate>Forum</translate></th>
                  <th><translate>Date</translate></th>
                  <th><translate>Presenter</translate></th>
                  <th><translate>Link</translate></th>
                </tr>
              </thead>
              <tbody>
                {resources.presentations.map(
                  (p, i) => (
                    <tr key={i}>
                      <td><a href={p.forum_link}>{p.forum}</a></td>
                      <td>{p.date}</td>
                      <td>{p.presenter}</td>
                      <td><a href={p.link}>{p.title}</a></td>
                    </tr>
                  )
                )}
              </tbody>
            </table>

            <h2><translate>Older Articles</translate></h2>
            <table className="versions">
              <thead>
                <tr>
                  <th><translate>Forum</translate></th>
                  <th><translate>Date</translate></th>
                  <th><translate>Link</translate></th>
                </tr>
              </thead>
              <tbody>
                {resources.older_articles.map(
                  (oa, i) => (
                    <tr key={i}>
                      <td><a href={oa.forum_link}>{oa.forum}</a></td>
                      <td>{oa.date}</td>
                      <td><a href={oa.link}>{oa.title}</a><br></br>
                          <a href={oa.link2}>{oa.title2}</a><br></br>
                          <a href={oa.link3}>{oa.title3}</a>
                      </td>
                    </tr>
                  )
                )}
              </tbody>
            </table>

            <h2><translate>Older Presentations</translate></h2>
            <table className="versions">
              <thead>
                <tr>
                  <th><translate>Forum</translate></th>
                  <th><translate>Date</translate></th>
                  <th><translate>Presenter</translate></th>
                  <th><translate>Link</translate></th>
                </tr>
              </thead>
              <tbody>
                {resources.older_presentations.map(
                  (op, i) => (
                    <tr key={i}>
                      <td><a href={op.forum_link}>{op.forum}</a></td>
                      <td>{op.date}</td>
                      <td>{op.presenter}</td>
                      <td><a href={op.link}>{op.title}</a></td>
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

module.exports = Resources;
