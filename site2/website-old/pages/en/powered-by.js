

const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');
const Container = CompLibrary.Container;
const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const GridBlock = CompLibrary.GridBlock;

const CWD = process.cwd();

const translate = require('../../server/translate.js').translate;

const siteConfig = require(`${CWD}/siteConfig.js`);
const users = require(`${CWD}/data/users.js`)

class Users extends React.Component {
  render() {
    let language = this.props.language || '';


    return (
        <div className="docMainWrapper wrapper">
          <Container className="mainContainer documentContainer postContainer">
            <div className="post">
              <header className="postHeader">
                <h1><translate>Companies using or contributing to Apache Pulsar</translate></h1>
                <hr />
              </header>

              <div class="logo-wrapper">
                {
                  users.map(
                      c => (
                          (() => {
                            if (c.hasOwnProperty('logo_white')) {
                              return <div className="logo-box-background-for-white">
                                <a href={c.url} title={c.name} target="_blank">
                                  <img src={c.logo} alt={c.name} className={c.logo.endsWith('.svg') ? 'logo-svg' : ''}/>
                                </a>
                              </div>
                            } else {
                              return <div className="logo-box">
                                <a href={c.url} title={c.name} target="_blank">
                                  <img src={c.logo} alt={c.name} className={c.logo.endsWith('.svg') ? 'logo-svg' : ''}/>
                                </a>
                              </div>
                            }
                          })()
                      )
                  )}
              </div>
            </div>
          </Container>
        </div>
    );
  }
}

module.exports = Users;
