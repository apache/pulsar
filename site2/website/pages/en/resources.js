/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');
const Container = CompLibrary.Container;

const CWD = process.cwd();

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
              <h1>Resources</h1>
              <hr />
            </header>
            
            <h2>Articles</h2>
            <table className="versions">
              <thead>
                <tr>
                  <th>Forum</th>
                  <th>Link</th>
                </tr>
              </thead>
              <tbody>
                {resources.articles.map(
                  (a, i) => (
                    <tr key={i}>
                      <td><a href={a.forum_link}>{a.forum}</a></td>
                      <td><a href={a.link}>{a.title}</a></td>
                    </tr>
                  )
                )}
              </tbody>
            </table>

            <h2>Presentations</h2>
            <table className="versions">
              <thead>
                <tr>
                  <th>Forum</th>
                  <th>Data</th>
                  <th>Presenter</th>
                  <th>Link</th>
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

          </div>
        </Container>
      </div>
    );
  }
}

module.exports = Resources;
