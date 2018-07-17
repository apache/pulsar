/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');
const Container = CompLibrary.Container;
const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const GridBlock = CompLibrary.GridBlock;

const CWD = process.cwd();

const siteConfig = require(`${CWD}/siteConfig.js`);
const team = require(`${CWD}/data/team.js`)

class Team extends React.Component {
  render() {
    let language = this.props.language || '';
    

    return (
      <div className="docMainWrapper wrapper">
        <Container className="mainContainer documentContainer postContainer">
          <div className="post">
            <header className="postHeader">
              <h1>Contact</h1>
              <hr />
            </header>
            <p>
              A successful project requires many people to play many roles. 
              Some members write code or documentation, while others are valuable as testers, 
              submitting patches and suggestions.
            </p>
            <p>
              The team is comprised of Members and Contributors. 
              Members have direct access to the source of a project and actively evolve the codebase. 
              Contributors improve the project through submission of patches and 
              suggestions to the Members. The number of Contributors to the project is unbounded. 
              Get involved today. All contributions to the project are greatly appreciated.
            </p>
            
            <h2>Committers</h2>
            <p>
              The following is a list of developers with commit privileges that have directly 
              contributed to the project in one way or another.
            </p>
            <table className="versions">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Apache Id</th>
                  <th>Organization</th>
                  <th>Roles</th>
                </tr>
              </thead>
              <tbody>
                {team.committers.map(
                  c => (
                    <tr key={c.apacheId}>
                      <td>{c.name}</td>
                      <td>{c.apacheId}</td>
                      <td>{c.org}</td>
                      <td>{c.roles}</td>
                    </tr>
                  )
                )}
              </tbody>
            </table>

            <h2>Mentors</h2>
            <p>The following people are the mentors of this incubator project</p>
            <table className="versions">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Apache Id</th>
                </tr>
              </thead>
              <tbody>
                {team.mentors.map(
                  m => (
                    <tr key={m.apacheId}>
                      <td>{m.name}</td>
                      <td>{m.apacheId}</td>
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

module.exports = Team;
