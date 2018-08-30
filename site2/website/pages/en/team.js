

const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');
const Container = CompLibrary.Container;
const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const GridBlock = CompLibrary.GridBlock;

const CWD = process.cwd();

const translate = require('../../server/translate.js').translate;

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
              <h1><translate>Team</translate></h1>
              <hr />
            </header>
            <p>
            <translate>
              A successful project requires many people to play many roles.
              Some members write code or documentation, while others are valuable as testers,
              submitting patches and suggestions.
            </translate>
            </p>
            <p>
            <translate>
              The team is comprised of Members and Contributors.
              Members have direct access to the source of a project and actively evolve the codebase.
              Contributors improve the project through submission of patches and
              suggestions to the Members. The number of Contributors to the project is unbounded.
              Get involved today. All contributions to the project are greatly appreciated.
            </translate>
            </p>

            <h2><translate>Committers</translate></h2>
            <p>
            <translate>
              The following is a list of developers with commit privileges that have directly
              contributed to the project in one way or another.
            </translate>
            </p>
            <table className="versions">
              <thead>
                <tr>
                  <th><translate>Name</translate></th>
                  <th><translate>Apache Id</translate></th>
                  <th><translate>Organization</translate></th>
                  <th><translate>Roles</translate></th>
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

            <h2><translate>Mentors</translate></h2>
            <p><translate>The following people are the mentors of this incubator project</translate></p>
            <table className="versions">
              <thead>
                <tr>
                  <th><translate>Name</translate></th>
                  <th><translate>Apache Id</translate></th>
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
