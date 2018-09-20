const React = require('react');
const CompLibrary = require('../../core/CompLibrary.js');

const Container = CompLibrary.Container;
const siteConfig = require(`${process.cwd()}/siteConfig.js`);

class AdminRestApi extends React.Component {
  render() {
    const swaggerUrl = `${siteConfig.baseUrl}swagger/swagger.json`

    return (
      <div className="pageContainer">
        <Container className="mainContainer documentContainer postContainer" >
          <redoc spec-url={`${swaggerUrl}`} lazy-rendering="true"></redoc>
          <script src="//cdn.jsdelivr.net/npm/redoc/bundles/redoc.standalone.js"/>
        </Container>
      </div>
    );
  }
}

module.exports = AdminRestApi;
