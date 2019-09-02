const React = require('react');
const CompLibrary = require('../../core/CompLibrary.js');

const Container = CompLibrary.Container;
const siteConfig = require(`${process.cwd()}/siteConfig.js`);

class SourceRestApi extends React.Component {
  render() {
    const swaggerUrl = `${siteConfig.baseUrl}swagger/swaggersource.json`

    return (
      <div className="pageContainer">
        <Container className="mainContainer documentContainer postContainer" >
          <script base-url={`${swaggerUrl}`} src="js/getSwaggerByVersion.js"></script>
        </Container>
      </div>
    );
  }
}

module.exports = SourceRestApi;
