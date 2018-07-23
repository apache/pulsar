
const React = require('react');
const CompLibrary = require('../../core/CompLibrary.js');

const Container = CompLibrary.Container;
const siteConfig = require(`${process.cwd()}/siteConfig.js`);

class AdminRestApi extends React.Component {
  render() {
    const swaggerUrl = `${siteConfig.baseUrl}swagger/swagger.json`

    const swagger = `
      const ui = SwaggerUIBundle({
        url: "${swaggerUrl}",
        dom_id: '#swagger-ui',
        presets: [
          SwaggerUIBundle.presets.apis
        ],
        filter: true,
        //deepLinking: true,
        //displayOperationId: true,
        showCommonExtensions: true,
        showExtensions: true,
        //defaultModelRendering: "model",
        defaultModelsExpandDepth: 0,
        docExpansion: "list",
        layout: "BaseLayout"
      })
    `

    return (
      <div className="pageContainer">
        <Container className="mainContainer documentContainer postContainer" >
          <div id="swagger-ui" />
          <link rel={'stylesheet'} href={'//cdnjs.cloudflare.com/ajax/libs/swagger-ui/3.17.4/swagger-ui.css'} />
          <script src={'//unpkg.com/swagger-ui-dist@3/swagger-ui-bundle.js'} />
          <script dangerouslySetInnerHTML={{__html: swagger }}></script>
        </Container>
      </div>
    );
  }
}

module.exports = AdminRestApi;
