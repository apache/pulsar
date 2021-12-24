const React = require("react");
import Layout from "@theme/Layout";
import ReactMd from "react-md-file";

class ReleaseNotes extends React.Component {
  render() {
    return (
      <Layout>
        <div className="tailwind">
          <div className="my-12 container">
            <header className="postHeader">
              <h1>Apache Pulsar Release Notes</h1>
              <hr />
            </header>
            <div className="my-12 container"></div>
            {/* <ReactMarkdown source={markdown} /> */}
            <ReactMd fileName="../../release-notes.md" />
          </div>
        </div>
      </Layout>
    );
  }
}

// module.exports = ReleaseNotes;
export default ReleaseNotes;
