const React = require('react');
const CompLibrary = require('../../core/CompLibrary.js');

const Container = CompLibrary.Container;
const CWD = process.cwd();
const releases = require(`${CWD}/releases.json`);

class PulsarCli extends React.Component {
    render() {
        const latestVersion = releases[0];
        const url = "../js/getCliByVersion.js?latestVersion=" + latestVersion;
        return (
            <div className="pageContainer">
                <Container className="mainContainer documentContainer postContainer" >
                    <span id="latestVersion" style={{display:'none'}}>{latestVersion}</span>
                    <span id="clientModule" style={{display: 'none'}}>pulsar</span>
                    <script src={url}></script>
                </Container>
            </div>
    );
    }
}

module.exports = PulsarCli;
