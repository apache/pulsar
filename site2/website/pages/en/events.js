
const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');
const Container = CompLibrary.Container;
const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const GridBlock = CompLibrary.GridBlock;

const CWD = process.cwd();

const translate = require('../../server/translate.js').translate;

const siteConfig = require(`${CWD}/siteConfig.js`);

const iframeSrc = "https://calendar.google.com/calendar/embed?showTitle=0&amp;showPrint=0&amp;showCalendars=0&amp;mode=AGENDA&amp;height=300&amp;wkst=1&amp;hl=en&amp;bgcolor=%23FFFFFF&amp;src=22kknj432ap5io49lvsjaac71o%40group.calendar.google.com&amp;color=%232952A3&amp;ctz=America%2FLos_Angeles";

class Events extends React.Component {
  render() {

    return (
      <div className="docMainWrapper wrapper">
        <Container className="mainContainer documentContainer postContainer">
          <div className="post">
            <header className="postHeader">
              <h1><translate>Events</translate></h1>
              <hr />
            </header>
            <h2><translate>Calander</translate></h2>

            <iframe src={iframeSrc} 
              style={{borderWidth: 0}} 
              height={300} width={640} 
              frameBorder={0} scrolling={"no"}>
            </iframe>
            
            <h2><translate>Groups</translate></h2>
            <MarkdownBlock>
              - [Apache Pulsar Bay Area Meetup Group](https://www.meetup.com/Apache-Pulsar-Meetup-Group/)
            </MarkdownBlock>
            <MarkdownBlock>
              - [Japan Pulsar User Group](https://japan-pulsar-user-group.connpass.com/)
            </MarkdownBlock>
          </div>
        </Container>
      </div>
    );
  }
}

module.exports = Events;
