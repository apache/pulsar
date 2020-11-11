
const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');
const Container = CompLibrary.Container;
const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const GridBlock = CompLibrary.GridBlock;

const CWD = process.cwd();

const translate = require('../../server/translate.js').translate;

const siteConfig = require(`${CWD}/siteConfig.js`);

const iframeSrc = "https://calendar.google.com/calendar/embed?src=apache.pulsar.slack%40gmail.com";

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
            <h2><translate>Calender</translate></h2>

            <iframe src={iframeSrc} 
              style={{borderWidth: 0}} 
              height={300} width={640} 
              frameBorder={0} scrolling={"no"}>
            </iframe>
            <h2><translate>Events</translate></h2>            
            <MarkdownBlock>
              - [Pulsar Summit Asia 2020](https://pulsar-summit.org/en/event/asia-2020)
            </MarkdownBlock>
            <MarkdownBlock>
              - [Pulsar Summit Virtual Conference](https://www.youtube.com/playlist?list=PLqRma1oIkcWjVlPfaWlf3VO9W-XWsF_4-)
            </MarkdownBlock>
            <MarkdownBlock>
              - [ApacheCon @Home](https://www.youtube.com/watch?v=iIABx20uvmw&list=PLU2OcwpQkYCy_awEe5xwlxGTk5UieA37m)
            </MarkdownBlock>
            <MarkdownBlock>
              - [ApacheCon @Home Pulsar/BookKeeper track](https://www.apachecon.com/acah2020/tracks/pulsar.html)
            </MarkdownBlock>
            <MarkdownBlock>
              - [Pulsar Training: Developing Pulsar Applications with Jesse Anderson](https://www.eventbrite.com/e/developing-pulsar-applications-online-training-tickets-122334542911)
            </MarkdownBlock>
            <MarkdownBlock>
              - [Monthly webinar](https://www.youtube.com/watch?v=Owl_ncQbVwk&list=PLqRma1oIkcWhfmUuJrMM5YIG8hjju62Ev)
            </MarkdownBlock>
            <MarkdownBlock>
              - [Weekly TGIP](https://www.youtube.com/watch?v=Bss2OYq7SVk)
            </MarkdownBlock>
            <MarkdownBlock>
              - [Pulsar Developer Community biweekly meetup](https://github.com/streamnative/pulsar-community-loc-cn/)
            </MarkdownBlock>
            <MarkdownBlock>
              - [SF Bay Area Apache Pulsar Meetup](https://www.meetup.com/SF-Bay-Area-Apache-Pulsar-Meetup/)
            </MarkdownBlock>
            <h2><translate>Groups</translate></h2>
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
