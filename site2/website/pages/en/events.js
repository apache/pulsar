
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
              - [Pulsar Summit Europe 2021](https://pulsar-summit.org/en/event/europe-2021) 6 October 2021
            </MarkdownBlock>
            <MarkdownBlock>
              - [Pulsar Summit Asia 2021](https://pulsar-summit.org/en/event/asia-2021) 20-21 November 2021
            </MarkdownBlock>
            <MarkdownBlock>
              - [Pulsar Summit North America 2021](https://pulsar-summit.org/en/event/north-america-2021) 16-17 June 2021
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
            <h2><translate>Groups</translate></h2>
            <MarkdownBlock>
              - [NorCal Apache Pulsar Neighborhood Meetup Group](https://www.meetup.com/nor-cal-apache-pulsar-meetup-group/)
            </MarkdownBlock>
            <MarkdownBlock>
              - [Netherlands Apache Pulsar Meetup Group](https://www.meetup.com/netherlands-apache-pulsar-meetup/)
            </MarkdownBlock>
            <MarkdownBlock>
              - [SoCal Apache Pulsar Neighborhood Meetup Group](https://www.meetup.com/socal-apache-pulsar-meetup-group/)
            </MarkdownBlock>
            <MarkdownBlock>
              - [New York City Apache Pulsar Meetup](https://www.meetup.com/new-york-city-apache-pulsar-meetup/)
            </MarkdownBlock>
            <MarkdownBlock>
              - [Beijing Apache Pulsar® Meetup by StreamNative](https://www.meetup.com/beijing-apache-pulsar-meetup-by-streamnative/)
            </MarkdownBlock>
            <MarkdownBlock>
              - [SF Bay Area Apache Pulsar Meetup](https://www.meetup.com/SF-Bay-Area-Apache-Pulsar-Meetup/)
            </MarkdownBlock>
            <MarkdownBlock>
              - [Japan Pulsar User Group](https://japan-pulsar-user-group.connpass.com/)
            </MarkdownBlock>
             <h2><translate>Replays</translate></h2>
            <MarkdownBlock>
              - [Pulsar Summit Virtual Conference](https://www.youtube.com/playlist?list=PLqRma1oIkcWjVlPfaWlf3VO9W-XWsF_4-) 9 September 2020
            </MarkdownBlock>
          </div>
        </Container>
      </div>
    );
  }
}

module.exports = Events;
