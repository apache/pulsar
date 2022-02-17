import React from "react";
import Layout from "@theme/Layout";
import ReactMarkdown from "react-markdown";
import Translate, { translate } from "@docusaurus/Translate";
const iframeSrc = "https://calendar.google.com/calendar/embed?src=apache.pulsar.slack%40gmail.com";

export default function Page(props) {
  return (
    <Layout>
      <div className="tailwind">
        <div className="my-12 container">
          <header className="postHeader">
            <h1>
              <Translate>Events</Translate>
            </h1>
            <hr />
          </header>
          <h2>
            <Translate>Calender</Translate>
          </h2>
          <iframe
            src={iframeSrc}
            style={{ borderWidth: 0 }}
            height={300}
            width={640}
            frameBorder={0}
            scrolling={"no"}
          ></iframe>
          <h2>
            <Translate>Events</Translate>
          </h2>

          <ReactMarkdown>
            - [Pulsar Summit Europe
            2021](https://pulsar-summit.org/en/event/europe-2021) 6 October 2021
          </ReactMarkdown>
          <ReactMarkdown>
            - [Pulsar Summit Asia
            2021](https://pulsar-summit.org/en/event/asia-2021) 20-21 November
            2021
          </ReactMarkdown>
          <ReactMarkdown>
            - [Pulsar Summit North America
            2021](https://pulsar-summit.org/en/event/north-america-2021) 16-17
            June 2021
          </ReactMarkdown>
          <ReactMarkdown>
            - [ApacheCon
            @Home](https://www.youtube.com/watch?v=iIABx20uvmw&list=PLU2OcwpQkYCy_awEe5xwlxGTk5UieA37m)
          </ReactMarkdown>
          <ReactMarkdown>
            - [ApacheCon @Home Pulsar/BookKeeper
            track](https://www.apachecon.com/acah2020/tracks/pulsar.html)
          </ReactMarkdown>
          <ReactMarkdown>
            - [Pulsar Training: Developing Pulsar Applications with Jesse
            Anderson](https://www.eventbrite.com/e/developing-pulsar-applications-online-training-tickets-122334542911)
          </ReactMarkdown>
          <ReactMarkdown>
            - [Monthly
            webinar](https://www.youtube.com/watch?v=Owl_ncQbVwk&list=PLqRma1oIkcWhfmUuJrMM5YIG8hjju62Ev)
          </ReactMarkdown>
          <ReactMarkdown>
            - [Weekly TGIP](https://www.youtube.com/watch?v=Bss2OYq7SVk)
          </ReactMarkdown>
          <ReactMarkdown>
            - [Pulsar Developer Community biweekly
            meetup](https://github.com/streamnative/pulsar-community-loc-cn/)
          </ReactMarkdown>
          <h2>
            <Translate>Groups</Translate>
          </h2>
          <ReactMarkdown>
            - [NorCal Apache Pulsar Neighborhood Meetup
            Group](https://www.meetup.com/nor-cal-apache-pulsar-meetup-group/)
          </ReactMarkdown>
          <ReactMarkdown>
            - [Netherlands Apache Pulsar Meetup
            Group](https://www.meetup.com/netherlands-apache-pulsar-meetup/)
          </ReactMarkdown>
          <ReactMarkdown>
            - [SoCal Apache Pulsar Neighborhood Meetup
            Group](https://www.meetup.com/socal-apache-pulsar-meetup-group/)
          </ReactMarkdown>
          <ReactMarkdown>
            - [New York City Apache Pulsar
            Meetup](https://www.meetup.com/new-york-city-apache-pulsar-meetup/)
          </ReactMarkdown>
          <ReactMarkdown>
            - [Beijing Apache Pulsar® Meetup by
            StreamNative](https://www.meetup.com/beijing-apache-pulsar-meetup-by-streamnative/)
          </ReactMarkdown>
          <ReactMarkdown>
            - [SF Bay Area Apache Pulsar
            Meetup](https://www.meetup.com/SF-Bay-Area-Apache-Pulsar-Meetup/)
          </ReactMarkdown>
          <ReactMarkdown>
            - [Japan Pulsar User
            Group](https://japan-pulsar-user-group.connpass.com/)
          </ReactMarkdown>
          <h2>
            <Translate>Replays</Translate>
          </h2>
          <ReactMarkdown>
            - [Pulsar Summit Virtual
            Conference](https://www.youtube.com/playlist?list=PLqRma1oIkcWjVlPfaWlf3VO9W-XWsF_4-)
            9 September 2020
          </ReactMarkdown>
        </div>
      </div>
    </Layout>
  );
}
