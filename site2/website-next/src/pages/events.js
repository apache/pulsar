import * as React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import TabsUnstyled from '@mui/base/TabsUnstyled';
import TabsListUnstyled from '@mui/base/TabsListUnstyled';
import TabPanelUnstyled from '@mui/base/TabPanelUnstyled';
import TabUnstyled from '@mui/base/TabUnstyled';
import EventCards from "../components/EventCards";
import FeaturedEvent from "../components/FeaturedEvent";
const resObj = require("../../data/events.js");
export default function Events() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={`Events`}
      description="Apache Pulsar Events"
    >    
      <div className="page-wrap tailwind">
        <section className="hero">
            <div className="inner text--left">
              <div className="flex flex-col md:flex-row">
                <div className="md:w-1/2">
                  <h1>Events</h1>
                  <p>Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.</p>

                </div>
                <div className="mt-12 md:mt-0 md:w-1/2">
                  <FeaturedEvent
                    hidden="false" // use true to hide and false to show the featured event card
                    title="Super Summit"
                    description="This is a short description of an upcoming event. Lorem ipsum dolor sit amet, consectetur adipiscing elit."
                    date="July 4th, 2022"
                    link="https://www.google.com"
                  />
                </div>
              </div>
            </div>
        </section>
        <section className="main-content waves-bg pt-12 pb-48 mb-24">
          <TabsUnstyled defaultValue={0} className="tabs tabs--resources block my-24 relative z-5">
          <TabsListUnstyled className="block text--center tabs-bar py-8 px-4">
              <TabUnstyled className="mx-2">Events</TabUnstyled>
              <TabUnstyled className="mx-2">Groups</TabUnstyled>
              <TabUnstyled className="mx-2">Replays</TabUnstyled>
            </TabsListUnstyled>
            <TabPanelUnstyled value={0}> <EventCards type="events" events={resObj.events} /></TabPanelUnstyled>
            <TabPanelUnstyled value={1}><EventCards type="groups" events={resObj.groups} /></TabPanelUnstyled>
            <TabPanelUnstyled value={2}><EventCards type="replays" events={resObj.replays} /></TabPanelUnstyled>
          </TabsUnstyled>
        </section>
      </div>
    </Layout>
  );
}
