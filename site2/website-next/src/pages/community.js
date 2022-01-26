import React, { useEffect, componentDidMount } from "react";
import clsx from "clsx";
import SineWaves from "sine-waves";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import useBaseUrl from '@docusaurus/useBaseUrl';
import styles from "./index.module.css";
import CommunityList from "../components/CommunityList";
import PromoCallout from "../components/promoCallout";
import PillButton from "../components/PillButton";
import GroupsIcon from '@mui/icons-material/Groups';
import WavySeparatorFive from '@site/static/img/separator-5.svg';
import WavySeparatorSix from '@site/static/img/separator-6.svg';
import Svg from "../components/Svg";
import { docUrl, githubUrl } from "../utils/index";
import Stack from "@mui/material/Stack";
import Button from "@mui/material/Button";
const teamObj = require("../../data/team.js");
export default function Community() {
  const { siteConfig } = useDocusaurusContext();
  useEffect((d) => {
    if(location.hash){
      let hash = location.hash;
      let id = hash.substring(1);
      let target = document.getElementById(id);
      if(target){
        target.scrollIntoView({
          behavior: 'smooth', // smooth scroll
          block: 'start' // the upper border of the element will be aligned at the top of the visible part of the window of the scrollable area.
        });
      }
    }
    
  });

  return (
    <Layout
      title={`Community`}
      description="Learn about the basics of using Apache Pulsar"
    >    
      <div className="page-wrap tailwind">
        <div className="hero-bg absolute z-0">
          <img className="relative" src={useBaseUrl('/img/community-hero-bg.jpg')} />
        </div>
        <section id="welcome" className="hero hero--welcome pt-24 relative">
          <div className="inner cf">
            <h1>Welcome to the Pulsar Community</h1>
            <div className="cf">
              <div className="md:w-2/3 md:float-left">
                <p className="text-lg">The Apache Pulsar community includes people from around the globe who are developing and using the messaging and streaming platform for real-time workloads. We welcome contributions from anyone with a passion for distributed systems.</p>
              </div>
            </div>
          </div>
        </section>
        
          <section id="team" className="main-content relative">
            <div className="inner pb-0 cf">
              <div className="cf flex flex-col md:items-center md:flex-row">
                <div className="md:w-1/2 md:pr-12">
                  <h2>About the Community</h2>
                  <p>The Pulsar community is composed of members of the Project Management Committee (PMC), committers, and contributors. Committers have direct access to the source of a project and actively evolve the codebase. Contributors improve the project through submission of patches and suggestions to be reviewed by the committers. The number of committers and contributors to the project is unbounded. </p>
                  
                </div>
                <div className="image-bg-container p-8 md:w-1/2">
                  <img className="" src={useBaseUrl('/img/community-photo-small.jpg')} />
                </div>
              </div>
              <div className="cf py-12 flex flex-col sm:flex-row">
                <div className="sm:w-1/3">
                  <h3>A successful project requires many people to play many roles.</h3>
                </div>
                <div className="sm:w-2/3 sm:pl-8">
                  <p>Some write code or documentation, while others are valuable as testers, submitting patches, and suggestions. Get involved today! All contributions to the project are greatly appreciated.</p>
                  <p>Read the <a href="https://www.apache.org/foundation/policies/conduct" className="secondary-cta" target="_blank">Apache Code of Conduct</a> and <a href="https://www.apache.org/foundation/policies/conduct#reporting-guidelines" className="secondary-cta" target="_blank">Reporting Guidelines</a>.</p>
                </div>
              </div>
            </div>
          </section>
          <WavySeparatorFive></WavySeparatorFive>
          <section id="discusssions" className="">
            <div className="inner pt-12">
              
              <h2 className="text--center">Discussions</h2>
              <div className="cf py-12 flex flex-col md:flex-row">
                <div className="md:w-1/3 md:flex md:flex-center md:p-12">
                  <img src={useBaseUrl('/img/mailing-list.svg')} />
                </div>
                <div className="md:w-2/3">
                  <h3>Mailing Lists</h3>
                  <p>The primary place for discussions is on the mailing lists which includes Pulsar committers.</p>
                    <div className="flex flex-col md:flex-row">
                      <div className="discussion-box md:w-1/2 md:pr-2">
                        <h4>User List</h4>
                        <p>General mailing list for user-related discussions.</p>
                       
                          <PillButton
                            variant=""
                            target=""
                            href="mailto:users-subscribe@pulsar.apache.org"
                          >Subscribe</PillButton>
                          <PillButton
                            variant="grey"
                            target="_blank"
                            href="mailto:users-unsubscribe@pulsar.apache.org"
                          >Unsubscribe</PillButton>
                        <p><strong>You can access the archive <a className="secondary-cta" href="http://mail-archives.apache.org/mod_mbox/pulsar-dev/">here</a>.</strong></p>
                      </div>
                      <div className="discussion-box md:w-1/2 md:pr-2">
                        <h4>Developer List</h4>
                        <p>Questions and discussions related to Pulsar development.</p>
                        
                          <PillButton
                            variant=""
                            target=""
                            href="mailto:dev-subscribe@pulsar.apache.org"
                          >Subscribe</PillButton>
                          <PillButton
                            variant="grey"
                            target="_blank"
                            href="mailto:dev-unsubscribe@pulsar.apache.org"
                          >Unsubscribe</PillButton>
                       
                        <p><strong>You can access the archive <a  className="secondary-cta" href="http://mail-archives.apache.org/mod_mbox/pulsar-dev/">here</a>.</strong></p>
                      </div>
                    </div>

                </div>
               
              </div>
              <div className="cf flex flex-col md:flex-row py-12">
                <div className="md:w-1/3">
                  <img src={useBaseUrl('/img/Slack_Mark.svg')} />
                </div>
                <div className="md:w-2/3">
                  <h3>Slack</h3>
                  <div className="">
                      <p>The Pulsar community maintains its own Slack instance (separate from ASF Slack) used for informal discussions for developers and users. </p>
                      <p>Slack channels are great for quick questions or discussions on specialized topics. Remember that we strongly encourage communication via the mailing lists, and we prefer to discuss more complex subjects by email. Developers should be careful to move or duplicate all official or useful discussions to the issue tracking system and/or the dev mailing list.</p>
                      
                        <PillButton
                          variant=""
                          target="_blank"
                          href="https://www.google.com/url?q=https://apache-pulsar.slack.com/&sa=D&source=docs&ust=1641865410610919&usg=AOvVaw3iDpbML7pbfCR7HJBqoUck"
                        >PULSAR SLACK</PillButton>
                        <PillButton
                          variant="grey"
                          target="_blank"
                          href="https://apache-pulsar.herokuapp.com/"
                        >Unsubscribe</PillButton>
                  </div>

                </div>
                
              </div>
            </div>
            <div className="inner pt-12">
              <div className="text--center md:text-left md:grid md:grid-cols-3 md:gap-x-4 md:gap-y-4">
                <div className="discussion-box">
                  <div className="icon-wrap text--center md:text-left md:flex md:items-center">
                    <img className="mx-auto md:m-0" src={useBaseUrl('/img/logo-stackoverflow.svg')} />
                  </div>
                  <h3>Stack Overflow</h3>
                  <p>For technical questions, we ask that you post them to <a href="https://stackoverflow.com/tags/apache-pulsar" target="_blank">Stack Overflow</a> using the tag “apache-pulsar”.
.</p>
                </div>
                <div className="discussion-box">
                  <div className="icon-wrap flex items-center">
                    <img className="mx-auto md:m-0" src={useBaseUrl('/img/wechat-logo.svg')} />
                  </div>
                  
                  <h3>WeChat</h3>
                  <p>There are several WeChat groups that are used for informal discussions for devs and users. To join these WeChat tech groups, you can add Bot with the WeChat ID: StreamNative_BJ</p>
                </div>
                <div className="discussion-box">
                  <div className="icon-wrap text-2xl flex items-center">
                   <GroupsIcon className="icon-wrap-icon mx-auto md:m-0" />
                  </div>
                  <h3>Community Meetings</h3>
                  <p>The community meeting occurs biweekly on Tuesdays and Thursdays to discuss new proposals, open pull requests, and host open discussions. Details and how to join <a href="https://github.com/apache/pulsar/wiki/Community-Meetings" target="_blank">here.</a></p>
                </div>

              </div>
            </div>
           
          </section>
          <WavySeparatorSix></WavySeparatorSix>
          <section id="governance" className="py-12">
            <div className="inner">
              <h2>Project Governance</h2>
              <p>Apache Pulsar is independently managed by its Project Management Committee (PMC)—the governing body tasked with project management including technical direction, voting on new committers and PMC members, setting policies, and formally voting on software product releases.</p>
                <PillButton
                  variant=""
                  target="_blank"
                  href="https://community.apache.org/projectIndependence"
                >ASF PROJECT INDEPENDENCE OVERVIEW</PillButton>
                <PillButton
                  variant="grey"
                  target="_blank"
                  href="https://www.apache.org/foundation/governance/pmcs.html"
                >ASF PMC OVERVIEW</PillButton>
                <PillButton
                  variant=""
                  target="_blank"
                  href="https://www.apache.org/theapacheway/index.html"
                >THE APACHE WAY</PillButton>
            </div>
          </section>
          <section id="how-to-contribute" className="py-12">
            <div className="inner">
              <h2 className="text-center sm:text-left">How to Contribute</h2>
              <div className="">
                <div className="flex flex-col  sm:flex-row items-center py-12">
                  <div className="sm:w-1/3 section-icon px-12">
                    <img src={useBaseUrl('/img/contribute.svg')} />
                  </div>
                  <div className="sm:w-2/3">
                    <h3>Contributing to the Project</h3>
                    <p>Pulsar has many different opportunities for contributions -- you can write new examples/tutorials, add new user-facing libraries, write new Pulsar IO connectors, participate in documentation, and more. Read our <a href="https://pulsar.apache.org/en/contributing/" className="secondary-cta">Guide to Contributing</a> and <a href="/coding-guide/" className="secondary-cta">Coding Guide</a> to get started.</p>
                  </div>
                </div>
                <div className="flex flex-col  sm:flex-row items-center  py-12">
                  <div className="sm:w-1/3 section-icon px-12">
                    <img src={useBaseUrl('/img/report-bugs.svg')} />
                  </div>
                  <div className="sm:w-2/3 ">
                    <h3>Reporting Bugs</h3>
                    <p>If you encounter a problem with Pulsar, the first places to ask for help are the user mailing list or Stack Overflow.</p>
                    <p>If, after having asked for help, you suspect that you have found a bug in Pulsar, you should report it to the developer mailing list or by opening GitHub Issue. Please provide as much detail as you can on your problem. Don’t forget to indicate which version of Pulsar you are running and on which environment.</p>
                  </div>
                </div>
                <div className="flex flex-col  sm:flex-row items-center py-12">
                  <div className="sm:w-1/3 section-icon  px-12">
                    <img src={useBaseUrl('/img/report-vulnerabillity.svg')} />
                  </div>
                  <div className="sm:w-2/3">
                  <h3>Reporting a Vulnerability</h3>
                  <p>To report a vulnerability for Pulsar, contact the <a className="secondary-cta" href="https://www.apache.org/security/projects.html" target="_blank">Apache Security Team</a>.</p>
                  </div>
                </div>
              </div>
            </div>
          </section>
          <WavySeparatorSix></WavySeparatorSix>
          <section id="community" className="py-12">
            <div className="inner">
              <h2 className="text--center">Meet the Community</h2>
              <CommunityList list={teamObj.committers} />
            </div>
          </section>
          <PromoCallout url="https://www.google.com" linkText="Read the Blog"  text="Check out the latest blog post!"/>
      </div>
    </Layout>
  );
}
