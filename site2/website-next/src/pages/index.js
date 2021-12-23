import React from "react";
import clsx from "clsx";
import SineWaves from "sine-waves";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import styles from "./index.module.css";
import HomepageFeatures from "../components/HomepageFeatures";
import HomeQuotes from "../components/HomeQuotes";
import SubHeroBlock from "../components/SubHeroBlock";
import PromoCallout from "../components/promoCallout";
import PillButton from "../components/PillButton";
import GitHubIcon from '@mui/icons-material/GitHub';
import Svg from "../components/Svg";
import { docUrl, githubUrl } from "../utils/index";
import Stack from "@mui/material/Stack";
import Button from "@mui/material/Button";
var startWaves = function(){
  var waves = new SineWaves({
    el: document.getElementById('waves'),
    speed: 2,
    width: function() {
      return document.body.clientWidth;
    },
    height: function() {
      return 300;
    },
    ease: 'SineInOut',
    wavesWidth: '60%',
    waves: [
      {
        timeModifier: 3,
        lineWidth: 4,
        amplitude: -25,
        wavelength: 25
      },
      {
        timeModifier: 2,
        lineWidth: 4,
        amplitude: -50,
        wavelength: 50
      },
      {
        timeModifier: 1,
        lineWidth: 4,
        amplitude: -100,
        wavelength: 100
      },
      {
        timeModifier: 0.5,
        lineWidth: 4,
        amplitude: -125,
        wavelength: 125
      },
      {
        timeModifier: 1.25,
        lineWidth: 4,
        amplitude: -150,
        wavelength: 150
      }
    ],
  
    // Called on window resize
    resizeEvent: function() {
      var gradient = this.ctx.createLinearGradient(0, 0, this.width, 0);
      gradient.addColorStop(0,"rgba(24, 143, 255, 1)");
      gradient.addColorStop(0.5,"rgba(70, 78, 86, 1)");
      
      var index = -1;
      var length = this.waves.length;
      while(++index < length){
        this.waves[index].strokeStyle = gradient;
      }
      
      // Clean Up
      index = void 0;
      length = void 0;
      gradient = void 0;
    }
  });
};
var intId = setInterval(function(){
  if(document.getElementById('waves')){
      startWaves();
      clearInterval(intId);
  }
}, 500);

export default function Home() {
  const { siteConfig } = useDocusaurusContext();
  const quotesArr = [
    {
      name: 'Greg Methvin',
      company: 'Iterable',
      content: "Pulsar is unique in that it supports both streaming and queueing use cases, while also supporting a wide feature set that makes it a viable alternative to many other distributed messaging technologies currently being used in our architecture. Pulsar covers all of our use cases for Kafka, RabbitMQ, and SQS. This lets us focus on building expertise and tooling around a single unified system",
    },
    {
      name: 'Paul Au',
      company: 'Travers + Todd',
      content: "Pulsar covers all of our use cases for Kafka, RabbitMQ, and SQS. This lets us focus on building expertise and tooling around a single unified system. Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt",
    },
    {
      name: 'Mike Lee',
      company: 'Travers + Todd',
      content: "Enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Pulsar covers all of our use cases for Kafka, RabbitMQ, and SQS. This lets us focus on building expertise and tooling around a single unified system",
    }
  ]
  return (
    <Layout
      title={`Hello from ${siteConfig.title}`}
      description="Description will go into a meta tag in <head />"
    >    
      <div className="page-wrap tailwind">
        <section className="home-hero pt-24">
          <div className="inner">
            <div className="float66">
              <h1>Real-time, Continuous Data Feeds</h1>
              <p>The most scalable cloud-native distributed messaging and streaming platform</p>
            </div>
          </div>
        </section>
        <div id="waves-wrapper">
          <canvas id="waves"></canvas>
        </div>
        <div className="home-ctas">
          <div className="inner">
          <Stack spacing={2} direction="row">
            <PillButton
              variant=""
              target=""
              href={docUrl("")}
            >Read the docs</PillButton>
            <PillButton
              variant="grey"
              target="_blank"
              href={githubUrl()}
            ><GitHubIcon className="btn-icon"></GitHubIcon>  Github</PillButton>
          </Stack>
          </div>
        </div>
        
        <PromoCallout 
          url="https://www.google.com" 
          linkText="Read the Blog" 
          text="Check out the latest blog post!"
        />
        <SubHeroBlock 
          heading="What is Apache Pulsar?" 
          content="Apache Pulsar is a cloud-native, multi-tenant, high-performance solution for server-to-server messaging and queuing built on the publisher-subscribe (pub-sub) pattern. Pulsar combines the best features of a traditional messaging system like RabbitMQ with those of a pub-sub system like Apache Kafka -- scaling up or down dynamically without downtime. It’s used by thousands of companies for high-performance data pipelines, microservices, instant messaging, data integrations, and more."
        />
      
        <section className="waves-bg home-features py-48 mb-24">
          <div className="mt-8 inner relative z-5">
            <h2 className="text--center">Pulsar Features</h2>
            <HomepageFeatures />
          </div>
          <div className="home-quotes pb-24 mb-24">
            <SubHeroBlock 
            className="test"
            heading="Pulsar Users" 
            content="Run in production at Yahoo! scale with millions of messages per second across millions of topics, Pulsar is now used by thousands of companies for real-time workloads."/>
            <HomeQuotes quotes={quotesArr} />
            <p className="text--center">
              <a href="/case-studies" class="secondary-cta">Read Case Studies</a>
            </p>
          </div>
          <div className="final-cta relative z-5 py-12">
            <div className="inner inner--narrow text--center">
              <h2 className="">Get real-time with Pulsar</h2>
              <p className="">
                <PillButton
                  variant=""
                  target=""
                  href="/quickstart"> 
                    Quickstart Guide
                </PillButton>
              </p>
            </div>
          </div>
        </section>
      </div>
    </Layout>
  );
}
