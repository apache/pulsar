import React, { useState } from "react";
import Layout from "@theme/Layout";
import TabsUnstyled from '@mui/base/TabsUnstyled';
import TabsListUnstyled from '@mui/base/TabsListUnstyled';
import TabPanelUnstyled from '@mui/base/TabPanelUnstyled';
import TabUnstyled from '@mui/base/TabUnstyled';
import CaseStudyCards from "../components/CaseStudyCards";
const csObj = 
  {

    financial_services:  [
      {
        company: "Belvedere Trading",
        link: 'https://www.youtube.com/watch?v=myJ43xfZHsg',
        image: require("@site/static/img/belvedere-trading.png").default,
        description: "Leveraging Pulsar to stream exchange market data to the cloud."
      },
      {
        company: "Bestpay / Orange Financial",
        link: 'https://streamnative.io/success-story/bestpay/',
        image: require("@site/static/img/bestpay.png").default,
        description: "Together with a unified computing engine like Spark, Apache Pulsar is able to boost the efficiency of our risk-control decision deployment. Thus, we are able to provide merchants and consumers with safe, convenient, and efficient services."
      },
    ],
    healthcare: [
      {
        company: "Vivy",
        link: 'https://www.datastax.com/blog/how-apache-pulsar-helps-vivy-connect-healthcare-professionals-and-users',
        image: require("@site/static/img/vivy.png").default,
        description: "We found Pulsar, an alternative we really liked from the development point of view—the client is really nice, as well as from the operational point of view; it supports separate bookies and less load on ZooKeeper."
      },
    ],
    software_it:  [
      {
        company: "Appen China",
        link: 'https://www.youtube.com/watch?v=N195msPyC_A',
        image: require('@site/static/img/appen-logo.png').default,
        description: "When serving a large volume of data collection and annotation, we faced some challenges on task distribution, anti-scamming and AI model training. We adopted the Apache Pulsar and NoSQL database solution to resolve those pain points and keep the flexibility. "
      },
      {
        company: "BIGIO",
        link: 'https://www.youtube.com/watch?v=CdPd5zAPsgc',
        image: require("@site/static/img/bigo.png").default,
        description: "We have adopted Apache Pulsar to build our Message Processing System, especially in Real-Time ETL, short-form video recommendation and AB-test Real-Time Data report."
      },
      {
        company: "Clever Cloud",
        link: 'https://www.youtube.com/watch?v=-pQ6zRz6ij8',
        image: require("@site/static/img/clever-cloud.png").default,
        description: "We're using Apache Pulsar as the foundation for our cloud backbone and developed KoP (Kafka on Pulsar)."
      },
      {
        company: "Cogito Corp",
        link: 'https://www.youtube.com/watch?v=vE7aKZT9r7k',
        image: require("@site/static/img/cogito-corp.png").default,
        description: "Cogito uses Pulsar for streaming real time audio and analytic results and utilizes the vital deduplication feature"
      },
      {
        company: "EMQ",
        link: 'https://streamnative.io/success-story/emq/',
        image: require("@site/static/img/emq.png").default,
        description: "ActorCloud uses Apache Pulsar to store and process streaming data, leverages Apache Pulsar Functions to handle data faster and analyzes IoT data through the SQL engine exposed to the upper layer."
      },
      {
        company: "Huawei Cloud",
        link: 'https://www.youtube.com/watch?v=RC2zIiShjKE',
        image: require("@site/static/img/huawei.png").default,
        description: "Huawei Cloud IoT requires a reliable messaging platform. After comparing the capabilities and features of multiple messaging systems, the design of Apache Pulsar is what made it our choice. "
      },
      {
        company: "Instructure",
        link: 'https://www.youtube.com/watch?v=ViX1wJTmRmk',
        image: require("@site/static/img/instructure.png").default,
        description: "We researched, advocated, built, integrated, and established Apache Pulsar at Instructure in less than a year."
      },
      {
        company: "Intuit",
        link: 'https://www.youtube.com/watch?v=CmyHUN5MRUU',
        image: require("@site/static/img/intuit.png").default,
        description: "We adopted Pulsar for our next generation platform and adapted it for Intuit specific requirements."
      },
      {
        company: "Iterable",
        link: 'https://www.infoq.com/articles/pulsar-customer-engagement-platform/',
        image: require("@site/static/img/iterable.png").default,
        description: "Pulsar provided the right balance of scalability, reliability, and features to replace RabbitMQ at Iterable and, ultimately, to replace other messaging systems like Kafka and Amazon SQS."
      },
      {
        company: "Ksyun",
        link: 'https://www.youtube.com/watch?v=jw5UVhsEIcQ',
        image: require("@site/static/img/ksyun.png").default,
        description: "Currently, our service supports log query and monitoring for many businesses, and processes tens of terabytes of data every day. With Pulsar, we can scale up partitions and merge partitions easily, and process millions of topics."
      },
      {
        company: "Micro Focus",
        link: 'https://www.youtube.com/watch?v=GKh7a8-ZjD4',
        image: require("@site/static/img/micro-focus.jpg").default,
        description: "Modern IT and application environments are increasingly complex, transitioning to cloud, and large in scale. The managed resources, services and applications in these environments generate tremendous data that needs to be observed, consumed and analyzed in real time (or later) by management tools to create insights and to drive operational actions and decisions."
      },
      {
        company: "Narvar",
        link: 'https://www.youtube.com/watch?v=vS4yk4bbLN0',
        image: require("@site/static/img/narvar.png").default,
        description: "Narvar’s platform is built with pub-sub messaging at its core, making reliability, scalability, maintainability, and flexibility business critical."
      },
      {
        company: "Netdata",
        link: 'https://www.youtube.com/watch?v=h2-cjNJjzzE',
        image: require("@site/static/img/netdata.png").default,
        description: "The heart of Netdata Cloud is Pulsar. Almost every message coming from and going to the open source agents passes through Pulsar. Pulsar's infinite number of topics has given us the flexibility we needed and in some cases, every single Netdata Agent has its own unique Pulsar topic."
      },
      {
        company: "Newland",
        link: 'https://www.youtube.com/watch?v=flTQ7H6ygE0',
        image: require("@site/static/img/newland.png").default,
        description: "Apache Pulsar has multi-layer and segment-centric architecture and supports geo-replication. We can query data with PulsarSQL, and create complex processing logic without deploying other systems with Pulsar Functions. "
      },
      {
        company: "Nutanix",
        link: 'https://www.youtube.com/watch?v=Bx4csRi1b8Y&list=PLA7KYGkuAD071myyg4X5ShsDHsOaIpHOq&index=1',
        image: require("@site/static/img/nutanix.png").default,
        description: "Apache Pulsar offers server as well as client side support for the structured streaming. We have been using Pulsar for asynchronous communication among microservices in our Nutanix Beam app for over an year in production."
      },
      {
        company: "OVHCloud",
        link: 'https://www.youtube.com/watch?v=ra9o9_vPeUE',
        image: require("@site/static/img/ovhcloud.png").default,
        description: "We decided to shift and build the foundation of our 'topic-as-a-service' product called ioStream on Apache Pulsar."
      },
      {
        company: "Pandio",
        link: 'https://pandio.com/managed-pulsar-power-page/',
        image: require("@site/static/img/pandio.png").default,
        description: "Pulsar’s flexibility makes it easy to scale and increase your capacity across hundreds of nodes as your needs change Reliable, Low-Latency: Pulsar enables you to scale to more than a million topics with little latency (< 5ms) for publishing."
      },
      {
        company: "ProxyClick",
        link: 'https://www.proxyclick.com/blog/meet-the-proxyclicker-thomas-mouchart',
        image: require("@site/static/img/proxyclick.png").default,
        description: "It gives us consistency with the messages in the queue. It also allows us to replay messages, and it’s a very powerful tool for the distributed systems that like us."
      },
      {
        company: "Softtech",
        link: 'https://www.youtube.com/watch?v=meDBzeintyE',
        image: require("@site/static/img/softtech.png").default,
        description: "Softtech built an event-based consent management system with an average throughput of 500 Million messages per day on Pulsar."
      },
      {
        company: "Splunk",
        link: 'https://databricks.com/session_na20/apache-pulsar-the-next-generation-messaging-and-queuing-system',
        image: require("@site/static/img/splunk.png").default,
        description: "Pulsar guarantees data consistency and durability while maintaining strict SLAs for throughput and latency. Furthermore, Apache Pulsar integrates Pulsar Functions, a lambda style framework to write serverless functions to natively process data immediately upon arrival. This serverless stream processing approach is ideal for lightweight processing tasks like filtering, data routing and transformations."
      },
      {
        company: "Tencent",
        link: 'https://www.youtube.com/watch?v=ZZHMg3Yuuas',
        image: require("@site/static/img/tencent.png").default,
        description: "After nearly 10 years of development of Tencent Game big data, the daily data transmission volume can reach 1.7 trillion. As the key component of the big data platform, the MQ system is critical to provide real-time service operational quality assurance, which requires the support of various applications such as real-time game operational service, real-time index data analysis, and real-time personalized recommendation. "
      },
      {
        company: "TurtleQueue",
        link: 'https://turtlequeue.com/blog/Why%20TurtleQueue',
        image: require("@site/static/img/turtlequeue.png").default,
        description: "Apache Pulsar (upon which TurtleQueue is built) builds on top of the same foundation and improves on it. It exposes a cursor that advances to consume the next message. The cursor's position can be changed to something else, like the beginning of the queue."
      },
      {
        company: "Tuya",
        link: 'https://streamnative.io/success-story/tuya/',
        image: require("@site/static/img/tuya.png").default,
        description: "Tuya settled on Apache Pulsar because it proved to be the most adept at handling the accumulation of messages and repeated consumption. The addition of Pulsar has made Tuya’s message system much more efficient, resulting in lower operational and maintenance costs."
      },
      {
        company: "Yahoo!",
        link: 'https://yahooeng.tumblr.com/post/150078336821/open-sourcing-pulsar-pub-sub-messaging-at-scale#notes?ref_url=https://yahooeng.tumblr.com/post/150078336821/open-sourcing-pulsar-pub-sub-messaging-at-scale/embed#_=_',
        image: require("@site/static/img/yahoo.png").default,
        description: "We deployed our first Pulsar instance in Q2 2015. Pulsar use has rapidly grown since then, and as of today, Yahoo runs Pulsar at scale."
      },
      {
        company: "Yahoo! Japan!",
        link: 'https://www.youtube.com/watch?v=M11MkYC_K3k',
        image: require("@site/static/img/yahoo-japan.png").default,
        description: "We adopted Pulsar because of its great performance, scalability and multi-tenancy capability. Indeed, Pulsar has played an important role to provide our 100+ services in various areas such as e-commerce, media, advertising and more. "
      },
      {
        company: "Zhaopin",
        link: 'https://streamnative.io/success-story/zhaopin/',
        image: require("@site/static/img/zhaopin.png").default,
        description: "We are very happy with our choice of Pulsar and the performance and reliability it provides."
      }
    ],
    telcom: [
      {
        company: "Verizon Media",
        link: 'https://www.youtube.com/watch?v=pDAh-gh-aZ0',
        image: require("@site/static/img/verizon-media.png").default,
        description: "Apache Pulsar provides various solutions for TLS proxy and Pulsar is the only messaging system that supports SNI proxy to leverage various enterprise proxy solutions."
      },
      {
        company: 'China Mobile',
        link: 'https://www.youtube.com/watch?v=ZWUASlc1Xss',
        image: require("@site/static/img/china-mobile.png").default,
        description: "China Mobile's practice and experience in Pulsar will be shared, such as Pulsar's Kubernetes cluster optimization and tenant function improvement."
      },
      {
        company: "GeTui",
        link: 'https://streamnative.io/success-story/getui/',
        image: require("@site/static/img/getui.png").default,
        description: "We adopted Pulsar for the new priority-based push notification solution."
      },
    ],
    retail: [
      {
        company: "Flipkart",
        link: 'https://www.youtube.com/watch?v=2nzV27lHsis',
        image: require("@site/static/img/flipkart.png").default,
        description: "At Flipkart, there are multiple use-cases for high throughput messaging like streaming/batch pipelines, ordered processing, auditing, etc. Pulsar offers different kinds of isolation mechanisms: cluster peering, isolation groups, produce/dispatch quotas, etc. We identified that offering topic-as-a-service can take away operational complexity for these teams and help us enforce stricter SLAs around uptime and geo-replication. Therefore we approached building a scalable and multi-tenant platform with Pulsar as the choice of backend."
      },
      {
        company: "Edge by Ascential / One Click Retail",
        link: 'https://techmonitor.ai/techonology/software/apache-pulsar',
        image: require("@site/static/img/edge-by-ascential.jpg").default,
        description: "Because of Pulsar’s unique combination of messaging and stream processing, we’ve been able to replace multiple systems with one solution that works seamlessly in our Kubernetes environment."
      },
      {
        company: "Overstock",
        link: 'https://www.youtube.com/watch?v=pmaCG1SHAW8',
        image: require("@site/static/img/overstock.jpg").default,
        description: "By combining Apache Pulsar Functions with Apache Ignite, we achieve low latency lookup performance for real-time enrichment of data, which is useful for search and other real-time use cases."
      },
      {
        company: "Su Ning",
        link: 'https://www.youtube.com/watch?v=_6ExVHgUg2g',
        image: require("@site/static/img/su-ning.png").default,
        description: "Building Apache Pulsar from scratch on top of the Kafka integration platform helps to achieve the goal of multi-site high availability"
      },
      {
        company: "THG",
        link: 'https://medium.com/thg-tech-blog/a-feather-in-their-caps-4c4083cf9f46',
        image: require("@site/static/img/thg.png").default,
        description: "We quickly tested Pulsar and found it simple enough to validate some example scenarios in a day of effort"
      },
    ],
    transportation_logistics: [
      {
        company: "Keytop",
        link: 'https://www.youtube.com/watch?v=ITR-J_D8wFg',
        image: require("@site/static/img/keytop.png").default,
        description: "Pulsar is an ideal streaming data platform for our parking system. We customize a messaging system with EMQX, Pulsar and Sink to deal with our data."
      },
    ],
    apprel_manufacturing: [
      {
        company: "Dada Group",
        link: 'https://www.youtube.com/watch?v=F402mUTmOIc',
        image: require("@site/static/img/dada-group.png").default,
        description: "Apache Pulsar has attracted our attention with its excellent features and great architecture."
      },
    ]
  
};

let allArr = [];
Object.keys(csObj).forEach(key => {
  allArr = [...allArr, ...csObj[key]];
});


export default function CaseStudies() {
  return (
    <Layout
      title={`Case Studies`}
      description="Pulsar Case Stdies"
    >    
      <div className="page-wrap tailwind">
        <section className="hero">
          <div className="inner text--left">
            <div className="row">
              <div className="col col--8">
                <h1>Case Studies</h1>
                <p>Organizations around the globe rely on Apache Pulsar to manage their most demanding real-time requirements.</p>

              </div>
            </div>
          </div>
        </section>
        <section className="main-content waves-bg pt-12 pb-48 mb-24">
          <div className="block text--center tabs-bar py-8 px-4">
          </div>
          <div className="my-12 relative z-5">
            
            <TabsUnstyled defaultValue={0} className="tabs tabs--resources block my-24 relative z-5">
              <TabsListUnstyled className="block text--center tabs-bar py-8 px-4">
                <TabUnstyled className="mx-2">All Case Studies</TabUnstyled>
                <TabUnstyled className="mx-2">Healthcare</TabUnstyled>
                <TabUnstyled className="mx-2">Financial Services</TabUnstyled>
                <TabUnstyled className="mx-2">Retail</TabUnstyled>
                <TabUnstyled className="mx-2">Software/IT</TabUnstyled>
                <TabUnstyled className="mx-2">Telcom</TabUnstyled>
                <TabUnstyled className="mx-2">Transportation/Logistics</TabUnstyled>
              </TabsListUnstyled>
              
              <TabPanelUnstyled value={0}><CaseStudyCards cards={allArr} /></TabPanelUnstyled>
              <TabPanelUnstyled value={1}><CaseStudyCards cards={csObj.healthcare} /></TabPanelUnstyled>
              <TabPanelUnstyled value={2}><CaseStudyCards cards={csObj.financial_services} /></TabPanelUnstyled>
              <TabPanelUnstyled value={3}><CaseStudyCards cards={csObj.retail} /></TabPanelUnstyled>
              <TabPanelUnstyled value={4}><CaseStudyCards cards={csObj.software_it} /></TabPanelUnstyled>
              <TabPanelUnstyled value={5}><CaseStudyCards cards={csObj.telcom} /></TabPanelUnstyled>
              <TabPanelUnstyled value={6}><CaseStudyCards cards={csObj.transportation_logistics} /></TabPanelUnstyled>
            </TabsUnstyled>
          </div>
        </section>
      </div>
    </Layout>
  );
}
