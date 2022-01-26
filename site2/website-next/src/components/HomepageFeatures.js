import React from "react";
import clsx from "clsx";
import styles from "./HomepageFeatures.module.css";
import ReactMarkdown from "react-markdown";
import { docUrl } from "../utils/index";

const FeatureList = (language) => [
  {
    title: "Cloud-native",
    Svg: require("../../static/img/Technology-Solution.svg").default,
    content: "A multiple layer approach separating compute from storage to work with cloud infrastructures and Kubernetes",
  },
  {
    title: "Serverless functions",
    Svg: require("../../static/img/proven-in-production.svg").default,
    content:"Write serverless functions with developer-friendly APIs to natively process data immediately upon arrival. No need to run your own stream processing engine",
  },
  {
    title: "Horizontally scalable",
    Svg: require("../../static/img/horizontally-scalable.svg").default,
    content: "Expand capacity seamlessly to hundreds of nodes.",
  },
  {
    title: "Low latency with durability",
    Svg: require("../../static/img/low-latency.svg").default,
    content:"Low publish latency (< 5ms) at scale with strong durability guarantees.",
  },
  {
    title: "Geo-replication",
    Svg: require("../../static/img/geo-replication.svg").default,
    content: "Configurable replication between data centers across multiple geographic regions.",
  },
  {
    title: "Multi-tenancy",
    Svg: require("../../static/img/multi-tenancy.svg").default,
    content:"Built from the ground up as a multi-tenant system. Supports isolation, authentication, authorization and quotas.",
  },
  {
    title: "Persistent storage",
    Svg: require("../../static/img/persistent-storage.svg").default,
    content:"Persistent message storage based on Apache BookKeeper. IO-level isolation between write and read operations.",
  },
  {
    title: "Client libraries",
    Svg: require("../../static/img/client-libraries.svg").default,
    content: "Flexible messaging models with high-level APIs for Java, Go, Python, C++, Node.js, WebSocket and C#.",
  },
  {
    title: "Operability",
    Svg: require("../../static/img/operability.svg").default,
    content:"REST Admin API for provisioning, administration, tools and monitoring. Can be deployed on bare metal, Kubernetes, Amazon Web Services(AWS), and DataCenter Operating System(DC/OS).",
  },
];

function Feature({ Svg, title, content }) {
  return (
    <div className="col col--4 mb-24 icon-feature">
      <div className="text--left feature-image mb-4 padding-horiz--md">
        <Svg className={styles.featureSvg} alt={title} />
      </div>
      <div className="text--left padding-horiz--md">
        <ReactMarkdown children={title} className="text-xl font-bold text-primary" />
        <p className="mt-6">{content}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(props) {
  return (
    <section id="home-features" className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList(props.language || "").map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
