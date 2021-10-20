import React from "react";
import clsx from "clsx";
import styles from "./HomepageFeatures.module.css";
import ReactMarkdown from "react-markdown";
import { docUrl } from "../utils/index";

const FeatureList = (language) => [
  {
    // title: "Pulsar Functions",
    title: `[Pulsar Functions](${docUrl("functions-overview", language)})`,
    // Svg: require("../../static/img/undraw_docusaurus_mountain.svg").default,
    content: (
      <>
        Easy to deploy, lightweight compute process, developer-friendly APIs, no
        need to run your own stream processing engine.
      </>
    ),
  },
  {
    title: `[Proven in production](${docUrl(
      "concepts-architecture-overview",
      language
    )})`,
    // Svg: require("../../static/img/undraw_docusaurus_tree.svg").default,
    content: (
      <>
        Run in production at Yahoo! scale for over 5 years, with millions of
        messages per second across millions of topics.
      </>
    ),
  },
  {
    title: `[Horizontally scalable](${docUrl(
      "concepts-architecture-overview",
      language
    )})`,
    // Svg: require("../../static/img/undraw_docusaurus_react.svg").default,
    content: <>Expand capacity seamlessly to hundreds of nodes.</>,
  },
  {
    content:
      "Low publish latency (< 5ms) at scale with strong durability guarantees.",
    title: `[Low latency with durability](${docUrl(
      "concepts-architecture-overview",
      language
    )})`,
  },
  {
    content:
      "Configurable replication between data centers across multiple geographic regions.",
    title: `[Geo-replication](${docUrl("administration-geo", language)})`,
  },
  {
    content:
      "Built from the ground up as a multi-tenant system. Supports isolation, authentication, authorization and quotas.",
    title: `[Multi-tenancy](${docUrl("concepts-multi-tenancy", language)})`,
  },
  {
    content:
      "Persistent message storage based on Apache BookKeeper. IO-level isolation between write and read operations.",
    title: `[Persistent storage](${docUrl(
      "concepts-architecture-overview#persistent-storage",
      language
    )})`,
  },
  {
    content:
      "Flexible messaging models with high-level APIs for Java, Go, Python, C++, Node.js, WebSocket and C#.",
    title: `[Client libraries](${docUrl("client-libraries", language)})`,
  },
  {
    content:
      "REST Admin API for provisioning, administration, tools and monitoring. Can be deployed on bare metal, Kubernetes, Amazon Web Services(AWS), and DataCenter Operating System(DC/OS).",
    title: `[Operability](${docUrl("admin-api-overview", language)})`,
  },
];

function Feature({ Svg, title, content }) {
  return (
    <div className={clsx("col col--4 mb-24")}>
      {/* <div className="text--center">
        <Svg className={styles.featureSvg} alt={title} />
      </div> */}
      <div className="text--center padding-horiz--md">
        <ReactMarkdown children={title} className="text-2xl font-bold text-primary" />
        <p className="mt-6 text-xl">{content}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(props) {
  return (
    <section className={styles.features}>
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
