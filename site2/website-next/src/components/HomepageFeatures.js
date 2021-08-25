import React from "react";
import clsx from "clsx";
import styles from "./HomepageFeatures.module.css";

const FeatureList = [
  {
    title: "Pulsar Functions",
    // Svg: require("../../static/img/undraw_docusaurus_mountain.svg").default,
    description: (
      <>
        Easy to deploy, lightweight compute process, developer-friendly APIs, no
        need to run your own stream processing engine.
      </>
    ),
  },
  {
    title: "Proven in production",
    // Svg: require("../../static/img/undraw_docusaurus_tree.svg").default,
    description: (
      <>
        Run in production at Yahoo! scale for over 5 years, with millions of
        messages per second across millions of topics.
      </>
    ),
  },
  {
    title: "Horizontally scalable",
    // Svg: require("../../static/img/undraw_docusaurus_react.svg").default,
    description: <>Expand capacity seamlessly to hundreds of nodes.</>,
  },
];

function Feature({ Svg, title, description }) {
  return (
    <div className={clsx("col col--4")}>
      {/* <div className="text--center">
        <Svg className={styles.featureSvg} alt={title} />
      </div> */}
      <div className="text--center padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
