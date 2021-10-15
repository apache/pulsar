import React from "react";
import clsx from "clsx";
import Layout from "@theme/Layout";
import Link from "@docusaurus/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import styles from "./index.module.css";
import HomepageFeatures from "../components/HomepageFeatures";
import Svg from "../components/Svg";
import Button from "../components/Button";
import { docUrl, githubUrl } from "../utils/index";

function HomepageHeader(props) {
  const { siteConfig } = useDocusaurusContext();
  return (
    <header className={clsx("tailwind hero", styles.heroBanner)}>
      <div className="flex flex-col items-center container">
        <Svg src="/img/pulsar.svg" className="h-48" />
        <h2 className="mt-12 font-medium mb-12">{siteConfig.tagline}</h2>
        <div className={styles.buttons}>
          <Button href={docUrl("", props.language)} className="mr-6">
            READ THE DOCS
          </Button>
          <Button href={githubUrl()}>GITHUB</Button>
        </div>
      </div>
    </header>
  );
}

export default function Home() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={`Hello from ${siteConfig.title}`}
      description="Description will go into a meta tag in <head />"
    >
      <HomepageHeader />
      <main className="tailwind">
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
