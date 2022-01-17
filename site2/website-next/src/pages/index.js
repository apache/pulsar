import React from "react";
import clsx from "clsx";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import styles from "./index.module.css";
import HomepageFeatures from "../components/HomepageFeatures";
import Svg from "../components/Svg";
import { docUrl, githubUrl } from "../utils/index";
import Stack from "@mui/material/Stack";
import Button from "@mui/material/Button";

function HomepageHeader(props) {
  const { siteConfig } = useDocusaurusContext();
  return (
    <header className={clsx("tailwind hero", styles.heroBanner)}>
      <div className="flex flex-col items-center container mt-24">
        <Svg src="/img/pulsar.svg" className="h-16 lg:h-36" />
        <h2 className="mt-12 font-medium mb-12">{siteConfig.tagline}</h2>
        <Stack spacing={2} direction="row">
          <Button
            variant="contained"
            size="large"
            href={docUrl("", props.language)}
            className="text-white"
          >
            READ THE DOCS
          </Button>
          <Button
            variant="outlined"
            size="large"
            href={githubUrl()}
            className="text-primary"
          >
            GITHUB
          </Button>
        </Stack>
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
        <div className="mt-8">
          <HomepageFeatures />
        </div>
      </main>
      <p align="center">
        <small style={{ color: "black", fontSize: "1.7rem" }}>
          <a className="hover:no-underline" href="/powered-by">
            Companies Powered by Pulsar
          </a>
        </small>
      </p>
      <div style={{ textAlign: "center" }}>
        <p>
          Apache Pulsar is available under the{" "}
          <a href="https://www.apache.org/licenses">
            Apache License, version 2.0
          </a>
          .
        </p>
      </div>
    </Layout>
  );
}
