import React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";

export default function Home() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={`Pulsar Basics`}
      description="Learn about the basics of using Apache Pulsar"
    >    
      <div className="page-wrap tailwind">
        <section className="hero">
            <div className="inner text--center">
                <h1>Pulsar Basics</h1>
            </div>
        </section>
      </div>
    </Layout>
  );
}
