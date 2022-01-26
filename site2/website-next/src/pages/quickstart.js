import React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";

export default function Quickstart() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={`Quickstart`}
      description="Learn about the basics of using Apache Pulsar"
    >    
      <div className="page-wrap tailwind">
        <section className="hero">
            <div className="inner text--center">
                <h1>Quickstart</h1>
            </div>
        </section>
      </div>
    </Layout>
  );
}
