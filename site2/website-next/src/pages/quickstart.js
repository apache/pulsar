import React from "react";
import clsx from "clsx";
import SineWaves from "sine-waves";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import styles from "./index.module.css";
import HomepageFeatures from "../components/HomepageFeatures";
import SubHeroBlock from "../components/SubHeroBlock";
import PromoCallout from "../components/promoCallout";
import PillButton from "../components/PillButton";
import Svg from "../components/Svg";
import { docUrl, githubUrl } from "../utils/index";
import Stack from "@mui/material/Stack";
import Button from "@mui/material/Button";

export default function Home() {
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
