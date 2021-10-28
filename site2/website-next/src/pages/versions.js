import * as React from "react";
import Layout from "@theme/Layout";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableRow from "@mui/material/TableRow";
import Link from "@mui/material/Link";
import VersionsTable from "../components/VersionsTable";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import { docUrl } from "../utils/index";
const versions = require("../../versions.json");
const oldversions = require("../../oldversions.json");

export default function DenseTable() {
  const { siteConfig } = useDocusaurusContext();
  const latestStableVersion = versions[0];
  const repoUrl = `https://github.com/${siteConfig.organizationName}/${siteConfig.projectName}`;
  return (
    <Layout>
      <div className="tailwind">
        <div className="my-12 container">
          <h1 className="mb-6">{siteConfig.title} Versions</h1>
          <h3 className="mb-4" id="latest">
            Latest Stable Version
          </h3>
          <p className="mb-2">Latest stable release of Apache Pulsar.</p>
          <VersionsTable
            data={[{ name: latestStableVersion }]}
            type="stable"
          ></VersionsTable>
          <h3 className="mt-8 mb-4" id="latest">
            Latest Version
          </h3>
          <p className="mb-2">
            Here you can find the latest documentation and unreleased code.
          </p>
          <VersionsTable
            data={[{ name: "next" }]}
            type="stable"
          ></VersionsTable>
          <h3 className="mt-8 mb-4" id="latest">
            Past Version
          </h3>
          <p className="mb-2">
            Here you can find documentation for previous versions of Apache
            Pulsar.
          </p>
          <VersionsTable
            data={versions
              .filter((item) => item != latestStableVersion)
              .concat(oldversions)
              .map((item) => ({
                name: item
              }))}
            type="stable"
          ></VersionsTable>
        </div>
      </div>
    </Layout>
  );
}
