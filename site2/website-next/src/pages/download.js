import * as React from "react";
import Layout from "@theme/Layout";
import ReleaseTable from "../components/ReleaseTable";
import ConnectorTable from "../components/ConnectorTable";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Translate, { translate } from "@docusaurus/Translate";

const versions = require(`../../versions.json`);
const releases = require(`../../releases.json`);
const pulsarManagerReleases = require(`../../pulsar-manager-release.json`);
const pulsarAdaptersReleases = require(`../../pulsar-adapters-release.json`);
const connectors = require(`../../data/connectors.js`);

function getLatestArchiveMirrorUrl(version, type) {
  return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-${version}/apache-pulsar-${version}-${type}.tar.gz`;
}

function getLatestOffloadersMirrorUrl(version) {
  return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-${version}/apache-pulsar-offloaders-${version}-bin.tar.gz`;
}

function getLatestAdaptersMirrorUrl(version) {
  return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-adapters-${version}/apache-pulsar-adapters-${version}-src.tar.gz`;
}

function distUrl(version, type) {
  return `https://www.apache.org/dist/pulsar/pulsar-${version}/apache-pulsar-${version}-${type}.tar.gz`;
}

function distOffloadersUrl(version) {
  return `https://www.apache.org/dist/pulsar/pulsar-${version}/apache-pulsar-offloaders-${version}-bin.tar.gz`;
}

function distAdaptersUrl(version) {
  return `https://downloads.apache.org/pulsar/pulsar-adapters-${version}/apache-pulsar-adapters-${version}-src.tar.gz`;
}

function archiveUrl(version, type) {
  if (version.includes("incubating")) {
    return `https://archive.apache.org/dist/incubator/pulsar/pulsar-${version}/apache-pulsar-${version}-${type}.tar.gz`;
  } else {
    return `https://archive.apache.org/dist/pulsar/pulsar-${version}/apache-pulsar-${version}-${type}.tar.gz`;
  }
}

function pularManagerArchiveUrl(version, type) {
  return `https://archive.apache.org/dist/pulsar/pulsar-manager/pulsar-manager-${version}/apache-pulsar-manager-${version}-${type}.tar.gz`;
}

function connectorDistUrl(name, version) {
  return `https://www.apache.org/dist/pulsar/pulsar-${version}/connectors/pulsar-io-${name}-${version}.nar`;
}

function connectorDownloadUrl(name, version) {
  return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-${version}/connectors/pulsar-io-${name}-${version}.nar`;
}

function getLatestPulsarManagerArchiveMirrorUrl(version, type) {
  return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-manager/pulsar-manager-${version}/apache-pulsar-manager-${version}-${type}.tar.gz`;
}

function pulsarManagerDistUrl(version, type) {
  return `https://www.apache.org/dist/pulsar/pulsar-manager/pulsar-manager-${version}/apache-pulsar-manager-${version}-${type}.tar.gz`;
}

function language(props) {
  return props.language ? props.language : "";
}

export default function page(props) {
  const { siteConfig } = useDocusaurusContext();
  const latestVersion = releases[0];
  const latestPulsarManagerVersion = pulsarManagerReleases[0];
  const latestPulsarAdaptersVersion = pulsarAdaptersReleases[0];
  const latestArchiveMirrorUrl = getLatestArchiveMirrorUrl(
    latestVersion,
    "bin"
  );
  const latestSrcArchiveMirrorUrl = getLatestArchiveMirrorUrl(
    latestVersion,
    "src"
  );
  const latestPulsarManagerArchiveMirrorUrl =
    getLatestPulsarManagerArchiveMirrorUrl(latestPulsarManagerVersion, "bin");
  const latestPulsarManagerSrcArchiveMirrorUrl =
    getLatestPulsarManagerArchiveMirrorUrl(latestPulsarManagerVersion, "src");
  const latestArchiveUrl = distUrl(latestVersion, "bin");
  const latestSrcArchiveUrl = distUrl(latestVersion, "src");
  const pulsarManagerLatestArchiveUrl = pulsarManagerDistUrl(
    latestPulsarManagerVersion,
    "bin"
  );
  const pulsarManagerLatestSrcArchiveUrl = pulsarManagerDistUrl(
    latestPulsarManagerVersion,
    "src"
  );
  const latest = [
    {
      release: "Binary",
      link: latestArchiveMirrorUrl,
      linkText: `apache-pulsar-${latestVersion}-bin.tar.gz`,
      asc: `${latestArchiveUrl}.asc`,
      sha512: `${latestArchiveUrl}.sha512`,
    },
    {
      release: "Source",
      link: latestSrcArchiveMirrorUrl,
      linkText: `apache-pulsar-${latestVersion}-src.tar.gz`,
      asc: `${latestSrcArchiveUrl}.asc`,
      sha512: `${latestSrcArchiveUrl}.sha512`,
    },
  ];
  const offloaders = [
    {
      release: "Offloaders",
      link: getLatestOffloadersMirrorUrl(latestVersion),
      linkText: `apache-pulsar-offloaders-${latestVersion}-bin.tar.gz`,
      asc: `${distOffloadersUrl(latestVersion)}.asc`,
      sha512: `${distOffloadersUrl(latestVersion)}.sha512`,
    },
  ];
  return (
    <Layout>
      <div className="tailwind">
        <div className="my-12 container">
          <h1 className="mb-6">{siteConfig.title} Download</h1>
          <h2 className="mb-4" id="latest">
            <Translate>Release notes</Translate>
          </h2>
          <p>
            <a href={`${siteConfig.baseUrl}${language(props)}/release-notes`}>
              Release notes
            </a>{" "}
            for all Pulsar's versions
          </p>
          <h2 id="latest">
            <Translate>Current version (Stable)</Translate> {latestVersion}
          </h2>
          <ReleaseTable data={latest}></ReleaseTable>
          <h3>
            <Translate>Tiered storage offloaders</Translate>
          </h3>
          <ReleaseTable data={offloaders}></ReleaseTable>
          <h3 id="connectors">
            <Translate>Pulsar IO connectors</Translate>
          </h3>
          <ConnectorTable
            data={connectors.map((connector) => {
              return {
                connector: connector.link,
                connectorText: connector.longName,
                archive: `${connectorDownloadUrl(
                  connector.name,
                  latestVersion
                )}`,
                archiveText: `pulsar-io-${connector.name}-${latestVersion}.nar`,
                asc: `${connectorDistUrl(connector.name, latestVersion)}.asc`,
                sha512: `${connectorDistUrl(
                  connector.name,
                  latestVersion
                )}.sha512`,
              };
            })}
          ></ConnectorTable>
        </div>
      </div>
    </Layout>
  );
}
